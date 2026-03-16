package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"miniKV/api/proto/kvpb"
	"miniKV/internal/router"
)

// getGRPCClient 连接到目标 gRPC 节点
func getGRPCClient(addr string) (kvpb.KVServiceClient, *grpc.ClientConn, error) {
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, nil, fmt.Errorf("无法连接到内部节点 %s: %v", addr, err)
	}
	return kvpb.NewKVServiceClient(conn), conn, nil
}

func main() {
	// 初始化哈希环
	ring := router.NewHashRing(10)
	ring.AddNode("localhost:50051")
	ring.AddNode("localhost:50052")
	fmt.Println("🗺️ 路由网关已初始化，当前管理节点群: [50051, 50052]")

	// Quorum 机制
	const (
		ReplicationFactor = 2 // N: 每个 key 存几个副本
		WriteQuorum       = 2 // W: 写入至少成功几个才算成功
		ReadQuorum        = 1 // R: 读取查询几个节点（取最新）
	)

	// ==========================================
	// PUT 并发多副本写入 + Quorum W 检测
	// ==========================================
	http.HandleFunc("/put", func(w http.ResponseWriter, r *http.Request) {
		key := r.URL.Query().Get("key")
		val := r.URL.Query().Get("value")
		if key == "" || val == "" {
			http.Error(w, "缺少参数 key 或 value", http.StatusBadRequest)
			return
		}

		targetNodes := ring.GetNodes(key, ReplicationFactor)
		if len(targetNodes) == 0 {
			http.Error(w, "集群中没有可用节点", http.StatusInternalServerError)
			return
		}
		fmt.Printf("[路由日志] PUT 键 '%s' -> 副本群: %v\n", key, targetNodes)

		// 网关生成时间戳，保证所有副本的同一次写入拥有相同版本
		ts := time.Now().UnixNano()

		// 并发写入所有副本
		var mu sync.Mutex
		successCount := 0
		var wg sync.WaitGroup

		for _, node := range targetNodes {
			wg.Add(1)
			go func(node string) {
				defer wg.Done()

				client, conn, err := getGRPCClient(node)
				if err != nil {
					fmt.Printf("[路由日志] 连接节点 [%s] 失败：%v\n", node, err)
					return
				}
				defer conn.Close()

				ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
				defer cancel()

				resp, err := client.Put(ctx, &kvpb.PutRequest{
					Key:       key,
					Value:     val,
					Timestamp: ts,
				})
				if err != nil {
					fmt.Printf("[路由日志] 写入节点 [%s] 失败：%v\n", node, err)
					return
				}

				fmt.Printf("[路由日志] 写入节点 [%s] 成功：%s\n", node, resp.Message)
				mu.Lock()
				successCount++
				mu.Unlock()
			}(node)
		}

		wg.Wait()

		// Quorum W 判断：成功副本数 >= W 才返回成功
		if successCount >= WriteQuorum {
			w.Header().Set("Content-Type", "application/json")
			fmt.Fprintf(w, `{"status":"ok","success_replicas":%d,"required_w":%d,"total_n":%d}`,
				successCount, WriteQuorum, ReplicationFactor)
		} else {
			http.Error(w,
				fmt.Sprintf("写入失败：成功 %d 副本，不满足 W=%d", successCount, WriteQuorum),
				http.StatusInternalServerError)
		}
	})

	// ==========================================
	// GET 并发 Quorum R + 时间戳比较取最新
	// ==========================================
	http.HandleFunc("/get", func(w http.ResponseWriter, r *http.Request) {
		key := r.URL.Query().Get("key")
		if key == "" {
			http.Error(w, "缺少参数 key", http.StatusBadRequest)
			return
		}

		targetNodes := ring.GetNodes(key, ReplicationFactor)
		fmt.Printf("[路由日志] GET 键 '%s' -> 查询副本群: %v\n", key, targetNodes)

		// 并发查询所有副本
		type readResult struct {
			node string
			resp *kvpb.GetResponse
			err  error
		}

		results := make(chan readResult, len(targetNodes))

		for _, node := range targetNodes {
			go func(node string) {
				client, conn, err := getGRPCClient(node)
				if err != nil {
					results <- readResult{node: node, err: err}
					return
				}
				defer conn.Close()

				ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
				defer cancel()

				resp, err := client.Get(ctx, &kvpb.GetRequest{Key: key})
				results <- readResult{node: node, resp: resp, err: err}
			}(node)
		}

		// 收集 R 个成功结果，比较时间戳取最新
		var bestResp *kvpb.GetResponse
		var bestNode string
		successCount := 0

		for i := 0; i < len(targetNodes); i++ {
			res := <-results
			if res.err != nil {
				fmt.Printf("[路由日志] 查询节点 [%s] 失败：%v\n", res.node, res.err)
				continue
			}
			if res.resp == nil || !res.resp.Found {
				continue
			}

			successCount++
			// 比较 kv 时间戳，保留最新的版本
			if bestResp == nil || res.resp.Timestamp > bestResp.Timestamp {
				bestResp = res.resp
				bestNode = res.node
			}

			// 已经拿到 R 个成功结果，可以提前返回
			if successCount >= ReadQuorum {
				break
			}
		}

		if bestResp != nil {
			fmt.Printf("[路由日志] GET 键 '%s' 命中节点 [%s], 时间戳: %d\n", key, bestNode, bestResp.Timestamp)
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(map[string]interface{}{
				"key":       key,
				"value":     bestResp.Value,
				"timestamp": bestResp.Timestamp,
				"from_node": bestNode,
			})
		} else {
			http.Error(w, "数据未找到", http.StatusNotFound)
		}
	})

	// ==========================================
	// DEL 并发多副本删除 + Quorum W 检测
	// ==========================================
	http.HandleFunc("/del", func(w http.ResponseWriter, r *http.Request) {
		key := r.URL.Query().Get("key")
		if key == "" {
			http.Error(w, "缺少参数 key", http.StatusBadRequest)
			return
		}

		targetNodes := ring.GetNodes(key, ReplicationFactor)
		if len(targetNodes) == 0 {
			http.Error(w, "集群中没有可用节点", http.StatusInternalServerError)
			return
		}
		fmt.Printf("[路由日志] DEL 键 '%s' -> 副本群: %v\n", key, targetNodes)

		var mu sync.Mutex
		successCount := 0
		var wg sync.WaitGroup

		for _, node := range targetNodes {
			wg.Add(1)
			go func(node string) {
				defer wg.Done()

				client, conn, err := getGRPCClient(node)
				if err != nil {
					fmt.Printf("[路由日志] 连接节点 [%s] 失败：%v\n", node, err)
					return
				}
				defer conn.Close()

				ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
				defer cancel()

				resp, err := client.Delete(ctx, &kvpb.DeleteRequest{Key: key})
				if err != nil {
					fmt.Printf("[路由日志] 删除节点 [%s] 失败：%v\n", node, err)
					return
				}

				fmt.Printf("[路由日志] 删除节点 [%s] 成功：%s\n", node, resp.Message)
				mu.Lock()
				successCount++
				mu.Unlock()
			}(node)
		}

		wg.Wait()

		if successCount >= WriteQuorum {
			w.Header().Set("Content-Type", "application/json")
			fmt.Fprintf(w, `{"status":"ok","deleted_replicas":%d,"required_w":%d}`,
				successCount, WriteQuorum)
		} else {
			http.Error(w,
				fmt.Sprintf("删除失败：成功 %d 副本，不满足 W=%d", successCount, WriteQuorum),
				http.StatusInternalServerError)
		}
	})

	// 控制台前端
	//http.HandleFunc("/", handleDashboard)

	// 启动网关
	fmt.Println("🚦 网关服务启动成功，监听外界请求端口 :8080...")
	//fmt.Println("🌐 打开浏览器访问控制台: http://localhost:8080")
	log.Fatal(http.ListenAndServe(":8080", nil))
}
