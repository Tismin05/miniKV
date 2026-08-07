package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"miniKV/api/proto/kvpb"
	"miniKV/internal/router"
)

var lastTimestamp atomic.Int64

// nextTimestamp 返回进程内严格递增的 Unix 纳秒版本，避免同一时钟刻度内的
// 两个网关请求得到相同的 LWW 时间戳。
func nextTimestamp() int64 {
	for {
		now := time.Now().UnixNano()
		last := lastTimestamp.Load()
		if now <= last {
			now = last + 1
		}
		if lastTimestamp.CompareAndSwap(last, now) {
			return now
		}
	}
}

// getGRPCClient 创建短生命周期的客户端连接。调用方拥有该连接，并必须在 RPC
// 完成后关闭它。
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
		if key == "" {
			http.Error(w, "缺少参数 key 或 value", http.StatusBadRequest)
			return
		}

		targetNodes := ring.GetNodes(key, ReplicationFactor)
		if len(targetNodes) == 0 {
			http.Error(w, "集群中没有可用节点", http.StatusInternalServerError)
			return
		}
		fmt.Printf("[路由日志] PUT 键 '%s' -> 副本群: %v\n", key, targetNodes)

		// 同一个 timestamp 让本次写入的全部副本视为同一个 LWW 版本。
		ts := nextTimestamp()

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

				if !resp.Success {
					fmt.Printf("[路由日志] 写入节点 [%s] 被拒绝：%s\n", node, resp.Message)
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

		// 比较每个有响应的副本，避免较旧的 Value 掩盖较新的 tombstone；
		// ReadQuorum 仍是返回结果所需的最少成功响应数。
		var bestResp *kvpb.GetResponse
		var bestNode string
		successCount := 0

		for i := 0; i < len(targetNodes); i++ {
			res := <-results
			if res.err != nil {
				fmt.Printf("[路由日志] 查询节点 [%s] 失败：%v\n", res.node, res.err)
				continue
			}
			if res.resp == nil {
				continue
			}

			successCount++
			if !res.resp.Found && !res.resp.Deleted {
				continue
			}
			// Timestamp 是主版本。相同时 Delete 优先；两个 Put 则与存储层
			// 一样按 Value 字典序决胜。
			if bestResp == nil || res.resp.Timestamp > bestResp.Timestamp ||
				(res.resp.Timestamp == bestResp.Timestamp && ((res.resp.Deleted && !bestResp.Deleted) ||
					(!res.resp.Deleted && !bestResp.Deleted && res.resp.Value > bestResp.Value))) {
				bestResp = res.resp
				bestNode = res.node
			}

		}

		if successCount >= ReadQuorum && bestResp != nil && bestResp.Found && !bestResp.Deleted {
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
		// Delete 必须与 Put 一样使用网关生成的 LWW 版本。
		ts := nextTimestamp()

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

				resp, err := client.Delete(ctx, &kvpb.DeleteRequest{Key: key, Timestamp: ts})
				if err != nil {
					fmt.Printf("[路由日志] 删除节点 [%s] 失败：%v\n", node, err)
					return
				}

				if !resp.Success {
					fmt.Printf("[路由日志] 删除节点 [%s] 被拒绝：%s\n", node, resp.Message)
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
