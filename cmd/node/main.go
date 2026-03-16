package main

import (
	"flag"
	"fmt"
	"log"
	"net"

	"google.golang.org/grpc"

	// 注意：这里的 my-kv-store 记得替换成你 go.mod 里的真实模块名
	"miniKV/api/proto/kvpb"
	"miniKV/internal/server"
	"miniKV/internal/storage"
)

func main() {
	// 1. 引入 flag 包，解析命令行传入的端口号（默认 50051）
	port := flag.Int("port", 50051, "gRPC server port")
	flag.Parse()

	// 为每个节点分配独立的 AOF 日志文件，防止冲突
	fileName := fmt.Sprintf("kv_data_%d.log", *port)
	store, err := storage.NewKVStore(fileName)
	if err != nil {
		log.Fatalf("节点存储引擎启动失败: %v", err)
	}
	fmt.Printf("KV 节点引擎启动成功！(数据文件: %s)\n", fileName)

	// 监听动态传入的 TCP 端口
	address := fmt.Sprintf(":%d", *port)
	lis, err := net.Listen("tcp", address)
	if err != nil {
		log.Fatalf("无法监听端口: %v", err)
	}

	grpcServer := grpc.NewServer()
	kvService := server.NewGRPCServer(store)
	kvpb.RegisterKVServiceServer(grpcServer, kvService)

	fmt.Printf("gRPC 节点已启动，监听 TCP 端口 %s...\n", address)

	if err := grpcServer.Serve(lis); err != nil {
		log.Fatalf("gRPC 服务器异常退出: %v", err)
	}
}
