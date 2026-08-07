package server

import (
	"context"

	"miniKV/api/proto/kvpb"
	"miniKV/internal/storage"
)

// GRPCServer 实现了自动生成的 kvpb.KVServiceServer 接口
type GRPCServer struct {
	// 必须要嵌入这个结构体，以保证向前兼容
	kvpb.UnimplementedKVServiceServer
	store *storage.KVStore
}

// NewGRPCServer 创建一个面向本地 KVStore 的 gRPC 传输适配器。
func NewGRPCServer(store *storage.KVStore) *GRPCServer {
	return &GRPCServer{store: store}
}

// 实现 .proto 中定义的 Put, Get, Delete 方法

func (s *GRPCServer) Put(ctx context.Context, req *kvpb.PutRequest) (*kvpb.PutResponse, error) {
	// 网关会为一次写入的所有副本提供相同 timestamp。
	err := s.store.Put(req.Key, req.Value, req.Timestamp) // 透传 timestamp
	if err != nil {
		return &kvpb.PutResponse{Success: false, Message: err.Error()}, nil
	}
	return &kvpb.PutResponse{Success: true, Message: "写入成功"}, nil
}

func (s *GRPCServer) Get(ctx context.Context, req *kvpb.GetRequest) (*kvpb.GetResponse, error) {
	// 已删除的 key 会返回 Found=false，但仍保留 Deleted 和 Timestamp，
	// 让网关能将 tombstone 与其他副本的 Value 进行比较。
	result := s.store.Get(req.Key)
	return &kvpb.GetResponse{
		Found: result.Found, Value: string(result.Value), Timestamp: result.Timestamp, Deleted: result.Deleted,
	}, nil
}

func (s *GRPCServer) Delete(ctx context.Context, req *kvpb.DeleteRequest) (*kvpb.DeleteResponse, error) {
	// Delete 使用请求携带的版本；固定 timestamp 会输给更新的 Put。
	err := s.store.Delete(req.Key, req.Timestamp)
	if err != nil {
		return &kvpb.DeleteResponse{Success: false, Message: err.Error()}, nil
	}
	return &kvpb.DeleteResponse{Success: true, Message: "删除成功"}, nil
}
