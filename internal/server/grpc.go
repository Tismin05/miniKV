package server

import (
	"context"

	"miniKV/api/proto/kvpb" // 导入刚才自动生成的代码
	"miniKV/internal/storage"
)

// GRPCServer 实现了自动生成的 kvpb.KVServiceServer 接口
type GRPCServer struct {
	// 必须要嵌入这个结构体，以保证向前兼容
	kvpb.UnimplementedKVServiceServer
	store *storage.KVStore
}

// NewGRPCServer 实例化 gRPC 服务
func NewGRPCServer(store *storage.KVStore) *GRPCServer {
	return &GRPCServer{store: store}
}

// 实现 .proto 中定义的 Put, Get, Delete 方法

func (s *GRPCServer) Put(ctx context.Context, req *kvpb.PutRequest) (*kvpb.PutResponse, error) {
	err := s.store.Put(req.Key, req.Value)
	if err != nil {
		return &kvpb.PutResponse{Success: false, Message: err.Error()}, nil
	}
	return &kvpb.PutResponse{Success: true, Message: "写入成功"}, nil
}

func (s *GRPCServer) Get(ctx context.Context, req *kvpb.GetRequest) (*kvpb.GetResponse, error) {
	val, exists := s.store.Get(req.Key)
	if !exists {

		return &kvpb.GetResponse{Found: false, Value: ""}, nil
	}
	return &kvpb.GetResponse{Found: true, Value: val}, nil
}

func (s *GRPCServer) Delete(ctx context.Context, req *kvpb.DeleteRequest) (*kvpb.DeleteResponse, error) {
	err := s.store.Delete(req.Key)
	if err != nil {
		return &kvpb.DeleteResponse{Success: false, Message: err.Error()}, nil
	}
	return &kvpb.DeleteResponse{Success: true, Message: "删除成功"}, nil
}
