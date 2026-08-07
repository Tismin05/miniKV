package server

import (
	"context"
	"testing"

	"miniKV/api/proto/kvpb"
	"miniKV/internal/storage"
)

func TestGRPCVersionedDeleteAndTypedGet(t *testing.T) {
	store, err := storage.NewKVStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	server := NewGRPCServer(store)
	ctx := context.Background()

	if response, err := server.Put(ctx, &kvpb.PutRequest{Key: "key", Value: "value", Timestamp: 10}); err != nil || !response.Success {
		t.Fatalf("Put = %#v, %v", response, err)
	}
	if response, err := server.Delete(ctx, &kvpb.DeleteRequest{Key: "key", Timestamp: 20}); err != nil || !response.Success {
		t.Fatalf("Delete = %#v, %v", response, err)
	}
	if response, err := server.Put(ctx, &kvpb.PutRequest{Key: "key", Value: "stale", Timestamp: 15}); err != nil || !response.Success {
		t.Fatalf("stale Put = %#v, %v", response, err)
	}

	response, err := server.Get(ctx, &kvpb.GetRequest{Key: "key"})
	if err != nil {
		t.Fatal(err)
	}
	if response.Found || !response.Deleted || response.Timestamp != 20 || response.Value != "" {
		t.Fatalf("Get tombstone = %#v", response)
	}

	if response, err := server.Put(ctx, &kvpb.PutRequest{Key: "literal", Value: "<TOMBSTONE>", Timestamp: 30}); err != nil || !response.Success {
		t.Fatalf("literal Put = %#v, %v", response, err)
	}
	response, err = server.Get(ctx, &kvpb.GetRequest{Key: "literal"})
	if err != nil || !response.Found || response.Deleted || response.Value != "<TOMBSTONE>" {
		t.Fatalf("Get literal = %#v, %v", response, err)
	}
}
