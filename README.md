# miniKV

miniKV 是一个基于 **Go** 实现的轻量级、分布式键值存储系统。项目借鉴 **Amazon Dynamo** 架构实现 **AP** 分布式存储。

## 核心功能与演进状态

当前已完成 **最小可用 AP 架构**。

### 已完成特性 

- [x] **混合存储引擎**：融合 LSM-Tree 思想与 **Bitcask 架构** 的单机存储引擎，基于内存建立全量索引提升读性能，支持热数据直接命中，配合 WAL 预写日志和 SSTable 追加写落盘。
- [x] **后台垃圾回收 (Compaction)**：使用墓碑机制标注删除，自动回收无效的旧版本，合并 SSTable 文件。
- [x] **一致性哈希路由 (Consistent Hashing)**：内部实现虚拟节点哈希环，将数据均匀分布并支持多副本定位。
- [x] **多副本复制 (Replication)**：基于 `N=2` 的 Replication Factor，写/删操作并发写入多台物理节点。
- [x] **可调一致性 (Quorum R/W)**：实现了 `W+R > N` 的 Quorum 机制。目前默认 `N=2, W=2, R=1`。
- [x] **冲突解决 (Last-Write-Wins, LWW)**：通过网关全局生成的 `Timestamp` 实现全链路透传，读取时多副本间比对时间戳以返回最新的可用版本。

### 未来路线图

- [ ] **故障检测 (Failure Detection)**：引入基于 Gossip 的协议，去中心化的节点状态同步与心跳检测。
- [ ] **临时故障处理 (Hinted Handoff)**：当目标节点短暂离线时，由其他节点代存数据，并在其恢复后移交。
- [ ] **反熵同步 (Anti-Entropy / Merkle Tree)**：构建 Merkle Tree 并通过后台比对 Hash 来修复多副本之间可能存在的轻微数据不一致情况。

---

##  系统架构

目前采用的是 **网关路由模式 (方案 A)**，易于掌控核心逻辑；未来可逐渐下沉为完全无中心化的 **Dynamo 对等网络模式 (方案 B)**。

### 组件说明

1. **分布式路由网关 (Gateway)** 
   - 监听外界 HTTP 请求 (`:8080`)。
   - 包含 HashRing（一致性哈希），并且负责实现 Quorum 写/读、并行请求和全局 Timestamp 的生成。
2. **存储节点 (Storage Nodes)**
   - 纯粹的 "Dumb Server"，暴露 gRPC 接口提供真正的存取删操作。
   - 每个节点只管理本地的 `KVStore` 实例。

### 读写链路 (基于 Quorum 和 Timestamp)

* **写操作 (Put/Del)**：网关接收请求 -> 生成基于时钟的全局 `Timestamp` -> 查哈希环定位 `N` 个副本目标 -> `goroutine` 并发发送 gRPC 写请求 -> 收集 ACK，达到 `W` 个时响应成功。
* **读操作 (Get)**：网关接收并查哈希环 -> 发起 `N` 个并发 gRPC 读查请求 -> 收到成功响应后依靠响应体中的 `Timestamp` 比较保留最新 -> 收集达到 `R` 个立即返回客户端。

---

## 如何运行

### 1. 安装依赖并运行测试

需要 Go 1.24 或更高版本。首次使用时下载依赖并执行完整测试：

```bash
go mod download
go test ./...
go test -race ./...
go test -bench=. ./internal/storage
```

测试使用临时目录和进程内 gRPC 服务，不依赖固定本地目录或手工启动的节点。

### 2. 编译最新的 Protobuf

如修改 `api/proto/kv.proto`，请先安装 `protoc`、`protoc-gen-go` 和 `protoc-gen-go-grpc`，再重新生成桩代码：

```bash
protoc --go_out=. --go_opt=module=miniKV \\
  --go-grpc_out=. --go-grpc_opt=module=miniKV api/proto/kv.proto
```

### 3. 启动集群节点
可在一个机器上通过不同端口模拟分布式多节点：
```bash
# 启动节点 A
go run cmd/node/main.go -port 50051

# 启动节点 B
go run cmd/node/main.go -port 50052
```

### 4. 启动路由网关与可视化控制台
```bash
go run cmd/gateway/main.go
```
启动后，网关在 `:8080` 监听。

### 5. 实验与测试
通过 CLI 进行测试：
```bash
# 写入数据
curl "http://localhost:8080/put?key=hello&value=world"

# 读取数据
curl "http://localhost:8080/get?key=hello"

# 删除数据
curl "http://localhost:8080/del?key=hello"
```
