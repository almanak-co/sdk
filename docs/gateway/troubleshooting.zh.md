# 网关故障排除指南

本指南帮助诊断和解决网关安全架构的常见问题。

## 快速诊断

### 检查网关健康状态

```bash
# Docker
docker-compose -f deploy/docker/docker-compose.yml exec gateway \
  grpc_health_probe -addr=:50051

# Kubernetes
kubectl exec deploy/almanak-gateway -c gateway -- \
  grpc_health_probe -addr=:50051
```

预期输出：`status: SERVING`

### 查看容器日志

```bash
# 网关日志
docker-compose logs gateway

# 策略日志
docker-compose logs strategy

# Kubernetes
kubectl logs deploy/almanak-gateway -c gateway
kubectl logs deploy/almanak-gateway -c strategy
```

### 检查网络连接

从策略容器内部：

```bash
# 测试网关连接
docker-compose exec strategy \
  python -c "import socket; print(socket.gethostbyname('gateway'))"

# 应返回内部 IP（172.x.x.x 或 10.x.x.x）
```

## 常见问题

### 问题：策略无法连接到网关

**症状：**
- `connection refused` 错误
- `failed to connect to gateway` 日志
- 策略启动后立即失败

**原因和解决方案：**

1. **网关未就绪**
   ```bash
   # 检查网关状态
   docker-compose ps gateway

   # 等待健康检查
   docker-compose up -d --wait
   ```

2. **网关地址错误**
   ```python
   # 检查策略中的环境变量
   import os
   print(os.environ.get("GATEWAY_HOST"))  # 应为 "gateway" 或 "localhost"
   print(os.environ.get("GATEWAY_PORT"))  # 应为 "50051"
   ```

3. **网络配置错误**
   ```bash
   # 验证两个容器在同一网络上
   docker network inspect deploy_internal
   ```

### 问题：RPC 调用出现 "Method Not Allowed"

**症状：**
- `ValidationError: method 'debug_traceTransaction' is not allowed`
- RPC 调用因权限错误失败

**原因：** RPC 方法被网关允许列表阻止。

**解决方案：**

只允许以下方法：
- `eth_call`、`eth_getBalance`、`eth_getTransactionCount`
- `eth_getTransactionReceipt`、`eth_getBlockByNumber`、`eth_getBlockByHash`
- `eth_blockNumber`、`eth_chainId`、`eth_gasPrice`、`eth_estimateGas`
- `eth_getLogs`、`eth_getCode`、`eth_getStorageAt`
- `eth_sendRawTransaction`、`net_version`

如果您需要被阻止的方法，请联系支持讨论替代方案。

### 问题："Chain Not Configured"

**症状：**
- `Chain 'xyz' is not configured`
- RPC 调用因前置条件错误失败

**原因：** 该链不在允许列表中或未配置。

**解决方案：**

允许的链：`ethereum`、`arbitrum`、`base`、`optimism`、`polygon`、`avalanche`、`bsc`、`sonic`、`plasma`

```bash
# 验证网关中配置了 RPC 源
docker-compose exec gateway env | grep -E 'RPC_URL|ALCHEMY'
```

### 问题：速率限制

**症状：**
- `Rate limited, retry after X seconds`
- `RESOURCE_EXHAUSTED` gRPC 状态

**原因：** 对网关的请求过多。

**解决方案：**

1. **实现缓存**
   ```python
   from functools import lru_cache
   import time

   @lru_cache(maxsize=100)
   def cached_price(token, timestamp_minute):
       return gateway.get_price(token)

   # 使用分钟级缓存键
   price = cached_price("ETH", int(time.time() / 60))
   ```

2. **批量请求**
   ```python
   # 替代多次单独调用
   prices = gateway.get_prices(["ETH", "BTC", "USDC"])
   ```

3. **降低轮询频率**

### 问题：状态未持久化

**症状：**
- 状态已保存但未检索到
- 重启后状态不同

**原因和解决方案：**

1. **strategy_id 错误**
   ```python
   # 确保一致的 strategy_id
   state = GatewayStateManager(gateway_client)

   # 保存
   await state.save(strategy_id="my-strategy", data=data)

   # 使用相同 ID 加载
   data = await state.load(strategy_id="my-strategy")  # 相同的 ID！
   ```

2. **超出状态大小限制**
   ```python
   # 最大状态大小为 1MB
   import sys
   print(sys.getsizeof(data))  # 检查大小
   ```

3. **数据库未配置**
   ```bash
   # 验证网关中的 DATABASE_URL
   docker-compose exec gateway env | grep DATABASE_URL
   ```

### 问题：外部网络访问被阻止

**症状：**
- `socket.gaierror: [Errno -2] Name or service not known`
- `ConnectionError: No route to host`
- HTTP 请求超时失败

**原因：** 这是预期行为！策略容器没有互联网访问权限。

**解决方案：**

使用网关提供的服务代替：

```python
# 替代直接 HTTP
# requests.get("https://api.coingecko.com/...")

# 使用网关集成
from almanak.framework.integrations import coingecko
prices = await coingecko.get_price("ethereum")
```

### 问题：容器安全错误

**症状：**
- `Operation not permitted`
- `Read-only file system`
- `Permission denied`

**原因：** 安全加固正在正常工作。

**解决方案：**

1. **写入文件**
   ```python
   # 只有 /tmp 是可写的
   with open("/tmp/cache.json", "w") as f:
       f.write(data)

   # 不能写 /app/data.json（只读）
   ```

2. **运行时安装包**
   ```bash
   # 不允许 - 请在 Dockerfile 中安装
   # pip install some-package  # 会失败
   ```

### 问题：指标不可用

**症状：**
- `/metrics` 端点返回 404
- Prometheus 无法抓取指标

**原因和解决方案：**

1. **指标已禁用**
   ```yaml
   # 检查 values.yaml 或环境
   gateway:
     metrics:
       enabled: true
   ```

2. **端口错误**
   ```bash
   # 默认指标端口为 9090
   curl http://gateway:9090/metrics
   ```

3. **网络策略阻止**
   ```yaml
   # 确保监控命名空间可以访问指标
   networkPolicy:
     allowMetricsScraping: true
   ```

## 调试工具

### gRPC 反射

网关支持 gRPC 反射用于调试：

```bash
# 列出服务
grpcurl -plaintext localhost:50051 list

# 描述服务
grpcurl -plaintext localhost:50051 describe almanak.gateway.MarketService

# 调用方法
grpcurl -plaintext -d '{"chain": "arbitrum", "token": "ETH"}' \
  localhost:50051 almanak.gateway.MarketService/GetPrice
```

### 审计日志

网关操作以 JSON 格式记录：

```bash
# 查看审计日志
docker-compose logs gateway | grep gateway_request

# 使用 jq 解析
docker-compose logs gateway 2>&1 | grep gateway_request | \
  tail -1 | jq -r '.timestamp, .service, .method, .latency_ms'
```

### 网络调试

从策略容器内部测试网络隔离：

```bash
# 进入策略容器
docker-compose exec strategy /bin/sh

# 测试 DNS（外部域名应失败）
nslookup google.com

# 测试连接（应失败）
ping -c 1 8.8.8.8

# 测试网关（应成功）
nslookup gateway
nc -zv gateway 50051
```

### 健康端点

```bash
# 网关 gRPC 健康
grpc_health_probe -addr=:50051

# 网关 HTTP 健康
curl http://gateway:9090/health

# 网关指标
curl http://gateway:9090/metrics
```

## 获取帮助

如果您仍有问题：

1. **检查日志** 查找具体错误信息
2. **验证配置** 对照文档
3. **运行诊断** 使用上述命令
4. **联系支持** 并提供：
   - 日志中的错误信息
   - 重现步骤
   - 网关和策略版本
   - 配置（已脱敏密钥）
