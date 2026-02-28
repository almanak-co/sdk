# Gateway Troubleshooting Guide

This guide helps diagnose and resolve common issues with the Gateway Security Architecture.

## Quick Diagnostics

### Check Gateway Health

```bash
# Docker
docker-compose -f deploy/docker/docker-compose.yml exec gateway \
  grpc_health_probe -addr=:50051

# Kubernetes
kubectl exec deploy/almanak-gateway -c gateway -- \
  grpc_health_probe -addr=:50051
```

Expected output: `status: SERVING`

### Check Container Logs

```bash
# Gateway logs
docker-compose logs gateway

# Strategy logs
docker-compose logs strategy

# Kubernetes
kubectl logs deploy/almanak-gateway -c gateway
kubectl logs deploy/almanak-gateway -c strategy
```

### Check Network Connectivity

From the strategy container:

```bash
# Test gateway connectivity
docker-compose exec strategy \
  python -c "import socket; print(socket.gethostbyname('gateway'))"

# Should return internal IP (172.x.x.x or 10.x.x.x)
```

## Common Issues

### Issue: Strategy Cannot Connect to Gateway

**Symptoms:**
- `connection refused` errors
- `failed to connect to gateway` logs
- Strategy starts but immediately fails

**Causes & Solutions:**

1. **Gateway not ready**
   ```bash
   # Check gateway status
   docker-compose ps gateway

   # Wait for health check
   docker-compose up -d --wait
   ```

2. **Wrong gateway address**
   ```python
   # Check environment variables in strategy
   import os
   print(os.environ.get("GATEWAY_HOST"))  # Should be "gateway" or "localhost"
   print(os.environ.get("GATEWAY_PORT"))  # Should be "50051"
   ```

3. **Network misconfiguration**
   ```bash
   # Verify both containers are on same network
   docker network inspect deploy_internal
   ```

### Issue: "Method Not Allowed" for RPC Calls

**Symptoms:**
- `ValidationError: method 'debug_traceTransaction' is not allowed`
- RPC calls fail with permission errors

**Cause:** The RPC method is blocked by the gateway allowlist.

**Solution:**

Only these methods are allowed:
- `eth_call`, `eth_getBalance`, `eth_getTransactionCount`
- `eth_getTransactionReceipt`, `eth_getBlockByNumber`, `eth_getBlockByHash`
- `eth_blockNumber`, `eth_chainId`, `eth_gasPrice`, `eth_estimateGas`
- `eth_getLogs`, `eth_getCode`, `eth_getStorageAt`
- `eth_sendRawTransaction`, `net_version`

If you need a blocked method, contact support to discuss alternatives.

### Issue: "Chain Not Configured"

**Symptoms:**
- `Chain 'xyz' is not configured`
- RPC calls fail with precondition error

**Cause:** The chain is not in the allowed list or not configured.

**Solution:**

Allowed chains: `ethereum`, `arbitrum`, `base`, `optimism`, `polygon`, `avalanche`, `bsc`, `sonic`, `plasma`

```bash
# Verify RPC source is configured in gateway
docker-compose exec gateway env | grep -E 'RPC_URL|ALCHEMY'
```

### Issue: Rate Limiting

**Symptoms:**
- `Rate limited, retry after X seconds`
- `RESOURCE_EXHAUSTED` gRPC status

**Cause:** Too many requests to the gateway.

**Solutions:**

1. **Implement caching**
   ```python
   from functools import lru_cache
   import time

   @lru_cache(maxsize=100)
   def cached_price(token, timestamp_minute):
       return gateway.get_price(token)

   # Use with minute-level cache key
   price = cached_price("ETH", int(time.time() / 60))
   ```

2. **Batch requests**
   ```python
   # Instead of multiple single calls
   prices = gateway.get_prices(["ETH", "BTC", "USDC"])
   ```

3. **Reduce polling frequency**

### Issue: State Not Persisting

**Symptoms:**
- State saved but not retrieved
- Different state between restarts

**Causes & Solutions:**

1. **Wrong strategy_id**
   ```python
   # Ensure consistent strategy_id
   state = GatewayStateManager(gateway_client)

   # Save
   await state.save(strategy_id="my-strategy", data=data)

   # Load with same ID
   data = await state.load(strategy_id="my-strategy")  # Same ID!
   ```

2. **State size limit exceeded**
   ```python
   # Max state size is 1MB
   import sys
   print(sys.getsizeof(data))  # Check size
   ```

3. **Database not configured**
   ```bash
   # Verify DATABASE_URL in gateway
   docker-compose exec gateway env | grep DATABASE_URL
   ```

### Issue: External Network Access Blocked

**Symptoms:**
- `socket.gaierror: [Errno -2] Name or service not known`
- `ConnectionError: No route to host`
- HTTP requests fail with timeout

**Cause:** This is expected behavior! Strategy containers have no internet access.

**Solution:**

Use gateway-provided services instead:

```python
# Instead of direct HTTP
# requests.get("https://api.coingecko.com/...")

# Use gateway integration
from almanak.framework.integrations import coingecko
prices = await coingecko.get_price("ethereum")
```

### Issue: Container Security Errors

**Symptoms:**
- `Operation not permitted`
- `Read-only file system`
- `Permission denied`

**Cause:** Security hardening is working correctly.

**Solutions:**

1. **Writing files**
   ```python
   # Only /tmp is writable
   with open("/tmp/cache.json", "w") as f:
       f.write(data)

   # NOT /app/data.json (read-only)
   ```

2. **Installing packages at runtime**
   ```bash
   # Not allowed - install in Dockerfile instead
   # pip install some-package  # Will fail
   ```

### Issue: Metrics Not Available

**Symptoms:**
- `/metrics` endpoint returns 404
- Prometheus can't scrape metrics

**Causes & Solutions:**

1. **Metrics disabled**
   ```yaml
   # Check values.yaml or environment
   gateway:
     metrics:
       enabled: true
   ```

2. **Wrong port**
   ```bash
   # Default metrics port is 9090
   curl http://gateway:9090/metrics
   ```

3. **Network policy blocking**
   ```yaml
   # Ensure monitoring namespace can reach metrics
   networkPolicy:
     allowMetricsScraping: true
   ```

## Debug Tools

### gRPC Reflection

The gateway supports gRPC reflection for debugging:

```bash
# List services
grpcurl -plaintext localhost:50051 list

# Describe a service
grpcurl -plaintext localhost:50051 describe almanak.gateway.MarketService

# Call a method
grpcurl -plaintext -d '{"chain": "arbitrum", "token": "ETH"}' \
  localhost:50051 almanak.gateway.MarketService/GetPrice
```

### Audit Logs

Gateway operations are logged in JSON format:

```bash
# View audit logs
docker-compose logs gateway | grep gateway_request

# Parse with jq
docker-compose logs gateway 2>&1 | grep gateway_request | \
  tail -1 | jq -r '.timestamp, .service, .method, .latency_ms'
```

### Network Debugging

Test network isolation from inside the strategy container:

```bash
# Enter strategy container
docker-compose exec strategy /bin/sh

# Test DNS (should fail for external domains)
nslookup google.com

# Test connectivity (should fail)
ping -c 1 8.8.8.8

# Test gateway (should succeed)
nslookup gateway
nc -zv gateway 50051
```

### Health Endpoints

```bash
# Gateway gRPC health
grpc_health_probe -addr=:50051

# Gateway HTTP health
curl http://gateway:9090/health

# Gateway metrics
curl http://gateway:9090/metrics
```

## Getting Help

If you're still having issues:

1. **Check logs** for specific error messages
2. **Verify configuration** against the documentation
3. **Run diagnostics** using the commands above
4. **Contact support** with:
   - Error messages from logs
   - Steps to reproduce
   - Gateway and strategy versions
   - Configuration (sanitized of secrets)
