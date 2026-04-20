# Guía de solución de problemas de la pasarela

Esta guía ayuda a diagnosticar y resolver problemas comunes con la arquitectura de seguridad de la pasarela.

## Diagnósticos rápidos

### Verificar la salud de la pasarela

```bash
# Docker
docker-compose -f deploy/docker/docker-compose.yml exec gateway \
  grpc_health_probe -addr=:50051

# Kubernetes
kubectl exec deploy/almanak-gateway -c gateway -- \
  grpc_health_probe -addr=:50051
```

Salida esperada: `status: SERVING`

### Consultar logs de contenedores

```bash
# Logs de la pasarela
docker-compose logs gateway

# Logs de la estrategia
docker-compose logs strategy

# Kubernetes
kubectl logs deploy/almanak-gateway -c gateway
kubectl logs deploy/almanak-gateway -c strategy
```

### Verificar conectividad de red

Desde el contenedor de estrategia:

```bash
# Probar conectividad a la pasarela
docker-compose exec strategy \
  python -c "import socket; print(socket.gethostbyname('gateway'))"

# Debería devolver IP interna (172.x.x.x o 10.x.x.x)
```

## Problemas comunes

### Problema: La estrategia no puede conectarse a la pasarela

**Síntomas:**
- Errores `connection refused`
- Logs `failed to connect to gateway`
- La estrategia inicia pero falla inmediatamente

**Causas y soluciones:**

1. **Pasarela no lista**
   ```bash
   # Verificar estado de la pasarela
   docker-compose ps gateway

   # Esperar al health check
   docker-compose up -d --wait
   ```

2. **Dirección de pasarela incorrecta**
   ```python
   # Verificar variables de entorno en la estrategia
   import os
   print(os.environ.get("GATEWAY_HOST"))  # Debería ser "gateway" o "localhost"
   print(os.environ.get("GATEWAY_PORT"))  # Debería ser "50051"
   ```

3. **Configuración de red incorrecta**
   ```bash
   # Verificar que ambos contenedores están en la misma red
   docker network inspect deploy_internal
   ```

### Problema: "Method Not Allowed" para llamadas RPC

**Síntomas:**
- `ValidationError: method 'debug_traceTransaction' is not allowed`
- Las llamadas RPC fallan con errores de permisos

**Causa:** El método RPC está bloqueado por la lista de permisos de la pasarela.

**Solución:**

Solo estos métodos están permitidos:
- `eth_call`, `eth_getBalance`, `eth_getTransactionCount`
- `eth_getTransactionReceipt`, `eth_getBlockByNumber`, `eth_getBlockByHash`
- `eth_blockNumber`, `eth_chainId`, `eth_gasPrice`, `eth_estimateGas`
- `eth_getLogs`, `eth_getCode`, `eth_getStorageAt`
- `eth_sendRawTransaction`, `net_version`

Si necesitas un método bloqueado, contacta a soporte para discutir alternativas.

### Problema: "Chain Not Configured"

**Síntomas:**
- `Chain 'xyz' is not configured`
- Las llamadas RPC fallan con error de precondición

**Causa:** La cadena no está en la lista permitida o no está configurada.

**Solución:**

Cadenas permitidas: `ethereum`, `arbitrum`, `base`, `optimism`, `polygon`, `avalanche`, `bsc`, `sonic`, `plasma`

```bash
# Verificar que la fuente RPC está configurada en la pasarela
docker-compose exec gateway env | grep -E 'RPC_URL|ALCHEMY'
```

### Problema: Límite de velocidad

**Síntomas:**
- `Rate limited, retry after X seconds`
- Estado gRPC `RESOURCE_EXHAUSTED`

**Causa:** Demasiadas peticiones a la pasarela.

**Soluciones:**

1. **Implementar caché**
   ```python
   from functools import lru_cache
   import time

   @lru_cache(maxsize=100)
   def cached_price(token, timestamp_minute):
       return gateway.get_price(token)

   # Usar con clave de caché a nivel de minuto
   price = cached_price("ETH", int(time.time() / 60))
   ```

2. **Agrupar peticiones**
   ```python
   # En lugar de múltiples llamadas individuales
   prices = gateway.get_prices(["ETH", "BTC", "USDC"])
   ```

3. **Reducir la frecuencia de polling**

### Problema: El estado no persiste

**Síntomas:**
- Estado guardado pero no recuperado
- Estado diferente entre reinicios

**Causas y soluciones:**

1. **strategy_id incorrecto**
   ```python
   # Asegurar un strategy_id consistente
   state = GatewayStateManager(gateway_client)

   # Guardar
   await state.save(strategy_id="my-strategy", data=data)

   # Cargar con el mismo ID
   data = await state.load(strategy_id="my-strategy")  # ¡Mismo ID!
   ```

2. **Límite de tamaño de estado excedido**
   ```python
   # Tamaño máximo del estado: 1 MB
   import sys
   print(sys.getsizeof(data))  # Verificar tamaño
   ```

3. **Base de datos no configurada**
   ```bash
   # Verificar DATABASE_URL en la pasarela
   docker-compose exec gateway env | grep DATABASE_URL
   ```

### Problema: Acceso a red externa bloqueado

**Síntomas:**
- `socket.gaierror: [Errno -2] Name or service not known`
- `ConnectionError: No route to host`
- Las peticiones HTTP fallan por timeout

**Causa:** ¡Este es el comportamiento esperado! Los contenedores de estrategia no tienen acceso a Internet.

**Solución:**

Usa los servicios proporcionados por la pasarela en su lugar:

```python
# En lugar de HTTP directo
# requests.get("https://api.coingecko.com/...")

# Usar la integración de la pasarela
from almanak.framework.integrations import coingecko
prices = await coingecko.get_price("ethereum")
```

### Problema: Errores de seguridad del contenedor

**Síntomas:**
- `Operation not permitted`
- `Read-only file system`
- `Permission denied`

**Causa:** El endurecimiento de seguridad está funcionando correctamente.

**Soluciones:**

1. **Escribir archivos**
   ```python
   # Solo /tmp es escribible
   with open("/tmp/cache.json", "w") as f:
       f.write(data)

   # NO /app/data.json (solo lectura)
   ```

2. **Instalar paquetes en tiempo de ejecución**
   ```bash
   # No permitido - instala en el Dockerfile en su lugar
   # pip install some-package  # Fallará
   ```

### Problema: Métricas no disponibles

**Síntomas:**
- El endpoint `/metrics` devuelve 404
- Prometheus no puede recopilar métricas

**Causas y soluciones:**

1. **Métricas deshabilitadas**
   ```yaml
   # Verificar values.yaml o entorno
   gateway:
     metrics:
       enabled: true
   ```

2. **Puerto incorrecto**
   ```bash
   # Puerto de métricas por defecto: 9090
   curl http://gateway:9090/metrics
   ```

3. **Política de red bloqueando**
   ```yaml
   # Asegurar que el namespace de monitoreo puede alcanzar las métricas
   networkPolicy:
     allowMetricsScraping: true
   ```

## Herramientas de depuración

### Reflexión gRPC

La pasarela soporta reflexión gRPC para depuración:

```bash
# Listar servicios
grpcurl -plaintext localhost:50051 list

# Describir un servicio
grpcurl -plaintext localhost:50051 describe almanak.gateway.MarketService

# Llamar a un método
grpcurl -plaintext -d '{"chain": "arbitrum", "token": "ETH"}' \
  localhost:50051 almanak.gateway.MarketService/GetPrice
```

### Logs de auditoría

Las operaciones de la pasarela se registran en formato JSON:

```bash
# Ver logs de auditoría
docker-compose logs gateway | grep gateway_request

# Parsear con jq
docker-compose logs gateway 2>&1 | grep gateway_request | \
  tail -1 | jq -r '.timestamp, .service, .method, .latency_ms'
```

### Depuración de red

Probar el aislamiento de red desde dentro del contenedor de estrategia:

```bash
# Entrar al contenedor de estrategia
docker-compose exec strategy /bin/sh

# Probar DNS (debería fallar para dominios externos)
nslookup google.com

# Probar conectividad (debería fallar)
ping -c 1 8.8.8.8

# Probar pasarela (debería funcionar)
nslookup gateway
nc -zv gateway 50051
```

### Endpoints de salud

```bash
# Salud gRPC de la pasarela
grpc_health_probe -addr=:50051

# Salud HTTP de la pasarela
curl http://gateway:9090/health

# Métricas de la pasarela
curl http://gateway:9090/metrics
```

## Obtener ayuda

Si sigues teniendo problemas:

1. **Revisa los logs** buscando mensajes de error específicos
2. **Verifica la configuración** contra la documentación
3. **Ejecuta los diagnósticos** usando los comandos anteriores
4. **Contacta a soporte** con:
   - Mensajes de error de los logs
   - Pasos para reproducir
   - Versiones de la pasarela y la estrategia
   - Configuración (sin secretos)
