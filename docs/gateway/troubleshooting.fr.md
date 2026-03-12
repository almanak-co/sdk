# Guide de dépannage de la passerelle

Ce guide aide à diagnostiquer et résoudre les problèmes courants avec l'architecture de sécurité de la passerelle.

## Diagnostics rapides

### Vérifier la santé de la passerelle

```bash
# Docker
docker-compose -f deploy/docker/docker-compose.yml exec gateway \
  grpc_health_probe -addr=:50051

# Kubernetes
kubectl exec deploy/almanak-gateway -c gateway -- \
  grpc_health_probe -addr=:50051
```

Sortie attendue : `status: SERVING`

### Consulter les logs des conteneurs

```bash
# Logs de la passerelle
docker-compose logs gateway

# Logs de la stratégie
docker-compose logs strategy

# Kubernetes
kubectl logs deploy/almanak-gateway -c gateway
kubectl logs deploy/almanak-gateway -c strategy
```

### Vérifier la connectivité réseau

Depuis le conteneur de stratégie :

```bash
# Tester la connectivité à la passerelle
docker-compose exec strategy \
  python -c "import socket; print(socket.gethostbyname('gateway'))"

# Devrait retourner une IP interne (172.x.x.x ou 10.x.x.x)
```

## Problèmes courants

### Problème : La stratégie ne peut pas se connecter à la passerelle

**Symptômes :**
- Erreurs `connection refused`
- Logs `failed to connect to gateway`
- La stratégie démarre mais échoue immédiatement

**Causes et solutions :**

1. **Passerelle pas prête**
   ```bash
   # Vérifier le statut de la passerelle
   docker-compose ps gateway

   # Attendre le health check
   docker-compose up -d --wait
   ```

2. **Mauvaise adresse de passerelle**
   ```python
   # Vérifier les variables d'environnement dans la stratégie
   import os
   print(os.environ.get("GATEWAY_HOST"))  # Devrait être "gateway" ou "localhost"
   print(os.environ.get("GATEWAY_PORT"))  # Devrait être "50051"
   ```

3. **Mauvaise configuration réseau**
   ```bash
   # Vérifier que les deux conteneurs sont sur le même réseau
   docker network inspect deploy_internal
   ```

### Problème : "Method Not Allowed" pour les appels RPC

**Symptômes :**
- `ValidationError: method 'debug_traceTransaction' is not allowed`
- Les appels RPC échouent avec des erreurs de permission

**Cause :** La méthode RPC est bloquée par la liste d'autorisation de la passerelle.

**Solution :**

Seules ces méthodes sont autorisées :
- `eth_call`, `eth_getBalance`, `eth_getTransactionCount`
- `eth_getTransactionReceipt`, `eth_getBlockByNumber`, `eth_getBlockByHash`
- `eth_blockNumber`, `eth_chainId`, `eth_gasPrice`, `eth_estimateGas`
- `eth_getLogs`, `eth_getCode`, `eth_getStorageAt`
- `eth_sendRawTransaction`, `net_version`

Si vous avez besoin d'une méthode bloquée, contactez le support pour discuter des alternatives.

### Problème : "Chain Not Configured"

**Symptômes :**
- `Chain 'xyz' is not configured`
- Les appels RPC échouent avec une erreur de précondition

**Cause :** La chaîne n'est pas dans la liste autorisée ou n'est pas configurée.

**Solution :**

Chaînes autorisées : `ethereum`, `arbitrum`, `base`, `optimism`, `polygon`, `avalanche`, `bsc`, `sonic`, `plasma`

```bash
# Vérifier que la source RPC est configurée dans la passerelle
docker-compose exec gateway env | grep -E 'RPC_URL|ALCHEMY'
```

### Problème : Limitation de débit

**Symptômes :**
- `Rate limited, retry after X seconds`
- Statut gRPC `RESOURCE_EXHAUSTED`

**Cause :** Trop de requêtes vers la passerelle.

**Solutions :**

1. **Implémenter le cache**
   ```python
   from functools import lru_cache
   import time

   @lru_cache(maxsize=100)
   def cached_price(token, timestamp_minute):
       return gateway.get_price(token)

   # Utiliser avec une clé de cache au niveau de la minute
   price = cached_price("ETH", int(time.time() / 60))
   ```

2. **Regrouper les requêtes**
   ```python
   # Au lieu de multiples appels individuels
   prices = gateway.get_prices(["ETH", "BTC", "USDC"])
   ```

3. **Réduire la fréquence de polling**

### Problème : L'état ne persiste pas

**Symptômes :**
- État sauvegardé mais non récupéré
- État différent entre les redémarrages

**Causes et solutions :**

1. **Mauvais strategy_id**
   ```python
   # Assurer un strategy_id cohérent
   state = GatewayStateManager(gateway_client)

   # Sauvegarder
   await state.save(strategy_id="my-strategy", data=data)

   # Charger avec le même ID
   data = await state.load(strategy_id="my-strategy")  # Même ID !
   ```

2. **Limite de taille d'état dépassée**
   ```python
   # Taille maximale de l'état : 1 Mo
   import sys
   print(sys.getsizeof(data))  # Vérifier la taille
   ```

3. **Base de données non configurée**
   ```bash
   # Vérifier DATABASE_URL dans la passerelle
   docker-compose exec gateway env | grep DATABASE_URL
   ```

### Problème : Accès réseau externe bloqué

**Symptômes :**
- `socket.gaierror: [Errno -2] Name or service not known`
- `ConnectionError: No route to host`
- Les requêtes HTTP échouent avec un timeout

**Cause :** C'est le comportement attendu ! Les conteneurs de stratégie n'ont pas d'accès Internet.

**Solution :**

Utilisez les services fournis par la passerelle à la place :

```python
# Au lieu de HTTP direct
# requests.get("https://api.coingecko.com/...")

# Utiliser l'intégration passerelle
from almanak.framework.integrations import coingecko
prices = await coingecko.get_price("ethereum")
```

### Problème : Erreurs de sécurité des conteneurs

**Symptômes :**
- `Operation not permitted`
- `Read-only file system`
- `Permission denied`

**Cause :** Le durcissement de sécurité fonctionne correctement.

**Solutions :**

1. **Écrire des fichiers**
   ```python
   # Seul /tmp est accessible en écriture
   with open("/tmp/cache.json", "w") as f:
       f.write(data)

   # PAS /app/data.json (lecture seule)
   ```

2. **Installer des packages au runtime**
   ```bash
   # Non autorisé - installez dans le Dockerfile à la place
   # pip install some-package  # Échouera
   ```

### Problème : Métriques non disponibles

**Symptômes :**
- Le endpoint `/metrics` retourne 404
- Prometheus ne peut pas scraper les métriques

**Causes et solutions :**

1. **Métriques désactivées**
   ```yaml
   # Vérifier values.yaml ou l'environnement
   gateway:
     metrics:
       enabled: true
   ```

2. **Mauvais port**
   ```bash
   # Port de métriques par défaut : 9090
   curl http://gateway:9090/metrics
   ```

3. **Politique réseau bloquante**
   ```yaml
   # S'assurer que le namespace de monitoring peut atteindre les métriques
   networkPolicy:
     allowMetricsScraping: true
   ```

## Outils de débogage

### Réflexion gRPC

La passerelle supporte la réflexion gRPC pour le débogage :

```bash
# Lister les services
grpcurl -plaintext localhost:50051 list

# Décrire un service
grpcurl -plaintext localhost:50051 describe almanak.gateway.MarketService

# Appeler une méthode
grpcurl -plaintext -d '{"chain": "arbitrum", "token": "ETH"}' \
  localhost:50051 almanak.gateway.MarketService/GetPrice
```

### Logs d'audit

Les opérations de la passerelle sont enregistrées au format JSON :

```bash
# Voir les logs d'audit
docker-compose logs gateway | grep gateway_request

# Parser avec jq
docker-compose logs gateway 2>&1 | grep gateway_request | \
  tail -1 | jq -r '.timestamp, .service, .method, .latency_ms'
```

### Débogage réseau

Tester l'isolation réseau depuis l'intérieur du conteneur de stratégie :

```bash
# Entrer dans le conteneur de stratégie
docker-compose exec strategy /bin/sh

# Tester DNS (devrait échouer pour les domaines externes)
nslookup google.com

# Tester la connectivité (devrait échouer)
ping -c 1 8.8.8.8

# Tester la passerelle (devrait réussir)
nslookup gateway
nc -zv gateway 50051
```

### Endpoints de santé

```bash
# Santé gRPC de la passerelle
grpc_health_probe -addr=:50051

# Santé HTTP de la passerelle
curl http://gateway:9090/health

# Métriques de la passerelle
curl http://gateway:9090/metrics
```

## Obtenir de l'aide

Si vous rencontrez toujours des problèmes :

1. **Vérifiez les logs** pour des messages d'erreur spécifiques
2. **Vérifiez la configuration** par rapport à la documentation
3. **Exécutez les diagnostics** en utilisant les commandes ci-dessus
4. **Contactez le support** avec :
   - Les messages d'erreur des logs
   - Les étapes pour reproduire
   - Les versions de la passerelle et de la stratégie
   - La configuration (sans les secrets)
