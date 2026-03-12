# Variables de entorno

Todas las estrategias se ejecutan a través del **sidecar de pasarela** (iniciado automáticamente por `almanak strat run`). La pasarela almacena los secretos, proporciona acceso RPC y ejecuta transacciones.

Crea un archivo `.env` en el directorio de tu estrategia con las variables que se indican a continuación.

---

## Obligatorias

Estas variables deben configurarse antes de ejecutar cualquier estrategia.

| Variable | Descripción | Ejemplo |
|----------|-------------|---------|
| `ALMANAK_PRIVATE_KEY` | Clave privada de la wallet para firmar transacciones y derivar la dirección de tu wallet | `0x4c0883a6...` |

### Acceso RPC (recomendado; se usan RPCs públicos gratuitos si no se configura)

| Variable | Prioridad | Descripción | Ejemplo |
|----------|----------|-------------|---------|
| `ALMANAK_{CHAIN}_RPC_URL` | 1 (más alta) | URL RPC por cadena con prefijo ALMANAK | `https://arb-mainnet.infura.io/v3/KEY` |
| `{CHAIN}_RPC_URL` | 2 | URL RPC por cadena (ej: `ARBITRUM_RPC_URL`) | `https://arb-mainnet.infura.io/v3/KEY` |
| `ALMANAK_RPC_URL` | 3 | URL RPC genérica para todas las cadenas | `https://your-rpc.com/v1/KEY` |
| `RPC_URL` | 4 | URL RPC genérica básica | `https://your-rpc.com/v1/KEY` |
| `ALCHEMY_API_KEY` | 5 (respaldo) | Clave API de Alchemy -- URLs construidas automáticamente por cadena | `abc123def456` |
| `TENDERLY_API_KEY_{CHAIN}` | 6 (respaldo) | Clave API de Tenderly por cadena (ej: `TENDERLY_API_KEY_ARBITRUM`) | `abc123...` |

Cualquier proveedor funciona: Infura, QuickNode, auto-hospedado, Alchemy, etc. `ALCHEMY_API_KEY` es un respaldo opcional que construye automáticamente URLs para todas las cadenas soportadas. Si no se configura ninguna, la pasarela usa RPCs públicos gratuitos (con límites de velocidad, mejor esfuerzo).

!!! warning
    Nunca hagas commit de claves privadas. Usa una wallet de prueba dedicada para desarrollo.

**Nota:** La pasarela también acepta `ALMANAK_GATEWAY_PRIVATE_KEY` (con su propio prefijo). Si se configura, tiene prioridad. De lo contrario, la pasarela usa `ALMANAK_PRIVATE_KEY` -- así que solo necesitas una variable.

---

## Claves API opcionales

Configura estas según los protocolos y funcionalidades que use tu estrategia.

| Variable | Cuándo se necesita | Obtener clave |
|----------|-------------|-----------|
| `ENSO_API_KEY` | Enrutamiento de swaps vía el agregador Enso Finance | [enso.finance](https://enso.finance/) |
| `COINGECKO_API_KEY` | Mejora los límites de velocidad para datos de precios (funciona sin clave) | [coingecko.com/en/api](https://www.coingecko.com/en/api) |
| `ALMANAK_API_KEY` | Funcionalidades de la plataforma: `strat push`, `strat pull`, despliegue | [app.almanak.co](https://app.almanak.co/) |
| `THEGRAPH_API_KEY` | Backtesting con datos de subgraphs (volúmenes DEX, APYs de préstamo) | [thegraph.com/studio](https://thegraph.com/studio/) |

---

## Específicas por protocolo

Solo necesarias si tu estrategia usa estos protocolos específicos.

### Kraken

| Variable | Descripción |
|----------|-------------|
| `KRAKEN_API_KEY` | Clave API de Kraken ([obtener credenciales](https://www.kraken.com/u/security/api)) |
| `KRAKEN_API_SECRET` | Secreto API de Kraken |

### Polymarket

| Variable | Descripción |
|----------|-------------|
| `POLYMARKET_WALLET_ADDRESS` | Dirección de wallet de Polymarket |
| `POLYMARKET_PRIVATE_KEY` | Clave de firma de Polymarket |
| `POLYMARKET_API_KEY` | Clave API CLOB |
| `POLYMARKET_SECRET` | Secreto HMAC |
| `POLYMARKET_PASSPHRASE` | Frase secreta API |

### Pendle

| Variable | Descripción |
|----------|-------------|
| `ALMANAK_GATEWAY_PENDLE_API_KEY` | Clave API del protocolo Pendle |

---

## Wallet Safe

Para estrategias que se ejecutan a través de un multisig de Gnosis Safe.

| Variable | Descripción |
|----------|-------------|
| `ALMANAK_GATEWAY_SAFE_ADDRESS` | Dirección de la wallet Safe |
| `ALMANAK_GATEWAY_SAFE_MODE` | `direct` (Anvil/umbral-1) o `zodiac` (producción) |
| `ALMANAK_GATEWAY_ZODIAC_ROLES_ADDRESS` | Dirección del módulo Zodiac Roles (modo zodiac) |
| `ALMANAK_GATEWAY_SIGNER_SERVICE_URL` | URL del servicio de firma remoto (modo zodiac) |
| `ALMANAK_GATEWAY_SIGNER_SERVICE_JWT` | JWT del firmante remoto (modo zodiac) |

---

## Backtesting

### URLs RPC de archivo

Necesarias para datos históricos on-chain (precios de Chainlink, cálculos TWAP). Los nodos RPC estándar no soportan consultas de estado histórico. Usa proveedores con soporte de archivo como Alchemy (de pago), QuickNode o Infura.

Patrón: `ARCHIVE_RPC_URL_{CHAIN}` (ej: `ARCHIVE_RPC_URL_ARBITRUM`, `ARCHIVE_RPC_URL_ETHEREUM`, `ARCHIVE_RPC_URL_BASE`, `ARCHIVE_RPC_URL_OPTIMISM`, `ARCHIVE_RPC_URL_POLYGON`, `ARCHIVE_RPC_URL_AVALANCHE`)

### Claves API de exploradores de bloques

Opcionales, para datos históricos de precio de gas. Patrón: `{EXPLORER}_API_KEY`

| Variable | Explorador |
|----------|----------|
| `ETHERSCAN_API_KEY` | [etherscan.io](https://etherscan.io/apis) |
| `ARBISCAN_API_KEY` | [arbiscan.io](https://arbiscan.io/apis) |
| `BASESCAN_API_KEY` | [basescan.org](https://basescan.org/apis) |
| `OPTIMISTIC_ETHERSCAN_API_KEY` | [optimistic.etherscan.io](https://optimistic.etherscan.io/apis) |
| `POLYGONSCAN_API_KEY` | [polygonscan.com](https://polygonscan.com/apis) |
| `SNOWTRACE_API_KEY` | [snowtrace.io](https://snowtrace.io/apis) |
| `BSCSCAN_API_KEY` | [bscscan.com](https://bscscan.com/apis) |

---

## `.env` de inicio rápido

```bash
# Obligatorio
ALMANAK_PRIVATE_KEY=0xYOUR_PRIVATE_KEY

# Acceso RPC (elige uno)
RPC_URL=https://your-rpc-provider.com/v1/your-key
# ALCHEMY_API_KEY=your_alchemy_key  # alternativa: construye URLs automáticamente por cadena

# Recomendado
ENSO_API_KEY=your_enso_key
COINGECKO_API_KEY=your_coingecko_key
```

Todos los demás ajustes de la pasarela y del framework tienen valores predeterminados razonables y no necesitan configurarse. Consulta [`.env.example`](https://github.com/almanak-co/almanak-sdk/blob/main/.env.example) para la lista completa de opciones avanzadas.
