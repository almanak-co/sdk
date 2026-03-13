# Primeros pasos

Esta guía te acompaña en la instalación del SDK de Almanak, la creación de tu primera estrategia y su ejecución local en un fork de Anvil -- sin necesidad de wallet ni claves API.

## Requisitos previos

- **Python 3.12+**
- **uv** (gestor de paquetes Python):

```bash
curl -LsSf https://astral.sh/uv/install.sh | sh
```

- **Foundry** (proporciona Anvil para pruebas en fork local):

```bash
curl -L https://foundry.paradigm.xyz | bash
foundryup
```

## Instalación

```bash
pipx install almanak
```

O con [uv](https://docs.astral.sh/uv/):

```bash
uv tool install almanak
```

Esto instala el CLI `almanak` globalmente. Cada estrategia creada también tiene `almanak` como dependencia local en su propio `.venv/` -- este patrón de doble instalación es estándar (igual que CrewAI, Dagster, etc.).

**¿Usas un agente de codificación IA?** Enséñale el SDK con un solo comando:

```bash
almanak agent install
```

Esto detecta automáticamente tu plataforma (Claude Code, Codex, Cursor, Copilot y [6 más](agent-skills.md)) e instala la habilidad de construcción de estrategias.

## 1. Obtener una estrategia

### Opción A: Copiar una demo funcional (recomendado para principiantes)

```bash
almanak strat demo
```

Esto muestra un menú interactivo con 13 estrategias de demostración funcionales. Elige una y se copia en tu directorio actual, lista para ejecutar. También puedes saltar el menú:

```bash
almanak strat demo --name uniswap_rsi
```

### Opción B: Crear desde una plantilla

```bash
almanak strat new
```

Sigue las indicaciones interactivas para elegir una plantilla, cadena y nombre. Esto crea un **proyecto Python autónomo** con:

- `strategy.py` - Tu implementación de estrategia con el método `decide()`
- `config.json` - Configuración de cadena, protocolo y parámetros
- `pyproject.toml` - Dependencias y metadatos `[tool.almanak]`
- `uv.lock` - Dependencias bloqueadas (creado por `uv sync`)
- `.venv/` - Entorno virtual por estrategia (creado por `uv sync`)
- `.env` - Variables de entorno (completa tus claves después)
- `.gitignore` - Reglas de ignorar Git
- `.python-version` - Pin de versión de Python (3.12)
- `__init__.py` - Exportaciones del paquete
- `tests/` - Estructura de pruebas
- `AGENTS.md` - Guía para agentes IA

El scaffold ejecuta `uv sync` automáticamente para instalar dependencias. Para añadir paquetes adicionales:

```bash
uv add pandas-ta          # Actualiza pyproject.toml + uv.lock + .venv/
uv run pytest tests/ -v   # Ejecutar tests en el venv de la estrategia
```

## 2. Ejecutar en un fork local de Anvil

La forma más rápida de probar tu estrategia -- sin claves de wallet, sin fondos reales, sin riesgo:

```bash
cd my_strategy
almanak strat run --network anvil --once
```

Este comando automáticamente:

1. **Inicia un fork de Anvil** de la cadena especificada en tu `config.json` (se usan RPCs públicos gratuitos por defecto)
2. **Usa una wallet de Anvil por defecto** -- no necesitas `ALMANAK_PRIVATE_KEY`
3. **Inicia la pasarela** sidecar en segundo plano
4. **Financia tu wallet** con los tokens listados en `anvil_funding` (ver abajo)
5. **Ejecuta una iteración** del método `decide()` de tu estrategia

### Financiación de wallet en Anvil

Añade un bloque `anvil_funding` a tu `config.json` para financiar automáticamente tu wallet cuando se inicie el fork:

```json
{
    "anvil_funding": {
        "ETH": 10,
        "USDC": 10000,
        "WETH": 5
    }
}
```

Los tokens nativos (ETH, AVAX, etc.) se financian vía `anvil_setBalance`. Los tokens ERC-20 se financian mediante manipulación de slots de almacenamiento. Esto ocurre automáticamente cada vez que se inicia el fork.

### Mejor rendimiento RPC (opcional)

Los RPCs públicos gratuitos funcionan pero tienen límites de velocidad. Para un forking más rápido, configura una clave de Alchemy en tu `.env`:

```bash
ALCHEMY_API_KEY=your_alchemy_key
```

Esto construye automáticamente las URLs RPC para todas las cadenas soportadas. Cualquier proveedor funciona -- consulta [Variables de entorno](environment-variables.md) para el orden de prioridad completo.

## 3. Ejecutar en la red principal

!!! warning
    La ejecución en la red principal usa **fondos reales**. Comienza con cantidades pequeñas y usa una wallet dedicada.

Para ejecutar en cadenas en producción, necesitas una clave privada de wallet en tu `.env`:

```bash
# .env
ALMANAK_PRIVATE_KEY=0xYOUR_PRIVATE_KEY

# Acceso RPC (elige uno)
ALCHEMY_API_KEY=your_alchemy_key
# o: RPC_URL=https://your-rpc-provider.com/v1/your-key
```

Luego ejecuta sin la flag `--network anvil`:

```bash
almanak strat run --once
```

!!! tip
    Primero prueba con `--dry-run` para simular sin enviar transacciones:

    ```bash
    almanak strat run --dry-run --once
    ```

Consulta [Variables de entorno](environment-variables.md) para la lista completa de opciones de configuración, incluyendo claves API específicas de protocolos.

## Estructura de una estrategia

Una estrategia implementa el método `decide()`, que recibe un `MarketSnapshot` y devuelve un `Intent`:

```python
from decimal import Decimal
from almanak import IntentStrategy, Intent, MarketSnapshot

class MyStrategy(IntentStrategy):
    def decide(self, market: MarketSnapshot) -> Intent | None:
        price = market.price("ETH")
        balance = market.balance("USDC")

        if price < Decimal("2000") and balance.balance_usd > Decimal("500"):
            return Intent.swap(
                from_token="USDC",
                to_token="ETH",
                amount_usd=Decimal("500"),
            )
        return Intent.hold(reason="Sin oportunidad")
```

## Intenciones disponibles

| Intención | Descripción |
|--------|-------------|
| `SwapIntent` | Intercambios de tokens en DEXs |
| `HoldIntent` | Sin acción, esperar al siguiente ciclo |
| `LPOpenIntent` | Abrir posición de liquidez |
| `LPCloseIntent` | Cerrar posición de liquidez |
| `BorrowIntent` | Pedir prestado de protocolos de préstamo |
| `RepayIntent` | Devolver activos prestados |
| `SupplyIntent` | Suministrar a protocolos de préstamo |
| `WithdrawIntent` | Retirar de protocolos de préstamo |
| `StakeIntent` | Hacer staking de tokens |
| `UnstakeIntent` | Deshacer staking de tokens |
| `PerpOpenIntent` | Abrir posición de perpetuos |
| `PerpCloseIntent` | Cerrar posición de perpetuos |
| `FlashLoanIntent` | Operaciones de préstamo flash |
| `CollectFeesIntent` | Recolectar comisiones de LP |
| `PredictionBuyIntent` | Comprar acciones de mercado de predicción |
| `PredictionSellIntent` | Vender acciones de mercado de predicción |
| `PredictionRedeemIntent` | Canjear ganancias de mercado de predicción |
| `VaultDepositIntent` | Depositar en una bóveda |
| `VaultRedeemIntent` | Canjear desde una bóveda |
| `BridgeIntent` | Transferir tokens entre cadenas |
| `EnsureBalanceIntent` | Meta-intent que se resuelve a un `BridgeIntent` o `HoldIntent` para asegurar un saldo mínimo de tokens en una cadena destino |

## Generación del manifiesto de permisos (carteras Safe)

Al desplegar una estrategia a través de una cartera Safe con restricciones Zodiac Roles, el agente necesita un conjunto explícito de permisos de contratos. El SDK puede generar este manifiesto automáticamente inspeccionando a qué contratos y selectores de funciones compilan los intents de tu estrategia:

```bash
# Desde el directorio de la estrategia
almanak strat permissions

# Directorio explícito
almanak strat permissions -d strategies/demo/uniswap_rsi

# Especificar cadena
almanak strat permissions --chain base

# Escribir en archivo
almanak strat permissions -o manifest.json
```

El comando lee `supported_protocols` e `intent_types` de tu decorador `@almanak_strategy`, compila intents sintéticos a través del compilador real y extrae el conjunto mínimo de direcciones de contratos y selectores de funciones necesarios. La salida es un manifiesto JSON que se puede aplicar a un módulo Zodiac Roles. Si la estrategia soporta múltiples cadenas, la salida es un array JSON con un manifiesto por cadena; usa `--chain` para generar solo una cadena.

!!! note "Solo para despliegues Safe/Zodiac"
    Los manifiestos de permisos solo son necesarios al ejecutar a través de una cartera Safe con Zodiac Roles. Para pruebas locales en Anvil o ejecución con clave directa, no se requieren permisos.

## Próximos pasos

- [Variables de entorno](environment-variables.md) - Todas las opciones de configuración
- [Referencia API](api/index.md) - Documentación completa de la API de Python
- [Referencia CLI](cli/almanak.md) - Todos los comandos CLI
- [API de Pasarela](gateway/api-reference.md) - Servicios gRPC de la pasarela
