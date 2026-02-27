![Almanak](assets/logo-dark.svg){ .hero-logo }

El SDK de Almanak proporciona un framework completo para desarrollar, probar y desplegar agentes DeFi autónomos. Construido sobre una arquitectura basada en intenciones, las estrategias se expresan como intenciones de alto nivel con un mínimo de código.

## Características

- **Arquitectura basada en intenciones** - Expresa tu lógica de trading como intenciones de alto nivel (Swap, LP, Borrow, etc.). El framework se encarga de la compilación y ejecución.
- **Gestión de estado de tres niveles** - Persistencia automática con niveles HOT/WARM/COLD para mayor fiabilidad.
- **Backtesting completo** - Simulación PnL, paper trading en forks de Anvil y barrido de parámetros.
- **Soporte multi-cadena** - Ethereum, Arbitrum, Optimism, Base, Avalanche, Polygon, BSC, Sonic, Plasma, Blast, Mantle, Berachain, y más.
- **Integración de protocolos** - Uniswap V3, Aave V3, Morpho Blue, GMX V2, Pendle, Polymarket, Kraken, y más.
- **Diseño sin custodia** - Control total de tus fondos a través de cuentas inteligentes Safe.
- **Listo para producción** - Alertas integradas, detección de bloqueos, gestión de emergencias y despliegues canary.

## Instalación

```bash
pip install almanak
```

## Inicio rápido

```bash
# Crear una nueva estrategia a partir de una plantilla
almanak strat new

# Ejecutarla en un fork local de Anvil -- sin necesidad de wallet ni claves API
cd my_strategy
almanak strat run --network anvil --once
```

Las pruebas en fork de Anvil son el punto de partida recomendado. El SDK inicia automáticamente un fork local, usa una wallet pre-financiada por defecto y ejecuta tu estrategia sin configuración alguna. Consulta [Primeros pasos](getting-started.md) para el tutorial completo.

## Escribir una estrategia

Las estrategias implementan el método `decide()`, que recibe un `MarketSnapshot` y devuelve un `Intent` (o `None` para saltar el ciclo):

```python
from decimal import Decimal
from almanak.framework.intents import Intent
from almanak.framework.strategies import IntentStrategy, MarketSnapshot

class MyStrategy(IntentStrategy):
    """Una estrategia simple de reversión a la media."""

    def decide(self, market: MarketSnapshot) -> Intent | None:
        eth_price = market.price("ETH")
        usdc = market.balance("USDC")

        if eth_price < Decimal("2000") and usdc.balance_usd > Decimal("500"):
            return Intent.swap(
                from_token="USDC",
                to_token="ETH",
                amount_usd=Decimal("500"),
            )
        return Intent.hold(reason="Esperando mejores condiciones")
```

## Arquitectura

```text
almanak/
  framework/           # Framework de estrategias V2
    strategies/        # Clase base IntentStrategy
    intents/           # Vocabulario de intenciones y compilador
    state/             # Gestión de estado de tres niveles
    execution/         # Orquestación de transacciones
    backtesting/       # PnL, paper trading, barrido de parámetros
    connectors/        # Adaptadores de protocolos
    data/              # Oráculos de precios, indicadores
    alerting/          # Notificaciones Slack/Telegram
    services/          # Detección de bloqueos, gestión de emergencias
  gateway/             # Sidecar gRPC de la pasarela
  transaction_builder/ # Construcción de transacciones de bajo nivel
  core/                # Enumeraciones, modelos, utilidades
  cli/                 # Interfaz de línea de comandos
```

Todas las estrategias se ejecutan a través de una **arquitectura exclusiva de pasarela** para mayor seguridad. El sidecar de pasarela almacena todos los secretos y expone una API gRPC controlada. Los contenedores de estrategia no tienen secretos ni acceso directo a Internet.

## Comentarios y solicitudes de funcionalidades

¿Tienes una idea, encontraste un bug o quieres solicitar una funcionalidad? Visita nuestro [Discord](https://discord.gg/yuCMvQv3rN) y publica en el canal correspondiente. Monitoreamos activamente los comentarios y los usamos para dar forma a la hoja de ruta del SDK.

## Próximos pasos

- [Primeros pasos](getting-started.md) - Instalación y tutorial de tu primera estrategia
- [Referencia CLI](cli/almanak.md) - Todos los comandos CLI
- [Referencia API](api/index.md) - Documentación completa de la API de Python
- [Pasarela](gateway/api-reference.md) - API gRPC de la pasarela
