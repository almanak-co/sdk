---
name: almanak-strategy-builder
description: Build, test, and deploy DeFi strategies using the Almanak SDK. Use when writing IntentStrategy classes, composing Intent objects (Swap, LP, Borrow, Supply, Perp, Bridge, Stake, Vault, Prediction), backtesting with PnL or paper trading, or running strategies via the almanak CLI.
metadata:
  version: "2.0.0"
  author: Almanak
  license: Apache-2.0
---

# Almanak Strategy Builder

You are helping a quant build DeFi strategies using the Almanak SDK.
Strategies are Python classes that return Intent objects. The framework handles
compilation to transactions, execution, and state management.

<!-- almanak-sdk-start: quick-start -->

## Quick Start

```bash
# Install
pip install almanak

# Scaffold a new strategy
almanak strat new --template mean_reversion --name my_rsi --chain arbitrum

# Run on local Anvil fork (auto-starts gateway + Anvil)
cd my_rsi
almanak strat run --network anvil --once

# Run a single iteration on mainnet
almanak strat run --once

# Browse and copy a working demo strategy
almanak strat demo
```

**Minimal strategy (3 files):**

```
my_strategy/
  strategy.py    # IntentStrategy subclass with decide() method
  config.json    # Runtime parameters (chain, tokens, thresholds)
  .env           # Secrets (ALMANAK_PRIVATE_KEY, ALCHEMY_API_KEY)
```

```python
# strategy.py
from decimal import Decimal
from almanak.framework.strategies import IntentStrategy, MarketSnapshot, almanak_strategy
from almanak.framework.intents import Intent

@almanak_strategy(
    name="my_strategy",
    version="1.0.0",
    supported_chains=["arbitrum"],
    supported_protocols=["uniswap_v3"],
    intent_types=["SWAP", "HOLD"],
)
class MyStrategy(IntentStrategy):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.trade_size = Decimal(str(self.config.get("trade_size_usd", "100")))

    def decide(self, market: MarketSnapshot) -> Intent | None:
        rsi = market.rsi("WETH", period=14)
        if rsi.value < 30:
            return Intent.swap(
                from_token="USDC", to_token="WETH",
                amount_usd=self.trade_size, max_slippage=Decimal("0.005"),
            )
        return Intent.hold(reason=f"RSI={rsi.value:.1f}, waiting")
```

<!-- almanak-sdk-end: quick-start -->

<!-- almanak-sdk-start: core-concepts -->

## Core Concepts

### IntentStrategy

All strategies inherit from `IntentStrategy` and implement one method:

```python
def decide(self, market: MarketSnapshot) -> Intent | None
```

The framework calls `decide()` on each iteration with a fresh `MarketSnapshot`.
Return an `Intent` object (swap, LP, borrow, etc.) or `Intent.hold()`.

### Lifecycle

1. `__init__`: Extract config parameters, set up state
2. `decide(market)`: Called each iteration - return an Intent
3. `on_intent_executed(intent, success, result)`: Optional callback after execution
4. `get_status()`: Optional - return dict for monitoring dashboards
5. `supports_teardown()` / `generate_teardown_intents()`: Optional safe shutdown

### @almanak_strategy Decorator

Attaches metadata used by the framework and CLI:

```python
@almanak_strategy(
    name="my_strategy",              # Unique identifier
    description="What it does",      # Human-readable description
    version="1.0.0",                 # Strategy version
    author="Your Name",              # Optional
    tags=["trading", "rsi"],         # Optional tags for discovery
    supported_chains=["arbitrum"],   # Which chains this runs on
    supported_protocols=["uniswap_v3"],  # Which protocols it uses
    intent_types=["SWAP", "HOLD"],   # Intent types it may return
)
```

### Config Access

In `__init__`, read parameters from `self.config` (dict loaded from config.json):

```python
def __init__(self, *args, **kwargs):
    super().__init__(*args, **kwargs)
    self.trade_size = Decimal(str(self.config.get("trade_size_usd", "100")))
    self.rsi_period = int(self.config.get("rsi_period", 14))
    self.base_token = self.config.get("base_token", "WETH")
```

Also available: `self.chain` (str), `self.wallet_address` (str).

<!-- almanak-sdk-end: core-concepts -->

<!-- almanak-sdk-start: intent-vocabulary -->

## Intent Reference

All intents are created via `Intent` factory methods. Import:

```python
from almanak.framework.intents import Intent
```

### Trading

**Intent.swap** - Exchange tokens on a DEX

```python
Intent.swap(
    from_token="USDC",           # Token to sell
    to_token="WETH",             # Token to buy
    amount_usd=Decimal("1000"),  # Amount in USD (use amount_usd OR amount)
    amount=Decimal("500"),       # Amount in token units (alternative to amount_usd)
    max_slippage=Decimal("0.005"),  # Max slippage (0.5%)
    protocol="uniswap_v3",      # Optional: specific DEX
    chain="arbitrum",            # Optional: override chain
    destination_chain="base",    # Optional: cross-chain swap
)
```

Use `amount="all"` to swap the entire balance.

### Liquidity Provision

**Intent.lp_open** - Open a concentrated LP position

```python
Intent.lp_open(
    pool="WETH/USDC",               # Pool identifier
    amount0=Decimal("1.0"),          # Amount of token0
    amount1=Decimal("2000"),         # Amount of token1
    range_lower=Decimal("1800"),     # Lower price bound
    range_upper=Decimal("2200"),     # Upper price bound
    protocol="uniswap_v3",          # Default: uniswap_v3
    chain=None,                      # Optional override
)
```

**Intent.lp_close** - Close an LP position

```python
Intent.lp_close(
    position_id="12345",     # NFT token ID from lp_open result
    pool="WETH/USDC",        # Optional pool identifier
    collect_fees=True,       # Collect accumulated fees
    protocol="uniswap_v3",
)
```

**Intent.collect_fees** - Harvest LP fees without closing

```python
Intent.collect_fees(
    pool="WETH/USDC",
    protocol="traderjoe_v2",
)
```

### Lending / Borrowing

**Intent.supply** - Deposit collateral into a lending protocol

```python
Intent.supply(
    protocol="aave_v3",
    token="WETH",
    amount=Decimal("10"),
    use_as_collateral=True,   # Enable as collateral (default: True)
    market_id=None,           # Required for Morpho Blue
)
```

**Intent.borrow** - Borrow tokens against collateral

```python
Intent.borrow(
    protocol="aave_v3",
    collateral_token="WETH",
    collateral_amount=Decimal("10"),
    borrow_token="USDC",
    borrow_amount=Decimal("5000"),
    interest_rate_mode="variable",  # Aave: "variable" or "stable"
    market_id=None,                 # Required for Morpho Blue
)
```

**Intent.repay** - Repay borrowed tokens

```python
Intent.repay(
    protocol="aave_v3",
    token="USDC",
    amount=Decimal("5000"),
    repay_full=False,        # Set True to repay entire debt
    market_id=None,
)
```

**Intent.withdraw** - Withdraw from lending protocol

```python
Intent.withdraw(
    protocol="aave_v3",
    token="WETH",
    amount=Decimal("10"),
    withdraw_all=False,      # Set True to withdraw everything
    market_id=None,
)
```

### Perpetuals

**Intent.perp_open** - Open a perpetual futures position

```python
Intent.perp_open(
    market="ETH-USD",
    collateral_token="USDC",
    collateral_amount=Decimal("1000"),
    size_usd=Decimal("5000"),
    is_long=True,
    leverage=Decimal("5"),
    max_slippage=Decimal("0.01"),
    protocol="gmx_v2",
)
```

**Intent.perp_close** - Close a perpetual futures position

```python
Intent.perp_close(
    market="ETH-USD",
    collateral_token="USDC",
    is_long=True,
    size_usd=None,               # None = close full position
    max_slippage=Decimal("0.01"),
    protocol="gmx_v2",
)
```

### Bridging

**Intent.bridge** - Cross-chain token transfer

```python
Intent.bridge(
    token="USDC",
    amount=Decimal("1000"),
    from_chain="arbitrum",
    to_chain="base",
    max_slippage=Decimal("0.005"),
    preferred_bridge=None,       # Optional: specific bridge protocol
)
```

### Staking

**Intent.stake** - Liquid staking deposit

```python
Intent.stake(
    protocol="lido",
    token_in="ETH",
    amount=Decimal("10"),
    receive_wrapped=True,    # Receive wrapped token (e.g., wstETH)
)
```

**Intent.unstake** - Withdraw from liquid staking

```python
Intent.unstake(
    protocol="lido",
    token_in="wstETH",
    amount=Decimal("10"),
)
```

### Flash Loans

**Intent.flash_loan** - Borrow and repay in a single transaction

```python
Intent.flash_loan(
    provider="aave",         # "aave", "balancer", "morpho", or "auto"
    token="USDC",
    amount=Decimal("100000"),
    callback_intents=[...],  # Intents to execute with the borrowed funds
)
```

### Vaults (ERC-4626)

**Intent.vault_deposit** - Deposit into an ERC-4626 vault

```python
Intent.vault_deposit(
    vault="0x...",           # Vault contract address
    asset_token="USDC",
    amount=Decimal("1000"),
)
```

**Intent.vault_redeem** - Redeem shares from an ERC-4626 vault

```python
Intent.vault_redeem(
    vault="0x...",
    shares_amount=Decimal("1000"),
)
```

### Prediction Markets

```python
Intent.prediction_buy(protocol="polymarket", market="...", amount_usd=Decimal("100"))
Intent.prediction_sell(protocol="polymarket", market="...", amount_shares=Decimal("50"))
Intent.prediction_redeem(protocol="polymarket", market="...")
```

### Control Flow

**Intent.hold** - Do nothing this iteration

```python
Intent.hold(reason="RSI in neutral zone")
```

**Intent.sequence** - Execute multiple intents in order

```python
Intent.sequence(
    intents=[
        Intent.swap(from_token="USDC", to_token="WETH", amount_usd=Decimal("1000")),
        Intent.supply(protocol="aave_v3", token="WETH", amount=Decimal("0.5")),
    ],
    description="Buy WETH then supply to Aave",
)
```

### Chained Amounts

Use `"all"` to reference the full output of a prior intent:

```python
Intent.sequence(intents=[
    Intent.swap(from_token="USDC", to_token="WETH", amount_usd=Decimal("1000")),
    Intent.supply(protocol="aave_v3", token="WETH", amount="all"),  # Uses swap output
])
```

<!-- almanak-sdk-end: intent-vocabulary -->

<!-- almanak-sdk-start: market-snapshot-api -->

## Market Data API

The `MarketSnapshot` passed to `decide()` provides these methods:

### Prices

```python
price = market.price("WETH")                    # Decimal, USD price
price = market.price("WETH", quote="USDC")      # Price in USDC terms
```

### Balances

```python
bal = market.balance("USDC")
bal.balance       # Decimal - token amount
bal.balance_usd   # Decimal - USD value
bal.symbol        # str
bal.address       # str - token contract address
```

`TokenBalance` supports numeric comparisons: `bal > Decimal("100")`.

### Technical Indicators

All indicators accept `token`, `period` (int), and `timeframe` (str, default `"4h"`).

```python
rsi = market.rsi("WETH", period=14, timeframe="4h")
rsi.value          # Decimal (0-100)
rsi.is_oversold    # bool (value < 30)
rsi.is_overbought  # bool (value > 70)
rsi.signal         # "BUY" | "SELL" | "HOLD"

macd = market.macd("WETH", fast_period=12, slow_period=26, signal_period=9)
macd.macd_line     # Decimal
macd.signal_line   # Decimal
macd.histogram     # Decimal
macd.is_bullish_crossover  # bool
macd.is_bearish_crossover  # bool

bb = market.bollinger_bands("WETH", period=20, std_dev=2.0)
bb.upper_band      # Decimal
bb.middle_band     # Decimal
bb.lower_band      # Decimal
bb.bandwidth        # Decimal
bb.percent_b        # Decimal (0.0 = at lower band, 1.0 = at upper band)
bb.is_squeeze       # bool

stoch = market.stochastic("WETH", k_period=14, d_period=3)
stoch.k_value       # Decimal
stoch.d_value       # Decimal
stoch.is_oversold   # bool
stoch.is_overbought # bool

atr_val = market.atr("WETH", period=14)
atr_val.value       # Decimal (absolute)
atr_val.value_percent  # Decimal (as % of price)
atr_val.is_high_volatility  # bool

sma = market.sma("WETH", period=20)
ema = market.ema("WETH", period=12)
# Both return MAData with: .value, .is_price_above, .is_price_below, .signal

adx = market.adx("WETH", period=14)
adx.adx             # Decimal (0-100)
adx.plus_di         # Decimal
adx.minus_di        # Decimal
adx.is_strong_trend # bool (adx > 25)
adx.is_uptrend      # bool

obv = market.obv("WETH")
obv.obv             # Decimal
obv.signal_line     # Decimal
obv.is_bullish      # bool

cci = market.cci("WETH", period=20)
cci.value            # Decimal
cci.is_oversold      # bool (value < -100)

ichimoku = market.ichimoku("WETH", timeframe="4h")
# IchimokuData with tenkan_sen, kijun_sen, senkou_span_a/b, chikou_span
```

### Extended Price and Balance Data

```python
# Full price data with 24h stats
pd = market.price_data("WETH")
pd.price             # Decimal - current price
pd.price_24h_ago     # Decimal
pd.change_24h_pct    # Decimal
pd.high_24h          # Decimal
pd.low_24h           # Decimal

# Quick USD balance for a token
usd_val = market.balance_usd("WETH")  # Decimal - USD value of holdings

# Total portfolio value across all tokens
total = market.total_portfolio_usd()   # Decimal

# Wallet activity (for copy trading strategies)
signals = market.wallet_activity(action_types=["SWAP", "LP_OPEN"])

# Prediction market price
pred_price = market.prediction_price("polymarket", "market_id")
```

### Context Properties

```python
market.chain            # str - current chain name
market.wallet_address   # str - wallet address
market.timestamp        # datetime - snapshot timestamp
```

<!-- almanak-sdk-end: market-snapshot-api -->

<!-- almanak-sdk-start: state-management -->

## State Management

Persist data between iterations using `self.state` (dict-like, backed by gateway):

```python
def decide(self, market):
    # Read state
    last_trade = self.state.get("last_trade_time")
    position_id = self.state.get("lp_position_id")

    # Write state (persisted automatically)
    self.state["last_trade_time"] = datetime.now().isoformat()
    self.state["consecutive_holds"] = self.state.get("consecutive_holds", 0) + 1
```

### on_intent_executed Callback

After execution, access results (position IDs, swap amounts) via the callback:

```python
def on_intent_executed(self, intent, success: bool, result):
    if success and result.position_id:
        self.state["lp_position_id"] = result.position_id
    if success and result.swap_amounts:
        self.state["last_swap"] = {
            "amount_in": str(result.swap_amounts.amount_in),
            "amount_out": str(result.swap_amounts.amount_out),
        }
```

<!-- almanak-sdk-end: state-management -->

<!-- almanak-sdk-start: configuration -->

## Configuration

### config.json

```json
{
    "strategy_id": "my_rsi_strategy",
    "chain": "arbitrum",
    "base_token": "WETH",
    "quote_token": "USDC",
    "rsi_period": 14,
    "rsi_oversold": 30,
    "rsi_overbought": 70,
    "trade_size_usd": 1000,
    "max_slippage_bps": 50,
    "anvil_funding": {
        "USDC": "10000",
        "WETH": "5"
    }
}
```

Required fields: `strategy_id`, `chain`.
All other fields are strategy-specific and accessed via `self.config.get(key, default)`.

### .env

```bash
# Required
ALMANAK_PRIVATE_KEY=0x...

# RPC access (set at least one)
ALCHEMY_API_KEY=your_key
# RPC_URL=https://...

# Optional
# ENSO_API_KEY=
# COINGECKO_API_KEY=
# ALMANAK_API_KEY=
```

### anvil_funding

When running on Anvil (`--network anvil`), the framework auto-funds the wallet
with tokens specified in `anvil_funding`. Values are in token units (not USD).

<!-- almanak-sdk-end: configuration -->

<!-- almanak-sdk-start: token-resolution -->

## Token Resolution

Use `get_token_resolver()` for all token lookups. Never hardcode addresses.

```python
from almanak.framework.data.tokens import get_token_resolver

resolver = get_token_resolver()

# Resolve by symbol
token = resolver.resolve("USDC", "arbitrum")
# -> ResolvedToken(symbol="USDC", address="0xaf88...", decimals=6, chain="arbitrum")

# Resolve by address
token = resolver.resolve("0xaf88d065e77c8cC2239327C5EDb3A432268e5831", "arbitrum")

# Convenience
decimals = resolver.get_decimals("arbitrum", "USDC")  # -> 6
address = resolver.get_address("arbitrum", "USDC")     # -> "0xaf88..."

# For DEX swaps (auto-wraps native tokens: ETH->WETH, MATIC->WMATIC)
token = resolver.resolve_for_swap("ETH", "arbitrum")   # -> WETH

# Resolve trading pair
usdc, weth = resolver.resolve_pair("USDC", "WETH", "arbitrum")
```

Resolution order: memory cache -> disk cache -> static registry -> gateway on-chain lookup.

Never default to 18 decimals. If the token is unknown, `TokenNotFoundError` is raised.

<!-- almanak-sdk-end: token-resolution -->

<!-- almanak-sdk-start: backtesting -->

## Backtesting

### PnL Backtest (historical prices, no on-chain execution)

```bash
almanak strat backtest pnl \
    --start 2024-01-01 --end 2024-06-01 \
    --initial-capital 10000
```

### Paper Trading (Anvil fork with real execution, PnL tracking)

```bash
almanak strat backtest paper \
    --duration 3600 --interval 60 \
    --initial-capital 10000
```

Paper trading runs the full strategy loop on an Anvil fork with real transaction
execution, equity curve tracking, and JSON result logs.

### Parameter Sweep

```bash
almanak strat backtest sweep \
    --start 2024-01-01 --end 2024-06-01 \
    --param "rsi_oversold:20,25,30" \
    --param "rsi_overbought:70,75,80"
```

Runs the PnL backtest across all parameter combinations and ranks by Sharpe ratio.

### Programmatic Backtesting

```python
from almanak.framework.backtesting import BacktestEngine

engine = BacktestEngine(
    strategy_class=MyStrategy,
    config={...},
    start_date="2024-01-01",
    end_date="2024-06-01",
    initial_capital=10000,
)
results = engine.run()
results.sharpe_ratio
results.max_drawdown
results.total_return
results.plot()  # Matplotlib equity curve
```

<!-- almanak-sdk-end: backtesting -->

<!-- almanak-sdk-start: cli-commands -->

## CLI Commands

### Strategy Management

```bash
almanak strat new                     # Interactive strategy scaffolding
almanak strat new -t mean_reversion -n my_rsi -c arbitrum  # Non-interactive
almanak strat demo                    # Browse and copy a working demo strategy
```

**Templates:** `blank`, `dynamic_lp`, `mean_reversion`, `bollinger`, `basis_trade`, `lending_loop`, `copy_trader`

### Running Strategies

```bash
almanak strat run --once              # Single iteration (from strategy dir)
almanak strat run -d path/to/strat --once  # Explicit directory
almanak strat run --network anvil --once   # Local Anvil fork
almanak strat run --interval 30       # Continuous (30s between iterations)
almanak strat run --dry-run --once    # No transactions submitted
almanak strat run --fresh --once      # Clear state before running
almanak strat run --id abc123 --once  # Resume previous run
almanak strat run --dashboard         # Launch live monitoring dashboard
```

### Backtesting

```bash
almanak strat backtest pnl            # Historical PnL simulation
almanak strat backtest paper          # Paper trading on Anvil fork
almanak strat backtest sweep          # Parameter sweep optimization
almanak strat backtest block          # Block-based backtest (legacy)
```

### Teardown

```bash
almanak strat teardown plan           # Preview teardown intents
almanak strat teardown execute        # Execute teardown
```

### Gateway

```bash
almanak gateway                       # Start standalone gateway
almanak gateway --network anvil       # Gateway for local Anvil testing
almanak gateway --port 50052          # Custom port
```

### Agent Skill Management

```bash
almanak agent install                 # Auto-detect platforms and install
almanak agent install -p claude       # Install for specific platform
almanak agent install -p all          # Install for all 9 platforms
almanak agent update                  # Update installed skill files
almanak agent status                  # Check installation status
```

### Documentation

```bash
almanak docs path                     # Path to bundled LLM docs
almanak docs dump                     # Print full LLM docs
almanak docs agent-skill              # Path to bundled agent skill
almanak docs agent-skill --dump       # Print agent skill content
```

<!-- almanak-sdk-end: cli-commands -->

<!-- almanak-sdk-start: supported-chains -->

## Supported Chains and Protocols

### Chains

| Chain | Enum Value | Config Name |
|-------|-----------|-------------|
| Ethereum | `ETHEREUM` | `ethereum` |
| Arbitrum | `ARBITRUM` | `arbitrum` |
| Optimism | `OPTIMISM` | `optimism` |
| Base | `BASE` | `base` |
| Avalanche | `AVALANCHE` | `avalanche` |
| Polygon | `POLYGON` | `polygon` |
| BSC | `BSC` | `bsc` |
| Sonic | `SONIC` | `sonic` |
| Plasma | `PLASMA` | `plasma` |
| Blast | `BLAST` | `blast` |
| Mantle | `MANTLE` | `mantle` |
| Berachain | `BERACHAIN` | `berachain` |

<!-- almanak-sdk-end: supported-chains -->

<!-- almanak-sdk-start: supported-protocols -->

### Protocols

| Protocol | Enum Value | Type | Config Name |
|----------|-----------|------|-------------|
| Uniswap V3 | `UNISWAP_V3` | DEX / LP | `uniswap_v3` |
| PancakeSwap V3 | `PANCAKESWAP_V3` | DEX / LP | `pancakeswap_v3` |
| SushiSwap V3 | `SUSHISWAP_V3` | DEX / LP | `sushiswap_v3` |
| TraderJoe V2 | `TRADERJOE_V2` | DEX / LP | `traderjoe_v2` |
| Aerodrome | `AERODROME` | DEX / LP | `aerodrome` |
| Enso | `ENSO` | Aggregator | `enso` |
| Pendle | `PENDLE` | Yield | `pendle` |
| MetaMorpho | `METAMORPHO` | Lending | `metamorpho` |
| LiFi | `LIFI` | Bridge | `lifi` |
| Vault | `VAULT` | ERC-4626 | `vault` |

### Networks

| Network | Enum Value | Description |
|---------|-----------|-------------|
| Mainnet | `MAINNET` | Production chains |
| Anvil | `ANVIL` | Local fork for testing |
| Sepolia | `SEPOLIA` | Testnet |

<!-- almanak-sdk-end: supported-protocols -->

<!-- almanak-sdk-start: common-patterns -->

## Common Patterns

### RSI Mean Reversion (Trading)

```python
def decide(self, market):
    rsi = market.rsi(self.base_token, period=self.rsi_period)
    quote_bal = market.balance(self.quote_token)
    base_bal = market.balance(self.base_token)

    if rsi.is_oversold and quote_bal.balance_usd >= self.trade_size:
        return Intent.swap(
            from_token=self.quote_token, to_token=self.base_token,
            amount_usd=self.trade_size, max_slippage=Decimal("0.005"),
        )
    if rsi.is_overbought and base_bal.balance_usd >= self.trade_size:
        return Intent.swap(
            from_token=self.base_token, to_token=self.quote_token,
            amount_usd=self.trade_size, max_slippage=Decimal("0.005"),
        )
    return Intent.hold(reason=f"RSI={rsi.value:.1f} in neutral zone")
```

### LP Rebalancing

```python
def decide(self, market):
    price = market.price(self.base_token)
    position_id = self.state.get("lp_position_id")

    if position_id:
        # Check if price is out of range - close and reopen
        if price < self.state.get("range_lower") or price > self.state.get("range_upper"):
            return Intent.lp_close(position_id=position_id, protocol="uniswap_v3")

    # Open new position centered on current price
    atr = market.atr(self.base_token)
    half_range = price * atr.value_percent * 2
    return Intent.lp_open(
        pool="WETH/USDC",
        amount0=Decimal("1"), amount1=Decimal("2000"),
        range_lower=price - half_range,
        range_upper=price + half_range,
    )
```

### Multi-Step with IntentSequence

```python
def decide(self, market):
    return Intent.sequence(
        intents=[
            Intent.swap(from_token="USDC", to_token="WETH", amount_usd=Decimal("5000")),
            Intent.supply(protocol="aave_v3", token="WETH", amount="all"),
            Intent.borrow(
                protocol="aave_v3",
                collateral_token="WETH", collateral_amount=Decimal("0"),
                borrow_token="USDC", borrow_amount=Decimal("3000"),
            ),
        ],
        description="Leverage loop: buy WETH, supply, borrow USDC",
    )
```

### Alerting

```python
from almanak.framework.alerting import AlertManager

class MyStrategy(IntentStrategy):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.alerts = AlertManager.from_config(self.config.get("alerting", {}))

    def decide(self, market):
        rsi = market.rsi("WETH")
        if rsi.value < 20:
            self.alerts.send("Extreme oversold: RSI={:.1f}".format(rsi.value), level="warning")
        # ... trading logic
```

### Safe Teardown

Implement teardown so the strategy can cleanly exit positions:

```python
def supports_teardown(self) -> bool:
    return True

def generate_teardown_intents(self, mode, market=None) -> list[Intent]:
    intents = []
    position_id = self.state.get("lp_position_id")
    if position_id:
        intents.append(Intent.lp_close(position_id=position_id))
    # Swap all base token back to quote
    intents.append(Intent.swap(
        from_token=self.base_token, to_token=self.quote_token,
        amount="all", max_slippage=Decimal("0.03"),
    ))
    return intents
```

### Error Handling

Always wrap `decide()` in try/except and return `Intent.hold()` on error:

```python
def decide(self, market):
    try:
        # ... strategy logic
    except Exception as e:
        logger.exception(f"Error in decide(): {e}")
        return Intent.hold(reason=f"Error: {e}")
```

<!-- almanak-sdk-end: common-patterns -->

<!-- almanak-sdk-start: troubleshooting -->

## Troubleshooting

| Error | Cause | Fix |
|-------|-------|-----|
| `TokenNotFoundError` | Token symbol not in registry | Use exact symbol (e.g., "WETH" not "ETH" for swaps). Check `resolver.resolve("TOKEN", "chain")`. |
| `Gateway not available` | Gateway not running | Use `almanak strat run` (auto-starts gateway) or start manually with `almanak gateway`. |
| `ALMANAK_PRIVATE_KEY not set` | Missing .env | Add `ALMANAK_PRIVATE_KEY=0x...` to your `.env` file. |
| `Anvil not found` | Foundry not installed | Install: `curl -L https://foundry.paradigm.xyz \| bash && foundryup` |
| `RSI data unavailable` | Insufficient price history | The gateway needs time to accumulate data. Try a longer timeframe or wait. |
| `Insufficient balance` | Wallet doesn't have enough tokens | For Anvil: add `anvil_funding` to config.json. For mainnet: fund the wallet. |
| `Slippage exceeded` | Trade too large or pool illiquid | Increase `max_slippage` or reduce trade size. |
| Intent compilation fails | Wrong parameter types | Ensure amounts are `Decimal`, not `float`. Use `Decimal(str(value))`. |

### Debugging Tips

- Use `--verbose` flag for detailed logging: `almanak strat run --once --verbose`
- Use `--dry-run` to test decide() without submitting transactions
- Use `--log-file out.json` for machine-readable JSON logs
- Check strategy state: `self.state` persists between iterations
- Paper trade first: `almanak strat backtest paper` runs real execution on Anvil

<!-- almanak-sdk-end: troubleshooting -->
