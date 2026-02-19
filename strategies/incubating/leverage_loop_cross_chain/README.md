# Leverage Loop Cross-Chain Strategy

> **NORTH STAR TEST CASE** for Multi-Chain PRD Implementation

This strategy is the canonical validation test for the entire multi-chain system. Every design decision in the multi-chain PRD should be evaluated against: *"Does this enable the Leverage Loop strategy to work elegantly?"*

## Overview

Execute a 5-step DeFi leverage loop across two chains in a single `decide()` call:

```
BASE CHAIN                              ARBITRUM CHAIN
──────────                              ──────────────

┌──────────────┐                        ┌──────────────┐
│ 1. SWAP      │                        │ 3. SUPPLY    │
│ USDC → WETH  │                        │ WETH → Aave  │
│ (Uniswap V3) │                        │ (Aave V3)    │
└──────┬───────┘                        └──────┬───────┘
       │                                       │
       │     ┌──────────────┐                  ▼
       └────►│ 2. BRIDGE    │           ┌──────────────┐
             │ WETH Base→Arb│──────────►│ 4. BORROW    │
             │ (Across)     │           │ USDC         │
             └──────────────┘           │ (Aave V3)    │
                                        └──────┬───────┘
                                               │
                                               ▼
                                        ┌──────────────┐
                                        │ 5. PERP_OPEN │
                                        │ ETH Long     │
                                        │ (GMX V2)     │
                                        └──────────────┘
```

## What This Tests

| Component | How It's Tested |
|-----------|-----------------|
| Multi-chain config | Base + Arbitrum in single strategy |
| Multi-protocol | Uniswap V3, Aave V3, GMX V2 |
| Cross-chain bridge | Base → Arbitrum asset transfer |
| Sequential dependencies | Each step depends on previous step's output |
| `amount="all"` pattern | Chained outputs flow through steps |
| Risk guards | Aave health factor, GMX leverage limits |
| State management | Positions tracked across both chains |
| Remediation | Recovery if bridge succeeds but later step fails |

## Configuration

```python
@dataclass
class LeverageLoopConfig(HotReloadableConfig):
    # Entry conditions
    min_usdc_to_start: Decimal = Decimal("1000")
    min_health_factor: Decimal = Decimal("1.5")

    # Position sizing
    swap_amount_usd: Decimal = Decimal("1000")
    borrow_amount_usd: Decimal = Decimal("500")
    perp_size_usd: Decimal = Decimal("1000")

    # Risk parameters
    max_leverage: Decimal = Decimal("3.0")
    max_slippage_swap: Decimal = Decimal("0.005")
    max_slippage_bridge: Decimal = Decimal("0.01")

    # Protocol preferences
    preferred_bridge: Optional[str] = None
    interest_rate_mode: str = "variable"
```

## Usage

### 1. Environment Setup

```bash
# Multi-chain mode
export ALMANAK_CHAINS=base,arbitrum

# Per-chain RPC URLs
export ALMANAK_BASE_RPC_URL=https://base-mainnet.g.alchemy.com/v2/xxx
export ALMANAK_ARBITRUM_RPC_URL=https://arb-mainnet.g.alchemy.com/v2/xxx

# Wallet
export ALMANAK_PRIVATE_KEY=0x...
```

### 2. Run Strategy

```bash
# Dry run (no execution)
python -m src.cli.run --strategy leverage_loop_cross_chain --dry-run

# Run on Anvil forks
docker-compose -f docker-compose.multichain.yaml up -d
python -m src.cli.run --strategy leverage_loop_cross_chain

# Run with custom config
python -m src.cli.run --strategy leverage_loop_cross_chain \
    --config strategies/leverage_loop_cross_chain/config.yaml
```

### 3. Multi-Anvil Testing

```yaml
# docker-compose.multichain.yaml
services:
  anvil-base:
    image: ghcr.io/foundry-rs/foundry:latest
    command: anvil --fork-url ${BASE_RPC_URL} --port 8545
    ports: ["8545:8545"]

  anvil-arbitrum:
    image: ghcr.io/foundry-rs/foundry:latest
    command: anvil --fork-url ${ARBITRUM_RPC_URL} --port 8545
    ports: ["8546:8545"]
```

### 4. Run Integration Tests

```bash
# Run the leverage loop strategy tests
pytest tests/integration/multichain/test_leverage_loop.py -v

# Run all multi-chain tests
pytest tests/integration/multichain/ -v
```

## Remediation Scenarios

| Failure Point | What Happens | Remediation |
|--------------|--------------|-------------|
| Step 1 (Swap) fails | No action taken | Auto-retry next iteration |
| Step 2 (Bridge) fails before deposit | No action taken | Auto-retry |
| Step 2 (Bridge) stuck in transit | WETH left source, not credited | Wait or contact bridge support |
| Step 3 (Supply) fails | WETH on Arbitrum, not supplied | Retry supply or hold |
| Step 4 (Borrow) fails | WETH supplied, no borrow | Retry borrow or withdraw |
| Step 5 (Perp) fails | USDC borrowed, no perp | Retry perp or repay and unwind |

## Implementation Status

- [x] Multi-chain config (`chains=["base", "arbitrum"]`)
- [x] `Intent.swap()` with `chain` parameter
- [x] `Intent.bridge()` (new intent type)
- [x] `amount="all"` chained output pattern
- [x] `Intent.supply()` with `chain`, `protocol` parameters
- [x] `Intent.borrow()` with `interest_rate_mode` parameter
- [x] `Intent.perp_open()` with `leverage` parameter
- [x] `market.balance(token, chain=)` API
- [x] `market.aave_health_factor(chain=)` API
- [x] Multi-anvil test setup
- [x] Integration tests for strategy
- [ ] Dashboard multi-chain view (see US-027)

## Acceptance Criteria (US-026)

| Criterion | Status |
|-----------|--------|
| Strategy compiles with `chains=['base', 'arbitrum']` | ✅ |
| `protocols={'base': ['uniswap_v3'], 'arbitrum': ['aave_v3', 'gmx_v2']}` | ✅ |
| `Intent.swap()` with `chain='base'`, `protocol='uniswap_v3'` | ✅ |
| `Intent.bridge()` with `from_chain='base'`, `to_chain='arbitrum'` | ✅ |
| `Intent.supply()` with `chain='arbitrum'`, `protocol='aave_v3'` | ✅ |
| `Intent.borrow()` with `chain='arbitrum'`, `protocol='aave_v3'`, `interest_rate_mode='variable'` | ✅ |
| `Intent.perp_open()` with `chain='arbitrum'`, `protocol='gmx_v2'`, `leverage=Decimal('2.0')` | ✅ |
| `amount='all'` flows output of step N to input of step N+1 | ✅ |
| `market.balance('USDC', chain='base')` returns Base-specific balance | ✅ |
| `market.aave_health_factor(chain='arbitrum')` returns Aave health factor | ✅ |
| Bridge delays handled gracefully by state machine | ✅ (infrastructure ready) |
| If borrow fails, WETH remains supplied on Aave (not lost) | ✅ (design verified) |
| P&L includes bridge fees, Aave interest, GMX funding rates | ✅ (tracked via protocol metrics) |
| Typecheck passes | ✅ |
| Integration test passes on multi-anvil fork | ✅ |

## Related Documentation

- [Multi-Chain PRD](../../tasks/prd-multi-chain-strategy-support.md) - Full PRD with all user stories and requirements
- [NORTH STAR Section](../../tasks/prd-multi-chain-strategy-support.md#north-star-the-leverage-loop-cross-chain-strategy) - This strategy as the validation test
- [Multi-Chain Orchestrator Tests](../../tests/integration/multichain/test_multichain_orchestrator.py) - Unit tests for multi-chain execution

---

**Note**: This strategy is intentionally simple in its trading logic. In production, you would add:
- Entry/exit signals based on market conditions
- Position rebalancing logic
- Automatic deleveraging when health factor drops
- Profit taking and stop loss mechanisms

The purpose here is to validate the multi-chain *infrastructure*, not to be a profitable trading strategy.
