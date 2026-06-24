# Benqi Lending Lifecycle (Tutorial Demo)

A **tutorial** that walks the four canonical lending legs **once, back-to-back**
on BENQI (a Compound V2 fork on Avalanche):

```text
supply USDC -> borrow USDT -> repay USDT -> withdraw USDC
```

It exists to teach the BENQI/Compound-V2 intent vocabulary (qiToken supply,
`Comptroller.enterMarkets` collateral, ERC20 borrow/repay) and the
standalone-supply accounting contract (a first-class `SUPPLY` event + `supply:`
FIFO lot — VIB-3586). It is **not** a quant strategy: it builds no leverage,
holds no position, and defends no health factor.

> **Looking for a real BENQI quant strategy?** See the sibling demo
> **`benqi_looping`** — recursive leverage with a health-factor defense that
> holds the levered position and unwinds on HF-danger or teardown.

## Chain

avalanche

## Quick Start

```bash
almanak strat demo --name benqi_lending_lifecycle
cd benqi_lending_lifecycle
uv run almanak strat run --network anvil --interval 15
```

## Configuration

Edit `config.json` to adjust strategy parameters. See `strategy.py` for details.
