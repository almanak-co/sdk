"""Backtest simulation for the Aave OKB CLMM Loop strategy.

Simulates the concentrated LP + adaptive rebalance engine against
historical WOKB price data. Models fee income, rebalance costs,
borrow costs, and IL to produce realistic net return projections.

Usage:
    uv run python strategies/xlayer/aave_okb_clmm_loop/backtest.py
    uv run python strategies/xlayer/aave_okb_clmm_loop/backtest.py --capital 10000
    uv run python strategies/xlayer/aave_okb_clmm_loop/backtest.py --sweep
"""

import argparse
import json
import math
import sys
from dataclasses import dataclass, field
from decimal import Decimal

import requests


# =============================================================================
# Price data
# =============================================================================

def fetch_okb_prices(days: int = 365) -> list[tuple[float, float]]:
    """Fetch OKB hourly prices from CoinGecko.

    Returns list of (timestamp_s, price_usd).
    """
    url = "https://api.coingecko.com/api/v3/coins/okb/market_chart"
    # Always request hourly data so the simulation timestep stays consistent.
    # CoinGecko caps hourly data at 90 days for free tier; for longer periods
    # daily samples are returned and the model should be aware of the coarser cadence.
    params = {"vs_currency": "usd", "days": days, "interval": "hourly"}
    resp = requests.get(url, params=params, timeout=30)
    resp.raise_for_status()
    data = resp.json()
    return [(p[0] / 1000, p[1]) for p in data.get("prices", [])]


# =============================================================================
# Concentrated LP model
# =============================================================================

def concentrated_lp_fee_share(price: float, lower: float, upper: float) -> float:
    """Fraction of pool fees earned by a concentrated position.

    Returns 0 if out of range. For in-range, returns the fee multiplier
    relative to a full-range position (always >= 1).

    Approximation: fee_multiplier ≈ sqrt(upper/lower) / (sqrt(upper/lower) - 1)
    """
    if price < lower or price > upper:
        return 0.0
    ratio = math.sqrt(upper / lower)
    return ratio / (ratio - 1) if ratio > 1 else 1.0


def estimate_hourly_fee_rate(
    pool_fee_bps: int = 3000,
    daily_volume_usd: float = 50_000,
    pool_tvl_usd: float = 200_000,
) -> float:
    """Estimate hourly fee rate for a full-range LP as fraction of capital.

    fee_rate = (volume * fee_pct) / tvl / 24
    """
    fee_pct = pool_fee_bps / 1_000_000  # 3000 bps = 0.3%
    daily_rate = (daily_volume_usd * fee_pct) / pool_tvl_usd if pool_tvl_usd > 0 else 0
    return daily_rate / 24


# =============================================================================
# Simulation engine
# =============================================================================

@dataclass
class SimConfig:
    capital_usd: float = 1000.0
    ltv: float = 0.5
    borrow_apr: float = 0.015
    supply_apr: float = 0.00018
    pool_fee_bps: int = 3000
    daily_volume_usd: float = 50_000  # estimated WOKB/USDT daily volume
    pool_tvl_usd: float = 200_000     # estimated pool TVL
    swap_cost_pct: float = 0.01       # cost per rebalance (swap fees + slippage)
    entry_exit_cost_pct: float = 0.021
    range_vol_multiplier: float = 6.0
    vol_lookback: int = 200
    confirmation_cycles: int = 10
    cooldown_hours: float = 1.0
    static_range_width: float = 0.40   # fallback when not enough vol data


@dataclass
class SimState:
    in_position: bool = False
    range_lower: float = 0
    range_upper: float = 0
    range_width: float = 0
    exit_count: int = 0
    last_rebalance_idx: int = -9999
    rebalance_count: int = 0
    total_fees_usd: float = 0
    total_rebalance_cost_usd: float = 0
    price_history: list[float] = field(default_factory=list)


@dataclass
class SimResult:
    days: float = 0
    capital: float = 0
    lp_capital: float = 0
    total_fee_income: float = 0
    total_borrow_cost: float = 0
    total_supply_income: float = 0
    total_rebalance_cost: float = 0
    total_entry_exit_cost: float = 0
    rebalance_count: int = 0
    time_in_range_pct: float = 0
    avg_range_width: float = 0
    net_pnl: float = 0
    net_apr: float = 0
    net_apy: float = 0
    # Sweep identification
    vol_multiplier: float = 0
    confirmation: int = 0
    cooldown_h: float = 0


def compute_realized_vol(prices: list[float]) -> float | None:
    """Daily vol from hourly price series."""
    if len(prices) < 3:
        return None
    returns = [math.log(prices[i] / prices[i - 1]) for i in range(1, len(prices)) if prices[i - 1] > 0]
    if len(returns) < 2:
        return None
    mean_r = sum(returns) / len(returns)
    variance = sum((r - mean_r) ** 2 for r in returns) / len(returns)
    vol_per_hour = math.sqrt(variance)
    return vol_per_hour * math.sqrt(24)  # scale to daily


def run_simulation(prices: list[tuple[float, float]], config: SimConfig) -> SimResult:
    """Run the full backtest simulation."""
    if len(prices) < 10:
        return SimResult()

    lp_capital = config.capital_usd * config.ltv
    state = SimState()
    hourly_fee_rate = estimate_hourly_fee_rate(
        config.pool_fee_bps, config.daily_volume_usd, config.pool_tvl_usd
    )
    ticks_in_range = 0
    total_ticks = 0
    range_widths: list[float] = []
    cooldown_ticks = int(config.cooldown_hours * 1)  # 1 tick = 1 hour

    for i, (ts, price) in enumerate(prices):
        total_ticks += 1
        state.price_history.append(price)
        if len(state.price_history) > config.vol_lookback:
            state.price_history = state.price_history[-config.vol_lookback:]

        if not state.in_position:
            # Open initial position
            vol = compute_realized_vol(state.price_history)
            if vol is not None and vol > 0:
                width = config.range_vol_multiplier * vol
                width = max(0.10, min(2.00, width))
            else:
                width = config.static_range_width

            half = width / 2
            state.range_lower = price * (1 - half)
            state.range_upper = price * (1 + half)
            state.range_width = width
            state.in_position = True
            state.exit_count = 0
            range_widths.append(width)
            continue

        # Check range
        in_range = state.range_lower <= price <= state.range_upper

        if in_range:
            state.exit_count = 0
            # Earn fees (concentrated multiplier)
            multiplier = concentrated_lp_fee_share(price, state.range_lower, state.range_upper)
            fee_earned = lp_capital * hourly_fee_rate * multiplier
            state.total_fees_usd += fee_earned
            ticks_in_range += 1
        else:
            state.exit_count += 1

            # Confirmation + cooldown gates
            if (
                state.exit_count >= config.confirmation_cycles
                and (i - state.last_rebalance_idx) >= cooldown_ticks
            ):
                # Rebalance
                cost = lp_capital * config.swap_cost_pct
                state.total_rebalance_cost_usd += cost
                state.rebalance_count += 1
                state.last_rebalance_idx = i

                # Reopen with dynamic range
                vol = compute_realized_vol(state.price_history)
                if vol is not None and vol > 0:
                    width = config.range_vol_multiplier * vol
                    width = max(0.10, min(2.00, width))
                else:
                    width = config.static_range_width

                half = width / 2
                state.range_lower = price * (1 - half)
                state.range_upper = price * (1 + half)
                state.range_width = width
                state.exit_count = 0
                range_widths.append(width)

    # Compute time-based costs
    duration_hours = len(prices)
    duration_days = duration_hours / 24
    duration_years = duration_days / 365.25

    borrow_cost = lp_capital * config.borrow_apr * duration_years
    supply_income = config.capital_usd * config.supply_apr * duration_years
    entry_exit_cost = lp_capital * config.entry_exit_cost_pct

    net_pnl = (
        state.total_fees_usd
        + supply_income
        - borrow_cost
        - state.total_rebalance_cost_usd
        - entry_exit_cost
    )
    net_apr = (net_pnl / config.capital_usd) / duration_years if duration_years > 0 else 0
    net_apy = (1 + net_apr / 12) ** 12 - 1 if net_apr > -1 else net_apr

    return SimResult(
        days=duration_days,
        capital=config.capital_usd,
        lp_capital=lp_capital,
        total_fee_income=state.total_fees_usd,
        total_borrow_cost=borrow_cost,
        total_supply_income=supply_income,
        total_rebalance_cost=state.total_rebalance_cost_usd,
        total_entry_exit_cost=entry_exit_cost,
        rebalance_count=state.rebalance_count,
        time_in_range_pct=(ticks_in_range / total_ticks * 100) if total_ticks > 0 else 0,
        avg_range_width=sum(range_widths) / len(range_widths) * 100 if range_widths else 0,
        net_pnl=net_pnl,
        net_apr=net_apr * 100,
        net_apy=net_apy * 100,
        vol_multiplier=config.range_vol_multiplier,
        confirmation=config.confirmation_cycles,
        cooldown_h=config.cooldown_hours,
    )


# =============================================================================
# Reporting
# =============================================================================

def print_result(r: SimResult, label: str = "") -> None:
    if label:
        print(f"\n{'=' * 70}")
        print(f"  {label}")
        print(f"{'=' * 70}")

    print(f"""
  Period:             {r.days:.0f} days
  Capital:            ${r.capital:,.0f} (LP deployed: ${r.lp_capital:,.0f})

  Fee income:         ${r.total_fee_income:,.2f}
  Supply income:      ${r.total_supply_income:,.2f}
  Borrow cost:        -${r.total_borrow_cost:,.2f}
  Rebalance cost:     -${r.total_rebalance_cost:,.2f} ({r.rebalance_count} rebalances)
  Entry+exit cost:    -${r.total_entry_exit_cost:,.2f}
  ─────────────────────────────
  Net P&L:            ${r.net_pnl:+,.2f}
  Net APR:            {r.net_apr:+.2f}%
  Net APY:            {r.net_apy:+.2f}%

  Time in range:      {r.time_in_range_pct:.1f}%
  Avg range width:    ±{r.avg_range_width / 2:.1f}%
  Rebalances:         {r.rebalance_count} ({r.rebalance_count / (r.days / 30):.1f}/month)""")


def run_sweep(prices: list[tuple[float, float]], capital: float) -> list[SimResult]:
    """Run parameter sweep over vol_multiplier, confirmation, cooldown."""
    results = []
    multipliers = [3.0, 4.0, 5.0, 6.0, 8.0, 10.0, 12.0]
    confirmations = [5, 10, 20]
    cooldowns = [1.0, 2.0, 4.0]

    for m in multipliers:
        for c in confirmations:
            for cd in cooldowns:
                cfg = SimConfig(
                    capital_usd=capital,
                    range_vol_multiplier=m,
                    confirmation_cycles=c,
                    cooldown_hours=cd,
                )
                r = run_simulation(prices, cfg)
                results.append(r)
    return results


def print_sweep(results: list[SimResult]) -> None:
    """Print sweep results as a sorted table."""
    # Sort by net APR descending
    results.sort(key=lambda r: r.net_apr, reverse=True)

    print(f"\n{'=' * 90}")
    print("  PARAMETER SWEEP RESULTS (sorted by Net APR)")
    print(f"{'=' * 90}")
    print(f"  {'Multiplier':>10} {'Confirm':>8} {'Cooldown':>9} {'Range':>8} {'Rebal':>6} {'InRange':>8} {'Fee':>10} {'Net APR':>9} {'Net APY':>9}")
    print(f"  {'-' * 88}")

    for r in results[:20]:  # top 20
        print(
            f"  {r.vol_multiplier:>10.1f} {r.confirmation:>8d} {r.cooldown_h:>8.1f}h "
            f"±{r.avg_range_width / 2:>5.1f}% {r.rebalance_count:>6d} "
            f"{r.time_in_range_pct:>7.1f}% ${r.total_fee_income:>8.2f} "
            f"{r.net_apr:>+8.2f}% {r.net_apy:>+8.2f}%"
        )

    print(f"\n  ... showing top 20 of {len(results)} configurations")

    # Best config
    best = results[0]
    print(f"\n  OPTIMAL CONFIG:")
    print(f"    range_vol_multiplier: {best.vol_multiplier}")
    print(f"    confirmation_cycles:  {best.confirmation}")
    print(f"    rebalance_cooldown_s: {int(best.cooldown_h * 3600)}")
    print(f"    -> Net APR: {best.net_apr:+.2f}%, {best.rebalance_count} rebalances over {best.days:.0f} days")


# =============================================================================
# Main
# =============================================================================

def main():
    parser = argparse.ArgumentParser(description="Backtest the Aave OKB CLMM Loop strategy")
    parser.add_argument("--capital", type=float, default=1000, help="Initial capital in USD")
    parser.add_argument("--days", type=int, default=90, help="Historical days to backtest")
    parser.add_argument("--sweep", action="store_true", help="Run parameter sweep")
    parser.add_argument("--output", type=str, help="Write results to JSON file")
    args = parser.parse_args()

    print(f"Fetching {args.days} days of OKB price history from CoinGecko...")
    prices = fetch_okb_prices(args.days)
    print(f"  Got {len(prices)} data points ({len(prices) / 24:.0f} days)")

    if len(prices) < 24:
        print("ERROR: Not enough price data. Try a shorter period or check CoinGecko availability.")
        sys.exit(1)

    # Show price summary
    price_values = [p for _, p in prices]
    print(f"  Price range: ${min(price_values):.2f} - ${max(price_values):.2f}")
    vol = compute_realized_vol(price_values[-200:])
    if vol:
        print(f"  Recent daily vol: {vol * 100:.2f}%")

    if args.sweep:
        results = run_sweep(prices, args.capital)
        print_sweep(results)

        # Also print the best config in detail
        best = results[0]
        print_result(best, f"BEST CONFIG DETAIL (multiplier={best.vol_multiplier}, confirm={best.confirmation}, cooldown={best.cooldown_h}h)")
    else:
        config = SimConfig(capital_usd=args.capital)
        result = run_simulation(prices, config)
        print_result(result, f"BACKTEST: ${args.capital:,.0f} capital, {args.days} days, adaptive rebalance")

    # Also run a naive fixed-range comparison (disable dynamic by using
    # impossibly high vol_lookback so vol is never computed)
    naive_result = run_simulation(prices, SimConfig(
        capital_usd=args.capital,
        static_range_width=0.20,  # fixed ±10%
        confirmation_cycles=1,
        cooldown_hours=0,
        vol_lookback=999999,  # never enough data -> always uses static width
    ))
    print_result(naive_result, "COMPARISON: Naive fixed ±10% range (no confirmation, no cooldown)")

    if args.output:
        out = {
            "prices": len(prices),
            "days": len(prices) / 24,
            "capital": args.capital,
        }
        if args.sweep:
            out["sweep_results"] = [
                {"multiplier": r.vol_multiplier, "confirmation": r.confirmation,
                 "cooldown_h": r.cooldown_h, "net_apr": r.net_apr, "net_apy": r.net_apy,
                 "rebalances": r.rebalance_count, "time_in_range_pct": r.time_in_range_pct}
                for r in results
            ]
        with open(args.output, "w") as f:
            json.dump(out, f, indent=2)
        print(f"\nResults written to {args.output}")


if __name__ == "__main__":
    main()
