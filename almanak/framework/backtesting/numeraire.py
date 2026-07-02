"""Numeraire reporting projection for backtesting (VIB-5127).

A strategy declares the asset its performance is measured in via
``@almanak_strategy(quote_asset=...)`` -> :class:`almanak.core.models.quote_asset.QuoteAsset`
(``fiat_usd`` by default, or a ``token`` identified by ``(chain_id, address)``).
Both backtest engines compute and conserve value **in USD** internally; this
module is the single reporting layer that projects the already-computed USD
equity curve into the declared numeraire:

    value_in_numeraire(t) = value_usd(t) / numeraire_token_usd_price(t)

Because the projection divides the *aggregate* USD portfolio value by one
scalar per timestamp, it is exact for any position mix (spot / LP / lending /
perp): division distributes over the cash + spot + positions sum. The numeraire
view therefore inherits the USD layer's accuracy exactly — no feed
re-derivation, no per-position math.

Design invariants:

* ``fiat_usd`` (the default) resolves to ``None`` here and every caller treats
  ``None`` as "USD path, emit nothing additive" — a USD strategy's result is
  byte-for-byte identical to pre-VIB-5127.
* The USD conservation core (``SimulatedPortfolio.apply_fill`` /
  ``value_usd``) is never touched; ``value_usd`` always stays USD.
* The numeraire token must be priceable across the whole window. A missing /
  non-positive price is a hard failure (``ValueError``), not a silent zero —
  there is no honest fallback for "value the portfolio in WETH but WETH price
  is unknown".

Each engine computes its numeraire metrics with **its own** equity-derived
helpers (the PnL engine annualizes daily with a risk-free rate; the paper
trader annualizes hourly with none), so that within a single result the USD
and numeraire metrics share one convention and are directly comparable.
"""

from __future__ import annotations

from bisect import bisect_right
from datetime import datetime
from decimal import Decimal
from typing import TYPE_CHECKING

from almanak.core.chains import ChainRegistry
from almanak.core.chains._helpers import chain_name_for_id
from almanak.core.models.quote_asset import QuoteAsset
from almanak.framework.backtesting.models import NumeraireMetrics
from almanak.framework.backtesting.paper.token_registry import is_token_known, resolve_to_canonical_symbol

if TYPE_CHECKING:
    from collections.abc import Callable, Sequence

    from almanak.framework.backtesting.models import BacktestMetrics, EquityPoint, TradeRecord


def resolve_numeraire_symbol(strategy: object, chain: str) -> str | None:
    """Resolve ``strategy.quote_asset`` to an UPPERCASE numeraire token symbol.

    Returns ``None`` for the USD default (``fiat_usd`` quote asset, or a strategy
    that exposes no ``quote_asset`` at all) — callers treat ``None`` as the
    unchanged USD path. For a token quote asset, validates that the token lives
    on the backtest ``chain`` and returns its canonical symbol (the same
    UPPERCASE key used in ``MarketState.prices``).

    Args:
        strategy: The strategy under test. ``quote_asset`` is read defensively
            via ``getattr`` because the backtest strategy contract
            (``BacktestableStrategy``) only requires ``deployment_id`` +
            ``decide``; the production ``IntentStrategy`` exposes ``quote_asset``.
        chain: The backtest chain name (e.g. ``"arbitrum"``), as carried by the
            engine config.

    Raises:
        ValueError: if the quote-asset token is on a different chain than the
            backtest (a hard config error — the numeraire token must be priced
            on the same chain the backtest values positions on).
    """
    quote_asset = QuoteAsset.parse(getattr(strategy, "quote_asset", None))
    if quote_asset.is_usd:
        return None

    # Token kind: QuoteAsset.__post_init__ guarantees non-None chain_id/address
    # for the token kind (mypy can't infer that from is_usd alone).
    chain_id = quote_asset.chain_id
    address = quote_asset.address
    assert chain_id is not None and address is not None  # token-kind invariant

    # The numeraire token must live on the backtest chain. Compare by numeric
    # chain_id (resolving the config chain name through the registry) so aliases
    # / casing on the config side never cause a false mismatch.
    backtest_descriptor = ChainRegistry.try_resolve(chain)
    backtest_chain_id = backtest_descriptor.chain_id if backtest_descriptor is not None else None
    if backtest_chain_id != chain_id:
        qa_chain_name = chain_name_for_id(chain_id) or "unknown"
        raise ValueError(
            f"Strategy quote_asset is a token on chain_id={chain_id} "
            f"({qa_chain_name}) but the backtest runs on chain={chain!r} "
            f"(chain_id={backtest_chain_id}). The numeraire token must live on the "
            "backtest chain so its USD price comes from the same feeds that value "
            "the portfolio."
        )

    # Normalize known symbols to UPPERCASE -- the canonical key space for every
    # price lookup (MarketState.prices, the paper price caches, and get_price all
    # uppercase), so a mixed-case registry symbol (e.g. "USDC.e") still resolves
    # and dedupes correctly. An unknown token resolves to its checksummed address
    # (left as-is) and stays unpriceable -> fails loud at metrics time; an unknown
    # token cannot be a numeraire.
    symbol = resolve_to_canonical_symbol(chain_id, address)
    return symbol.upper() if is_token_known(chain_id, address) else symbol


def numeraire_token_address(strategy: object, chain: str) -> tuple[str, str] | None:
    """Return ``(chain, address)`` for a token numeraire, else ``None``.

    The contract address is read straight from the strategy's declared
    :class:`QuoteAsset` (the canonical ``(chain_id, address)`` token identity),
    so the PnL engine can register it with the data provider and the numeraire
    resolves its CoinGecko coin id by contract address -- exactly like any other
    non-native ERC20 -- instead of an honest miss that fails loud at metrics
    time (VIB-5127). ``None`` for the USD default (``fiat_usd``) and for a
    strategy that declares no quote asset.

    ``chain`` is the backtest chain name. :func:`resolve_numeraire_symbol` is
    always called first and has already verified the quote-asset token lives on
    it (raising otherwise), so pairing ``chain`` with the quote-asset address
    yields the ``SYMBOL -> (chain, address)`` entry the provider's
    contract-endpoint resolver expects.

    Args:
        strategy: The strategy under test. ``quote_asset`` is read defensively
            via ``getattr`` (the backtest strategy contract does not require it).
        chain: The backtest chain name (e.g. ``"base"``), as carried by config.
    """
    quote_asset = QuoteAsset.parse(getattr(strategy, "quote_asset", None))
    if quote_asset.is_usd or quote_asset.address is None:
        return None
    return (chain, quote_asset.address)


def _project_numeraire_equity(
    equity_curve: Sequence[EquityPoint],
    numeraire_symbol: str | None,
) -> tuple[list[Decimal], list[datetime]] | None:
    """Project a USD equity curve into the numeraire unit.

    Returns ``(num_values, timestamps)`` aligned 1:1 with ``equity_curve``, or
    ``None`` when there is nothing to project (USD numeraire, or an empty curve
    — mirroring ``calculate_metrics``'s empty-curve short-circuit).

    Raises:
        ValueError: if any equity point lacks a usable numeraire price
            (``None`` or ``<= 0``). Fail-loud and unconditional: there is no
            honest way to value the portfolio in the numeraire when its price
            is unknown at some point in the window.
    """
    if numeraire_symbol is None or not equity_curve:
        return None

    num_values: list[Decimal] = []
    timestamps: list[datetime] = []
    for point in equity_curve:
        price = point.numeraire_price_usd
        if price is None or price <= Decimal("0"):
            raise ValueError(
                f"Numeraire {numeraire_symbol!r} is unpriceable at "
                f"{point.timestamp.isoformat()} (price={price!r}); cannot value the "
                "portfolio in the declared quote asset across the full backtest window. "
                "Ensure the numeraire token has price data for every tick."
            )
        num_values.append(point.value_usd / price)
        timestamps.append(point.timestamp)
    return num_values, timestamps


def compute_numeraire_metrics(
    equity_curve: Sequence[EquityPoint],
    *,
    numeraire_symbol: str | None,
    trading_days_per_year: int,
    risk_free_rate: Decimal,
) -> tuple[NumeraireMetrics | None, Decimal | None, Decimal | None]:
    """Numeraire metrics for the PnL (historical) engine — daily annualization.

    Mirrors ``pnl/metrics_calculator.calculate_metrics`` exactly (same pure
    helpers, same percentage/ratio conventions, same shared
    :func:`~almanak.framework.backtesting.pnl.metrics_calculator.compute_cagr`)
    but on the numeraire-denominated equity series, so the USD and numeraire
    metrics in one PnL result are directly comparable.

    Returns ``(numeraire_metrics, initial_capital_numeraire, final_capital_numeraire)``,
    or ``(None, None, None)`` for the USD path / empty curve.
    """
    from almanak.framework.backtesting.pnl.metrics_calculator import (
        calculate_max_drawdown,
        calculate_returns,
        calculate_sharpe_ratio,
        calculate_sortino_ratio,
        calculate_volatility,
        compute_cagr,
    )

    projected = _project_numeraire_equity(equity_curve, numeraire_symbol)
    if projected is None:
        return None, None, None
    assert numeraire_symbol is not None  # narrowed by projected is not None
    num_values, timestamps = projected

    initial = num_values[0]
    final = num_values[-1]
    total_pnl = final - initial
    total_return = (final - initial) / initial if initial > Decimal("0") else Decimal("0")

    trading_days = Decimal(str(trading_days_per_year))
    returns = calculate_returns(num_values)
    volatility = calculate_volatility(returns, trading_days)
    sharpe = calculate_sharpe_ratio(returns, volatility, risk_free_rate, trading_days)
    sortino = calculate_sortino_ratio(returns, risk_free_rate, trading_days)
    max_drawdown = calculate_max_drawdown(num_values)
    annualized = compute_cagr(total_return, timestamps)
    calmar = annualized / max_drawdown if max_drawdown > Decimal("0") else Decimal("0")

    metrics = NumeraireMetrics(
        numeraire=numeraire_symbol,
        total_pnl=total_pnl,
        total_return_pct=total_return * Decimal("100"),
        annualized_return_pct=annualized * Decimal("100"),
        sharpe_ratio=sharpe,
        sortino_ratio=sortino,
        volatility=volatility,
        max_drawdown_pct=max_drawdown,
        calmar_ratio=calmar,
    )
    return metrics, initial, final


def compute_numeraire_metrics_paper(
    equity_curve: Sequence[EquityPoint],
    *,
    numeraire_symbol: str | None,
) -> tuple[NumeraireMetrics | None, Decimal | None, Decimal | None]:
    """Numeraire metrics for the paper trader — hourly annualization, no risk-free.

    Mirrors ``paper/engine._calculate_metrics`` (paper's simplified hourly
    Sharpe / non-annualized volatility, no Sortino / Calmar / CAGR) on the
    numeraire-denominated equity series, so the USD and numeraire metrics in one
    paper result share the same convention. The unused fields stay ``0``, exactly
    as paper's USD ``BacktestMetrics`` leaves them.

    Returns ``(numeraire_metrics, initial_capital_numeraire, final_capital_numeraire)``,
    or ``(None, None, None)`` for the USD path / empty curve.
    """
    from almanak.framework.backtesting.paper.metrics_calculator import (
        calculate_max_drawdown,
        calculate_returns,
        calculate_sharpe_ratio,
        calculate_volatility,
    )

    projected = _project_numeraire_equity(equity_curve, numeraire_symbol)
    if projected is None:
        return None, None, None
    assert numeraire_symbol is not None  # narrowed by projected is not None
    num_values, _timestamps = projected

    initial = num_values[0]
    final = num_values[-1]
    total_pnl = final - initial
    total_return = (final - initial) / initial if initial > Decimal("0") else Decimal("0")

    returns = calculate_returns(num_values)
    volatility = calculate_volatility(returns)
    sharpe = calculate_sharpe_ratio(returns, volatility)
    max_drawdown = calculate_max_drawdown(num_values)

    metrics = NumeraireMetrics(
        numeraire=numeraire_symbol,
        total_pnl=total_pnl,
        total_return_pct=total_return * Decimal("100"),
        sharpe_ratio=sharpe,
        volatility=volatility,
        max_drawdown_pct=max_drawdown,
    )
    return metrics, initial, final


def _numeraire_price_lookup(
    equity_curve: Sequence[EquityPoint],
) -> Callable[[datetime], Decimal]:
    """Trade-timestamp -> numeraire USD price, backed by the equity curve.

    Trades are recorded at tick timestamps, which are exactly the equity-point
    timestamps, so lookups are exact matches in practice. Off-grid timestamps
    (defensive) resolve to the most recent earlier point; a timestamp before
    the first point resolves to the first point. Precondition (enforced by
    ``_project_numeraire_equity`` raising first): every point carries a
    positive ``numeraire_price_usd``.
    """
    timestamps = [point.timestamp for point in equity_curve]
    prices: list[Decimal] = []
    for point in equity_curve:
        # Explicit raise, not assert: an optimized run (python -O) must fail
        # just as loudly if the projection's validation was ever bypassed.
        if point.numeraire_price_usd is None:
            raise ValueError(
                f"Equity point at {point.timestamp.isoformat()} is missing numeraire_price_usd; "
                "the numeraire projection must validate the curve before the canonical merge."
            )
        prices.append(point.numeraire_price_usd)

    def lookup(timestamp: datetime) -> Decimal:
        index = bisect_right(timestamps, timestamp) - 1
        return prices[max(index, 0)]

    return lookup


def merge_numeraire_canonical(
    metrics: BacktestMetrics,
    numeraire_metrics: NumeraireMetrics,
    equity_curve: Sequence[EquityPoint],
    trades: Sequence[TradeRecord],
) -> None:
    """Fold the numeraire view into ``metrics`` as the canonical expression.

    Blueprint 31 §7 (numeraire-canonical merge): when a strategy declares a
    token quote asset there is exactly one performance story, told in the
    numeraire — every USD performance figure is derived from the numeraire
    figure, never computed independently from the USD equity series. Mutates
    ``metrics`` in place:

    * equity-derived fields (returns, sharpe, sortino, volatility, drawdown,
      calmar) <- the numeraire equity series (``numeraire_metrics``);
    * PnL amounts: native ``*_numeraire`` fields, with ``*_usd`` counterparts
      converted at the end-of-window reference price
      (``numeraire_price_usd_end``);
    * trade statistics: recomputed over per-trade numeraire PnLs (each realized
      trade's USD PnL converted at its own tick's numeraire price), through the
      same win/loss and attribution rules as the USD lane;
    * cost columns: the ``*_usd`` ledger is left untouched (dollars actually
      paid); ``*_numeraire`` convenience columns carry trade-time conversions;
    * ``performance_denomination`` names the canon; the legacy
      ``numeraire_metrics`` sub-block is NOT attached (v3 artifacts carry the
      merged fields instead).

    Engine-agnostic: both engines pass their own convention's
    ``numeraire_metrics`` (daily/riskfree for PnL, hourly for paper).
    """
    from almanak.framework.backtesting.pnl.calculators.attribution import calculate_all_attributions
    from almanak.framework.backtesting.pnl.metrics_calculator import trade_statistics_from_realized_pnls

    price_at = _numeraire_price_lookup(equity_curve)
    price_start = equity_curve[0].numeraire_price_usd
    price_end = equity_curve[-1].numeraire_price_usd
    # Explicit raise, not assert: these flow into numeraire_price_usd_start/_end
    # and every USD conversion below, and must fail loudly under python -O too.
    if price_start is None or price_end is None:
        raise ValueError(
            "Equity curve endpoints are missing numeraire_price_usd; "
            "the numeraire projection must validate the curve before the canonical merge."
        )

    def realized_net_numeraire(trade: TradeRecord) -> Decimal | None:
        if not trade.has_realized_pnl:
            return None
        return trade.realized_net_pnl() / price_at(trade.timestamp)

    realized_net_pnls = [pnl for pnl in (realized_net_numeraire(t) for t in trades) if pnl is not None]
    stats = trade_statistics_from_realized_pnls(realized_net_pnls, failed_count=metrics.failed_trades)

    # Denomination of the canonical expression.
    metrics.performance_denomination = numeraire_metrics.numeraire
    metrics.numeraire_price_usd_start = price_start
    metrics.numeraire_price_usd_end = price_end

    # Equity-derived fields <- the numeraire equity series.
    metrics.total_return_pct = numeraire_metrics.total_return_pct
    metrics.annualized_return_pct = numeraire_metrics.annualized_return_pct
    metrics.sharpe_ratio = numeraire_metrics.sharpe_ratio
    metrics.sortino_ratio = numeraire_metrics.sortino_ratio
    metrics.volatility = numeraire_metrics.volatility
    metrics.max_drawdown_pct = numeraire_metrics.max_drawdown_pct
    metrics.calmar_ratio = numeraire_metrics.calmar_ratio

    # PnL amounts: native numeraire + end-price-converted USD expression.
    # net == total for the same reason as the USD lane: the equity curve is
    # already net of execution costs.
    metrics.total_pnl_numeraire = numeraire_metrics.total_pnl
    metrics.net_pnl_numeraire = numeraire_metrics.total_pnl
    metrics.total_pnl_usd = numeraire_metrics.total_pnl * price_end
    metrics.net_pnl_usd = metrics.total_pnl_usd

    # Trade statistics over per-trade numeraire PnLs. Counts (win/loss/realized)
    # are sign-invariant under the positive-price conversion; magnitudes are
    # numeraire-native with end-price USD expressions.
    metrics.win_rate = stats.win_rate
    metrics.profit_factor = stats.profit_factor
    metrics.winning_trades = stats.winning_trades
    metrics.losing_trades = stats.losing_trades
    metrics.trades_with_realized_pnl = stats.trades_with_realized_pnl
    metrics.avg_trade_pnl_numeraire = stats.avg_trade_pnl
    metrics.largest_win_numeraire = stats.largest_win
    metrics.largest_loss_numeraire = stats.largest_loss
    metrics.avg_win_numeraire = stats.avg_win
    metrics.avg_loss_numeraire = stats.avg_loss
    metrics.avg_trade_pnl_usd = stats.avg_trade_pnl * price_end
    metrics.largest_win_usd = stats.largest_win * price_end
    metrics.largest_loss_usd = stats.largest_loss * price_end
    metrics.avg_win_usd = stats.avg_win * price_end
    metrics.avg_loss_usd = stats.avg_loss * price_end

    # Attribution maps: bucket the per-trade numeraire PnLs through the exact
    # USD-lane rules, expressed in USD at the end reference price (divide by
    # numeraire_price_usd_end to recover the native maps).
    by_protocol, by_intent, by_asset = calculate_all_attributions(
        list(trades),
        pnl_override=realized_net_numeraire,
    )
    metrics.pnl_by_protocol = {k: v * price_end for k, v in by_protocol.items()}
    metrics.pnl_by_intent_type = {k: v * price_end for k, v in by_intent.items()}
    metrics.pnl_by_asset = {k: v * price_end for k, v in by_asset.items()}

    # Realized: per-trade gross PnL converted at time-of-realization (mirrors
    # the USD lane's gross realized accumulation). Unrealized: an end-of-window
    # mark, so its honest conversion point IS the end price — the USD field is
    # unchanged by construction.
    metrics.realized_pnl_numeraire = sum(
        (t.pnl_usd / price_at(t.timestamp) for t in trades if t.has_realized_pnl and t.pnl_usd is not None),
        Decimal("0"),
    )
    metrics.unrealized_pnl_numeraire = metrics.unrealized_pnl / price_end
    metrics.realized_pnl = metrics.realized_pnl_numeraire * price_end
    metrics.unrealized_pnl = metrics.unrealized_pnl_numeraire * price_end

    # Cost columns: USD ledger untouched; numeraire columns are trade-time
    # conversions (failed fills carry zeroed costs and contribute nothing).
    metrics.total_fees_numeraire = sum((t.fee_usd / price_at(t.timestamp) for t in trades), Decimal("0"))
    metrics.total_slippage_numeraire = sum((t.slippage_usd / price_at(t.timestamp) for t in trades), Decimal("0"))
    metrics.total_gas_numeraire = sum((t.gas_cost_usd / price_at(t.timestamp) for t in trades), Decimal("0"))
    metrics.total_mev_cost_numeraire = sum(
        (t.estimated_mev_cost_usd / price_at(t.timestamp) for t in trades if t.estimated_mev_cost_usd is not None),
        Decimal("0"),
    )

    # v3 artifacts carry the merged fields; the legacy sub-block stays unset.
    metrics.numeraire_metrics = None


__all__ = [
    "resolve_numeraire_symbol",
    "numeraire_token_address",
    "compute_numeraire_metrics",
    "compute_numeraire_metrics_paper",
    "merge_numeraire_canonical",
]
