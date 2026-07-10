"""VIB-5722 — canonical PortfolioValuer must value multi-chain strategies.

Before VIB-5722 the runner gated the canonical valuer off for multi-chain
strategies and every valuer-internal ``market.price``/``market.balance`` dropped
the chain, so on a real multi-chain snapshot each read raised
``AmbiguousChainError`` and valuation fell to a chain-less fallback that stamped
``total_value_usd=$0.00`` at ``HIGH`` confidence after a real ~$30 mint.

These tests exercise ``PortfolioValuer.value()`` (and its leaf helpers) against a
REAL multi-chain ``MarketSnapshot`` built via ``MarketSnapshotBuilder`` — a
CHAIN-SENSITIVE surface: ``chain=None`` raises ``AmbiguousChainError`` and an
unconfigured chain raises ``ChainNotConfiguredError`` exactly as production does.
The doubles here therefore do NOT ignore the chain arg (unlike the legacy
``tests/unit/test_valuation.py`` doubles).
"""

from __future__ import annotations

from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from almanak.framework.data.market_snapshot import AmbiguousChainError
from almanak.framework.market.builders import MarketSnapshotBuilder
from almanak.framework.portfolio.models import TokenBalance, ValueConfidence
from almanak.framework.teardown.models import TeardownPositionSummary
from almanak.framework.valuation.portfolio_valuer import PortfolioValuer

WALLET = "0x1234567890123456789012345678901234567890"


class _ChainSensitiveOracle:
    """Price oracle keyed by (chain, SYMBOL). Raises for an unknown pair.

    Signature ``(token, quote, chain)`` — three positional params — so
    ``MarketSnapshot`` recognises it as chain-aware and threads ``chain=`` in.
    """

    def __init__(self, table: dict[tuple[str, str], Decimal]):
        self._table = {(c, t.upper()): p for (c, t), p in table.items()}

    def __call__(self, token: str, quote: str = "USD", chain: str | None = None) -> Decimal:
        key = (chain or "", (token or "").upper())
        if key in self._table:
            return self._table[key]
        raise ValueError(f"no price for {token} on {chain}")


class _ChainSensitiveBalances:
    """Balance provider keyed by (chain, SYMBOL). Returns a measured zero row
    for a configured-but-empty holding; raises for a token unknown on a chain."""

    def __init__(self, table: dict[tuple[str, str], Decimal]):
        self._table = {(c, t.upper()): b for (c, t), b in table.items()}

    def __call__(self, token: str, chain: str | None = None) -> TokenBalance:
        key = (chain or "", (token or "").upper())
        if key in self._table:
            bal = self._table[key]
            return TokenBalance(symbol=token, balance=bal, value_usd=Decimal("0"), price_usd=Decimal("0"))
        raise ValueError(f"no balance for {token} on {chain}")


def _multichain_market(
    *,
    chains: tuple[str, ...] = ("arbitrum", "hyperevm"),
    prices: dict[tuple[str, str], Decimal],
    balances: dict[tuple[str, str], Decimal],
):
    strategy_stub = SimpleNamespace(chain=chains[0], wallet_address=WALLET)
    return MarketSnapshotBuilder.for_strategy_runner(
        strategy=strategy_stub,
        chain=chains[0],
        chains=chains,
        multi_chain_price_oracle=_ChainSensitiveOracle(prices),
        multi_chain_balance_provider=_ChainSensitiveBalances(balances),
        runtime_surface="unit_test",
    )


class _MultiChainStrategy:
    """Minimal StrategyLike over two chains, no discoverable positions."""

    STRATEGY_METADATA = None

    def __init__(self, chains=("arbitrum", "hyperevm"), tracked=("USDC", "WETH")):
        self.deployment_id = "deploy-mc"
        self._chains = list(chains)
        self.chain = chains[0]
        self.wallet_address = WALLET
        self._tracked = list(tracked)

    def _get_tracked_tokens(self):
        return list(self._tracked)

    def get_supported_chains(self):
        return list(self._chains)

    def get_wallet_for_chain(self, chain: str) -> str:
        return WALLET

    def get_open_positions(self):
        return TeardownPositionSummary(deployment_id=self.deployment_id, timestamp=None, positions=[])


def _valuer_no_discovery() -> PortfolioValuer:
    valuer = PortfolioValuer()
    valuer._discovery = SimpleNamespace(  # type: ignore[attr-defined]
        discover=lambda _cfg: SimpleNamespace(positions=[], errors=[], perp_protocols_ok=set())
    )
    return valuer


# ---------------------------------------------------------------------------
# Sanity: the snapshot really IS chain-sensitive (unlike the legacy doubles).
# ---------------------------------------------------------------------------


def test_snapshot_is_chain_sensitive():
    market = _multichain_market(
        prices={("arbitrum", "USDC"): Decimal("1")},
        balances={("arbitrum", "USDC"): Decimal("5")},
    )
    assert market.chains == ("arbitrum", "hyperevm")
    # chain=None on a multi-chain snapshot MUST raise (the exact failure mode the
    # legacy chain-less valuer reads hit).
    with pytest.raises(AmbiguousChainError):
        market.price("USDC")
    with pytest.raises(AmbiguousChainError):
        market.balance("USDC")
    # explicit chain resolves
    assert market.price("USDC", chain="arbitrum") == Decimal("1")


# ---------------------------------------------------------------------------
# (a) wallet tokens valued per chain
# ---------------------------------------------------------------------------


def test_wallet_valued_across_both_chains():
    market = _multichain_market(
        prices={
            ("arbitrum", "USDC"): Decimal("1"),
            ("hyperevm", "WETH"): Decimal("2000"),
            ("arbitrum", "ETH"): Decimal("2000"),
            ("hyperevm", "HYPE"): Decimal("10"),
        },
        balances={
            ("arbitrum", "USDC"): Decimal("30"),
            ("hyperevm", "WETH"): Decimal("1"),
            ("arbitrum", "ETH"): Decimal("0"),
            ("hyperevm", "HYPE"): Decimal("0"),
        },
    )
    snap = _valuer_no_discovery().value(_MultiChainStrategy(), market, iteration_number=1)

    # $30 USDC (arbitrum) + $2000 WETH (hyperevm) = $2030 available cash.
    assert snap.available_cash_usd == Decimal("2030")
    # No read failed and no unpriced held token → HIGH, NOT the $0-at-HIGH bug.
    assert snap.value_confidence == ValueConfidence.HIGH
    assert snap.total_value_usd == Decimal("0")  # no deployed positions


def test_same_symbol_sums_across_chains():
    # USDC held on BOTH chains must AGGREGATE (same asset, same USD price), not
    # collide or drop one chain's holding.
    market = _multichain_market(
        prices={
            ("arbitrum", "USDC"): Decimal("1"),
            ("hyperevm", "USDC"): Decimal("1"),
            # native gas must resolve on EVERY chain for a HIGH snapshot
            ("arbitrum", "ETH"): Decimal("2000"),
            ("hyperevm", "HYPE"): Decimal("10"),
        },
        balances={
            ("arbitrum", "USDC"): Decimal("30"),
            ("hyperevm", "USDC"): Decimal("20"),
            ("arbitrum", "ETH"): Decimal("0"),
            ("hyperevm", "HYPE"): Decimal("0"),
        },
    )
    strat = _MultiChainStrategy(tracked=("USDC",))
    snap = _valuer_no_discovery().value(strat, market, iteration_number=1)
    assert snap.available_cash_usd == Decimal("50")
    assert snap.value_confidence == ValueConfidence.HIGH


def test_token_failing_on_all_chains_degrades():
    # A tracked token whose balance read RAISES on EVERY configured chain (RPC
    # down / misconfigured) must degrade confidence — NO LESS strict than the
    # single-chain path. This is the silent-under-count class the ticket kills.
    # Native gas resolves on both chains so it is not the confounding degrade.
    market = _multichain_market(
        prices={
            ("arbitrum", "USDC"): Decimal("1"),
            ("arbitrum", "ETH"): Decimal("2000"),
            ("hyperevm", "HYPE"): Decimal("10"),
        },
        balances={
            ("arbitrum", "USDC"): Decimal("10"),
            ("arbitrum", "ETH"): Decimal("0"),
            ("hyperevm", "HYPE"): Decimal("0"),
            # GHO present on NEITHER chain → balance raises on both.
        },
    )
    strat = _MultiChainStrategy(tracked=("USDC", "GHO"))
    snap = _valuer_no_discovery().value(strat, market, iteration_number=1)
    assert snap.value_confidence == ValueConfidence.ESTIMATED


def test_token_resolving_on_one_chain_does_not_degrade():
    # Control: the SAME failure on ONE chain but a successful read on the other
    # stays benign (the token simply lives on one chain) → HIGH.
    market = _multichain_market(
        prices={
            ("arbitrum", "USDC"): Decimal("1"),
            ("hyperevm", "WETH"): Decimal("2000"),
            ("arbitrum", "ETH"): Decimal("2000"),
            ("hyperevm", "HYPE"): Decimal("10"),
        },
        balances={
            ("arbitrum", "USDC"): Decimal("10"),
            ("hyperevm", "WETH"): Decimal("1"),  # raises on arbitrum, resolves on hyperevm
            ("arbitrum", "ETH"): Decimal("0"),
            ("hyperevm", "HYPE"): Decimal("0"),
        },
    )
    strat = _MultiChainStrategy(tracked=("USDC", "WETH"))
    snap = _valuer_no_discovery().value(strat, market, iteration_number=1)
    assert snap.value_confidence == ValueConfidence.HIGH
    assert snap.available_cash_usd == Decimal("2010")


def test_held_chain_price_failure_degrades_even_with_fungible_price():
    # balance>0 on hyperevm + its OWN-chain price fails, while a zero-balance
    # arbitrum supplies a fungible price → still degrade (held-but-unpriceable-
    # on-its-own-chain). Native gas resolves on both so it is not the degrade.
    market = _multichain_market(
        prices={
            ("arbitrum", "GHO"): Decimal("1"),  # fungible price from the ZERO-balance chain
            ("arbitrum", "ETH"): Decimal("2000"),
            ("hyperevm", "HYPE"): Decimal("10"),
            # NO ("hyperevm","GHO") price → the HELD chain can't price its own holding
        },
        balances={
            ("arbitrum", "GHO"): Decimal("0"),  # zero balance, supplies the price
            ("hyperevm", "GHO"): Decimal("5"),  # POSITIVE balance, price fails here
            ("arbitrum", "ETH"): Decimal("0"),
            ("hyperevm", "HYPE"): Decimal("0"),
        },
    )
    strat = _MultiChainStrategy(tracked=("GHO",))
    snap = _valuer_no_discovery().value(strat, market, iteration_number=1)
    assert snap.value_confidence == ValueConfidence.ESTIMATED
    # The held GHO (5 @ the fungible $1) still CONTRIBUTES to NAV — degrade, don't
    # drop. (Natives resolve at zero balance → $0.) Guards against a regression
    # that would silently drop the balance instead of degrading confidence.
    assert snap.available_cash_usd == Decimal("5")


def test_held_chain_priced_on_own_chain_no_degrade():
    # Control: the held chain CAN price its own holding → no degrade.
    market = _multichain_market(
        prices={
            ("arbitrum", "GHO"): Decimal("1"),
            ("hyperevm", "GHO"): Decimal("1"),  # priced on its OWN chain now
            ("arbitrum", "ETH"): Decimal("2000"),
            ("hyperevm", "HYPE"): Decimal("10"),
        },
        balances={
            ("arbitrum", "GHO"): Decimal("0"),
            ("hyperevm", "GHO"): Decimal("5"),
            ("arbitrum", "ETH"): Decimal("0"),
            ("hyperevm", "HYPE"): Decimal("0"),
        },
    )
    strat = _MultiChainStrategy(tracked=("GHO",))
    snap = _valuer_no_discovery().value(strat, market, iteration_number=1)
    assert snap.value_confidence == ValueConfidence.HIGH


# ---------------------------------------------------------------------------
# (d) native gas folded for each chain + status aggregation
# ---------------------------------------------------------------------------


def test_native_gas_folded_for_each_chain():
    # arbitrum native = ETH, hyperevm native = HYPE — two DISTINCT native rows.
    market = _multichain_market(
        prices={
            ("arbitrum", "USDC"): Decimal("1"),
            ("arbitrum", "ETH"): Decimal("2000"),
            ("hyperevm", "HYPE"): Decimal("10"),
        },
        balances={
            ("arbitrum", "USDC"): Decimal("10"),
            ("arbitrum", "ETH"): Decimal("1"),  # $2000 gas
            ("hyperevm", "HYPE"): Decimal("5"),  # $50 gas
        },
    )
    strat = _MultiChainStrategy(tracked=("USDC",))
    snap = _valuer_no_discovery().value(strat, market, iteration_number=1)

    symbols = {tb.symbol for tb in snap.wallet_balances}
    assert "ETH" in symbols and "HYPE" in symbols
    assert snap.snapshot_metadata.get("gas_native_status") == "ok"
    # $10 USDC + $2000 ETH gas + $50 HYPE gas
    assert snap.available_cash_usd == Decimal("2060")
    assert snap.value_confidence == ValueConfidence.HIGH


def test_native_gas_read_failure_not_masked_by_tracked_overlap():
    # ETH is native on BOTH arbitrum and base and is a tracked token. The native
    # read must run per chain for STATUS: a failed base native read must NOT be
    # masked as "already_tracked" behind the successful arbitrum tracked read.
    market = _multichain_market(
        chains=("arbitrum", "base"),
        prices={("arbitrum", "ETH"): Decimal("2000")},  # base ETH price absent
        balances={("arbitrum", "ETH"): Decimal("1")},  # base ETH balance absent → read raises
    )
    strat = _MultiChainStrategy(chains=("arbitrum", "base"), tracked=("ETH",))
    snap = _valuer_no_discovery().value(strat, market, iteration_number=1)
    # The base native BALANCE read raises → exact status is "balance_failed",
    # NOT masked as "already_tracked" by the successful arbitrum tracked read.
    assert snap.snapshot_metadata.get("gas_native_status") == "balance_failed"
    assert snap.value_confidence != ValueConfidence.HIGH


def test_native_gas_status_aggregates_first_failure():
    # hyperevm native price missing → aggregate status is that failure, and the
    # snapshot degrades from HIGH (fail-closed native-gas contract, extended to
    # the secondary chain by VIB-5722).
    market = _multichain_market(
        prices={
            ("arbitrum", "USDC"): Decimal("1"),
            ("arbitrum", "ETH"): Decimal("2000"),
            # NO ("hyperevm", "HYPE") price → price_missing on the secondary chain
        },
        balances={
            ("arbitrum", "USDC"): Decimal("10"),
            ("arbitrum", "ETH"): Decimal("1"),
            ("hyperevm", "HYPE"): Decimal("5"),
        },
    )
    strat = _MultiChainStrategy(tracked=("USDC",))
    snap = _valuer_no_discovery().value(strat, market, iteration_number=1)

    assert snap.snapshot_metadata.get("gas_native_status") == "price_missing"
    assert snap.value_confidence != ValueConfidence.HIGH


# ---------------------------------------------------------------------------
# (b)/(c) reprice dispatch passes each position's OWN chain
# ---------------------------------------------------------------------------


def test_reprice_dispatch_uses_each_positions_chain(monkeypatch):
    # A position on the primary chain and one on the secondary chain must each be
    # dispatched to the repricer with its OWN chain (item 1) and must both survive
    # the merge (item 6) — cross-chain positions sharing a key never collapse.
    from almanak.framework.teardown.models import PositionInfo, PositionType

    market = _multichain_market(
        prices={("arbitrum", "USDC"): Decimal("1")},
        balances={("arbitrum", "USDC"): Decimal("1")},
    )
    p_primary = PositionInfo(
        position_type=PositionType.VAULT,
        position_id="v-arb",
        chain="arbitrum",
        protocol="somevault",
        value_usd=Decimal("100"),
        details={"vault_address": "0xaaa", "wallet": WALLET},
    )
    p_secondary = PositionInfo(
        position_type=PositionType.VAULT,
        position_id="v-hyper",
        chain="hyperevm",
        protocol="somevault",
        value_usd=Decimal("200"),
        details={"vault_address": "0xbbb", "wallet": WALLET},
    )

    valuer = _valuer_no_discovery()
    strat = _MultiChainStrategy(tracked=("USDC",))
    strat.get_open_positions = lambda: TeardownPositionSummary(  # type: ignore[assignment]
        deployment_id=strat.deployment_id, timestamp=None, positions=[p_primary, p_secondary]
    )

    seen: list[tuple[str, str]] = []

    def _spy(position, chain, mkt):
        seen.append((position.position_id, chain))
        return position.value_usd, {}, True

    monkeypatch.setattr(valuer, "_reprice_position_enriched", _spy)

    valuer.value(strat, market, iteration_number=1)

    seen_map = dict(seen)
    assert seen_map["v-arb"] == "arbitrum"
    assert seen_map["v-hyper"] == "hyperevm"


def test_blank_chain_lp_stub_seeds_only_primary_discovery():
    # A blank/None-chain LP stub must NOT seed every chain's discovery: V3 tokenIds
    # are sequential per-NPM-per-chain and discovery does no ownership check, so a
    # same-id FOREIGN NFT on a secondary chain could be repriced into NAV. A
    # blank-chain position seeds ONLY the primary chain (strategy.chain).
    from almanak.framework.teardown.models import PositionInfo, PositionType

    valuer = PortfolioValuer()
    strat = _MultiChainStrategy(chains=("arbitrum", "hyperevm"), tracked=("USDC",))
    blank_lp = PositionInfo(
        position_type=PositionType.LP,
        position_id="uniswap_v3-WETH/USDC/500-777",
        chain="",  # blank!
        protocol="uniswap_v3",
        value_usd=Decimal("0"),
        details={"position_id": "777"},
    )
    cfg_primary = valuer._build_discovery_config(strat, [blank_lp], chain_override="arbitrum")
    cfg_secondary = valuer._build_discovery_config(strat, [blank_lp], chain_override="hyperevm")
    assert cfg_primary is not None and cfg_secondary is not None
    assert 777 in cfg_primary.lp_token_ids  # primary chain seeded
    assert 777 not in cfg_secondary.lp_token_ids  # secondary chain NOT seeded


def test_perp_stub_drop_is_chain_scoped():
    # VIB-5722: a venue scanned ok on chain A must NOT drop a same-venue stub on
    # chain B (whose discovery failed) — that would silently remove a real
    # position from NAV. perp_protocols_ok now carries (chain, protocol).
    from almanak.framework.teardown.models import PositionInfo, PositionType

    valuer = PortfolioValuer()

    def _stub(chain):
        return PositionInfo(
            position_type=PositionType.PERP,
            position_id=f"gmx_v2-ETH/USD-long-{chain}",
            chain=chain,
            protocol="gmx_v2",
            value_usd=Decimal("10000"),  # gross notional
            details={"market": "ETH/USD", "is_long": True},
        )

    ok = {("arbitrum", "gmx_v2")}  # scanned ok on arbitrum ONLY

    # Control — the arbitrum stub (its chain scanned ok) is dropped.
    merged_arb = valuer._merge_position_sources([_stub("arbitrum")], [], "arbitrum", ok)
    assert [p for p in merged_arb if p.position_type == PositionType.PERP] == []

    # Fix — the avalanche stub (its chain's discovery did NOT scan) SURVIVES.
    merged_avax = valuer._merge_position_sources([_stub("avalanche")], [], "arbitrum", ok)
    survivors = [p for p in merged_avax if p.position_type == PositionType.PERP]
    assert len(survivors) == 1
    assert survivors[0].value_usd == Decimal("10000")


# ---------------------------------------------------------------------------
# Leaf-level chain-threading proof (fails when the market.price chain= is reverted)
# ---------------------------------------------------------------------------


def test_price_curve_coins_threads_chain():
    market = _multichain_market(
        prices={("arbitrum", "USDC"): Decimal("1"), ("hyperevm", "USDC"): Decimal("2")},
        balances={},
    )
    valuer = PortfolioValuer()
    # Same symbol, different chains → different prices proves the chain arg is
    # threaded into the leaf ``market.price`` read (not dropped → AmbiguousChain).
    assert valuer._price_curve_coins(["USDC"], [""], "arbitrum", market) == [Decimal("1")]
    assert valuer._price_curve_coins(["USDC"], [""], "hyperevm", market) == [Decimal("2")]


# ---------------------------------------------------------------------------
# Item 8 — the degraded fallback lane (IntentStrategy.get_portfolio_snapshot)
# ---------------------------------------------------------------------------


class TestFallbackLaneVib5722:
    def _make_strat(self, *, chain="arbitrum", tracked=("USDC",), native_status="already_tracked"):
        from almanak.framework.strategies.intent_strategy import IntentStrategy

        strat = MagicMock(spec=IntentStrategy)
        strat.deployment_id = "fb"
        strat._chain = chain
        strat.chain = chain
        summary = MagicMock()
        summary.positions = []
        summary.total_value_usd = Decimal("0")
        strat.get_open_positions.return_value = summary
        strat._get_tracked_tokens.return_value = list(tracked)
        strat._append_native_gas_to_wallet.return_value = (native_status, Decimal("0"))
        return strat

    def test_single_chain_clean_reads_stay_high(self):
        # Byte-neutral: single-chain, all reads succeed → HIGH (no wallet_reads_failed).
        from almanak.framework.strategies.intent_strategy import IntentStrategy

        market = _multichain_market(
            chains=("arbitrum",),
            prices={("arbitrum", "USDC"): Decimal("1")},
            balances={("arbitrum", "USDC"): Decimal("10")},
        )
        snap = IntentStrategy.get_portfolio_snapshot(self._make_strat(tracked=("USDC",)), market)
        assert snap.value_confidence == ValueConfidence.HIGH
        assert snap.available_cash_usd == Decimal("10")

    def test_single_chain_raised_read_degrades_to_estimated(self):
        # A wallet read that RAISES (vs a measured-zero balance) must degrade
        # confidence to ESTIMATED (VIB-5722) — no more $-partial-at-HIGH.
        from almanak.framework.strategies.intent_strategy import IntentStrategy

        # USDC has a price but NO balance in the provider table → balance() raises.
        market = _multichain_market(
            chains=("arbitrum",),
            prices={("arbitrum", "USDC"): Decimal("1")},
            balances={},
        )
        snap = IntentStrategy.get_portfolio_snapshot(self._make_strat(tracked=("USDC",)), market)
        assert snap.value_confidence == ValueConfidence.ESTIMATED

    def test_multichain_fallback_sums_wallet_not_zero(self):
        # The field bug: multi-chain fallback stamped $0 because chain-less reads
        # raised AmbiguousChainError. Now it reads per chain and sums.
        from almanak.framework.strategies.intent_strategy import IntentStrategy

        market = _multichain_market(
            prices={("arbitrum", "USDC"): Decimal("1"), ("hyperevm", "WETH"): Decimal("2000")},
            balances={("arbitrum", "USDC"): Decimal("30"), ("hyperevm", "WETH"): Decimal("1")},
        )
        snap = IntentStrategy.get_portfolio_snapshot(self._make_strat(tracked=("USDC", "WETH")), market)
        assert snap.available_cash_usd == Decimal("2030")

    def test_fallback_catches_real_chain_not_configured_error(self):
        # The fallback must catch the market.errors ChainNotConfiguredError that
        # MarketSnapshot.balance actually raises — NOT the DISTINCT .multichain
        # re-export it previously named (which never matched → fell through to the
        # generic handler and spuriously flipped wallet_reads_failed). A token
        # absent on one chain is benign, so confidence stays HIGH.
        from almanak.framework.market.errors import ChainNotConfiguredError as RealCNC
        from almanak.framework.portfolio.models import TokenBalance as TB
        from almanak.framework.strategies.intent_strategy import IntentStrategy

        class _FakeMarket:
            chains = ("arbitrum", "hyperevm")

            def balance(self, token, *, chain=None):
                if chain == "hyperevm":
                    raise RealCNC(reason="not configured", chain="hyperevm", chains=("arbitrum",))
                return TB(symbol=token, balance=Decimal("30"), value_usd=Decimal("0"), price_usd=Decimal("0"))

            def price(self, token, quote="USD", *, chain=None):
                return Decimal("1")

        snap = IntentStrategy.get_portfolio_snapshot(self._make_strat(tracked=("USDC",)), _FakeMarket())
        assert snap.value_confidence == ValueConfidence.HIGH  # CNC benign, not a read failure
        assert snap.available_cash_usd == Decimal("30")

    def test_multichain_fallback_aggregates_symbol_to_one_row(self):
        # A token held on BOTH chains must collapse into ONE wallet row (summed),
        # mirroring the canonical valuer — not one row per (token, chain), which a
        # symbol-keyed consumer could collapse into an under-count.
        from almanak.framework.strategies.intent_strategy import IntentStrategy

        market = _multichain_market(
            prices={("arbitrum", "USDC"): Decimal("1"), ("hyperevm", "USDC"): Decimal("1")},
            balances={("arbitrum", "USDC"): Decimal("30"), ("hyperevm", "USDC"): Decimal("20")},
        )
        snap = IntentStrategy.get_portfolio_snapshot(self._make_strat(tracked=("USDC",)), market)
        usdc_rows = [b for b in snap.wallet_balances if b.symbol == "USDC"]
        assert len(usdc_rows) == 1
        assert usdc_rows[0].balance == Decimal("50")
        assert snap.available_cash_usd == Decimal("50")
