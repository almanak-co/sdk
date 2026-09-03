"""ALM-3473 root cause B — ``_reconcile_lending`` must prefer the
price-independent raw on-chain balance read (``MarketSnapshot.
lending_position_balances``) over the USD-valuation ``position_health()``
read, falling back to the latter only when the raw read is unmeasured.

This is the PRIMARY defect per the ticket and its triage: a stale/memoized
``position_health()`` valuation (or, more fundamentally, ANY USD-valuation
account-level aggregate) can report the exact pre-close collateral value for
a position that just had its collateral withdrawn on-chain. The raw read is
price-independent and reads the position's own reserve directly, so it
cannot be fooled by either failure mode.
"""

from __future__ import annotations

from decimal import Decimal
from typing import Any

from almanak.framework.teardown.models import PositionInfo, PositionType
from almanak.framework.teardown.plan_a_reconciliation import ReconciliationVerdict, _reconcile_lending

_CHAIN = "base"
_MARKET_ID = "0x13c42741a359ac4a8aa8287d2be109dcf28344484f91185f9a79bd5a805a55ae"


class _StaleValuationMarket:
    """Reproduces the EXACT ALM-3473 shape: the raw on-chain reserve read is
    genuinely zero (collateral just withdrawn), but ``position_health()``
    still reports the stale pre-withdraw USD valuation — the reported
    $147.4879976821872314355759962 defect."""

    def __init__(self, *, raw_balances: tuple[int | None, int | None], stale_collateral_usd: Decimal) -> None:
        self._raw_balances = raw_balances
        self._stale_collateral_usd = stale_collateral_usd
        self.lending_position_balances_calls: list[dict[str, Any]] = []
        self.position_health_calls = 0

    def lending_position_balances(
        self, protocol: str, token: str, *, market_id: str | None = None, chain: str | None = None
    ) -> tuple[int | None, int | None]:
        self.lending_position_balances_calls.append(
            {"protocol": protocol, "token": token, "market_id": market_id, "chain": chain}
        )
        return self._raw_balances

    def position_health(self, protocol: str, market_id: str, **_kw: Any) -> Any:  # noqa: ARG002
        self.position_health_calls += 1

        class _Health:
            collateral_value_usd = self._stale_collateral_usd
            debt_value_usd = Decimal("0")
            health_factor = None

        return _Health()

    def price(self, token: str) -> Decimal:  # pragma: no cover — must not be reached on the raw-authority path
        raise AssertionError(f"market.price({token!r}) must not be called when the raw read is measured")


def _morpho_supply_position() -> PositionInfo:
    return PositionInfo(
        position_type=PositionType.SUPPLY,
        position_id=_MARKET_ID,
        chain=_CHAIN,
        protocol="morpho_blue",
        value_usd=Decimal("147.4879976821872314355759962"),
        details={"market_id": _MARKET_ID, "collateral_token": "wstETH", "borrow_token": "USDC"},
    )


def test_raw_zero_balance_overrides_stale_positive_usd_valuation():
    """The exact ALM-3473 defect: on-chain collateral is genuinely zero
    (withdrawn) but position_health() still reports the stale pre-close USD
    value. The raw read must win — DIVERGED_CLOSED, not CONFIRMED_OPEN."""
    market = _StaleValuationMarket(
        raw_balances=(0, None), stale_collateral_usd=Decimal("147.4879976821872314355759962")
    )
    verdict, reason = _reconcile_lending(position=_morpho_supply_position(), market=market)

    assert verdict is ReconciliationVerdict.DIVERGED_CLOSED, reason
    assert market.lending_position_balances_calls == [
        {"protocol": "morpho_blue", "market_id": _MARKET_ID, "token": "wstETH", "chain": _CHAIN}
    ]
    assert market.position_health_calls == 0, (
        "position_health() must not even be consulted when the raw read is measured"
    )


def test_raw_nonzero_balance_confirms_open_even_when_usd_valuation_agrees():
    market = _StaleValuationMarket(raw_balances=(50_000_000_000_000_000, None), stale_collateral_usd=Decimal("147.49"))
    verdict, reason = _reconcile_lending(position=_morpho_supply_position(), market=market)

    assert verdict is ReconciliationVerdict.CONFIRMED_OPEN, reason
    assert "50000000000000000" in reason


def test_borrow_leg_reads_debt_raw_balance_not_supply():
    market = _StaleValuationMarket(raw_balances=(999, 0), stale_collateral_usd=Decimal("0"))
    borrow_position = PositionInfo(
        position_type=PositionType.BORROW,
        position_id=_MARKET_ID,
        chain=_CHAIN,
        protocol="morpho_blue",
        value_usd=Decimal("0"),
        details={"market_id": _MARKET_ID, "collateral_token": "wstETH", "borrow_token": "USDC"},
    )
    verdict, reason = _reconcile_lending(position=borrow_position, market=market)

    assert verdict is ReconciliationVerdict.DIVERGED_CLOSED, reason
    assert market.lending_position_balances_calls[0]["token"] == "USDC"


def test_borrow_leg_matches_by_value_not_identity():
    """A restored ``PositionInfo`` whose ``position_type`` is the persisted
    string ``"BORROW"`` (not the ``PositionType.BORROW`` enum singleton) must
    still be recognized as the BORROW leg. ``PositionType`` is a ``StrEnum``,
    so ``==`` (not ``is``) is required for this to hold regardless of which
    object identity the value carries after a deserialization round-trip."""
    market = _StaleValuationMarket(raw_balances=(999, 0), stale_collateral_usd=Decimal("0"))
    borrow_position = PositionInfo(
        position_type="BORROW",  # deliberately a bare string, not PositionType.BORROW
        position_id=_MARKET_ID,
        chain=_CHAIN,
        protocol="morpho_blue",
        value_usd=Decimal("0"),
        details={"market_id": _MARKET_ID, "collateral_token": "wstETH", "borrow_token": "USDC"},
    )
    verdict, reason = _reconcile_lending(position=borrow_position, market=market)

    assert verdict is ReconciliationVerdict.DIVERGED_CLOSED, reason
    assert market.lending_position_balances_calls[0]["token"] == "USDC", (
        "must still read the DEBT leg (USDC), not silently fall through to the collateral leg"
    )


def test_unresolved_market_id_skips_the_raw_read_entirely():
    """CodeRabbit finding: when ``_lending_market_id`` falls through to its
    ``position_id`` LAST-RESORT (no ``market_id``/``market`` detail key, and
    the synthetic resolver also fails), the raw path must be SKIPPED, not
    attempted with a garbage market id. Morpho's ``position(marketId,
    wallet)`` silently returns an all-zero struct for an unknown market key
    (no revert) -- trusting that zero would confidently misreport a
    genuinely-OPEN position as DIVERGED_CLOSED, which is worse than the
    original bug (a false OPEN at least fails safe)."""
    market = _StaleValuationMarket(
        raw_balances=(0, None),  # what a WRONG market key would silently read
        stale_collateral_usd=Decimal("147.49"),  # what the REAL position genuinely holds
    )
    position_with_no_market_id = PositionInfo(
        position_type=PositionType.SUPPLY,
        position_id="not-a-real-morpho-market-id",
        chain=_CHAIN,
        protocol="morpho_blue",
        value_usd=Decimal("147.49"),
        details={"collateral_token": "wstETH", "borrow_token": "USDC"},  # no market_id / market key
    )
    verdict, reason = _reconcile_lending(position=position_with_no_market_id, market=market)

    assert market.lending_position_balances_calls == [], (
        "the raw read must never be attempted with an unresolved (last-resort) market id"
    )
    assert verdict is ReconciliationVerdict.CONFIRMED_OPEN, reason
    assert market.position_health_calls == 1


def test_whole_account_protocol_still_uses_raw_read_with_no_market_id():
    """Codex finding: the market-id guard must not disable the raw-authority
    path for a WHOLE-ACCOUNT protocol (Aave family). Spark's real
    ``PositionInfo`` shape (``details={"asset": ...}``, no ``market_id``/
    ``market`` key at all -- see ``spark_lender/strategy.py``) previously
    tripped the same guard meant for Morpho's per-market danger, silently
    falling back to the stale/wrong USD valuation for EVERY Aave-family
    position -- reintroducing the exact defect class this PR fixes, just for
    a different protocol family."""
    market = _StaleValuationMarket(
        raw_balances=(0, 0),  # genuinely closed on-chain
        stale_collateral_usd=Decimal("42"),  # stale/wrong valuation, must NOT win
    )
    spark_position = PositionInfo(
        position_type=PositionType.SUPPLY,
        position_id="spark-supply-USDC-ethereum",
        chain="ethereum",
        protocol="spark",
        value_usd=Decimal("42"),
        details={"asset": "USDC", "type": "collateral"},  # Spark's REAL shape -- no market_id
    )
    verdict, reason = _reconcile_lending(position=spark_position, market=market)

    assert market.lending_position_balances_calls, (
        "the raw read must still be attempted for a whole-account protocol with no market_id"
    )
    assert verdict is ReconciliationVerdict.DIVERGED_CLOSED, reason


def test_unmeasured_raw_read_falls_back_to_position_health():
    """No reader registered for this protocol / token unresolvable: the raw
    leg is (None, None) — unmeasured, never treated as a confident verdict on
    its own. Must fall back to position_health(), exactly like before this
    fix existed."""

    class _NoReaderMarket:
        def __init__(self) -> None:
            self.position_health_calls = 0

        def lending_position_balances(self, *_a: Any, **_kw: Any) -> tuple[None, None]:
            return (None, None)

        def position_health(self, protocol: str, market_id: str, **_kw: Any) -> Any:  # noqa: ARG002
            self.position_health_calls += 1

            class _Health:
                collateral_value_usd = Decimal("0")
                debt_value_usd = Decimal("0")
                health_factor = None

            return _Health()

        def price(self, token: str) -> Decimal:
            return Decimal("1")

    market = _NoReaderMarket()
    verdict, reason = _reconcile_lending(position=_morpho_supply_position(), market=market)

    assert verdict is ReconciliationVerdict.DIVERGED_CLOSED, reason
    assert market.position_health_calls == 1, "an unmeasured raw read must fall back to position_health()"


def test_market_with_no_raw_balance_method_falls_back_cleanly():
    """A market/test-double with no ``lending_position_balances`` attribute at
    all (e.g. a legacy mock) must fall back exactly as before this fix,
    never raise AttributeError."""

    class _LegacyMarket:
        def position_health(self, protocol: str, market_id: str, **_kw: Any) -> Any:  # noqa: ARG002
            class _Health:
                collateral_value_usd = Decimal("0")
                debt_value_usd = Decimal("0")
                health_factor = None

            return _Health()

        def price(self, token: str) -> Decimal:
            return Decimal("1")

    verdict, reason = _reconcile_lending(position=_morpho_supply_position(), market=_LegacyMarket())
    assert verdict is ReconciliationVerdict.DIVERGED_CLOSED, reason


def test_raw_read_exception_is_treated_as_unmeasured_not_a_fault():
    class _FaultingMarket:
        def __init__(self) -> None:
            self.position_health_calls = 0

        def lending_position_balances(self, *_a: Any, **_kw: Any) -> tuple[int | None, int | None]:
            raise ConnectionError("gateway blip")

        def position_health(self, protocol: str, market_id: str, **_kw: Any) -> Any:  # noqa: ARG002
            self.position_health_calls += 1

            class _Health:
                collateral_value_usd = Decimal("0")
                debt_value_usd = Decimal("0")
                health_factor = None

            return _Health()

        def price(self, token: str) -> Decimal:
            return Decimal("1")

    market = _FaultingMarket()
    verdict, reason = _reconcile_lending(position=_morpho_supply_position(), market=market)

    assert verdict is ReconciliationVerdict.DIVERGED_CLOSED, reason
    assert market.position_health_calls == 1
