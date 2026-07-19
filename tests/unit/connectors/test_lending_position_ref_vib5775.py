"""Tests for the typed ``LendingPositionRef`` seam + market-id resolver (VIB-5775).

Graceful teardown of a synthetic-market lending loop (euler_v2 / silo_v2) was dying
with ``HealthUnavailableError: market_id is required to value a <proto> position``:
those connectors deliberately use token-derived synthetic market ids that their
intents never carry, and ``PositionHealthProvider.get_health`` took ``market_id`` as
a required caller argument with no tokens to derive it from.

VIB-5775 adds:

* :class:`LendingPositionRef` — the canonical, connector-agnostic position identity.
* ``AccountStateReadSpec.market_id_from_ref`` — a connector-declared PURE
  token-attribute resolver (euler_v2, silo_v2 reuse their existing
  ``_synthesize_market_id``; benqi returns its fixed whole-account id).
* ``LendingReadRegistry.resolve_market_id(ref)`` — dispatches to the resolver, fails
  closed (``None`` + WARNING) on ambiguity / no resolver / unknown protocol.
* ``get_health(..., ref=...)`` / ``MarketSnapshot.position_health(..., ref=...)`` —
  derive the market id from the ref ONLY when the caller passes no explicit one.
* ``generate_lending_unwind`` — constructs and passes the ref.

The resolver-correctness tests below assert the ref-derived id EXACTLY equals what
each connector's account-state (intent) path produces for the SAME tokens, so the two
paths can never drift (they share one ``_synthesize_market_id``).
"""

from __future__ import annotations

import logging
from decimal import Decimal
from typing import Any
from unittest.mock import MagicMock

import pytest

from almanak.connectors._strategy_base.lending_read_base import LendingPositionRef
from almanak.connectors._strategy_base.lending_read_registry import LendingReadRegistry
from almanak.connectors.benqi.lending_read import (
    _BENQI_MARKET_ID,
    _benqi_query_inputs_from_intent,
)
from almanak.connectors.euler_v2.lending_read import (
    EULER_V2_ACCOUNT_STATE_MARKETS,
    _euler_query_inputs_from_intent,
)
from almanak.connectors.silo_v2.lending_read import (
    SILO_V2_ACCOUNT_STATE_MARKETS,
    _silo_query_inputs_from_intent,
)


def _borrow_intent(collateral: str, loan: str, chain: str) -> MagicMock:
    """A BORROW-shaped intent naming both legs (the shape teardown mirrors)."""
    return MagicMock(intent_type="BORROW", collateral_token=collateral, borrow_token=loan, token=None, chain=chain)


def _supply_intent(collateral: str, chain: str) -> MagicMock:
    """A SUPPLY-shaped intent naming only the collateral leg."""
    return MagicMock(intent_type="SUPPLY", token=collateral, collateral_token=None, borrow_token=None, chain=chain)


# ---------------------------------------------------------------------------
# 1. Resolver correctness — ref-derived id == intent-path id (drift-proof)
# ---------------------------------------------------------------------------


def test_euler_ref_resolves_every_directed_pair_matching_intent_path() -> None:
    """For every catalogued Euler directed pair, ``resolve_market_id(ref)`` equals the
    market id both the catalogue key AND the intent path produce for the same tokens."""
    checked = 0
    for chain, table in EULER_V2_ACCOUNT_STATE_MARKETS.items():
        for market_id, params in table.items():
            loan = params.get("loan_token")
            if not loan:
                continue  # collateral-only entries covered separately
            collateral = params["collateral_token"]
            ref = LendingPositionRef(protocol="euler_v2", chain=chain, collateral_token=collateral, loan_token=loan)
            intent_id = _euler_query_inputs_from_intent(_borrow_intent(collateral, loan, chain))["market_id"]
            resolved = LendingReadRegistry.resolve_market_id(ref)
            assert resolved == market_id, (chain, market_id, resolved)
            assert resolved == intent_id, (market_id, intent_id)  # never drifts from intent path
            checked += 1
    assert checked > 0


def test_euler_ref_resolves_collateral_only_matching_intent_path() -> None:
    """A collateral-only ref (loan_token=None) resolves to the ``"<col>"`` id, exactly
    as a SUPPLY intent naming that collateral does."""
    for chain, table in EULER_V2_ACCOUNT_STATE_MARKETS.items():
        for market_id, params in table.items():
            if params.get("loan_token"):
                continue
            collateral = params["collateral_token"]
            ref = LendingPositionRef(protocol="euler_v2", chain=chain, collateral_token=collateral, loan_token=None)
            intent_id = _euler_query_inputs_from_intent(_supply_intent(collateral, chain))["market_id"]
            resolved = LendingReadRegistry.resolve_market_id(ref)
            assert resolved == market_id
            assert resolved == intent_id


def test_silo_ref_resolves_every_directed_pair_matching_intent_path() -> None:
    checked = 0
    for chain, table in SILO_V2_ACCOUNT_STATE_MARKETS.items():
        for market_id, params in table.items():
            loan = params.get("loan_token")
            collateral = params["collateral_token"]
            ref = LendingPositionRef(protocol="silo_v2", chain=chain, collateral_token=collateral, loan_token=loan)
            intent_id = _silo_query_inputs_from_intent(_borrow_intent(collateral, loan, chain))["market_id"]
            resolved = LendingReadRegistry.resolve_market_id(ref)
            assert resolved == market_id, (chain, market_id, resolved)
            assert resolved == intent_id
            checked += 1
    assert checked > 0


def test_benqi_ref_resolves_fixed_whole_account_id_regardless_of_tokens() -> None:
    """BENQI is a pooled whole-account read: any tokens (or none) → the fixed id,
    exactly as every BENQI intent's ``query_inputs_fn`` produces."""
    for collateral, loan in (("WAVAX", "USDC"), ("USDC", None), (None, None), ("ANYTHING", "ELSE")):
        ref = LendingPositionRef(protocol="benqi", chain="avalanche", collateral_token=collateral, loan_token=loan)
        resolved = LendingReadRegistry.resolve_market_id(ref)
        assert resolved == _BENQI_MARKET_ID
        intent_id = _benqi_query_inputs_from_intent(MagicMock())["market_id"]
        assert resolved == intent_id


def test_euler_ref_unknown_tokens_fail_closed_never_guess() -> None:
    ref = LendingPositionRef(
        protocol="euler_v2", chain="avalanche", collateral_token="NOTATOKEN", loan_token="ALSONOPE"
    )
    assert LendingReadRegistry.resolve_market_id(ref) is None


def test_real_euler_single_leg_market_is_valued_not_raised() -> None:
    """VIB-5775 real-catalogue proof: the euler ``"usdc"`` collateral-only entry
    (loan_token=None) is a legitimate SINGLE-LEG market. ``_build_price_oracle_dict``
    must value it from one collateral price, NOT raise the two-leg
    "no collateral/loan token symbols" error that stranded teardown."""
    from almanak.framework.data.position_health import (
        PRICE_SOURCE_SAME_ASSET_UNIT,
        PositionHealthProvider,
    )

    # Sanity: the real ethereum catalogue entry is single-leg.
    params = LendingReadRegistry.market_params("euler_v2", "ethereum", "usdc")
    assert params is not None and params.get("collateral_token") == "USDC" and params.get("loan_token") is None

    provider = PositionHealthProvider(chain="ethereum", gateway_client=MagicMock())
    oracle, source = provider._build_price_oracle_dict("euler_v2", "usdc", None, None)
    assert oracle == {"USDC": Decimal("1")}
    assert source == PRICE_SOURCE_SAME_ASSET_UNIT


# ---------------------------------------------------------------------------
# 2. Registry ``resolve_market_id`` — declared / undeclared / unknown / warnings
# ---------------------------------------------------------------------------


def test_resolver_unknown_protocol_returns_none_and_warns(caplog: pytest.LogCaptureFixture) -> None:
    ref = LendingPositionRef(protocol="not_a_protocol", chain="x", collateral_token="A", loan_token="B")
    with caplog.at_level(logging.WARNING):
        assert LendingReadRegistry.resolve_market_id(ref) is None
    assert any("no account-state spec" in r.message for r in caplog.records)


def test_resolver_whole_account_no_resolver_returns_none_without_warning(
    caplog: pytest.LogCaptureFixture,
) -> None:
    # aave_v3 publishes an account-state spec but declares NO market_id_from_ref
    # (whole-account, USD-native) and the ref carries no market_id → fail closed.
    # This is the BENIGN case (Aave legitimately has no synthetic id), so it must
    # NOT emit a WARNING (would spam every Aave teardown) — DEBUG at most.
    ref = LendingPositionRef(protocol="aave_v3", chain="arbitrum", collateral_token="wstETH", loan_token="USDC")
    with caplog.at_level(logging.WARNING):
        assert LendingReadRegistry.resolve_market_id(ref) is None
    assert not any(r.levelno >= logging.WARNING for r in caplog.records)


def test_resolver_per_market_no_resolver_no_id_returns_none_and_warns(
    caplog: pytest.LogCaptureFixture,
) -> None:
    # morpho_blue is PER-MARKET (publishes a market table) but declares no ref
    # resolver: it NEEDS an explicit market_id and got none → a real gap → WARNING.
    ref = LendingPositionRef(protocol="morpho_blue", chain="ethereum", collateral_token="wstETH", loan_token="USDC")
    with caplog.at_level(logging.WARNING):
        assert LendingReadRegistry.resolve_market_id(ref) is None
    assert any("per-market protocol" in r.message for r in caplog.records)


def test_resolver_no_resolver_declared_but_ref_has_market_id_returns_it_verbatim() -> None:
    # Isolated-market protocols (Morpho) declare no ref resolver: the ref carries the
    # explicit bytes32 market id, which must be returned verbatim (no derivation).
    ref = LendingPositionRef(
        protocol="morpho_blue",
        chain="ethereum",
        collateral_token="wstETH",
        loan_token="USDC",
        market_id="0xABC123",
    )
    assert LendingReadRegistry.resolve_market_id(ref) == "0xABC123"


def test_resolver_declared_but_returns_none_warns(caplog: pytest.LogCaptureFixture) -> None:
    ref = LendingPositionRef(protocol="euler_v2", chain="avalanche", collateral_token="NOTATOKEN", loan_token="NOPE")
    with caplog.at_level(logging.WARNING):
        assert LendingReadRegistry.resolve_market_id(ref) is None
    assert any("could not reconstruct a market_id" in r.message for r in caplog.records)


def test_resolver_raises_fails_closed_to_none_and_warns(
    caplog: pytest.LogCaptureFixture, monkeypatch: pytest.MonkeyPatch
) -> None:
    """D3.F2 hard gate: a connector resolver is contracted PURE + non-raising, but if a
    misbehaving one RAISES, ``resolve_market_id`` must fail CLOSED to ``None`` (never
    guess, never let it crash the teardown/valuation guard) AND surface a WARNING naming
    the protocol/ref. A bare (uncaught) propagation would crash the guard; a silent catch
    with no log would hide the misbehaving connector — this locks the middle path."""
    from almanak.connectors._strategy_base import lending_read_base as base

    spec = LendingReadRegistry._load_account_state_spec("euler_v2")
    assert spec is not None and spec.market_id_from_ref is not None

    def _boom(_ref: base.LendingPositionRef) -> str | None:
        raise RuntimeError("connector resolver blew up")

    # Swap the euler spec's resolver for a raising one (dataclass is frozen → rebuild).
    import dataclasses

    monkeypatch.setattr(
        LendingReadRegistry,
        "_load_account_state_spec",
        classmethod(lambda cls, proto: dataclasses.replace(spec, market_id_from_ref=_boom)),
    )

    ref = LendingPositionRef(protocol="euler_v2", chain="avalanche", collateral_token="WAVAX", loan_token="USDC")
    with caplog.at_level(logging.WARNING):
        assert LendingReadRegistry.resolve_market_id(ref) is None  # fail closed, no crash
    assert any("resolver raised" in r.message for r in caplog.records)


@pytest.mark.parametrize("protocol", ["EULER_V2", "euler-v2"])
def test_resolver_normalizes_protocol_case_and_hyphen(protocol: str) -> None:
    # Case + hyphen folding runs before dispatch, so loosely-spelled protocol
    # identifiers still resolve their synthetic id.
    ref = LendingPositionRef(protocol=protocol, chain="avalanche", collateral_token="WAVAX", loan_token="USDC")
    assert LendingReadRegistry.resolve_market_id(ref) == "wavax/usdc"


# ---------------------------------------------------------------------------
# 3. get_health derivation behaviour (empty+ref → derived; empty+no ref → raise;
#    explicit market_id → resolver never called)
# ---------------------------------------------------------------------------


def _euler_provider(monkeypatch: pytest.MonkeyPatch) -> tuple[Any, dict[str, Any]]:
    """A PositionHealthProvider whose downstream reads are stubbed so we can observe
    which ``market_id`` the derivation fed them, without any gateway/RPC."""
    from almanak.connectors._strategy_base.lending_read_base import LendingAccountState
    from almanak.framework.data import position_health as ph

    provider = ph.PositionHealthProvider(chain="avalanche")
    seen: dict[str, Any] = {}

    def fake_build_price_oracle_dict(protocol: str, market_id: str, *a: Any, **k: Any):
        seen["oracle_market_id"] = market_id
        return None, ""

    def fake_read_account_state(protocol: str, market_id: str, user_address: str, price_oracle: Any = None):
        seen["read_market_id"] = market_id
        return LendingAccountState(
            collateral_usd=Decimal("100"),
            debt_usd=Decimal("50"),
            health_factor=Decimal("2"),
            liquidation_threshold_bps=None,
            e_mode_category=None,
            lltv=Decimal("0.8"),
        )

    monkeypatch.setattr(provider, "_build_price_oracle_dict", fake_build_price_oracle_dict)
    monkeypatch.setattr(provider, "_read_account_state", fake_read_account_state)
    return provider, seen


def test_get_health_empty_market_id_with_ref_derives_and_threads_it(monkeypatch: pytest.MonkeyPatch) -> None:
    provider, seen = _euler_provider(monkeypatch)
    ref = LendingPositionRef(protocol="euler_v2", chain="avalanche", collateral_token="WAVAX", loan_token="USDC")

    result = provider.get_health(protocol="euler_v2", market_id="", user_address="0xabc", ref=ref)

    # The derived id ("wavax/usdc") reached BOTH downstream consumers, not "".
    assert seen["oracle_market_id"] == "wavax/usdc"
    assert seen["read_market_id"] == "wavax/usdc"
    assert result.market_id == "wavax/usdc"


def test_get_health_empty_market_id_no_ref_still_raises(monkeypatch: pytest.MonkeyPatch) -> None:
    # No ref → nothing to derive → the pre-existing empty-market_id error must fire
    # (from the real _build_price_oracle_dict, not the stub).
    from almanak.framework.data import position_health as ph

    provider = ph.PositionHealthProvider(chain="avalanche")
    with pytest.raises(ValueError, match="market_id is required"):
        provider.get_health(protocol="euler_v2", market_id="", user_address="0xabc", ref=None)


def test_get_health_explicit_market_id_never_calls_resolver(monkeypatch: pytest.MonkeyPatch) -> None:
    provider, seen = _euler_provider(monkeypatch)
    from almanak.connectors._strategy_base import lending_read_registry as reg

    called = {"n": 0}
    real = reg.LendingReadRegistry.resolve_market_id.__func__

    def spy(cls: Any, ref: Any) -> Any:  # pragma: no cover - asserted not called
        called["n"] += 1
        return real(cls, ref)

    monkeypatch.setattr(reg.LendingReadRegistry, "resolve_market_id", classmethod(spy))

    # Explicit market_id + a ref present: the ref must be IGNORED (byte-for-byte
    # unaffected), so the resolver is never consulted and the explicit id flows through.
    ref = LendingPositionRef(protocol="euler_v2", chain="avalanche", collateral_token="WAVAX", loan_token="USDC")
    provider.get_health(protocol="euler_v2", market_id="usdc", user_address="0xabc", ref=ref)

    assert called["n"] == 0
    assert seen["read_market_id"] == "usdc"


# ---------------------------------------------------------------------------
# 4. generate_lending_unwind passes the ref (mock-level)
# ---------------------------------------------------------------------------


class _RefCapturingMarket:
    """Minimal MarketSnapshot stub that records the ref passed to position_health."""

    def __init__(self) -> None:
        self.captured_ref: Any = None

    def position_health(self, protocol: str, market_id: str, ref: Any = None, **kwargs: Any) -> Any:
        self.captured_ref = ref
        # No debt → generate_lending_unwind short-circuits to the withdraw-all branch,
        # so no further market reads are needed for this mock.
        from types import SimpleNamespace

        return SimpleNamespace(collateral_value_usd=Decimal("0"), debt_value_usd=Decimal("0"), lltv=Decimal("0.8"))

    def price(self, token: str) -> Decimal:
        return Decimal("1")

    def balance(self, token: str, *, chain: str | None = None) -> Any:
        from types import SimpleNamespace

        return SimpleNamespace(balance=Decimal("0"))


def test_generate_lending_unwind_passes_typed_ref() -> None:
    from almanak.framework.teardown.lending_unwind import generate_lending_unwind

    market = _RefCapturingMarket()
    generate_lending_unwind(
        market=market,
        protocol="euler_v2",
        collateral_token="WAVAX",
        borrow_token="USDC",
        chain="avalanche",
    )

    ref = market.captured_ref
    assert isinstance(ref, LendingPositionRef)
    assert ref.protocol == "euler_v2"
    assert ref.chain == "avalanche"
    assert ref.collateral_token == "WAVAX"
    assert ref.loan_token == "USDC"
    # And that ref resolves to the correct synthetic id end-to-end.
    assert LendingReadRegistry.resolve_market_id(ref) == "wavax/usdc"


def test_generate_lending_unwind_chain_none_inherits_snapshot_chain() -> None:
    """Regression (codex/CodeRabbit review): ``chain`` defaults to None (the strategy's
    primary chain). Euler's synthetic-id resolver indexes a PER-CHAIN catalogue, so an
    empty chain resolves to None and the fix would be defeated for the documented
    ``chain=None`` caller. The ref must inherit the SNAPSHOT's pinned chain instead of
    ``""``, so resolution still succeeds."""
    from almanak.framework.teardown.lending_unwind import generate_lending_unwind

    market = _RefCapturingMarket()
    market.chain = "avalanche"  # the snapshot is pinned to the execution chain
    generate_lending_unwind(
        market=market,
        protocol="euler_v2",
        collateral_token="WAVAX",
        borrow_token="USDC",
        chain=None,  # documented default — must fall back to market.chain, NOT ""
    )

    ref = market.captured_ref
    assert ref.chain == "avalanche"  # inherited from the snapshot, not empty
    assert LendingReadRegistry.resolve_market_id(ref) == "wavax/usdc"  # resolves (would be None if chain="")


def test_silo_single_token_ref_requires_uniqueness_never_first_match() -> None:
    """VIB-5795 (Codex P2): silo markets are ISOLATED directed pairs, so a
    single-token ref resolves only when exactly one catalogue entry matches
    that leg. WAVAX spans three markets on each side → fail closed (None);
    USDC / sAVAX / BTC.b legs are unique → resolve. First-match guessing is
    reserved for the INTENT path, where trade context disambiguates."""
    from almanak.connectors._strategy_base.lending_read_base import LendingPositionRef
    from almanak.connectors._strategy_base.lending_read_registry import LendingReadRegistry

    def ref(collateral=None, loan=None):
        return LendingPositionRef(
            protocol="silo_v2", chain="avalanche", collateral_token=collateral, loan_token=loan
        )

    # Ambiguous on both sides — must fail closed.
    assert LendingReadRegistry.resolve_market_id(ref(collateral="WAVAX")) is None
    assert LendingReadRegistry.resolve_market_id(ref(loan="WAVAX")) is None
    # Unique legs — must resolve to the one directed pair.
    assert LendingReadRegistry.resolve_market_id(ref(collateral="USDC")) == "usdc/wavax"
    assert LendingReadRegistry.resolve_market_id(ref(loan="USDC")) == "wavax/usdc"
    assert LendingReadRegistry.resolve_market_id(ref(collateral="sAVAX")) == "savax/wavax"
    assert LendingReadRegistry.resolve_market_id(ref(collateral="BTC.b")) == "btc.b/wavax"
    assert LendingReadRegistry.resolve_market_id(ref(loan="BTC.b")) == "wavax/btc.b"
    # Both tokens named keeps the exact-pair contract (unchanged).
    assert LendingReadRegistry.resolve_market_id(ref(collateral="WAVAX", loan="USDC")) == "wavax/usdc"
