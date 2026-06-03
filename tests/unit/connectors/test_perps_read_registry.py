"""Foundation tests for the perps-read capability seam (VIB-4930 PR-1).

Pins the venue-neutral types + the empty-registry behaviour before any connector
publishes a spec. Mirrors the lending-read seam
(``test_lending_read_registry.py`` / ``lending_read_base`` /
``lending_read_registry``). PR-2 adds GMX parity oracles; this file only fixes
the abstraction's contract.
"""

from __future__ import annotations

import inspect
from decimal import Decimal

from almanak.connectors._strategy_base.perps_read_base import (
    EthCall,
    PerpsMarketMeta,
    PerpsPositionOnChain,
    PerpsPositionPlan,
    PerpsPositionQuery,
    PerpsPositionValue,
    PerpsReadResult,
)
from almanak.connectors._strategy_base.perps_read_registry import PerpsReadRegistry


def _position(**overrides) -> PerpsPositionOnChain:
    base = {
        "account": "0xWALLET",
        "market": "0xMarketAbC",
        "collateral_token": "0xUSDC",
        "size_in_usd": 1,
        "size_in_tokens": 1,
        "collateral_amount": 1,
        "is_long": True,
        "borrowing_factor": 0,
        "funding_fee_amount_per_size": 0,
        "increased_at_time": 0,
        "decreased_at_time": 0,
    }
    base.update(overrides)
    return PerpsPositionOnChain(**base)


# ---------------------------------------------------------------------------
# Types
# ---------------------------------------------------------------------------


def test_position_key_is_byte_identical_to_legacy_gmx_format():
    # The default "gmx" prefix preserves the pre-VIB-4930 reader's
    # "gmx-{market}-{collateral}-{side}" bytes (lower-cased).
    assert _position().position_key == "gmx-0xmarketabc-0xusdc-long"
    assert _position(is_long=False).position_key == "gmx-0xmarketabc-0xusdc-short"


def test_position_key_prefix_is_per_venue():
    pos = _position(key_prefix="aster", market="0xM", collateral_token="0xC")
    assert pos.position_key == "aster-0xm-0xc-long"


def test_is_active_tracks_size():
    assert _position(size_in_usd=5).is_active is True
    assert _position(size_in_usd=0).is_active is False


def test_read_result_distinguishes_failed_from_measured_empty():
    failed = PerpsReadResult(positions=(), ok=False)
    measured_empty = PerpsReadResult(positions=(), ok=True)
    # Empty≠Zero: an empty book with ok=True is measured; ok=False is unmeasured.
    assert failed.ok is False
    assert measured_empty.ok is True
    assert failed.positions == () == measured_empty.positions


def test_market_meta_and_position_value_are_plain_carriers():
    meta = PerpsMarketMeta(index_token_symbol="ETH", index_token_decimals=18)
    assert (meta.index_token_symbol, meta.index_token_decimals) == ("ETH", 18)
    value = PerpsPositionValue(
        market="0xM",
        is_long=True,
        size_usd=Decimal("100"),
        collateral_value_usd=Decimal("10"),
        entry_price_usd=Decimal("2"),
        mark_price_usd=Decimal("3"),
        unrealized_pnl_usd=Decimal("50"),
        pending_fees_usd=Decimal("0"),
        net_value_usd=Decimal("60"),
        leverage=Decimal("10"),
    )
    assert value.net_value_usd == Decimal("60")


def test_plan_composes_query_calls_and_reducer():
    call = EthCall(to="0xReader", data="0xdead")
    query = PerpsPositionQuery(chain="arbitrum", wallet_address="0xabc", targets={"reader": "0xReader"})
    plan = PerpsPositionPlan(
        query=query,
        calls=(call,),
        reduce=lambda q, results: PerpsReadResult((), ok=all(r is not None for r in results)),
    )
    assert plan.calls[0].to == "0xReader"
    assert plan.reduce(query, ["0x00"]).ok is True
    assert plan.reduce(query, [None]).ok is False


# ---------------------------------------------------------------------------
# Empty registry (no connector publishes a spec in PR-1)
# ---------------------------------------------------------------------------


def test_registry_reports_registered_venues():
    # gmx_v2 is the first published venue (PR-2); unregistered names resolve to nothing.
    assert "gmx_v2" in PerpsReadRegistry.supported_protocols()
    assert PerpsReadRegistry.has("gmx_v2") is True
    assert PerpsReadRegistry.canonical("gmx_v2") == "gmx_v2"
    assert PerpsReadRegistry.has("unknown_perp") is False
    assert PerpsReadRegistry.canonical("unknown_perp") is None
    assert PerpsReadRegistry.canonical(None) is None


def test_registry_resolve_returns_none_for_unknown_protocol():
    query = PerpsPositionQuery(chain="arbitrum", wallet_address="0xabc")
    assert PerpsReadRegistry.resolve_plan("unknown_perp", query) is None
    assert PerpsReadRegistry.market_metadata("unknown_perp", "0xMarket", "arbitrum") is None
    assert PerpsReadRegistry.value_position("unknown_perp") is None


def test_normalize_is_total_for_none_and_non_str():
    # ``_normalize`` is reached by ``market_metadata`` / ``value_position`` with a
    # loosely typed ``position.protocol`` — ``None`` / non-``str`` must normalise
    # to the empty string, never raise ``AttributeError`` on ``.lower()``.
    assert PerpsReadRegistry._normalize(None) == ""
    assert PerpsReadRegistry._normalize(123) == ""  # type: ignore[arg-type]
    # An empty string normalises to itself (no spec keyed under "").
    assert PerpsReadRegistry._normalize("") == ""


def test_public_entry_points_fail_closed_on_none_protocol():
    # Every public entry point that funnels through ``_normalize`` must fail
    # closed on a ``None`` protocol (no spec for "" ⇒ None / False) rather than
    # crashing the snapshot.
    assert PerpsReadRegistry.has(None) is False  # type: ignore[arg-type]
    assert PerpsReadRegistry.market_metadata(None, "0xM", "arbitrum") is None  # type: ignore[arg-type]
    assert PerpsReadRegistry.value_position(None) is None  # type: ignore[arg-type]
    assert PerpsReadRegistry.canonical(None) is None


def test_reset_cache_is_callable_on_empty_cache():
    PerpsReadRegistry.reset_cache()  # must not raise


# ---------------------------------------------------------------------------
# Gateway-boundary purity (the seam must stay egress-free + connector-agnostic)
# ---------------------------------------------------------------------------


def test_seam_is_egress_free_and_lazily_imports_connectors():
    import almanak.connectors._strategy_base.perps_read_base as base
    import almanak.connectors._strategy_base.perps_read_registry as registry

    for module in (base, registry):
        src = inspect.getsource(module)
        for forbidden in ("import requests", "import httpx", "import aiohttp", "HTTPProvider"):
            assert forbidden not in src, f"{module.__name__} must stay egress-free ({forbidden!r})"

    # The registry dispatches lazily: ``_SPEC_LOADERS`` holds connector module
    # paths as STRINGS (resolved via importlib on first lookup), so the module
    # must carry no eager ``import almanak.connectors.<venue>`` statement — that is
    # what keeps a broken sibling connector from poisoning an unrelated lookup.
    for line in inspect.getsource(registry).splitlines():
        stripped = line.strip()
        if stripped.startswith(("import ", "from ")):
            assert "almanak.connectors.gmx_v2" not in stripped
            assert "almanak.connectors.aster_perps" not in stripped
