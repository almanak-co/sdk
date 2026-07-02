"""GMX V2 pending-order teardown detection + fail-closed verify (VIB-5116).

Covers the Option-B fix for the GMX teardown strand: a pending (unfilled) GMX V2
order holds collateral in the OrderVault but is not a position, so teardown's
enumeration missed it and reported ``no_positions`` success. The fix:

* ``orders_read.decode_account_orders`` — decode ``getAccountOrders`` (the ABI is
  version-sensitive; the fixture is REAL Arbitrum chain bytes, not self-encoded,
  so this test would catch a struct drift the way it caught the original one).
* ``teardown_residual_discovery`` — surface each pending order as a residual,
  fail-closed (``ok=False``) on an unmeasured read (Empty != Zero).
* ``teardown_post_condition`` — fail the teardown closed while an order is still
  pending / a position is still open; report closed only on a clean measured read.
* framework ``residual_discovery`` — fold residuals into the enumeration and
  surface a loud sentinel on an unmeasured read.
* ``completeness`` — a discovered pending-order residual with no closing intent is
  flagged uncovered (fail loud), which is the whole point: teardown must not
  report success while committed capital is stranded.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

from almanak.connectors.gmx_v2.orders_read import (
    ORDER_TYPE_MARKET_INCREASE,
    build_account_orders_calldata,
    build_order_count_calldata,
    build_order_keys_calldata,
    decode_account_orders,
)
from almanak.connectors.gmx_v2.teardown_post_condition import gmx_v2_teardown_post_condition
from almanak.connectors.gmx_v2.teardown_residual_discovery import gmx_v2_teardown_residual_discovery
from almanak.framework.teardown.completeness import check_intent_coverage
from almanak.framework.teardown.models import PositionInfo, PositionType, TeardownPositionSummary
from almanak.framework.teardown.post_conditions import has_teardown_post_condition
from almanak.framework.teardown.registry_enumeration import _union_residuals

# Real Arbitrum ``getAccountOrders`` return, captured on a managed-Anvil fork with
# ONE pending MARKET_INCREASE order (VIB-5116 real-fork proof). Using real chain
# bytes (not a self-encode of the production ABI) is deliberate: a self-encoded
# fixture would drift in lockstep with a decode bug and hide it.
_FIXTURE = Path(__file__).parent / "fixtures" / "gmx_getaccountorders_arbitrum_vib5116.hex"
_REAL_ORDERS_BLOB = _FIXTURE.read_text().strip()

_WALLET = "0x54776446Aa29Fc49d152B4850bD410eA1E4d24bF"
_ORDER_KEY = "0x9edb04b6c2e51bbabed42b2e344208d78e8ff4c39e84ee31903a5659fd161b24"
_ETH_USD_MARKET = "0x70d95587d40A2caf56bd97485aB3Eec10Bee6336"
_USDC = "0xaf88d065e77c8cC2239327C5EDb3A432268e5831"
_DATA_STORE = "0xFD70de6b91282D8017aA4E741e9Ae325CAb992d8"

# Selector prefixes (calldata routing in the fake gateway).
_COUNT_SEL = build_order_count_calldata(_WALLET)[:10]
_KEYS_SEL = build_order_keys_calldata(_WALLET)[:10]
_ORDERS_SEL = build_account_orders_calldata(_DATA_STORE, _WALLET)[:10]


def _u256(n: int) -> str:
    return "0x" + n.to_bytes(32, "big").hex()


def _keys_blob(keys: list[str]) -> str:
    from eth_abi import encode

    return "0x" + encode(["bytes32[]"], [[bytes.fromhex(k[2:]) for k in keys]]).hex()


class _FakeGateway:
    """A gateway stub that serves canned eth_call returns by selector."""

    is_connected = True

    def __init__(self, *, count: Any = 1, keys: list[str] | None = None, orders_blob: str | None = _REAL_ORDERS_BLOB):
        self._count = count
        self._keys = keys if keys is not None else [_ORDER_KEY]
        self._orders_blob = orders_blob
        self.calls: list[str] = []

    def eth_call(self, chain: str, to: str, data: str, block: Any = None) -> Any:  # noqa: ARG002
        self.calls.append(data[:10])
        if data.startswith(_COUNT_SEL):
            return None if self._count is None else _u256(self._count)
        if data.startswith(_KEYS_SEL):
            return None if self._keys is None else _keys_blob(self._keys)
        if data.startswith(_ORDERS_SEL):
            return self._orders_blob
        return None


# ---------------------------------------------------------------------------
# 1) ABI lock — the decode must match REAL chain bytes exactly.
# ---------------------------------------------------------------------------
def test_decode_account_orders_matches_real_chain_bytes() -> None:
    orders = decode_account_orders(_REAL_ORDERS_BLOB)
    assert orders is not None and len(orders) == 1
    o = orders[0]
    assert o.order_key.lower() == _ORDER_KEY
    assert o.order_type == ORDER_TYPE_MARKET_INCREASE
    assert o.market.lower() == _ETH_USD_MARKET.lower()
    assert o.initial_collateral_token.lower() == _USDC.lower()
    assert o.initial_collateral_delta_amount == 50_000_000  # 50 USDC committed to the OrderVault
    assert o.size_delta_usd == 100 * 10**30  # $100 notional
    assert o.is_long is True


def test_account_order_list_key_matches_real_datastore_slot() -> None:
    # VIB-5116 real-fork regression: the DataStore ACCOUNT_ORDER_LIST set key must
    # be derived with keccak(abi.encode("ACCOUNT_ORDER_LIST")) — the ABI-encoded
    # string — NOT keccak(text=...). The wrong slot reads a real, always-empty set
    # (count=0), so detection silently finds no orders. Pinned to the exact slot
    # verified on a real Arbitrum DataStore (which returned a non-zero order count
    # for this wallet where the keccak(text) slot returned 0).
    from almanak.connectors.gmx_v2.orders_read import account_order_list_key

    key = "0x" + account_order_list_key(_WALLET).hex()
    assert key == "0x5f8a16a87a8473af24b12940e2db6bbd96b90ef03a7b326ff197930d446f4bae"


def test_read_open_positions_constructs_query_with_chain() -> None:
    # VIB-5116 real-fork regression (Bug B): PerpsPositionQuery requires `chain`;
    # read_open_positions must pass it or the post-condition's position verify
    # raises. Drive the REAL read_open_positions with a fake gateway serving an
    # empty getAccountPositions and assert it returns cleanly (ok, no positions),
    # never raises.
    from eth_abi import encode as _encode

    from almanak.connectors.gmx_v2 import perps_read as _pr
    from almanak.connectors.gmx_v2.teardown_reads import read_open_positions

    empty_positions_blob = "0x" + _encode([_pr._GET_ACCOUNT_POSITIONS_OUTPUT], [[]]).hex()

    class _PosGateway:
        is_connected = True

        def eth_call(self, chain: str, to: str, data: str, block: Any = None) -> Any:  # noqa: ARG002
            return empty_positions_blob

    result = read_open_positions(_PosGateway(), "arbitrum", _WALLET)
    assert result.ok is True
    assert list(result.positions) == []


def test_decode_account_orders_empty_and_unmeasured() -> None:
    assert decode_account_orders("0x") == []  # measured-empty return
    assert decode_account_orders(None) is None  # unmeasured (Empty != Zero)
    assert decode_account_orders("0xdeadbeef") is None  # undecodable -> None, never []


# ---------------------------------------------------------------------------
# 2) Connector residual discovery.
# ---------------------------------------------------------------------------
def test_discovery_surfaces_pending_order_with_collateral() -> None:
    res = gmx_v2_teardown_residual_discovery(_WALLET, "arbitrum", _FakeGateway())
    assert res.ok is True
    assert len(res.residuals) == 1
    r = res.residuals[0]
    assert r.protocol == "gmx_v2"
    assert r.identifier.lower() == _ORDER_KEY
    assert r.details["kind"] == "pending_order"
    assert r.details["market"].lower() == _ETH_USD_MARKET.lower()
    assert r.details["collateral_token"].lower() == _USDC.lower()
    assert r.details["collateral_amount_raw"] == "50000000"


def test_discovery_measured_empty_book_surfaces_nothing() -> None:
    res = gmx_v2_teardown_residual_discovery(_WALLET, "arbitrum", _FakeGateway(count=0))
    assert res.ok is True
    assert res.residuals == []


def test_discovery_unmeasured_count_reports_not_ok() -> None:
    # The COUNT read is unmeasured (None): the connector reports ok=False (never
    # swallows it as "no orders" — Empty != Zero). The FRAMEWORK then surfaces the
    # loud fail-closed sentinel (see test_framework_discovery_unmeasured_*).
    res = gmx_v2_teardown_residual_discovery(_WALLET, "arbitrum", _FakeGateway(count=None))
    assert res.ok is False
    assert res.residuals == []
    assert res.error


def test_discovery_unmeasured_keys_reports_not_ok() -> None:
    # count>0 but the key read is unmeasured: still an UNMEASURED identity -> the
    # connector reports ok=False (fail-closed), never "no orders".
    gw = _FakeGateway(count=1)
    gw._keys = None  # force the getBytes32ValuesAt read to be unmeasured
    res = gmx_v2_teardown_residual_discovery(_WALLET, "arbitrum", gw)
    assert res.ok is False
    assert res.error


def test_discovery_detail_drift_still_detects_via_keys() -> None:
    # getAccountOrders detail undecodable (struct drift) but count/keys stable:
    # detection must hold via the key list (a key-only residual is still surfaced).
    gw = _FakeGateway(orders_blob="0xdeadbeef")
    res = gmx_v2_teardown_residual_discovery(_WALLET, "arbitrum", gw)
    assert res.ok is True
    assert len(res.residuals) == 1
    assert res.residuals[0].identifier.lower() == _ORDER_KEY


# ---------------------------------------------------------------------------
# 3) Connector post-condition — fail-closed while pending, closed only on clean read.
# ---------------------------------------------------------------------------
def _pending_order_position() -> PositionInfo:
    return PositionInfo(
        position_type=PositionType.PERP,
        position_id=_ORDER_KEY,
        chain="arbitrum",
        protocol="gmx_v2",
        value_usd=__import__("decimal").Decimal("0"),
        details={"kind": "pending_order", "order_key": _ORDER_KEY, "market": _ETH_USD_MARKET},
    )


def test_post_condition_pending_order_fails_closed() -> None:
    # MEASURED residual (the order is genuinely in the on-chain pending set) →
    # FAILED, not UNVERIFIED (VIB-5573).
    result = gmx_v2_teardown_post_condition(_pending_order_position(), _WALLET, _FakeGateway())
    assert result.closed is False
    assert result.unmeasured is False
    assert result.residual.get("order_key", "").lower() == _ORDER_KEY


def test_post_condition_order_gone_reads_closed() -> None:
    # Order cancelled/executed -> no longer in the pending set -> closed on-chain.
    gw = _FakeGateway(count=0, keys=[], orders_blob="0x")
    result = gmx_v2_teardown_post_condition(_pending_order_position(), _WALLET, gw)
    assert result.closed is True


def test_post_condition_unmeasured_is_failclosed() -> None:
    # A read fault (count read came back None) → UNVERIFIED, never a fabricated
    # residual → FAILED (VIB-5573).
    gw = _FakeGateway(count=None)
    result = gmx_v2_teardown_post_condition(_pending_order_position(), _WALLET, gw)
    assert result.closed is False
    assert result.unmeasured is True
    assert result.error


def test_post_condition_missing_gateway_is_failclosed() -> None:
    result = gmx_v2_teardown_post_condition(_pending_order_position(), _WALLET, None)
    assert result.closed is False
    assert result.unmeasured is True
    assert result.error


def _open_perp_position() -> PositionInfo:
    # No pending_order kind -> the hook takes the open-position (getAccountPositions) path.
    return PositionInfo(
        position_type=PositionType.PERP,
        position_id="gmx-perp-eth",
        chain="arbitrum",
        protocol="gmx_v2",
        value_usd=__import__("decimal").Decimal("0"),
        details={"market": "ETH-USD"},  # a SYMBOL — the account-level rule ignores it (no addr mismatch)
    )


def test_post_condition_open_position_failclosed_when_any_active(monkeypatch) -> None:
    from types import SimpleNamespace

    from almanak.connectors.gmx_v2 import teardown_post_condition as tpc

    active = SimpleNamespace(is_active=True, market="0xMARKETADDR", size_in_usd=10)
    monkeypatch.setattr(tpc, "read_open_positions", lambda *a, **k: SimpleNamespace(ok=True, positions=[active]))
    r = tpc.gmx_v2_teardown_post_condition(_open_perp_position(), _WALLET, _FakeGateway())
    assert r.closed is False  # any active GMX position of this deployment fails closed
    assert r.unmeasured is False  # MEASURED residual → FAILED, not UNVERIFIED


def test_post_condition_open_position_closed_when_flat(monkeypatch) -> None:
    from types import SimpleNamespace

    from almanak.connectors.gmx_v2 import teardown_post_condition as tpc

    monkeypatch.setattr(tpc, "read_open_positions", lambda *a, **k: SimpleNamespace(ok=True, positions=[]))
    r = tpc.gmx_v2_teardown_post_condition(_open_perp_position(), _WALLET, _FakeGateway())
    assert r.closed is True


def test_post_condition_open_position_unmeasured_failclosed(monkeypatch) -> None:
    from types import SimpleNamespace

    from almanak.connectors.gmx_v2 import teardown_post_condition as tpc

    monkeypatch.setattr(tpc, "read_open_positions", lambda *a, **k: SimpleNamespace(ok=False, positions=[]))
    r = tpc.gmx_v2_teardown_post_condition(_open_perp_position(), _WALLET, _FakeGateway())
    # getAccountPositions read fault → UNVERIFIED, never FAILED (VIB-5573).
    assert r.closed is False and r.error
    assert r.unmeasured is True


# ---------------------------------------------------------------------------
# 4) Framework residual union + fail-loud completeness.
# ---------------------------------------------------------------------------
def test_union_residuals_is_additive() -> None:
    summary = TeardownPositionSummary.empty("deployment:x")
    residual = _pending_order_position()
    merged = _union_residuals(summary, [residual])
    assert len(merged.positions) == 1
    assert merged.positions[0].position_id == _ORDER_KEY
    # Idempotent: re-unioning the same residual does not double it.
    merged2 = _union_residuals(merged, [residual])
    assert len(merged2.positions) == 1


def test_pending_order_residual_is_uncovered_without_cancel_intent() -> None:
    # The core VIB-5116 fail-loud property: a discovered pending order has no
    # closing intent (there is no cancel verb yet), so completeness flags it
    # uncovered -> the teardown cannot report success while capital is stranded.
    residual = _pending_order_position()
    report = check_intent_coverage([residual], intents=[])
    assert not report.complete
    assert any(p.position_id == _ORDER_KEY for p in report.uncovered)


# ---------------------------------------------------------------------------
# 5) Framework-level scoping: chain-scoped, self-scoping via on-chain count.
# ---------------------------------------------------------------------------
class _FakeStrategy:
    def __init__(self, chains: list[str], gateway: Any) -> None:
        self._chains = chains
        self._gateway_client = gateway
        self.wallet_address = _WALLET

    @property
    def chains(self) -> list[str]:
        return self._chains


def test_framework_discovery_surfaces_gmx_residual_on_arbitrum() -> None:
    from almanak.framework.teardown.residual_discovery import discover_teardown_residuals

    positions = discover_teardown_residuals(_FakeStrategy(["arbitrum"], _FakeGateway()))
    assert len(positions) == 1
    assert positions[0].protocol == "gmx_v2"
    assert positions[0].position_id.lower() == _ORDER_KEY
    assert positions[0].details["source"] == "teardown_residual_discovery"


def test_framework_discovery_skips_non_gmx_chain() -> None:
    # A Base-only strategy never issues an Arbitrum/Avalanche GMX read.
    from almanak.framework.teardown.residual_discovery import discover_teardown_residuals

    gw = _FakeGateway()
    positions = discover_teardown_residuals(_FakeStrategy(["base"], gw))
    assert positions == []
    assert gw.calls == []  # no read issued at all


def test_framework_discovery_measured_empty_surfaces_nothing() -> None:
    # An Arbitrum strategy with no GMX orders (count=0) surfaces nothing — the
    # read is self-scoping (a non-GMX wallet simply has zero orders).
    from almanak.framework.teardown.residual_discovery import discover_teardown_residuals

    positions = discover_teardown_residuals(_FakeStrategy(["arbitrum"], _FakeGateway(count=0)))
    assert positions == []


def test_framework_discovery_no_gateway_undeterminable_fails_closed() -> None:
    # A strategy with no usable metadata (undeterminable connector usage) + no
    # gateway on a GMX chain fails closed (safe default) — C1 only relaxes the
    # fail-closed for strategies that PROVABLY do not use the connector.
    from almanak.framework.teardown.residual_discovery import discover_teardown_residuals

    positions = discover_teardown_residuals(_FakeStrategy(["arbitrum"], None))
    assert len(positions) == 1
    assert positions[0].details["kind"] == "residual_unverified"


def test_framework_discovery_unmeasured_read_surfaces_loud_sentinel() -> None:
    # Guardrail #2 (the exact VIB-5116 bug): an UNMEASURED read (count=None here)
    # must NOT be treated as "no residuals" and silently pass — the framework
    # surfaces a LOUD closure-failing sentinel so the teardown is not trusted as
    # complete. Fail-closed-loud is safe under teardown's inverted failure
    # semantics (loud, never blocks the next risk-reducing intent).
    from almanak.framework.teardown.residual_discovery import discover_teardown_residuals

    positions = discover_teardown_residuals(_FakeStrategy(["arbitrum"], _FakeGateway(count=None)))
    assert len(positions) == 1
    assert positions[0].protocol == "gmx_v2"
    assert positions[0].details["kind"] == "residual_unverified"
    # The sentinel is an enforceable PERP position with no closing intent, so the
    # completeness gate flags it uncovered -> teardown fails loud.
    report = check_intent_coverage(positions, intents=[])
    assert not report.complete


def test_framework_discovery_hook_crash_surfaces_sentinel(monkeypatch) -> None:
    # A misbehaving connector hook that RAISES must also fail-closed-loud, never
    # silently drop the sweep.
    from almanak.framework.teardown import residual_discovery as rd

    def _boom(**_kw: Any) -> Any:
        raise RuntimeError("hook exploded")

    monkeypatch.setattr(rd, "get_teardown_residual_discovery", lambda _p: _boom)
    positions = rd.discover_teardown_residuals(_FakeStrategy(["arbitrum"], _FakeGateway()))
    assert len(positions) == 1
    assert positions[0].details["kind"] == "residual_unverified"


def test_framework_discovery_survives_raising_compiler_property() -> None:
    # VIB-5116 real-fork regression: IntentStrategy.compiler is a PROPERTY that
    # RAISES RuntimeError when no compiler is configured — which is the live runner
    # teardown path (the runner owns the compiler, never assigns strategy._compiler).
    # A naive getattr(strategy, "compiler", None) does NOT swallow RuntimeError, so
    # it crashed the whole enumeration before the pending order could be surfaced.
    # Discovery must read _compiler directly / never raise, and still surface the
    # strand via the strategy's own _gateway_client.
    from almanak.framework.teardown.residual_discovery import discover_teardown_residuals

    class _StrategyRaisingCompiler:
        _compiler = None
        chains = ["arbitrum"]
        wallet_address = _WALLET

        def __init__(self, gw: Any) -> None:
            self._gateway_client = gw

        @property
        def compiler(self) -> Any:
            raise RuntimeError("IntentCompiler not configured. The StrategyRunner creates its own compiler.")

    positions = discover_teardown_residuals(_StrategyRaisingCompiler(_FakeGateway()))
    assert len(positions) == 1
    assert positions[0].position_id.lower() == _ORDER_KEY


# ---------------------------------------------------------------------------
# 6) Audit round (VIB-5116 #3130): C1–C5 regression tests.
# ---------------------------------------------------------------------------
from types import SimpleNamespace  # noqa: E402


class _MetaStrategy:
    """A strategy with STATIC STRATEGY_METADATA (declared connector usage)."""

    def __init__(self, meta: Any, gateway: Any, chains: list[str] | None = None) -> None:
        self.STRATEGY_METADATA = meta
        self._gateway_client = gateway
        self._chains = chains or ["arbitrum"]
        self.wallet_address = _WALLET

    @property
    def chains(self) -> list[str]:
        return self._chains


_GMX_META = SimpleNamespace(supported_protocols=["gmx_v2"], intent_types=["PERP_OPEN", "PERP_CLOSE"])
_NON_GMX_META = SimpleNamespace(supported_protocols=["uniswap_v3"], intent_types=["LP_OPEN", "LP_CLOSE"])


def test_c1_unmeasured_read_fails_closed_only_for_declared_gmx_users() -> None:
    from almanak.framework.teardown.residual_discovery import discover_teardown_residuals

    # GMX-declaring strategy + unmeasured read → loud fail-closed sentinel.
    used = discover_teardown_residuals(_MetaStrategy(_GMX_META, _FakeGateway(count=None)))
    assert len(used) == 1 and used[0].details["kind"] == "residual_unverified"

    # A strategy that provably does NOT use gmx_v2 (no protocol, no PERP intents) +
    # unmeasured read → NO sentinel (only a chain-overlap probe; kills false-FAIL).
    unused = discover_teardown_residuals(_MetaStrategy(_NON_GMX_META, _FakeGateway(count=None)))
    assert unused == []


def test_c1_undeterminable_metadata_defaults_to_fail_closed() -> None:
    from almanak.framework.teardown.residual_discovery import discover_teardown_residuals

    # No usable metadata → undeterminable → safe default is fail-closed.
    no_meta = discover_teardown_residuals(
        _MetaStrategy(SimpleNamespace(supported_protocols=[], intent_types=[]), _FakeGateway(count=None))
    )
    assert len(no_meta) == 1 and no_meta[0].details["kind"] == "residual_unverified"


def test_c1_no_gateway_fails_closed_for_gmx_user_only() -> None:
    from almanak.framework.teardown.residual_discovery import discover_teardown_residuals

    gmx = discover_teardown_residuals(_MetaStrategy(_GMX_META, None))
    assert len(gmx) == 1 and gmx[0].details["kind"] == "residual_unverified"
    non_gmx = discover_teardown_residuals(_MetaStrategy(_NON_GMX_META, None))
    assert non_gmx == []


def test_c1_measured_orders_surface_regardless_of_declaration() -> None:
    # Defense-in-depth: a MEASURED pending order is surfaced even if the strategy
    # did not declare gmx_v2 (a wallet that somehow holds GMX orders must not hide).
    from almanak.framework.teardown.residual_discovery import discover_teardown_residuals

    positions = discover_teardown_residuals(_MetaStrategy(_NON_GMX_META, _FakeGateway()))
    assert len(positions) == 1
    assert positions[0].position_id.lower() == _ORDER_KEY
    assert positions[0].details["kind"] == "pending_order"


def test_c2_decode_row_failure_fails_whole_decode(monkeypatch) -> None:
    # A row that fails conversion must fail the WHOLE decode to None (no silent
    # partial list that would DROP the failed row's pending order).
    from almanak.connectors.gmx_v2 import orders_read as _or

    def _boom(_addr: Any) -> str:
        raise ValueError("bad address")

    monkeypatch.setattr(_or, "to_checksum_address", _boom)
    assert _or.decode_account_orders(_REAL_ORDERS_BLOB) is None  # not a partial []


def test_c3_union_keeps_same_shape_residuals_from_distinct_protocols() -> None:
    from decimal import Decimal

    p_gmx = PositionInfo(PositionType.PERP, "0xkey", "arbitrum", "gmx_v2", Decimal("0"), {"kind": "pending_order"})
    p_other = PositionInfo(
        PositionType.PERP, "0xkey", "arbitrum", "other_perp", Decimal("0"), {"kind": "pending_order"}
    )
    merged = _union_residuals(TeardownPositionSummary.empty("d"), [p_gmx, p_other])
    # Same (chain, type, position_id) but different protocol → BOTH kept.
    assert len(merged.positions) == 2
    assert {p.protocol for p in merged.positions} == {"gmx_v2", "other_perp"}


def test_c4_truncated_read_flags_and_post_condition_fails_closed_on_notfound() -> None:
    from almanak.connectors.gmx_v2.orders_read import MAX_ORDER_RANGE
    from almanak.connectors.gmx_v2.teardown_reads import read_pending_orders

    # count beyond one window → truncated=True (aggregate still fires on count>0).
    gw = _FakeGateway(count=MAX_ORDER_RANGE + 5)
    result = read_pending_orders(gw, "arbitrum", _WALLET)
    assert result.ok is True and result.truncated is True

    # A key ABSENT from the truncated partial set must NOT read as closed.
    pos = PositionInfo(
        PositionType.PERP,
        "0xbeyondwindowkey",
        "arbitrum",
        "gmx_v2",
        __import__("decimal").Decimal("0"),
        {"kind": "pending_order", "order_key": "0xbeyondwindowkey"},
    )
    check = gmx_v2_teardown_post_condition(pos, _WALLET, gw)
    # TRUNCATED window: the key may lie beyond the partial set → cannot measure →
    # UNVERIFIED, not a fabricated residual → FAILED (VIB-5573).
    assert check.closed is False and check.error
    assert check.unmeasured is True


def test_c5_pending_order_residual_never_covered_by_perp_close() -> None:
    from almanak.framework.intents import Intent

    pending = PositionInfo(
        PositionType.PERP,
        "0xorderkey",
        "arbitrum",
        "gmx_v2",
        __import__("decimal").Decimal("0"),
        {"kind": "pending_order", "market": "ETH-USD", "is_long": True},
    )
    # A PERP_CLOSE on the same market+side must NOT cover the pending ORDER.
    close = Intent.perp_close(
        protocol="gmx_v2", market="ETH-USD", collateral_token="USDC", is_long=True, chain="arbitrum"
    )
    report = check_intent_coverage([pending], [close])
    assert not report.complete
    assert any(p.position_id == "0xorderkey" for p in report.uncovered)


# ---------------------------------------------------------------------------
# 7) The perp seam registers an on-chain closure post-condition (TD-21 S6).
# ---------------------------------------------------------------------------
def test_gmx_v2_registers_teardown_post_condition() -> None:
    assert has_teardown_post_condition("gmx_v2")
