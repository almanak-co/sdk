"""VIB-5631 — V3-NPM post-close verification: burned NPM position is measured-closed.

The reproduced contradiction (sushiswap_v3 / ethereum, NPM
tokenId 3014): the position was PROVABLY closed on-chain — burn tx confirmed,
the TD-14 post-condition passed via the gateway's QueryPositionLiquidity
("invalid token id" folded to a MEASURED liquidity=0) — yet the POST-teardown
Plan-A reconciliation reported the position "chain-confirmed open" and TD-15
flipped the teardown to FAILED.

Root cause: ``chain_verify_lp_open`` walked EVERY registered V3-fork NPM on the
chain for the bare token id. NPM token ids are per-contract monotonic counters,
so after the sushi NFT was burned, uniswap_v3's ethereum NPM still answered
``positions(3014)`` with a *stranger's unrelated live position* → liquidity > 0
→ CONFIRMED_OPEN → FAILED. The verification layer cried wolf on a correct
teardown.

The fix (mirrors VIB-5634 for V4):

* The Plan-A read is PROTOCOL-SCOPED — only the position's own protocol NPM is
  consulted (``discovery.npm_for_protocol``); a foreign NPM's identically
  numbered token is a different position and must never influence the verdict.
* The read is TRI-STATE via the gateway's QueryPositionLiquidity — the SAME
  gateway read TD-14 trusts, so the two lanes cannot contradict each other on a
  burned position: liquidity>0 = open, liquidity==0 = MEASURED closed (the
  burned-NFT revert folds to 0), None = read FAULT (unmeasured — Empty ≠ Zero,
  never silently "closed").
* Composition still only ever downgrades: a genuine residual on the OWN NPM
  still flips FAILED (fail-closed preserved); a read fault keeps the TD-14
  verdict (UNVERIFIABLE is a post-teardown no-op), never FAILED.

The family-level TD-14 hook (``_uniswap_v3_post_condition``) is exercised per
V3-NPM protocol — uniswap_v3, sushiswap_v3, pancakeswap_v3 — over the
closed / residual / unmeasured / bad-tokenId matrix, mirroring
``test_uniswap_v4_post_condition.py``.
"""

from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal
from types import SimpleNamespace

import pytest

from almanak.connectors._strategy_base.address_registry import AddressRegistry
from almanak.framework.teardown.models import (
    ClosureVerification,
    PositionInfo,
    PositionType,
    TeardownPositionSummary,
    VerificationStatus,
)
from almanak.framework.teardown.post_conditions import (
    _uniswap_v3_post_condition,
    get_teardown_post_condition,
)
from almanak.framework.teardown.teardown_manager import TeardownManager

WALLET = "0x5c63424B4B4b4B4b4b4B4b4B4B4b4b4b4B4B4b4B"

# One (protocol, chain) pair per V3-NPM family member with a live NPM
# deployment; addresses resolve through the connector-owned AddressRegistry so
# these tests can never drift from the emitted read targets.
V3_NPM_CASES = [
    ("uniswap_v3", "ethereum"),
    ("sushiswap_v3", "ethereum"),
    ("pancakeswap_v3", "bsc"),
]


def _npm(protocol: str, chain: str) -> str:
    address = AddressRegistry.resolve_contract_address(protocol, chain, ("position_manager", "nft"))
    assert address, f"expected a registered NPM for {protocol} on {chain}"
    return address


class _FakeGatewayClient:
    """GatewayClient double for the typed NPM reads.

    ``liquidity_by_npm`` maps a lowercased NPM address to what
    ``query_position_liquidity`` reports for it (``None`` = read fault). A
    query against an NPM absent from the map fails the test loudly — the
    protocol-scoped read must never consult a foreign protocol's NPM.
    """

    is_connected = True

    def __init__(self, liquidity_by_npm: dict[str, int | None]):
        self._by_npm = {k.lower(): v for k, v in liquidity_by_npm.items()}
        self.queried_npms: list[str] = []

    def query_position_liquidity(self, *, chain, position_manager, token_id, block=None):
        self.queried_npms.append(position_manager.lower())
        assert position_manager.lower() in self._by_npm, (
            f"query_position_liquidity consulted an unexpected NPM {position_manager} — "
            "the read must be scoped to the position's own protocol NPM (VIB-5631)"
        )
        return self._by_npm[position_manager.lower()]

    def query_position_tokens_owed(self, *, chain, position_manager, token_id, block=None):
        liquidity = self._by_npm.get(position_manager.lower())
        if liquidity is None:
            return None  # fault lane mirrors the liquidity read
        return (0, 0)


def _hook_position(protocol: str, chain: str, position_id: str = "3014") -> SimpleNamespace:
    return SimpleNamespace(
        protocol=protocol,
        position_id=position_id,
        chain=chain,
        details={},
    )


# ---------------------------------------------------------------------------
# TD-14 family hook, per V3-NPM protocol: closed / residual / unmeasured /
# bad-tokenId (mirrors test_uniswap_v4_post_condition.py's matrix)
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(("protocol", "chain"), V3_NPM_CASES)
class TestV3NpmFamilyPostCondition:
    def test_burned_npm_position_is_measured_closed(self, protocol: str, chain: str) -> None:
        """A burned NFT's 'Invalid token ID' revert is folded by the gateway
        read into liquidity=0 / tokensOwed=(0,0) — a MEASUREMENT, not an error
        (Empty ≠ Zero). The hook must report closed, not unmeasured."""
        gw = _FakeGatewayClient({_npm(protocol, chain): 0})
        result = _uniswap_v3_post_condition(_hook_position(protocol, chain), WALLET, gateway_client=gw)
        assert result.closed is True
        assert result.unmeasured is False

    def test_measured_residual_is_not_closed(self, protocol: str, chain: str) -> None:
        gw = _FakeGatewayClient({_npm(protocol, chain): 123})
        result = _uniswap_v3_post_condition(_hook_position(protocol, chain), WALLET, gateway_client=gw)
        assert result.closed is False
        assert result.unmeasured is False
        assert result.residual.get("liquidity") == 123

    def test_read_fault_is_unmeasured_never_closed(self, protocol: str, chain: str) -> None:
        gw = _FakeGatewayClient({_npm(protocol, chain): None})
        result = _uniswap_v3_post_condition(_hook_position(protocol, chain), WALLET, gateway_client=gw)
        assert result.unmeasured is True
        assert result.closed is False

    def test_bad_token_id_is_unmeasured_never_closed(self, protocol: str, chain: str) -> None:
        gw = _FakeGatewayClient({_npm(protocol, chain): 0})
        position = _hook_position(protocol, chain, position_id="not-a-token-id")
        result = _uniswap_v3_post_condition(position, WALLET, gateway_client=gw)
        assert result.unmeasured is True
        assert result.closed is False
        assert gw.queried_npms == []  # nothing to read without a numeric id

    def test_slug_resolves_to_family_hook(self, protocol: str, chain: str) -> None:
        assert get_teardown_post_condition(protocol) is _uniswap_v3_post_condition


# ---------------------------------------------------------------------------
# The sushi contradiction, reconstructed at the verification seam
# (TeardownManager.verify_closure_against_chain) with the REAL Plan-A read.
# ---------------------------------------------------------------------------


def _mgr(gateway_client: _FakeGatewayClient) -> TeardownManager:
    mgr = TeardownManager()
    mgr.compiler = SimpleNamespace(_gateway_client=gateway_client, is_connected=True)
    return mgr


class _Strategy:
    deployment_id = "deployment:30eb3fe38724"
    _gateway_network = ""


def _lp_position(protocol: str, chain: str, position_id: str = "3014") -> PositionInfo:
    return PositionInfo(
        position_type=PositionType.LP,
        position_id=position_id,
        chain=chain,
        protocol=protocol,
        value_usd=Decimal("0"),
    )


def _summary(*positions: PositionInfo) -> TeardownPositionSummary:
    return TeardownPositionSummary(
        deployment_id="deployment:30eb3fe38724", timestamp=datetime.now(UTC), positions=list(positions)
    )


def _td14_passed(total: int = 1) -> ClosureVerification:
    """The reproduced TD-14 verdict: 1 position passed on-chain
    post-condition checks → all_closed=True, CHAIN_VERIFIED."""
    return ClosureVerification(
        all_closed=True,
        positions_total=total,
        positions_closed=total,
        has_position_breakdown=True,
        verification_status=VerificationStatus.CHAIN_VERIFIED,
    )


@pytest.mark.asyncio
@pytest.mark.parametrize(("protocol", "chain"), V3_NPM_CASES)
async def test_burned_position_stays_chain_verified_not_failed(protocol: str, chain: str) -> None:
    """THE VIB-5631 regression: TD-14 passed (burn measured), then the Plan-A
    re-read runs against a chain where FOREIGN V3-fork NPMs hold unrelated live
    positions under the same token id. The verdict must remain chain-verified
    closed — pre-fix this flipped to FAILED ('STILL OPEN on-chain')."""
    own_npm = _npm(protocol, chain)
    liquidity_by_npm: dict[str, int | None] = {own_npm: 0}  # burned: measured 0
    for other_protocol, _other_chain in V3_NPM_CASES:
        if other_protocol != protocol:
            other = AddressRegistry.resolve_contract_address(other_protocol, chain, ("position_manager", "nft"))
            if other:  # a foreign fork's NPM on the SAME chain: stranger's live token
                liquidity_by_npm[other.lower()] = 999_999
    gateway = _FakeGatewayClient(liquidity_by_npm)

    out = await _mgr(gateway).verify_closure_against_chain(
        _Strategy(),
        verification=_td14_passed(),
        pre_execution_positions=_summary(_lp_position(protocol, chain)),
        market=None,
    )

    assert out.all_closed is True
    assert out.verification_status is VerificationStatus.CHAIN_VERIFIED
    # And the read consulted ONLY the position's own NPM.
    assert set(gateway.queried_npms) == {own_npm.lower()}


@pytest.mark.asyncio
async def test_genuine_residual_on_own_npm_still_fails_closed() -> None:
    """Fail-safe direction preserved: liquidity measured on the position's OWN
    NPM after teardown is residual on-chain risk → FAILED (TD-15 AC-(a))."""
    gateway = _FakeGatewayClient({_npm("sushiswap_v3", "ethereum"): 7_982_551})
    out = await _mgr(gateway).verify_closure_against_chain(
        _Strategy(),
        verification=_td14_passed(),
        pre_execution_positions=_summary(_lp_position("sushiswap_v3", "ethereum")),
        market=None,
    )
    assert out.all_closed is False
    assert out.verification_status is VerificationStatus.FAILED


@pytest.mark.asyncio
async def test_read_fault_keeps_td14_verdict_never_failed() -> None:
    """A gateway/RPC fault on the Plan-A re-read is UNMEASURED — it must not be
    fabricated into 'closed' NOR into a residual: the TD-14 proof stands."""
    gateway = _FakeGatewayClient({_npm("sushiswap_v3", "ethereum"): None})
    out = await _mgr(gateway).verify_closure_against_chain(
        _Strategy(),
        verification=_td14_passed(),
        pre_execution_positions=_summary(_lp_position("sushiswap_v3", "ethereum")),
        market=None,
    )
    assert out.all_closed is True
    assert out.verification_status is VerificationStatus.CHAIN_VERIFIED
