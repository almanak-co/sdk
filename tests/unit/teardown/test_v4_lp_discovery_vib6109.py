"""VIB-6109: Uniswap V4 teardown LP discovery (recovery-lane coverage).

The V3 wallet-scan (``balanceOf`` → ``tokenOfOwnerByIndex`` → ``positions``)
cannot enumerate Uniswap V4 positions: the V4 PositionManager is an ERC-721 that
is NOT ``ERC721Enumerable`` (verified on-chain: ``supportsInterface(0x780e9d63)``
is false and ``tokenOfOwnerByIndex`` reverts). Teardown recovery instead VERIFIES
the deployment's provable ``ownership.token_ids`` against the V4 PositionManager
via ``ownerOf`` + ``getPositionLiquidity`` (no ``StateView.getSlot0`` dependency).

These tests exercise ``discover_v4_lp_positions`` and its integration into
``discover_lp_positions`` + the recovery merge:

* an owned, still-open V4 id is surfaced as ``protocol='uniswap_v4'``;
* a burned/never-minted id (``ownerOf`` reverts ``NOT_MINTED``) is a measured skip;
* a drained id (``getPositionLiquidity == 0``) is skipped;
* an id owned by a different address is skipped;
* an UNMEASURED read on an OWNED id raises ``DiscoveryIncomplete`` (strict) —
  the orphan-risk this module exists to prevent;
* the recovery merge turns a discovered-open owned V4 id into an
  ``LP_CLOSE(protocol='uniswap_v4')``.
"""

from __future__ import annotations

import json
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from almanak.framework.teardown.discovery import (
    DiscoveryIncomplete,
    discover_lp_positions,
    discover_v4_lp_positions,
)
from almanak.framework.teardown.lp_recovery import (
    DeploymentLpOwnership,
    LpDiscoveryResult,
    merge_discovered_lp,
)
from almanak.framework.teardown.models import (
    PositionInfo,
    PositionType,
    TeardownMode,
    TeardownPositionSummary,
)

CHAIN = "base"
# Base V4 PositionManager (almanak/connectors/uniswap_v4/addresses.py).
BASE_V4_PM = "0x7c5f5a4bbd8fd63184577525326123b519429bdc"
WALLET = "0x1111111111111111111111111111111111111111"
OTHER = "0x2222222222222222222222222222222222222222"

_SEL_BALANCE_OF = "0x70a08231"
_SEL_OWNER_OF = "0x6352211e"
_SEL_V4_LIQUIDITY = "0x1efeed33"


def _rpc_response(result: str | None = None, success: bool = True, error: str = "") -> SimpleNamespace:
    return SimpleNamespace(
        success=success,
        result=json.dumps(result) if result is not None else "",
        error=error,
    )


def _word(addr: str) -> str:
    return "0x" + addr.replace("0x", "").lower().zfill(64)


class _FakeV4Rpc:
    """Dispatches ownerOf / getPositionLiquidity (and balanceOf → 0) by selector.

    ``owners``: tokenId → owner address; a tokenId ABSENT from the map (or mapped
    to ``None``) reverts ``NOT_MINTED`` (measured burned/not-here).
    ``liquidity``: tokenId → uint128 liquidity.
    ``owner_faults`` / ``liq_faults``: tokenIds whose ownerOf / liquidity read
    returns a transport FAULT (unmeasured).
    """

    def __init__(
        self,
        owners: dict[int, str],
        liquidity: dict[int, int],
        owner_faults: set[int] | None = None,
        liq_faults: set[int] | None = None,
    ) -> None:
        self.owners = owners
        self.liquidity = liquidity
        self.owner_faults = owner_faults or set()
        self.liq_faults = liq_faults or set()
        self.calls: list[tuple[str, int]] = []

    def Call(self, request, timeout=15.0):  # noqa: ARG002
        parsed = json.loads(request.params)
        calldata = parsed[0]["data"]
        token_id = int(calldata[-64:], 16)
        self.calls.append((calldata[:10], token_id))
        if calldata.startswith(_SEL_BALANCE_OF):
            return _rpc_response("0x0")  # V3 NPMs: wallet holds nothing
        if calldata.startswith(_SEL_OWNER_OF):
            if token_id in self.owner_faults:
                return _rpc_response(success=False, error="rpc down")
            owner = self.owners.get(token_id)
            if owner is None:
                return _rpc_response(success=False, error="execution reverted: NOT_MINTED")
            return _rpc_response(_word(owner))
        if calldata.startswith(_SEL_V4_LIQUIDITY):
            if token_id in self.liq_faults:
                return _rpc_response(success=False, error="rpc down")
            return _rpc_response(hex(self.liquidity.get(token_id, 0)))
        return _rpc_response(success=False, error="unexpected calldata")


def _client(rpc: _FakeV4Rpc) -> MagicMock:
    c = MagicMock()
    c.rpc = rpc
    return c


# ---------------------------------------------------------------------------
# discover_v4_lp_positions
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_owned_open_position_surfaced() -> None:
    rpc = _FakeV4Rpc(owners={2359: WALLET}, liquidity={2359: 12345})
    out = await discover_v4_lp_positions(_client(rpc), CHAIN, WALLET, {"2359"})
    assert len(out) == 1
    pos = out[0]
    assert pos.token_id == 2359
    assert pos.protocol == "uniswap_v4"
    assert pos.liquidity == 12345
    assert pos.npm_address.lower() == BASE_V4_PM


@pytest.mark.asyncio
async def test_not_minted_is_measured_skip_no_raise() -> None:
    # 2359 not in owners → ownerOf reverts NOT_MINTED → measured "not here".
    rpc = _FakeV4Rpc(owners={}, liquidity={})
    out = await discover_v4_lp_positions(_client(rpc), CHAIN, WALLET, {"2359"})
    assert out == []


@pytest.mark.asyncio
async def test_drained_position_skipped() -> None:
    rpc = _FakeV4Rpc(owners={2359: WALLET}, liquidity={2359: 0})
    out = await discover_v4_lp_positions(_client(rpc), CHAIN, WALLET, {"2359"})
    assert out == []


@pytest.mark.asyncio
async def test_owned_by_other_wallet_skipped() -> None:
    rpc = _FakeV4Rpc(owners={2359: OTHER}, liquidity={2359: 999})
    out = await discover_v4_lp_positions(_client(rpc), CHAIN, WALLET, {"2359"})
    assert out == []


@pytest.mark.asyncio
async def test_ownerof_fault_on_owned_raises_incomplete_strict() -> None:
    rpc = _FakeV4Rpc(owners={2359: WALLET}, liquidity={2359: 5}, owner_faults={2359})
    with pytest.raises(DiscoveryIncomplete):
        await discover_v4_lp_positions(_client(rpc), CHAIN, WALLET, {"2359"}, strict=True)


@pytest.mark.asyncio
async def test_liquidity_fault_on_owned_raises_incomplete_strict() -> None:
    rpc = _FakeV4Rpc(owners={2359: WALLET}, liquidity={2359: 5}, liq_faults={2359})
    with pytest.raises(DiscoveryIncomplete):
        await discover_v4_lp_positions(_client(rpc), CHAIN, WALLET, {"2359"}, strict=True)


@pytest.mark.asyncio
async def test_fault_non_strict_returns_partial_with_warning(caplog) -> None:
    rpc = _FakeV4Rpc(owners={1: WALLET, 2: WALLET}, liquidity={1: 5, 2: 7}, liq_faults={2})
    out = await discover_v4_lp_positions(_client(rpc), CHAIN, WALLET, {"1", "2"}, strict=False)
    assert {p.token_id for p in out} == {1}  # id 1 surfaced, id 2 unreadable but not fatal


@pytest.mark.asyncio
async def test_no_candidates_returns_empty() -> None:
    rpc = _FakeV4Rpc(owners={2359: WALLET}, liquidity={2359: 5})
    out = await discover_v4_lp_positions(_client(rpc), CHAIN, WALLET, set())
    assert out == []
    assert rpc.calls == []  # no reads issued


@pytest.mark.asyncio
async def test_unknown_chain_no_v4_pm_returns_empty() -> None:
    rpc = _FakeV4Rpc(owners={2359: WALLET}, liquidity={2359: 5})
    out = await discover_v4_lp_positions(_client(rpc), "chain-with-no-v4", WALLET, {"2359"})
    assert out == []
    assert rpc.calls == []


@pytest.mark.asyncio
async def test_unparseable_candidate_id_raises_incomplete_strict() -> None:
    # An unparseable id is a deployment-owned candidate we cannot verify — in
    # strict mode that is an orphan, exactly what DiscoveryIncomplete guards.
    # It must NOT be silently dropped, even when a valid owned id is also present.
    rpc = _FakeV4Rpc(owners={2359: WALLET}, liquidity={2359: 5})
    with pytest.raises(DiscoveryIncomplete) as exc:
        await discover_v4_lp_positions(
            _client(rpc), CHAIN, WALLET, {"2359", "not-a-number"}, strict=True
        )
    assert "not-a-number" in str(exc.value)


@pytest.mark.asyncio
async def test_unparseable_candidate_id_non_strict_warns_and_keeps_valid(caplog) -> None:
    rpc = _FakeV4Rpc(owners={2359: WALLET}, liquidity={2359: 5})
    with caplog.at_level("WARNING"):
        out = await discover_v4_lp_positions(
            _client(rpc), CHAIN, WALLET, {"2359", "not-a-number"}, strict=False
        )
    assert {p.token_id for p in out} == {2359}  # valid id still verified + surfaced
    assert "not parseable" in caplog.text
    assert "not-a-number" in caplog.text


@pytest.mark.asyncio
async def test_candidate_ids_ordered_numerically_not_lexicographically() -> None:
    # Lexicographic order would give 10, 100, 9; the documented output order is
    # (protocol, PositionManager, tokenId) numerically ascending.
    rpc = _FakeV4Rpc(
        owners={9: WALLET, 10: WALLET, 100: WALLET},
        liquidity={9: 1, 10: 2, 100: 3},
    )
    out = await discover_v4_lp_positions(_client(rpc), CHAIN, WALLET, {"10", "100", "9"})
    assert [p.token_id for p in out] == [9, 10, 100]


# ---------------------------------------------------------------------------
# discover_lp_positions integration (V3 walk + V4 candidate pass)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_discover_lp_positions_threads_candidates_to_v4() -> None:
    # V3 NPMs on base report 0 (balanceOf → 0x0); V4 candidate is open.
    rpc = _FakeV4Rpc(owners={2359: WALLET}, liquidity={2359: 42})
    out = await discover_lp_positions(
        client=_client(rpc),
        chain=CHAIN,
        wallet=WALLET,
        strict=True,
        candidate_token_ids={"2359"},
    )
    assert [(p.protocol, p.token_id) for p in out] == [("uniswap_v4", 2359)]


@pytest.mark.asyncio
async def test_discover_lp_positions_no_candidates_skips_v4() -> None:
    rpc = _FakeV4Rpc(owners={2359: WALLET}, liquidity={2359: 42})
    out = await discover_lp_positions(client=_client(rpc), chain=CHAIN, wallet=WALLET, strict=True)
    # No candidate ids → V4 pass skipped entirely; no ownerOf reads issued.
    assert out == []
    assert all(sel != _SEL_OWNER_OF for sel, _ in rpc.calls)


# ---------------------------------------------------------------------------
# recovery merge produces an LP_CLOSE(protocol='uniswap_v4')
# ---------------------------------------------------------------------------


def test_merge_produces_v4_lp_close_for_owned_open_position() -> None:
    from decimal import Decimal

    summary = TeardownPositionSummary(
        deployment_id="deployment:v4test",
        timestamp=None,
        positions=[
            PositionInfo(
                position_type=PositionType.LP,
                position_id="2359",
                chain=CHAIN,
                protocol="uniswap_v4",
                value_usd=Decimal("0"),
                details={"npm_address": BASE_V4_PM},
            )
        ],
    )
    result = LpDiscoveryResult(summary=summary, incomplete=False)
    ownership = DeploymentLpOwnership(token_ids=frozenset({"2359"}), had_lp_open=True, available=True)

    outcome = merge_discovered_lp(
        positions=TeardownPositionSummary(deployment_id="deployment:v4test", timestamp=None, positions=[]),
        intents=[],
        discovery=result,
        ownership=ownership,
        mode=TeardownMode.SOFT,
    )
    assert outcome.recovered_count == 1
    assert len(outcome.intents) == 1
    intent = outcome.intents[0]
    assert str(getattr(intent, "protocol", "")).lower() == "uniswap_v4"
    assert str(getattr(intent, "position_id", "")) == "2359"
