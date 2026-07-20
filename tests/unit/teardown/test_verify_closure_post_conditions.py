"""TeardownManager._verify_closure dispatches to registered post-conditions.

VIB-3742: the verifier must run protocol-specific on-chain post-conditions
on the pre-execution position snapshot, not just re-read the strategy's
in-memory state. This test replaces the registered TJ V2 post-condition
with a stub so we can drive the closed/residual paths without a live fork.
"""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from almanak.connectors._strategy_base.teardown_post_condition import (
    # Registration is framework-internal (manifest-driven via
    # CONNECTOR.teardown_post_condition); tests reach the private seam to
    # swap/restore hooks without building a whole connector manifest.
    _register_teardown_post_condition,
)
from almanak.framework.teardown.models import VerificationStatus
from almanak.framework.teardown.post_conditions import (
    ClosureCheckResult,
    get_teardown_post_condition,
)
from almanak.framework.teardown.teardown_manager import TeardownManager


@pytest.fixture
def _restore_traderjoe_v2_hook():
    """Snapshot + restore the TJ V2 hook so test mutations don't leak.

    If the registry started empty (no real hook hydrated), the mock is removed
    on teardown rather than left installed — otherwise later tests could take
    the wrong verification path (CodeRabbit).
    """
    from almanak.connectors._strategy_base.teardown_post_condition import _REGISTRY

    original = get_teardown_post_condition("traderjoe_v2")
    yield
    if original is not None:
        _register_teardown_post_condition("traderjoe_v2", original)
    else:
        _REGISTRY.pop("traderjoe_v2", None)


def _make_position_snapshot(*positions) -> SimpleNamespace:
    return SimpleNamespace(positions=list(positions))


def _make_strategy(open_positions: list | None = None) -> MagicMock:
    strategy = MagicMock()
    strategy.wallet_address = "0xabc"
    strategy.get_open_positions.return_value = SimpleNamespace(
        positions=open_positions or []
    )
    return strategy


@pytest.mark.asyncio
async def test_verify_closure_passes_when_post_condition_returns_closed(
    _restore_traderjoe_v2_hook,
):
    hook = MagicMock(return_value=ClosureCheckResult(closed=True, protocol="traderjoe_v2"))
    _register_teardown_post_condition("traderjoe_v2", hook)

    mgr = TeardownManager()
    snapshot = _make_position_snapshot(
        SimpleNamespace(
            protocol="traderjoe_v2",
            position_id="pos-1",
            chain="avalanche",
            details={"pool_address": "0xpool", "bin_ids": [1, 2, 3]},
        )
    )

    result = await mgr._verify_closure(
        strategy=_make_strategy(),
        pre_execution_positions=snapshot,
    )

    assert result is True
    hook.assert_called_once()


@pytest.mark.asyncio
async def test_verify_closure_fails_when_post_condition_returns_residual(
    _restore_traderjoe_v2_hook,
):
    hook = MagicMock(
        return_value=ClosureCheckResult(
            closed=False,
            protocol="traderjoe_v2",
            position_id="pos-1",
            residual={"bin_balances": {100: 4567}, "total_lb_tokens": 4567},
        )
    )
    _register_teardown_post_condition("traderjoe_v2", hook)

    mgr = TeardownManager()
    snapshot = _make_position_snapshot(
        SimpleNamespace(
            protocol="traderjoe_v2",
            position_id="pos-1",
            chain="avalanche",
            details={"pool_address": "0xpool", "bin_ids": [100]},
        )
    )

    result = await mgr._verify_closure(
        strategy=_make_strategy(),
        pre_execution_positions=snapshot,
    )

    assert result is False


@pytest.mark.asyncio
async def test_verify_closure_treats_hook_raise_as_unmeasured_unverified(
    _restore_traderjoe_v2_hook,
):
    """VIB-5573 (Q7): a hook that RAISES is a read fault, not a measured residual.

    Pre-VIB-5573 a raising hook was fail-closed to FAILED. That fabricated a
    residual → hosted shutdown + entry latch on a transient gateway/RPC blip.
    Empty ≠ Zero: a raise means "could not measure" → UNMEASURED → UNVERIFIED
    (honest don't-know, non-blocking), NEVER FAILED. Only a *measured* residual
    is FAILED (see ``test_verify_closure_fails_when_post_condition_returns_residual``).
    """
    hook = MagicMock(side_effect=RuntimeError("boom"))
    _register_teardown_post_condition("traderjoe_v2", hook)

    mgr = TeardownManager()
    snapshot = _make_position_snapshot(
        SimpleNamespace(
            protocol="traderjoe_v2",
            position_id="pos-1",
            chain="avalanche",
            details={"pool_address": "0xpool"},
        )
    )

    # The bool wrapper: an unmeasured position is NOT a measured residual, so it
    # does not fail the closure (no fabricated FAILED on a transient fault).
    result = await mgr._verify_closure(
        strategy=_make_strategy(open_positions=[]),
        pre_execution_positions=snapshot,
    )
    assert result is True

    # The detailed verdict: honest UNVERIFIED, not CHAIN_VERIFIED (unmeasured is
    # never counted as chain-proven) and not FAILED (no measured residual).
    detailed = await mgr._verify_closure_detailed(
        strategy=_make_strategy(open_positions=[]),
        pre_execution_positions=snapshot,
    )
    assert detailed.all_closed is True
    assert detailed.verification_status is VerificationStatus.UNVERIFIED


@pytest.mark.asyncio
async def test_vib5936_hook_proven_ids_threaded_onto_verification(
    _restore_traderjoe_v2_hook,
):
    """VIB-5936: a hook that MEASURED the position closed lands its full identity on
    ``ClosureVerification.hook_proven_position_keys`` — the signal TD-15's fold uses
    to refuse re-opening it off a whole-account aggregate. An UNMEASURED result
    must NOT count as hook-proven (Empty ≠ Zero — an unproven closure cannot
    suppress a residual)."""
    hook = MagicMock(return_value=ClosureCheckResult(closed=True, protocol="traderjoe_v2"))
    _register_teardown_post_condition("traderjoe_v2", hook)

    mgr = TeardownManager()
    snapshot = _make_position_snapshot(
        SimpleNamespace(
            protocol="traderjoe_v2",
            position_id="pos-1",
            chain="avalanche",
            details={"pool_address": "0xpool", "bin_ids": [1]},
        )
    )
    detailed = await mgr._verify_closure_detailed(
        strategy=_make_strategy(open_positions=[]),
        pre_execution_positions=snapshot,
    )
    assert detailed.hook_proven_position_keys == (("traderjoe_v2", "avalanche", "pos-1"),)
    assert detailed.verification_status is VerificationStatus.CHAIN_VERIFIED

    # UNMEASURED (hook returned unmeasured=True) → NOT hook-proven.
    unmeasured_hook = MagicMock(
        return_value=ClosureCheckResult(closed=False, protocol="traderjoe_v2", unmeasured=True, error="rpc blip")
    )
    _register_teardown_post_condition("traderjoe_v2", unmeasured_hook)
    detailed = await mgr._verify_closure_detailed(
        strategy=_make_strategy(open_positions=[]),
        pre_execution_positions=snapshot,
    )
    assert detailed.hook_proven_position_keys == ()
    assert detailed.verification_status is VerificationStatus.UNVERIFIED


@pytest.mark.asyncio
async def test_verify_closure_falls_back_to_in_memory_when_no_snapshot():
    """Legacy path (no pre_execution_positions) — uses get_open_positions()."""
    mgr = TeardownManager()
    # Strategy still reports open positions -> fail.
    strategy_with_residual = _make_strategy(open_positions=[object()])
    assert (
        await mgr._verify_closure(strategy=strategy_with_residual)
    ) is False

    # Strategy reports nothing open -> pass.
    strategy_clean = _make_strategy(open_positions=[])
    assert (await mgr._verify_closure(strategy=strategy_clean)) is True


@pytest.mark.asyncio
async def test_verify_closure_detailed_marks_in_memory_fallback_as_no_breakdown():
    """VIB-5085: the in-memory fallback (no pre-execution snapshot) returns a
    ClosureVerification with ``has_position_breakdown=False`` so lifecycle
    callers DON'T trust ``positions_closed=0`` and fall back to the intent
    count — otherwise a balance-driven teardown that closed real positions but
    exposes no PositionInfo rows would persist ``positions_closed=0`` on
    success (the inverse of the bug)."""
    mgr = TeardownManager()

    clean = await mgr._verify_closure_detailed(strategy=_make_strategy(open_positions=[]))
    assert clean.all_closed is True
    assert clean.has_position_breakdown is False
    assert clean.positions_total == 0

    # The snapshot path, by contrast, DOES carry a trustworthy breakdown.
    snapshot = _make_position_snapshot(
        SimpleNamespace(
            protocol="some_unregistered_protocol",
            position_id="pos-x",
            chain="ethereum",
            details={},
        )
    )
    detailed = await mgr._verify_closure_detailed(
        strategy=_make_strategy(open_positions=[]),
        pre_execution_positions=snapshot,
    )
    assert detailed.all_closed is True
    assert detailed.has_position_breakdown is True
    assert detailed.positions_total == 1
    assert detailed.positions_closed == 1


@pytest.mark.asyncio
async def test_verify_closure_skips_protocols_without_post_condition(
    _restore_traderjoe_v2_hook,
):
    """A protocol with no registered hook does NOT block closure (logged only).

    The pre-existing in-memory check is still authoritative for those.
    """
    mgr = TeardownManager()
    snapshot = _make_position_snapshot(
        SimpleNamespace(
            protocol="some_unregistered_protocol",
            position_id="pos-x",
            chain="ethereum",
            details={},
        )
    )

    result = await mgr._verify_closure(
        strategy=_make_strategy(open_positions=[]),
        pre_execution_positions=snapshot,
    )

    # No registered hook AND in-memory state is empty AND no failures recorded
    # -> verifier accepts. (Documents the current behaviour: protocols
    # without a registered post-condition rely on the in-memory check.)
    assert result is True


@pytest.mark.asyncio
async def test_verify_closure_aggregates_multiple_position_failures(
    _restore_traderjoe_v2_hook,
):
    """If two TJ V2 positions both have residuals, verify reports False."""
    calls = []

    def hook(*, position, wallet_address, gateway_client=None, rpc_url=None, block=None):
        calls.append(position.position_id)
        return ClosureCheckResult(
            closed=False,
            protocol="traderjoe_v2",
            position_id=position.position_id,
            residual={"bin_balances": {1: 1}},
        )

    _register_teardown_post_condition("traderjoe_v2", hook)

    mgr = TeardownManager()
    snapshot = _make_position_snapshot(
        SimpleNamespace(
            protocol="traderjoe_v2",
            position_id="pos-1",
            chain="avalanche",
            details={"pool_address": "0xa"},
        ),
        SimpleNamespace(
            protocol="traderjoe_v2",
            position_id="pos-2",
            chain="avalanche",
            details={"pool_address": "0xb"},
        ),
    )

    result = await mgr._verify_closure(
        strategy=_make_strategy(),
        pre_execution_positions=snapshot,
    )

    assert result is False
    assert calls == ["pos-1", "pos-2"]


# ---------------------------------------------------------------------------
# VIB-2932 / VIB-5472: verification_status — closure confidence, not just count.
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_verification_status_chain_verified_when_every_position_has_hook(
    _restore_traderjoe_v2_hook,
):
    """Every pre-exec position confirmed by an on-chain post-condition -> CHAIN_VERIFIED."""
    hook = MagicMock(return_value=ClosureCheckResult(closed=True, protocol="traderjoe_v2"))
    _register_teardown_post_condition("traderjoe_v2", hook)

    mgr = TeardownManager()
    snapshot = _make_position_snapshot(
        SimpleNamespace(protocol="traderjoe_v2", position_id="pos-1", chain="avalanche", details={}),
        SimpleNamespace(protocol="traderjoe_v2", position_id="pos-2", chain="avalanche", details={}),
    )

    detailed = await mgr._verify_closure_detailed(
        strategy=_make_strategy(open_positions=[]),
        pre_execution_positions=snapshot,
    )

    assert detailed.all_closed is True
    assert detailed.positions_total == 2
    assert detailed.positions_closed == 2
    assert detailed.verification_status is VerificationStatus.CHAIN_VERIFIED


@pytest.mark.asyncio
async def test_verification_status_unverified_when_a_position_lacks_a_hook(
    _restore_traderjoe_v2_hook,
):
    """A no-hook position counted closed-by-execution -> UNVERIFIED (visible, not chain-proven).

    This is the VIB-2932 surface: the closure count is still reported (Aave / Morpho
    looping has no hook today), but the operator can see it was not chain-confirmed.
    """
    hook = MagicMock(return_value=ClosureCheckResult(closed=True, protocol="traderjoe_v2"))
    _register_teardown_post_condition("traderjoe_v2", hook)

    mgr = TeardownManager()
    snapshot = _make_position_snapshot(
        SimpleNamespace(protocol="traderjoe_v2", position_id="pos-1", chain="avalanche", details={}),
        SimpleNamespace(protocol="aave_v3", position_id="pos-2", chain="ethereum", details={}),
    )

    detailed = await mgr._verify_closure_detailed(
        strategy=_make_strategy(open_positions=[]),
        pre_execution_positions=snapshot,
    )

    assert detailed.all_closed is True
    assert detailed.positions_total == 2
    assert detailed.positions_closed == 2  # both counted closed (one chain, one by-execution)
    assert detailed.verification_status is VerificationStatus.UNVERIFIED


@pytest.mark.asyncio
async def test_verification_status_failed_on_residual(
    _restore_traderjoe_v2_hook,
):
    """Any residual on-chain liquidity -> FAILED (pairs with all_closed=False)."""
    hook = MagicMock(
        return_value=ClosureCheckResult(
            closed=False,
            protocol="traderjoe_v2",
            position_id="pos-1",
            residual={"bin_balances": {1: 1}},
        )
    )
    _register_teardown_post_condition("traderjoe_v2", hook)

    mgr = TeardownManager()
    snapshot = _make_position_snapshot(
        SimpleNamespace(protocol="traderjoe_v2", position_id="pos-1", chain="avalanche", details={}),
    )

    detailed = await mgr._verify_closure_detailed(
        strategy=_make_strategy(),
        pre_execution_positions=snapshot,
    )

    assert detailed.all_closed is False
    assert detailed.verification_status is VerificationStatus.FAILED


@pytest.mark.asyncio
async def test_verification_status_in_memory_fallback_is_unverified_or_failed():
    """The in-memory fallback never reads the chain: clear -> UNVERIFIED, residual -> FAILED."""
    mgr = TeardownManager()

    clean = await mgr._verify_closure_detailed(strategy=_make_strategy(open_positions=[]))
    assert clean.all_closed is True
    assert clean.has_position_breakdown is False
    assert clean.verification_status is VerificationStatus.UNVERIFIED

    residual = await mgr._verify_closure_detailed(strategy=_make_strategy(open_positions=[object()]))
    assert residual.all_closed is False
    assert residual.verification_status is VerificationStatus.FAILED


# ─── VIB-5795 / VIB-5896 — curve (fungible-LP default) through the dispatch ───

_CURVE_3CRV = "0x6c3F90f043a72FA612cbac8115EE7e52BDe6E490"


class _CurveFakeGateway:
    """Scripted ``query_erc20_balance`` double for the fungible-LP hook."""

    def __init__(self, balance):
        self._balance = balance

    def query_erc20_balance(self, *, chain, token_address, wallet_address, block=None):
        return self._balance


def _curve_position() -> SimpleNamespace:
    return SimpleNamespace(
        protocol="curve",
        position_id="curve-3pool-lp",
        chain="ethereum",
        details={"lp_token": _CURVE_3CRV},
    )


@pytest.mark.asyncio
async def test_curve_teardown_is_chain_verified_via_fungible_lp_default():
    """A closed Curve LP (3Crv balanceOf == 0) now reaches CHAIN_VERIFIED.

    Pre-fix, no hook was registered under ``curve`` so the dispatch skipped the
    position and the teardown was structurally pinned at UNVERIFIED — the
    20260718-0026 quant-test false-negative (VIB-5795 curve facet). Uses the
    REAL registered framework default, not a stub, so this also guards the
    fungible-LP registration itself.
    """
    mgr = TeardownManager()
    mgr.compiler = SimpleNamespace(gateway_client=_CurveFakeGateway(balance=0))

    detailed = await mgr._verify_closure_detailed(
        strategy=_make_strategy(open_positions=[]),
        pre_execution_positions=_make_position_snapshot(_curve_position()),
    )

    assert detailed.all_closed is True
    assert detailed.positions_total == 1
    assert detailed.positions_closed == 1
    assert detailed.verification_status is VerificationStatus.CHAIN_VERIFIED


@pytest.mark.asyncio
async def test_curve_teardown_residual_balance_is_failed():
    """A MEASURED residual 3Crv balance must surface as FAILED, not silently pass."""
    mgr = TeardownManager()
    mgr.compiler = SimpleNamespace(gateway_client=_CurveFakeGateway(balance=288_540_000_000_000_000_000))

    detailed = await mgr._verify_closure_detailed(
        strategy=_make_strategy(open_positions=[]),
        pre_execution_positions=_make_position_snapshot(_curve_position()),
    )

    assert detailed.all_closed is False
    assert detailed.verification_status is VerificationStatus.FAILED


@pytest.mark.asyncio
async def test_curve_teardown_unreadable_balance_is_unverified_not_failed():
    """A read fault (None after retry) lowers to UNVERIFIED — never a fabricated FAILED."""
    mgr = TeardownManager()
    mgr.compiler = SimpleNamespace(gateway_client=_CurveFakeGateway(balance=None))

    detailed = await mgr._verify_closure_detailed(
        strategy=_make_strategy(open_positions=[]),
        pre_execution_positions=_make_position_snapshot(_curve_position()),
    )

    assert detailed.all_closed is True
    assert detailed.verification_status is VerificationStatus.UNVERIFIED


class _LendingFakeGateway:
    """Scripted double for the lending hooks (VIB-5795).

    ``query_erc20_balance`` serves the ERC-4626 shares read (supply leg);
    ``eth_call`` serves ``debtOf`` (debt leg) with a single scripted word
    (``None`` = read fault).
    """

    def __init__(self, *, shares=0, debt_word=0):
        self._shares = shares
        self._debt_word = debt_word

    def query_erc20_balance(self, *, chain, token_address, wallet_address, block=None):
        return self._shares

    def eth_call(self, *, chain, to, data, block=None):
        if self._debt_word is None:
            return None
        return "0x" + format(self._debt_word, "064x")


def _make_lending_strategy() -> MagicMock:
    """Like ``_make_strategy`` but with a REAL-shaped wallet address.

    The lending debt read validates the wallet as an EVM address before
    encoding it into calldata; the ``"0xabc"`` stub would (correctly) resolve
    to unmeasured.
    """
    strategy = _make_strategy(open_positions=[])
    strategy.wallet_address = "0x" + "11" * 20
    return strategy


def _euler_supply_position() -> SimpleNamespace:
    return SimpleNamespace(
        protocol="euler_v2",
        position_id="euler_v2-collateral-WETH-ethereum",
        chain="ethereum",
        position_type="SUPPLY",
        details={"asset": "WETH", "type": "collateral"},
    )


def _euler_borrow_position() -> SimpleNamespace:
    return SimpleNamespace(
        protocol="euler_v2",
        position_id="euler_v2-borrow-USDC-ethereum",
        chain="ethereum",
        position_type="BORROW",
        details={"asset": "USDC", "type": "borrow"},
    )


@pytest.mark.asyncio
async def test_lending_teardown_both_legs_flat_is_chain_verified():
    """A flat euler_v2 account (0 shares, 0 debt) now reaches CHAIN_VERIFIED.

    Pre-fix, no hook was registered for any LENDING protocol so the dispatch
    skipped both positions and the teardown was structurally pinned at
    UNVERIFIED — the VIB-5795 field runs. Uses the REAL manifest-registered
    hook, so this also guards the registration itself.
    """
    mgr = TeardownManager()
    mgr.compiler = SimpleNamespace(gateway_client=_LendingFakeGateway(shares=0, debt_word=0))

    detailed = await mgr._verify_closure_detailed(
        strategy=_make_lending_strategy(),
        pre_execution_positions=_make_position_snapshot(
            _euler_supply_position(), _euler_borrow_position()
        ),
    )

    assert detailed.all_closed is True
    assert detailed.positions_total == 2
    assert detailed.positions_closed == 2
    assert detailed.verification_status is VerificationStatus.CHAIN_VERIFIED


@pytest.mark.asyncio
async def test_lending_teardown_residual_debt_is_failed_despite_clean_collateral():
    """THE debt-leg trap: clean collateral must not green a live debt leg.

    The supply position measures closed (0 shares) but the borrow position
    reads residual debt — the teardown must be FAILED, never CHAIN_VERIFIED.
    Kills the mutant that drops the BORROW dispatch branch.
    """
    mgr = TeardownManager()
    mgr.compiler = SimpleNamespace(gateway_client=_LendingFakeGateway(shares=0, debt_word=5_000_000))

    detailed = await mgr._verify_closure_detailed(
        strategy=_make_lending_strategy(),
        pre_execution_positions=_make_position_snapshot(
            _euler_supply_position(), _euler_borrow_position()
        ),
    )

    assert detailed.all_closed is False
    assert detailed.verification_status is VerificationStatus.FAILED


@pytest.mark.asyncio
async def test_lending_teardown_unmeasured_leg_is_unverified_not_failed():
    """A debt-read fault lowers to UNVERIFIED — never a fabricated FAILED."""
    mgr = TeardownManager()
    mgr.compiler = SimpleNamespace(gateway_client=_LendingFakeGateway(shares=0, debt_word=None))

    detailed = await mgr._verify_closure_detailed(
        strategy=_make_lending_strategy(),
        pre_execution_positions=_make_position_snapshot(
            _euler_supply_position(), _euler_borrow_position()
        ),
    )

    assert detailed.all_closed is True
    assert detailed.verification_status is VerificationStatus.UNVERIFIED


def _silo_supply_position() -> SimpleNamespace:
    return SimpleNamespace(
        protocol="silo_v2",
        position_id="silo_v2-supply-USDC-avalanche",
        chain="avalanche",
        position_type="SUPPLY",
        details={"asset": "USDC", "type": "deposit"},
    )


@pytest.mark.asyncio
async def test_silo_teardown_flat_supply_is_chain_verified():
    """A flat silo_v2 supply position reaches CHAIN_VERIFIED via the REAL hook.

    Same seam as the euler tests — guards silo_v2's own manifest registration
    and its multi-market catalogue read through the final verdict composition.
    """
    mgr = TeardownManager()
    mgr.compiler = SimpleNamespace(gateway_client=_LendingFakeGateway(shares=0, debt_word=0))

    detailed = await mgr._verify_closure_detailed(
        strategy=_make_lending_strategy(),
        pre_execution_positions=_make_position_snapshot(_silo_supply_position()),
    )

    assert detailed.all_closed is True
    assert detailed.positions_closed == 1
    assert detailed.verification_status is VerificationStatus.CHAIN_VERIFIED


@pytest.mark.asyncio
async def test_silo_teardown_residual_shares_is_failed():
    """A MEASURED silo share residual (worth > dust) surfaces as FAILED."""
    mgr = TeardownManager()

    class _SiloResidualGateway:
        def query_erc20_balance(self, *, chain, token_address, wallet_address, block=None):
            return 10**18  # live shares in every catalogued USDC silo

        def eth_call(self, *, chain, to, data, block=None):
            # convertToAssets echoes a material asset value.
            return "0x" + format(5_000_000, "064x")

    mgr.compiler = SimpleNamespace(gateway_client=_SiloResidualGateway())

    detailed = await mgr._verify_closure_detailed(
        strategy=_make_lending_strategy(),
        pre_execution_positions=_make_position_snapshot(_silo_supply_position()),
    )

    assert detailed.all_closed is False
    assert detailed.verification_status is VerificationStatus.FAILED


def _benqi_snapshot_blob(error: int, qi_balance: int, borrow_balance: int, exchange_rate: int) -> str:
    return "0x" + "".join(format(v, "064x") for v in (error, qi_balance, borrow_balance, exchange_rate))


class _BenqiSnapshotGateway:
    """eth_call double returning a scripted getAccountSnapshot blob."""

    def __init__(self, blob: str):
        self._blob = blob

    def eth_call(self, *, chain, to, data, block=None):
        return self._blob


def _benqi_positions() -> tuple[SimpleNamespace, SimpleNamespace]:
    supply = SimpleNamespace(
        protocol="benqi",
        position_id="benqi-collateral-AVAX-avalanche",
        chain="avalanche",
        position_type="SUPPLY",
        details={"asset": "AVAX", "type": "collateral"},
    )
    borrow = SimpleNamespace(
        protocol="benqi",
        position_id="benqi-borrow-USDC-avalanche",
        chain="avalanche",
        position_type="BORROW",
        details={"asset": "USDC", "type": "borrow"},
    )
    return supply, borrow


@pytest.mark.asyncio
async def test_benqi_teardown_flat_account_is_chain_verified_at_td14_seam():
    """A flat benqi account (0 qiTokens, 0 debt) is CHAIN_VERIFIED at TD-14.

    This is the seam the benqi real-fork Run 2 proved ("2 position(s) passed
    on-chain post-condition checks"); the OVERALL CLI verdict additionally
    passes through the TD-15 pre-reconcile gate, whose benqi price-injection
    gap is ticketed separately (VIB-5911) and is not this seam's authority.
    """
    mgr = TeardownManager()
    mgr.compiler = SimpleNamespace(
        gateway_client=_BenqiSnapshotGateway(_benqi_snapshot_blob(0, 0, 0, 20_000_000_000_000_000))
    )

    detailed = await mgr._verify_closure_detailed(
        strategy=_make_lending_strategy(),
        pre_execution_positions=_make_position_snapshot(*_benqi_positions()),
    )

    assert detailed.all_closed is True
    assert detailed.positions_closed == 2
    assert detailed.verification_status is VerificationStatus.CHAIN_VERIFIED


@pytest.mark.asyncio
async def test_benqi_teardown_residual_debt_is_failed():
    """Residual benqi borrow balance → FAILED even with zero qiToken collateral."""
    mgr = TeardownManager()
    mgr.compiler = SimpleNamespace(
        gateway_client=_BenqiSnapshotGateway(_benqi_snapshot_blob(0, 0, 123_456, 20_000_000_000_000_000))
    )

    detailed = await mgr._verify_closure_detailed(
        strategy=_make_lending_strategy(),
        pre_execution_positions=_make_position_snapshot(*_benqi_positions()),
    )

    assert detailed.all_closed is False
    assert detailed.verification_status is VerificationStatus.FAILED
