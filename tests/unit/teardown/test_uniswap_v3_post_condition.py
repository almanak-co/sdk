"""Unit tests for the Uniswap V3 (and V3-fork) teardown post-condition.

VIB-XXXX — fix(teardown): post-teardown verifier no longer flags burnt LP
NFTs as still-open. Pre-fix, after a successful Uniswap V3 LP_CLOSE
(decrease_liquidity → collect_fees → burn NFT), the post-teardown
verifier raised "Post-teardown verification failed: positions still
open. Manual check required." even though the on-chain truth was
"NFT burnt, wallet holds only USDC". Root cause: no Uniswap V3
post-condition was registered, so the verifier fell through to the
legacy ``strategy.get_open_positions()`` check — which still returned
the position because ``on_teardown_completed`` (where the strategy
clears its tracked ``_position_id``) only runs AFTER verification.

These tests assert:

1. The ``uniswap_v3`` slug (and its V3-fork siblings) now has a
   registered post-condition.
2. The hook returns ``closed=True`` when the on-chain query returns
   ``liquidity=0`` AND ``tokens_owed=(0, 0)`` — the canonical "NFT
   burnt" result that the gateway already folds the
   ``positions(tokenId)`` "Invalid token ID" revert into.
3. The hook returns ``closed=False`` when residual liquidity OR fees
   remain on-chain — operators must NOT see a clean signal in this
   case.
4. Gateway / RPC failures are fail-closed (``closed=False`` with an
   error string), never silently treated as "closed".
5. Integration through ``TeardownManager._verify_closure``: the
   end-to-end scenario from the April 30 audit (burnt NFT, strategy
   tracker still set) returns ``True`` and does NOT raise the
   false-positive.
"""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime
from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from almanak.framework.teardown.models import (
    PositionInfo,
    PositionType,
    TeardownPositionSummary,
)
from almanak.framework.teardown.post_conditions import (
    ClosureCheckResult,
    _uniswap_v3_post_condition,
    get_teardown_post_condition,
    has_teardown_post_condition,
    register_teardown_post_condition,
)
from almanak.framework.teardown.teardown_manager import TeardownManager

WALLET = "0x54776446Aa29Fc49d152B4850bD410eA1E4d24bF"
NPM_ARBITRUM = "0xC36442b4a4522E871399CD717aBDD847Ab11FE88"


def _make_position(
    *,
    chain: str = "arbitrum",
    protocol: str = "uniswap_v3",
    position_id: str = "5460223",
    details: dict | None = None,
) -> SimpleNamespace:
    return SimpleNamespace(
        protocol=protocol,
        position_id=position_id,
        chain=chain,
        details=details or {},
    )


def _make_gateway(
    *,
    liquidity: int | None = 0,
    tokens_owed: tuple[int, int] | None = (0, 0),
) -> MagicMock:
    gateway = MagicMock()
    gateway.is_connected = True
    gateway.query_position_liquidity.return_value = liquidity
    gateway.query_position_tokens_owed.return_value = tokens_owed
    return gateway


# ---------------------------------------------------------------------------
# Registry
# ---------------------------------------------------------------------------


class TestRegistry:
    def test_uniswap_v3_registered_by_default(self) -> None:
        assert has_teardown_post_condition("uniswap_v3")
        # Case-insensitive lookup matches the existing TJ V2 contract.
        hook = get_teardown_post_condition("Uniswap_V3")
        assert hook is not None

    @pytest.mark.parametrize(
        "slug",
        ["uniswap_v3", "agni_finance", "sushiswap_v3"],
    )
    def test_v3_forks_registered(self, slug: str) -> None:
        """All V3-fork slugs share the canonical NPM ABI and reuse the
        Uniswap V3 hook. Registering them under the same hook means a
        teardown of e.g. SushiSwap V3 LP NFTs also gets on-chain
        verification, not the legacy in-memory fallback. The slug set
        is restricted to protocols whose ``almanak.core.contracts``
        registry carries a ``position_manager`` address — registering a
        slug without an NPM would cause every teardown of that protocol
        to fail-closed.
        """
        assert has_teardown_post_condition(slug)
        # All V3-fork slugs resolve to the same callable.
        assert get_teardown_post_condition(slug) is _uniswap_v3_post_condition

    def test_pancakeswap_v3_not_registered(self) -> None:
        """PancakeSwap V3 has connector coverage for swaps but no NPM
        registered in ``contracts.py`` today. Registering the hook
        without an NPM would silently fail-closed every PancakeSwap V3
        teardown — worse than not registering. Documents the gap so a
        future NPM addition lands the registration in the same change.
        """
        assert not has_teardown_post_condition("pancakeswap_v3")

    def test_aerodrome_not_registered_by_uniswap_v3_hook(self) -> None:
        """Aerodrome volatile/stable pools use ERC-20 LP tokens, not NFTs.
        The Uniswap V3 hook's NPM ABI does not apply, so the slug must
        NOT register here. Documents the registration boundary so a
        future change that "helpfully" extends this list to Aerodrome
        catches a test."""
        assert not has_teardown_post_condition("aerodrome")


# ---------------------------------------------------------------------------
# Direct hook calls
# ---------------------------------------------------------------------------


class TestUniswapV3PostCondition:
    def test_closed_when_burnt_nft(self) -> None:
        """The April 30 audit case: NFT burnt → liquidity=0, tokens_owed=(0,0).

        The gateway already folds the ``positions(tokenId)`` "Invalid token
        ID" revert into a value-0 response, so the hook sees the same
        shape as a decrease-without-burn that left no fees behind. Either
        way the position is closed.
        """
        gateway = _make_gateway(liquidity=0, tokens_owed=(0, 0))
        position = _make_position()

        result = _uniswap_v3_post_condition(
            position=position,
            wallet_address=WALLET,
            gateway_client=gateway,
        )

        assert result.closed is True
        assert result.error is None
        assert result.residual == {}
        gateway.query_position_liquidity.assert_called_once_with(
            chain="arbitrum",
            position_manager=NPM_ARBITRUM,
            token_id=5460223,
        )
        gateway.query_position_tokens_owed.assert_called_once_with(
            chain="arbitrum",
            position_manager=NPM_ARBITRUM,
            token_id=5460223,
        )

    def test_closed_when_decremented_without_burn(self) -> None:
        """Decrease-without-burn path: NFT shell still owned but empty.
        Same closure shape as burnt — the wallet has nothing to recover.
        """
        gateway = _make_gateway(liquidity=0, tokens_owed=(0, 0))
        position = _make_position()

        result = _uniswap_v3_post_condition(
            position=position,
            wallet_address=WALLET,
            gateway_client=gateway,
        )
        assert result.closed is True

    def test_open_when_residual_liquidity(self) -> None:
        """LP_CLOSE silently failed to decrease liquidity → still open."""
        gateway = _make_gateway(liquidity=1_234_567, tokens_owed=(0, 0))
        position = _make_position()

        result = _uniswap_v3_post_condition(
            position=position,
            wallet_address=WALLET,
            gateway_client=gateway,
        )

        assert result.closed is False
        assert result.residual["liquidity"] == 1_234_567
        assert result.residual["tokens_owed0"] == 0
        assert result.residual["tokens_owed1"] == 0
        assert result.residual["token_id"] == 5460223
        assert result.residual["position_manager"] == NPM_ARBITRUM

    def test_open_when_residual_fees_only(self) -> None:
        """Decrease succeeded but collect_fees failed → fees still owed."""
        gateway = _make_gateway(liquidity=0, tokens_owed=(100, 200))
        position = _make_position()

        result = _uniswap_v3_post_condition(
            position=position,
            wallet_address=WALLET,
            gateway_client=gateway,
        )

        assert result.closed is False
        assert result.residual["liquidity"] == 0
        assert result.residual["tokens_owed0"] == 100
        assert result.residual["tokens_owed1"] == 200

    def test_fail_closed_when_liquidity_query_returns_none(self) -> None:
        """Gateway/RPC error during the liquidity query → fail-closed.

        ``query_position_liquidity`` returns ``None`` when the call
        could not be completed (gateway disconnected, RPC timeout,
        malformed response). Treating that as "closed" would re-create
        the silent-leak class — fail-closed instead.
        """
        gateway = _make_gateway(liquidity=None, tokens_owed=(0, 0))
        position = _make_position()

        result = _uniswap_v3_post_condition(
            position=position,
            wallet_address=WALLET,
            gateway_client=gateway,
        )

        assert result.closed is False
        assert "query_position_liquidity returned None" in (result.error or "")
        # tokens_owed must NOT have been called when liquidity is unknown —
        # we already know we can't confirm closure.
        gateway.query_position_tokens_owed.assert_not_called()

    def test_fail_closed_when_tokens_owed_query_returns_none(self) -> None:
        """Liquidity OK but tokens_owed query fails → fail-closed."""
        gateway = _make_gateway(liquidity=0, tokens_owed=None)
        position = _make_position()

        result = _uniswap_v3_post_condition(
            position=position,
            wallet_address=WALLET,
            gateway_client=gateway,
        )

        assert result.closed is False
        assert "query_position_tokens_owed returned None" in (result.error or "")

    def test_fail_closed_when_liquidity_query_raises(self) -> None:
        """Unexpected exception in the liquidity query → fail-closed."""
        gateway = MagicMock()
        gateway.query_position_liquidity.side_effect = RuntimeError("rpc-down")
        position = _make_position()

        result = _uniswap_v3_post_condition(
            position=position,
            wallet_address=WALLET,
            gateway_client=gateway,
        )

        assert result.closed is False
        assert "rpc-down" in (result.error or "")

    def test_fail_closed_when_no_gateway_client(self) -> None:
        """Framework rule: no egress without the gateway. A missing
        gateway_client must NOT be silently coerced to closed=True —
        the verifier needs an authoritative on-chain read.
        """
        position = _make_position()

        result = _uniswap_v3_post_condition(
            position=position,
            wallet_address=WALLET,
            gateway_client=None,
        )

        assert result.closed is False
        assert "gateway_client" in (result.error or "")

    def test_fail_closed_when_lp_position_has_no_resolvable_nft_id(self) -> None:
        """An LP-typed position with neither a numeric ``position_id`` NOR any
        ``details`` key holding the NFT id cannot be verified — fail-closed
        so the operator notices. Falling through silently would re-introduce
        the false-positive class (verifier returns clean without checking
        anything).
        """
        gateway = _make_gateway()
        # PositionType.LP but symbolic position_id and empty details — the
        # framework convention is to put the numeric id in details, but
        # the strategy author forgot.
        position = SimpleNamespace(
            protocol="uniswap_v3",
            position_id="uniswap_v3_lp_some_pool",
            chain="arbitrum",
            position_type=SimpleNamespace(value="LP"),
            details={},
        )

        result = _uniswap_v3_post_condition(
            position=position,
            wallet_address=WALLET,
            gateway_client=gateway,
        )

        assert result.closed is False
        assert "numeric NFT" in (result.error or "")
        gateway.query_position_liquidity.assert_not_called()

    def test_skips_token_position_type_without_failing_closed(self) -> None:
        """A ``PositionType.TOKEN`` position with ``protocol='uniswap_v3'``
        (e.g. ``uniswap_rsi`` reporting a residual base-token balance with
        ``position_id='uniswap_rsi_token_0'``) must NOT be routed through
        the NFT closure check. Pre-fix, the hook ran ``int(position_id)``
        on the symbolic id and fail-closed every uniswap_rsi teardown —
        a regression on the most-used demo strategy.
        """
        gateway = _make_gateway()
        position = SimpleNamespace(
            protocol="uniswap_v3",
            position_id="uniswap_rsi_token_0",
            chain="arbitrum",
            position_type=SimpleNamespace(value="TOKEN"),
            details={"asset": "USDC", "balance": "10000000"},
        )

        result = _uniswap_v3_post_condition(
            position=position,
            wallet_address=WALLET,
            gateway_client=gateway,
        )

        # Hook reports closed=True with a residual note explaining it
        # deferred — verifier moves on to the next position.
        assert result.closed is True
        assert "skipped_reason" in result.residual
        # The hook MUST NOT have called the gateway — that's the whole
        # point of gating.
        gateway.query_position_liquidity.assert_not_called()

    def test_resolves_nft_id_from_details_nft_position_id(self) -> None:
        """The framework convention for V3 LP strategies (sushiswap_v3,
        uniswap_v3 LP lifecycle, pancakeswap_v3) is to store the actual
        numeric NFT id in ``details['nft_position_id']`` while
        ``position_id`` carries a human-readable label. The hook must
        read details first.
        """
        gateway = _make_gateway()
        position = SimpleNamespace(
            protocol="sushiswap_v3",
            # Symbolic label, NOT a numeric id.
            position_id="sushiswap-v3-lp-WETH-USDC-bsc",
            chain="arbitrum",
            position_type=SimpleNamespace(value="LP"),
            details={"nft_position_id": 5460223},
        )

        result = _uniswap_v3_post_condition(
            position=position,
            wallet_address=WALLET,
            gateway_client=gateway,
        )

        assert result.closed is True
        # The gateway saw the numeric NFT id from details, NOT the symbolic
        # label.
        called_token_id = gateway.query_position_liquidity.call_args.kwargs["token_id"]
        assert called_token_id == 5460223

    def test_resolves_nft_id_from_details_nft_id_alias(self) -> None:
        """The other key convention used in this repo (morpho_univ3_leveraged_lp,
        agni_lp_mantle, aave_uniswap_yield_stack, sushiswap_v3_optimism)
        spells the field ``nft_id`` instead of ``nft_position_id``. The
        hook tries both because the codebase didn't standardise.
        """
        gateway = _make_gateway()
        position = SimpleNamespace(
            protocol="uniswap_v3",
            position_id="univ3_lp_morpho_pool",
            chain="arbitrum",
            position_type=SimpleNamespace(value="LP"),
            details={"nft_id": 12345},
        )

        result = _uniswap_v3_post_condition(
            position=position,
            wallet_address=WALLET,
            gateway_client=gateway,
        )

        assert result.closed is True
        called_token_id = gateway.query_position_liquidity.call_args.kwargs["token_id"]
        assert called_token_id == 12345

    def test_resolves_nft_id_from_details_token_id_fallback(self) -> None:
        """Generic ``token_id`` fallback for strategies that use neither
        ``nft_position_id`` nor ``nft_id``.
        """
        gateway = _make_gateway()
        position = SimpleNamespace(
            protocol="uniswap_v3",
            position_id="some_label",
            chain="arbitrum",
            position_type=SimpleNamespace(value="LP"),
            details={"token_id": 999},
        )

        result = _uniswap_v3_post_condition(
            position=position,
            wallet_address=WALLET,
            gateway_client=gateway,
        )

        assert result.closed is True
        called_token_id = gateway.query_position_liquidity.call_args.kwargs["token_id"]
        assert called_token_id == 999

    def test_resolves_nft_id_from_details_position_id_mirror(self) -> None:
        """Some strategies mirror the ``position_id`` attribute into
        ``details["position_id"]`` for their own bookkeeping. The
        verifier should accept that as a valid NFT id source so a
        symbolic top-level ``position_id`` doesn't fail closed when
        the numeric id is right there in details.
        """
        gateway = _make_gateway()
        position = SimpleNamespace(
            protocol="uniswap_v3",
            position_id="strategy_label",
            chain="arbitrum",
            position_type=SimpleNamespace(value="LP"),
            details={"position_id": 42},
        )

        result = _uniswap_v3_post_condition(
            position=position,
            wallet_address=WALLET,
            gateway_client=gateway,
        )

        assert result.closed is True
        called_token_id = gateway.query_position_liquidity.call_args.kwargs["token_id"]
        assert called_token_id == 42

    def test_falls_back_to_numeric_position_id_when_details_empty(self) -> None:
        """Backward compatibility with strategies that store the numeric
        NFT id directly in ``position_id``. No details key needed.
        """
        gateway = _make_gateway()
        position = SimpleNamespace(
            protocol="uniswap_v3",
            position_id="5460223",  # numeric string, the simple shape.
            chain="arbitrum",
            position_type=SimpleNamespace(value="LP"),
            details={},
        )

        result = _uniswap_v3_post_condition(
            position=position,
            wallet_address=WALLET,
            gateway_client=gateway,
        )

        assert result.closed is True
        called_token_id = gateway.query_position_liquidity.call_args.kwargs["token_id"]
        assert called_token_id == 5460223

    def test_fail_closed_when_chain_missing(self) -> None:
        gateway = _make_gateway()
        position = _make_position(chain="")

        result = _uniswap_v3_post_condition(
            position=position,
            wallet_address=WALLET,
            gateway_client=gateway,
        )

        assert result.closed is False
        assert "position.chain" in (result.error or "")

    def test_fail_closed_when_no_npm_for_chain(self) -> None:
        """Unknown chain → no NPM registered → fail-closed."""
        gateway = _make_gateway()
        position = _make_position(chain="mars")

        result = _uniswap_v3_post_condition(
            position=position,
            wallet_address=WALLET,
            gateway_client=gateway,
        )

        assert result.closed is False
        assert "NonfungiblePositionManager" in (result.error or "")

    def test_sushiswap_v3_uses_same_hook(self) -> None:
        """Sanity: a SushiSwap V3 LP teardown lands on the same hook
        with SushiSwap's NPM address — different deployment, same ABI.
        """
        gateway = _make_gateway(liquidity=0, tokens_owed=(0, 0))
        position = _make_position(chain="arbitrum", protocol="sushiswap_v3")

        result = _uniswap_v3_post_condition(
            position=position,
            wallet_address=WALLET,
            gateway_client=gateway,
        )

        assert result.closed is True
        # The NPM address is SushiSwap's, NOT Uniswap's — same ABI,
        # different deployment.
        called_npm = gateway.query_position_liquidity.call_args.kwargs["position_manager"]
        assert called_npm.lower() != NPM_ARBITRUM.lower()
        assert called_npm.startswith("0x")


# ---------------------------------------------------------------------------
# NPM-registry coverage (VIB-3807)
#
# A regression test that walks every (protocol, chain) pair the post-condition
# registers for and asserts the on-chain NPM address resolves AND the hook
# routes through the gateway path (i.e. does not fall through to the
# fail-closed "no NPM registered" branch). If somebody accidentally drops a
# ``position_manager`` field from one of the registries in
# ``almanak.core.contracts``, this test fails with a clear pointer to the
# missing chain — replacing the old "unknown-chain only" walk that caught
# nothing for actual deployments.
# ---------------------------------------------------------------------------


def _v3_protocol_chain_pairs() -> list[tuple[str, str]]:
    """All (protocol, chain) pairs the V3 post-condition registers for.

    Read the protocol→registry mapping straight from
    ``post_conditions._V3_PROTOCOL_TO_REGISTRY`` and the per-chain entries
    from ``almanak.core.contracts``. Adding a new V3-fork to the
    production mapping (e.g. PancakeSwap V3 once its NPM lands in
    ``contracts.py``) automatically extends this parameterization — no
    test edit required.
    """
    from almanak.core import contracts as _contracts
    from almanak.framework.teardown.post_conditions import _V3_PROTOCOL_TO_REGISTRY

    pairs: list[tuple[str, str]] = []
    for protocol, registry_name in _V3_PROTOCOL_TO_REGISTRY.items():
        registry = getattr(_contracts, registry_name, {})
        for chain in registry:
            pairs.append((protocol, chain))
    return pairs


class TestNPMRegistryCoverage:
    @pytest.mark.parametrize(
        ("protocol", "chain"),
        _v3_protocol_chain_pairs(),
    )
    def test_npm_resolves_and_hook_routes_to_gateway_for_every_registered_chain(
        self,
        protocol: str,
        chain: str,
    ) -> None:
        """Every registered (protocol, chain) pair must:

        1. Have a non-empty ``position_manager`` in the contracts registry.
        2. Cause the hook to call through to the gateway (proof that the
           NPM lookup succeeded — the fail-closed path returns *without*
           hitting the gateway).

        Manually deleting one ``position_manager`` line from
        ``almanak/core/contracts.py`` should turn this test red with a
        clear "NonfungiblePositionManager" assertion.
        """
        from almanak.framework.teardown.post_conditions import _resolve_v3_position_manager

        npm = _resolve_v3_position_manager(protocol, chain)
        assert npm, (
            f"No position_manager registered for {protocol!r} on {chain!r} — "
            "the V3 teardown post-condition will fail-closed for every "
            "position on this chain."
        )
        assert npm.startswith("0x") and len(npm) == 42

        gateway = _make_gateway(liquidity=0, tokens_owed=(0, 0))
        position = _make_position(chain=chain, protocol=protocol)
        result = _uniswap_v3_post_condition(
            position=position,
            wallet_address=WALLET,
            gateway_client=gateway,
        )

        # Closed (because liquidity=0 and tokensOwed=(0,0)) is the proof
        # the hook reached the gateway path; fail-closed paths do not call
        # the gateway and would surface a non-empty ``error`` instead.
        assert result.closed is True, (
            f"hook fell through to fail-closed for {protocol}/{chain}: {result.error}"
        )
        gateway.query_position_liquidity.assert_called_once()
        called_npm = gateway.query_position_liquidity.call_args.kwargs["position_manager"]
        assert called_npm.lower() == npm.lower()


# ---------------------------------------------------------------------------
# Integration via TeardownManager._verify_closure
# ---------------------------------------------------------------------------


@pytest.fixture
def _restore_uniswap_v3_hook():
    """Snapshot + restore the Uniswap V3 hook so test mutations don't leak
    into other tests."""
    original = get_teardown_post_condition("uniswap_v3")
    yield
    if original is not None:
        register_teardown_post_condition("uniswap_v3", original)


def _make_lp_position_summary(position_id: str = "5460223") -> TeardownPositionSummary:
    return TeardownPositionSummary(
        deployment_id="accounting_quant_lp",
        timestamp=datetime.now(UTC),
        positions=[
            PositionInfo(
                position_type=PositionType.LP,
                position_id=position_id,
                chain="arbitrum",
                protocol="uniswap_v3",
                value_usd=Decimal("4.0"),
                details={"pool": "0xpool", "fee_tier": 500},
            )
        ],
    )


def _make_strategy_with_tracker_still_set(position_id: str = "5460223") -> MagicMock:
    """Mock a strategy whose ``get_open_positions()`` still reports the
    LP — the realistic state during ``_verify_closure``, before
    ``on_teardown_completed`` fires and clears ``_position_id``.
    """
    strategy = MagicMock()
    strategy.wallet_address = WALLET
    strategy.get_open_positions.return_value = _make_lp_position_summary(position_id)
    return strategy


def test_verify_closure_no_false_positive_after_burnt_nft(
    _restore_uniswap_v3_hook,
):
    """End-to-end: the April 30 audit scenario.

    Pre-fix behaviour: ``_verify_closure`` returned False because no
    ``uniswap_v3`` hook was registered, so the verifier fell back to
    ``strategy.get_open_positions()`` which still tracked the
    just-burnt position id. Operators saw a phantom "positions still
    open" error every teardown.

    Post-fix: with the hook registered, the verifier reads on-chain
    truth (liquidity=0, tokens_owed=(0,0)) and correctly returns True.
    """
    fake_gateway = _make_gateway(liquidity=0, tokens_owed=(0, 0))

    mgr = TeardownManager()
    # Plumb the gateway in the same shape ``_teardown_gateway_client``
    # discovers it (compiler._gateway_client / orchestrator._gateway_client).
    mgr.compiler = SimpleNamespace(_gateway_client=fake_gateway)

    strategy = _make_strategy_with_tracker_still_set()
    pre_exec = _make_lp_position_summary()

    result = asyncio.run(
        mgr._verify_closure(strategy=strategy, pre_execution_positions=pre_exec)
    )

    assert result is True
    # The strategy's in-memory tracker MUST NOT have been the
    # authoritative signal — on-chain truth dominated.
    fake_gateway.query_position_liquidity.assert_called_once()


def test_verify_closure_correctly_flags_residual_position(
    _restore_uniswap_v3_hook,
):
    """The same scenario, but LP_CLOSE silently failed: liquidity > 0
    on-chain. The verifier MUST flag this — the bug we are fixing
    must not make the verifier blind to real residuals.
    """
    fake_gateway = _make_gateway(liquidity=42, tokens_owed=(0, 0))

    mgr = TeardownManager()
    mgr.compiler = SimpleNamespace(_gateway_client=fake_gateway)

    # Strategy reports nothing open (e.g. tracker was cleared early) but
    # on-chain truth says there's residual liquidity. We expect
    # closed=False because the on-chain check is authoritative.
    strategy = MagicMock()
    strategy.wallet_address = WALLET
    strategy.get_open_positions.return_value = TeardownPositionSummary(
        deployment_id="x", timestamp=datetime.now(UTC), positions=[]
    )

    pre_exec = _make_lp_position_summary()

    result = asyncio.run(
        mgr._verify_closure(strategy=strategy, pre_execution_positions=pre_exec)
    )

    assert result is False


def test_verify_closure_aggregates_multi_protocol_failures(
    _restore_uniswap_v3_hook,
):
    """Multi-position teardown: one Uniswap V3 still open, one closed.
    ``_verify_closure`` must surface the failure even though one of the
    positions checks clean."""
    fake_gateway = MagicMock()
    fake_gateway.is_connected = True

    def liquidity_side_effect(*, chain, position_manager, token_id):
        # Position 111 is closed; position 222 still has residual liquidity.
        return 0 if token_id == 111 else 9999

    def tokens_owed_side_effect(*, chain, position_manager, token_id):
        return (0, 0)

    fake_gateway.query_position_liquidity.side_effect = liquidity_side_effect
    fake_gateway.query_position_tokens_owed.side_effect = tokens_owed_side_effect

    mgr = TeardownManager()
    mgr.compiler = SimpleNamespace(_gateway_client=fake_gateway)

    pre_exec = TeardownPositionSummary(
        deployment_id="multi",
        timestamp=datetime.now(UTC),
        positions=[
            PositionInfo(
                position_type=PositionType.LP,
                position_id="111",
                chain="arbitrum",
                protocol="uniswap_v3",
                value_usd=Decimal("0"),
                details={},
            ),
            PositionInfo(
                position_type=PositionType.LP,
                position_id="222",
                chain="arbitrum",
                protocol="uniswap_v3",
                value_usd=Decimal("0"),
                details={},
            ),
        ],
    )

    strategy = MagicMock()
    strategy.wallet_address = WALLET
    strategy.get_open_positions.return_value = TeardownPositionSummary(
        deployment_id="multi", timestamp=datetime.now(UTC), positions=[]
    )

    result = asyncio.run(
        mgr._verify_closure(strategy=strategy, pre_execution_positions=pre_exec)
    )

    assert result is False
    # Both positions must have been queried — early-exit on first
    # closure would mask later residuals.
    assert fake_gateway.query_position_liquidity.call_count == 2


# ---------------------------------------------------------------------------
# VIB-3822 — gateway-client lookup must surface the orchestrator's _client
# attribute, not just the compiler's _gateway_client. Without this, the
# Optimism uniswap_lp_optimism --discover teardown verifier could not read
# on-chain truth and fell through to the fail-closed branch in
# ``_uniswap_v3_post_condition``.
# ---------------------------------------------------------------------------


def test_teardown_gateway_client_falls_back_to_orchestrator_underscore_client(
    _restore_uniswap_v3_hook,
):
    """``GatewayExecutionOrchestrator`` stores its gateway client under
    ``self._client``; the compiler under ``_gateway_client``. The teardown
    verifier discovery flow (no ``get_open_positions`` → ``--discover``)
    only has the orchestrator handle. ``_teardown_gateway_client`` must
    surface that ``_client`` attribute or the V3 post-condition fails-closed
    with "requires a gateway_client" — exactly the VIB-3822 symptom on
    ``uniswap_lp_optimism``.
    """
    fake_gateway = _make_gateway(liquidity=0, tokens_owed=(0, 0))

    mgr = TeardownManager()
    mgr.compiler = None
    mgr.orchestrator = SimpleNamespace(_client=fake_gateway)

    resolved = mgr._teardown_gateway_client()
    assert resolved is fake_gateway


def test_verify_closure_uses_orchestrator_client_for_v3_post_condition(
    _restore_uniswap_v3_hook,
):
    """End-to-end: the discovery-path teardown (compiler not yet set up,
    only the orchestrator carries the gateway client) must reach the
    Uniswap V3 post-condition with a real client and confirm closure
    via on-chain truth — not return the fail-closed "requires a
    gateway_client" error that VIB-3822 reproduced on Optimism.
    """
    fake_gateway = _make_gateway(liquidity=0, tokens_owed=(0, 0))

    mgr = TeardownManager()
    mgr.compiler = None
    mgr.orchestrator = SimpleNamespace(_client=fake_gateway)

    strategy = MagicMock()
    strategy.wallet_address = WALLET
    strategy.get_open_positions.return_value = TeardownPositionSummary(
        deployment_id="optimism-lp", timestamp=datetime.now(UTC), positions=[]
    )

    pre_exec = TeardownPositionSummary(
        deployment_id="optimism-lp",
        timestamp=datetime.now(UTC),
        positions=[
            PositionInfo(
                position_type=PositionType.LP,
                position_id="1088512",
                chain="optimism",
                protocol="uniswap_v3",
                value_usd=Decimal("4.0"),
                details={"token_id": 1088512, "pool": "0xpool", "fee_tier": 500},
            )
        ],
    )

    result = asyncio.run(
        mgr._verify_closure(strategy=strategy, pre_execution_positions=pre_exec)
    )

    assert result is True
    fake_gateway.query_position_liquidity.assert_called_once()


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
