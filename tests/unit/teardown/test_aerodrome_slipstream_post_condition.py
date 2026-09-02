"""Aerodrome teardown post-condition: Slipstream NFT closure is chain-verified.

Before this hook existed, ``aerodrome_slipstream`` had no registered TD-14
post-condition (the V3-family hook registers only single-NPM connectors), so
every Slipstream teardown logged "no on-chain post-condition registered" and
was counted closed-by-execution (UNVERIFIED) even when the reconciliation lane
had already chain-confirmed liquidity == 0 on the reviewed manager. That is the
"lifecycle teardown verification cannot certify" symptom in ALM-3462.

The hook resolves the SAME reviewed manager the reconciliation lane uses (the
durable position must name it on a multi-generation chain) and applies the
shared NPM closure rule. Classic LP stays explicitly unmeasured because
Classic closes are clamped to the deployment's own liquidity.
"""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from almanak.connectors._strategy_base.teardown_post_condition import (
    get_teardown_post_condition,
    has_teardown_post_condition,
)
from almanak.connectors.aerodrome.addresses import slipstream_lp_deployments
from almanak.connectors.aerodrome.teardown_post_condition import aerodrome_teardown_post_condition
from almanak.framework.teardown.models import PositionType
from almanak.framework.teardown.post_conditions import _uniswap_v3_post_condition

CURRENT, LEGACY = slipstream_lp_deployments("base")
WALLET = "0x" + "11" * 20
TOKEN_ID = 5368077


def _position(
    *,
    protocol: str = "aerodrome_slipstream",
    chain: str = "base",
    position_id: str = str(TOKEN_ID),
    position_type: PositionType = PositionType.LP,
    details: dict | None = None,
) -> SimpleNamespace:
    return SimpleNamespace(
        protocol=protocol,
        chain=chain,
        position_id=position_id,
        position_type=position_type,
        details={"nft_manager": CURRENT.position_manager} if details is None else details,
    )


def _gateway(*, liquidity: int | None = 0, tokens_owed: tuple[int, int] | None = (0, 0)) -> MagicMock:
    gateway = MagicMock()
    gateway.is_connected = True
    gateway.query_position_liquidity.return_value = liquidity
    gateway.query_position_tokens_owed.return_value = tokens_owed
    return gateway


class TestRegistry:
    @pytest.mark.parametrize("slug", ["aerodrome_slipstream", "aerodrome"])
    def test_aerodrome_slugs_resolve_to_the_connector_hook(self, slug: str) -> None:
        assert has_teardown_post_condition(slug)
        assert get_teardown_post_condition(slug) is aerodrome_teardown_post_condition

    def test_aerodrome_is_not_served_by_the_v3_hook(self) -> None:
        """Slipstream has two reviewed managers; the single-NPM V3 hook must not claim it."""
        assert get_teardown_post_condition("aerodrome_slipstream") is not _uniswap_v3_post_condition


class TestSlipstreamClosure:
    def test_burnt_or_drained_nft_is_measured_closed_on_the_named_manager(self) -> None:
        gateway = _gateway()
        result = aerodrome_teardown_post_condition(_position(), WALLET, gateway_client=gateway, block=777)
        assert result.closed is True
        assert not result.unmeasured
        # The read went to the manager the position names, pinned to the close block.
        kwargs = gateway.query_position_liquidity.call_args.kwargs
        assert kwargs["position_manager"] == CURRENT.position_manager
        assert (kwargs["chain"], kwargs["token_id"], kwargs["block"]) == ("base", TOKEN_ID, 777)

    def test_legacy_manager_identity_is_honoured(self) -> None:
        gateway = _gateway()
        aerodrome_teardown_post_condition(
            _position(details={"nft_manager": LEGACY.position_manager}), WALLET, gateway_client=gateway
        )
        assert gateway.query_position_liquidity.call_args.kwargs["position_manager"] == LEGACY.position_manager

    def test_residual_liquidity_is_a_measured_failure(self) -> None:
        result = aerodrome_teardown_post_condition(_position(), WALLET, gateway_client=_gateway(liquidity=123))
        assert result.closed is False
        assert not result.unmeasured
        assert result.residual["liquidity"] == 123
        assert result.residual["position_manager"] == CURRENT.position_manager

    def test_residual_fees_are_a_measured_failure(self) -> None:
        result = aerodrome_teardown_post_condition(
            _position(), WALLET, gateway_client=_gateway(liquidity=0, tokens_owed=(5, 0))
        )
        assert result.closed is False
        assert not result.unmeasured
        assert result.residual["tokens_owed0"] == 5

    @pytest.mark.parametrize("liquidity, tokens_owed", [(None, (0, 0)), (0, None)])
    def test_read_fault_is_unmeasured_never_closed(self, liquidity, tokens_owed) -> None:
        result = aerodrome_teardown_post_condition(
            _position(), WALLET, gateway_client=_gateway(liquidity=liquidity, tokens_owed=tokens_owed)
        )
        assert result.closed is False
        assert result.unmeasured is True
        assert "returned None" in (result.error or "")

    def test_raising_read_is_unmeasured(self) -> None:
        gateway = _gateway()
        gateway.query_position_liquidity.side_effect = RuntimeError("rpc down")
        result = aerodrome_teardown_post_condition(_position(), WALLET, gateway_client=gateway)
        assert result.unmeasured is True
        assert "query_position_liquidity raised" in (result.error or "")

    def test_ambiguous_manager_identity_is_unmeasured_and_reads_nothing(self) -> None:
        """Two reviewed managers, no identity on the position: never probe either."""
        gateway = _gateway()
        result = aerodrome_teardown_post_condition(_position(details={}), WALLET, gateway_client=gateway)
        assert result.unmeasured is True
        assert "no unambiguous reviewed position manager" in (result.error or "")
        gateway.query_position_liquidity.assert_not_called()

    def test_unreviewed_manager_identity_is_unmeasured(self) -> None:
        gateway = _gateway()
        result = aerodrome_teardown_post_condition(
            _position(details={"nft_manager": "0x" + "ab" * 20}), WALLET, gateway_client=gateway
        )
        assert result.unmeasured is True
        gateway.query_position_liquidity.assert_not_called()

    def test_missing_gateway_is_unmeasured(self) -> None:
        result = aerodrome_teardown_post_condition(_position(), WALLET, gateway_client=None)
        assert result.unmeasured is True
        assert "requires a gateway_client" in (result.error or "")

    def test_missing_chain_is_unmeasured(self) -> None:
        result = aerodrome_teardown_post_condition(_position(chain=""), WALLET, gateway_client=_gateway())
        assert result.unmeasured is True

    def test_non_numeric_token_id_is_unmeasured(self) -> None:
        result = aerodrome_teardown_post_condition(
            _position(position_id="my-lp", details={"nft_manager": CURRENT.position_manager}),
            WALLET,
            gateway_client=_gateway(),
        )
        assert result.unmeasured is True
        assert "could not resolve a numeric NFT tokenId" in (result.error or "")


CLASSIC_POOL = "0x" + "cd" * 20


def _classic_position() -> SimpleNamespace:
    return _position(protocol="aerodrome", position_id=CLASSIC_POOL, details={})


class TestClassicClosure:
    """Classic LP: the pool contract is the LP token; zero balance proves closure."""

    def test_zero_lp_balance_is_measured_closed(self) -> None:
        gateway = _gateway()
        gateway.query_erc20_balance.return_value = 0
        result = aerodrome_teardown_post_condition(_classic_position(), WALLET, gateway_client=gateway, block=777)
        assert result.closed is True
        assert not result.unmeasured
        gateway.query_position_liquidity.assert_not_called()
        assert gateway.query_erc20_balance.call_args.kwargs.get("block") == 777

    def test_remaining_lp_balance_is_unmeasured_not_a_residual_failure(self) -> None:
        """A clamped close may legitimately leave foreign LP in the wallet: doubt, never FAILED."""
        gateway = _gateway()
        gateway.query_erc20_balance.return_value = 10**18
        result = aerodrome_teardown_post_condition(_classic_position(), WALLET, gateway_client=gateway)
        assert result.closed is False
        assert result.unmeasured is True
        assert "clamped" in (result.error or "")
        assert result.residual and result.residual.get("balance") == str(10**18)

    def test_read_fault_is_unmeasured(self) -> None:
        gateway = _gateway()
        gateway.query_erc20_balance.return_value = None
        result = aerodrome_teardown_post_condition(_classic_position(), WALLET, gateway_client=gateway)
        assert result.closed is False
        assert result.unmeasured is True


class TestScopeGates:
    def test_token_position_is_not_applicable(self) -> None:
        result = aerodrome_teardown_post_condition(
            _position(position_type=PositionType.TOKEN, position_id="aero_token_0"), WALLET, gateway_client=_gateway()
        )
        assert result.closed is True
        assert result.not_applicable is True


class TestThroughTeardownManager:
    """The verifier must now CHAIN_VERIFY a Slipstream close instead of falling back."""

    @staticmethod
    def _position_info(details: dict):
        from decimal import Decimal

        from almanak.framework.teardown.models import PositionInfo

        return PositionInfo(
            position_type=PositionType.LP,
            position_id=str(TOKEN_ID),
            chain="base",
            protocol="aerodrome_slipstream",
            value_usd=Decimal("100"),
            details=details,
        )

    @pytest.mark.asyncio
    async def test_named_manager_closure_is_chain_verified_at_the_close_block(self) -> None:
        from almanak.framework.teardown.models import VerificationStatus
        from almanak.framework.teardown.teardown_manager import TeardownManager

        gateway = _gateway()
        mgr = TeardownManager()
        mgr.compiler = SimpleNamespace(gateway_client=gateway)
        strategy = MagicMock()
        strategy.get_open_positions.return_value = SimpleNamespace(positions=[])
        strategy.wallet_address = WALLET

        detailed = await mgr._verify_closure_detailed(
            strategy=strategy,
            pre_execution_positions=SimpleNamespace(
                positions=[self._position_info({"nft_manager": CURRENT.position_manager})]
            ),
            close_receipt_block=888,
        )

        assert detailed.all_closed is True
        assert detailed.verification_status is VerificationStatus.CHAIN_VERIFIED
        assert gateway.query_position_liquidity.call_args.kwargs["block"] == 888

    @pytest.mark.asyncio
    async def test_ambiguous_manager_stays_unverified_not_failed(self) -> None:
        from almanak.framework.teardown.models import VerificationStatus
        from almanak.framework.teardown.teardown_manager import TeardownManager

        gateway = _gateway()
        mgr = TeardownManager()
        mgr.compiler = SimpleNamespace(gateway_client=gateway)
        strategy = MagicMock()
        strategy.get_open_positions.return_value = SimpleNamespace(positions=[])
        strategy.wallet_address = WALLET

        detailed = await mgr._verify_closure_detailed(
            strategy=strategy,
            pre_execution_positions=SimpleNamespace(positions=[self._position_info({})]),
            close_receipt_block=888,
        )

        assert detailed.all_closed is True
        assert detailed.verification_status is VerificationStatus.UNVERIFIED
        gateway.query_position_liquidity.assert_not_called()
