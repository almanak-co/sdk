from __future__ import annotations

from decimal import Decimal
from unittest.mock import MagicMock, patch

from almanak.connectors._strategy_base.base.compiler import BaseCompilerContext
from almanak.connectors.morpho_vault.compiler import MorphoVaultCompiler
from almanak.framework.intents.compiler import CompilationStatus
from almanak.framework.intents.vocabulary import IntentType, VaultRedeemIntent

VAULT_ADDRESS = "0xBEEF01735c132Ada46AA9aA4c54623cAA92A64CB"
WALLET_ADDRESS = "0xf39Fd6e51aad88F6F4ce6aB8827279cffFb92266"


def _ctx(*, chain: str = "ethereum", gateway_connected: bool = True) -> BaseCompilerContext:
    gateway_client = MagicMock()
    gateway_client.is_connected = gateway_connected
    return BaseCompilerContext(
        chain=chain,
        wallet_address=WALLET_ADDRESS,
        rpc_url=None,
        rpc_timeout=10.0,
        permission_discovery=False,
        allow_placeholder_prices=True,
        token_resolver=MagicMock(),
        gateway_client=gateway_client,
        price_oracle={},
        cache={},
        services=MagicMock(),
    )


def _redeem_intent(*, shares: Decimal | str = Decimal("1.5")) -> VaultRedeemIntent:
    return VaultRedeemIntent(
        protocol="metamorpho",
        vault_address=VAULT_ADDRESS,
        shares=shares,
        chain="ethereum",
    )


def _adapter(*, max_redeem: int = 123_000_000_000_000_000_000) -> MagicMock:
    adapter = MagicMock()
    adapter.sdk.get_decimals.return_value = 18
    adapter.sdk.get_max_redeem.return_value = max_redeem
    adapter.sdk.build_redeem_tx.return_value = {
        "to": VAULT_ADDRESS,
        "value": 0,
        "data": "0xredeem",
        "gas_estimate": 180_000,
    }
    return adapter


def test_compile_redeem_specific_shares_builds_action_bundle() -> None:
    compiler = MorphoVaultCompiler()
    adapter = _adapter()

    with patch("almanak.connectors.morpho_vault.compiler._build_adapter", return_value=adapter):
        result = compiler.compile_redeem(_ctx(), _redeem_intent(shares=Decimal("1.5")))

    assert result.status == CompilationStatus.SUCCESS
    assert result.total_gas_estimate == 180_000
    assert result.action_bundle is not None
    assert result.action_bundle.intent_type == IntentType.VAULT_REDEEM.value
    assert result.action_bundle.metadata["shares_wei"] == "1500000000000000000"
    assert result.action_bundle.metadata["redeem_all"] is False
    adapter.sdk.build_redeem_tx.assert_called_once_with(
        vault_address=VAULT_ADDRESS,
        shares=1_500_000_000_000_000_000,
        receiver=WALLET_ADDRESS,
        owner=WALLET_ADDRESS,
    )


def test_compile_redeem_all_uses_max_redeem() -> None:
    compiler = MorphoVaultCompiler()
    adapter = _adapter(max_redeem=42)

    with patch("almanak.connectors.morpho_vault.compiler._build_adapter", return_value=adapter):
        result = compiler.compile_redeem(_ctx(), _redeem_intent(shares="all"))

    assert result.status == CompilationStatus.SUCCESS
    assert result.action_bundle is not None
    assert result.action_bundle.metadata["shares_wei"] == "42"
    assert result.action_bundle.metadata["redeem_all"] is True
    adapter.sdk.get_max_redeem.assert_called_once_with(VAULT_ADDRESS, WALLET_ADDRESS)
    adapter.sdk.build_redeem_tx.assert_called_once_with(
        vault_address=VAULT_ADDRESS,
        shares=42,
        receiver=WALLET_ADDRESS,
        owner=WALLET_ADDRESS,
    )


def test_compile_redeem_all_fails_when_wallet_has_no_shares() -> None:
    compiler = MorphoVaultCompiler()
    adapter = _adapter(max_redeem=0)

    with patch("almanak.connectors.morpho_vault.compiler._build_adapter", return_value=adapter):
        result = compiler.compile_redeem(_ctx(), _redeem_intent(shares="all"))

    assert result.status == CompilationStatus.FAILED
    assert result.error == "No shares to redeem"
    adapter.sdk.build_redeem_tx.assert_not_called()


def test_compile_redeem_requires_connected_gateway() -> None:
    result = MorphoVaultCompiler().compile_redeem(_ctx(gateway_connected=False), _redeem_intent())

    assert result.status == CompilationStatus.FAILED
    assert "GatewayClient" in (result.error or "")


def test_compile_redeem_rejects_unsupported_chain() -> None:
    result = MorphoVaultCompiler().compile_redeem(_ctx(chain="optimism"), _redeem_intent())

    assert result.status == CompilationStatus.FAILED
    assert "not supported on chain 'optimism'" in (result.error or "")


def test_compile_redeem_returns_failed_result_on_adapter_exception() -> None:
    compiler = MorphoVaultCompiler()
    adapter = _adapter()
    adapter.sdk.get_decimals.side_effect = RuntimeError("vault decimals unavailable")

    with patch("almanak.connectors.morpho_vault.compiler._build_adapter", return_value=adapter):
        result = compiler.compile_redeem(_ctx(), _redeem_intent())

    assert result.status == CompilationStatus.FAILED
    assert result.error == "vault decimals unavailable"


# Morpho Vault V2 (adapter-based generation). V2 returns 0 from every ERC-4626
# ``max*`` view by design, so the v1 ``maxRedeem`` sizing reported "No shares
# to redeem" for every funded wallet. The compiler must size from ``balanceOf``
# and prove liquidity with a redeem simulation instead.

from almanak.connectors.morpho_vault.sdk import (  # noqa: E402
    VAULT_VERSION_V1,
    VAULT_VERSION_V2,
    VaultGatedError,
    VaultIlliquidError,
)
from almanak.framework.intents.vocabulary import VaultDepositIntent  # noqa: E402


def _v2_adapter(*, balance: int = 8_605_857_896_403_843_614_675, simulated_assets: int = 8_600_000_000) -> MagicMock:
    adapter = _adapter(max_redeem=0)  # V2: maxRedeem is 0 by design
    adapter.sdk.detect_vault_version.return_value = VAULT_VERSION_V2
    adapter.sdk.get_balance_of.return_value = balance
    adapter.sdk.simulate_redeem.return_value = simulated_assets
    adapter.sdk.check_redeem_gates.return_value = None
    adapter.sdk.check_deposit_gate.return_value = None
    return adapter


def test_compile_redeem_all_on_v2_sizes_from_balance_of_and_simulates() -> None:
    compiler = MorphoVaultCompiler()
    adapter = _v2_adapter(balance=12_345)

    with patch("almanak.connectors.morpho_vault.compiler._build_adapter", return_value=adapter):
        result = compiler.compile_redeem(_ctx(), _redeem_intent(shares="all"))

    assert result.status == CompilationStatus.SUCCESS, result.error
    assert result.action_bundle is not None
    assert result.action_bundle.metadata["shares_wei"] == "12345"
    assert result.action_bundle.metadata["vault_version"] == VAULT_VERSION_V2
    adapter.sdk.get_balance_of.assert_called_once_with(VAULT_ADDRESS, WALLET_ADDRESS)
    adapter.sdk.get_max_redeem.assert_not_called()
    adapter.sdk.check_redeem_gates.assert_called_once_with(VAULT_ADDRESS, WALLET_ADDRESS, WALLET_ADDRESS)
    adapter.sdk.simulate_redeem.assert_called_once_with(
        VAULT_ADDRESS, 12_345, receiver=WALLET_ADDRESS, owner=WALLET_ADDRESS
    )
    adapter.sdk.build_redeem_tx.assert_called_once_with(
        vault_address=VAULT_ADDRESS, shares=12_345, receiver=WALLET_ADDRESS, owner=WALLET_ADDRESS
    )


def test_compile_redeem_all_on_v2_with_empty_wallet_still_reports_no_shares() -> None:
    adapter = _v2_adapter(balance=0)

    with patch("almanak.connectors.morpho_vault.compiler._build_adapter", return_value=adapter):
        result = MorphoVaultCompiler().compile_redeem(_ctx(), _redeem_intent(shares="all"))

    assert result.status == CompilationStatus.FAILED
    assert result.error == "No shares to redeem"
    adapter.sdk.simulate_redeem.assert_not_called()


def test_compile_redeem_on_v2_fails_closed_when_simulation_reverts() -> None:
    adapter = _v2_adapter()
    adapter.sdk.simulate_redeem.side_effect = VaultIlliquidError("redeem simulation reverted: liquidity adapter short")

    with patch("almanak.connectors.morpho_vault.compiler._build_adapter", return_value=adapter):
        result = MorphoVaultCompiler().compile_redeem(_ctx(), _redeem_intent(shares="all"))

    assert result.status == CompilationStatus.FAILED
    assert "liquidity adapter short" in (result.error or "")
    adapter.sdk.build_redeem_tx.assert_not_called()  # the tx is never built, let alone sent


def test_compile_redeem_on_v2_fails_closed_when_gated() -> None:
    adapter = _v2_adapter()
    adapter.sdk.check_redeem_gates.side_effect = VaultGatedError("send-shares gate refuses wallet")

    with patch("almanak.connectors.morpho_vault.compiler._build_adapter", return_value=adapter):
        result = MorphoVaultCompiler().compile_redeem(_ctx(), _redeem_intent(shares=Decimal("1")))

    assert result.status == CompilationStatus.FAILED
    assert "gate refuses" in (result.error or "")
    adapter.sdk.simulate_redeem.assert_not_called()
    adapter.sdk.build_redeem_tx.assert_not_called()


def test_compile_redeem_on_v1_keeps_max_redeem_and_never_simulates() -> None:
    adapter = _adapter(max_redeem=42)
    adapter.sdk.detect_vault_version.return_value = VAULT_VERSION_V1

    with patch("almanak.connectors.morpho_vault.compiler._build_adapter", return_value=adapter):
        result = MorphoVaultCompiler().compile_redeem(_ctx(), _redeem_intent(shares="all"))

    assert result.status == CompilationStatus.SUCCESS
    assert result.action_bundle.metadata["vault_version"] == VAULT_VERSION_V1
    adapter.sdk.get_max_redeem.assert_called_once()
    adapter.sdk.get_balance_of.assert_not_called()
    adapter.sdk.simulate_redeem.assert_not_called()


def _deposit_intent() -> VaultDepositIntent:
    return VaultDepositIntent(
        protocol="metamorpho", vault_address=VAULT_ADDRESS, amount=Decimal("100"), chain="ethereum"
    )


def _deposit_ctx() -> BaseCompilerContext:
    ctx = _ctx()
    token = MagicMock()
    token.address = "0x" + "a0" * 20
    token.symbol = "USDC"
    token.decimals = 6
    ctx.services.resolve_token.return_value = token
    ctx.services.build_approve_tx.return_value = []
    return ctx


def test_compile_deposit_on_v2_checks_receive_gate_not_max_deposit() -> None:
    adapter = _v2_adapter()
    adapter.sdk.get_vault_asset.return_value = "0x" + "a0" * 20
    adapter.sdk.build_deposit_tx.return_value = {
        "to": VAULT_ADDRESS,
        "value": 0,
        "data": "0xdeposit",
        "gas_estimate": 1,
    }

    with patch("almanak.connectors.morpho_vault.compiler._build_adapter", return_value=adapter):
        result = MorphoVaultCompiler().compile_deposit(_deposit_ctx(), _deposit_intent())

    assert result.status == CompilationStatus.SUCCESS, result.error
    assert result.action_bundle.metadata["vault_version"] == VAULT_VERSION_V2
    adapter.sdk.check_deposit_gate.assert_called_once_with(VAULT_ADDRESS, WALLET_ADDRESS, WALLET_ADDRESS)
    adapter.sdk.get_max_deposit.assert_not_called()


def test_compile_deposit_on_v2_fails_closed_when_gated() -> None:
    adapter = _v2_adapter()
    adapter.sdk.get_vault_asset.return_value = "0x" + "a0" * 20
    adapter.sdk.check_deposit_gate.side_effect = VaultGatedError("receive-shares gate refuses wallet")

    with patch("almanak.connectors.morpho_vault.compiler._build_adapter", return_value=adapter):
        result = MorphoVaultCompiler().compile_deposit(_deposit_ctx(), _deposit_intent())

    assert result.status == CompilationStatus.FAILED
    assert "receive-shares gate" in (result.error or "")
    adapter.sdk.build_deposit_tx.assert_not_called()
