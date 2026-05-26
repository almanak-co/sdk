from __future__ import annotations

from decimal import Decimal
from unittest.mock import MagicMock, patch

from almanak.framework.connectors.base.compiler import BaseCompilerContext
from almanak.framework.connectors.morpho_vault.compiler import MorphoVaultCompiler
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

    with patch("almanak.framework.connectors.morpho_vault.compiler._build_adapter", return_value=adapter):
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

    with patch("almanak.framework.connectors.morpho_vault.compiler._build_adapter", return_value=adapter):
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

    with patch("almanak.framework.connectors.morpho_vault.compiler._build_adapter", return_value=adapter):
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

    with patch("almanak.framework.connectors.morpho_vault.compiler._build_adapter", return_value=adapter):
        result = compiler.compile_redeem(_ctx(), _redeem_intent())

    assert result.status == CompilationStatus.FAILED
    assert result.error == "vault decimals unavailable"
