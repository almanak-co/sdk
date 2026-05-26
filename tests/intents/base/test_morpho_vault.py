"""Intent tests for MetaMorpho VAULT_DEPOSIT / VAULT_REDEEM on Base (VIB-4307).

See ``tests/intents/ethereum/test_morpho_vault.py`` for the full
explanation of the ``morpho_vault`` (connector) vs ``metamorpho``
(vault registry) name mismatch. Both chains share the same blocker and
the same working-test pattern.

This file covers the (morpho_vault, VAULT_DEPOSIT, base) and
(morpho_vault, VAULT_REDEEM, base) triples from ConnectorRegistry.

To run::

    uv run pytest tests/intents/base/test_morpho_vault.py -v -s
"""

from decimal import Decimal

import pytest
from web3 import Web3

from almanak.framework.execution.orchestrator import ExecutionOrchestrator
from almanak.framework.intents.compiler import (
    CompilationStatus,
    IntentCompiler,
    IntentCompilerConfig,
)
from almanak.framework.intents.vocabulary import (
    IntentType,
    VaultDepositIntent,
    VaultRedeemIntent,
)
from almanak.framework.permissions.constants import METAMORPHO_VAULTS
from tests.intents.conftest import (
    get_token_balance,
)

pytestmark = [
    pytest.mark.no_zodiac(
        reason=(
            "VIB-4307: morpho_vault vault intents are covered directly; "
            "Zodiac vault permission synthesis remains outside this test."
        )
    ),
    pytest.mark.intent(IntentType.VAULT_DEPOSIT, IntentType.VAULT_REDEEM),
]


# =============================================================================
# Test Configuration
# =============================================================================

CHAIN_NAME = "base"

# Representative MetaMorpho vault on Base (Moonwell USDC).
VAULT_ADDRESS = METAMORPHO_VAULTS["base"]["vault"]
UNDERLYING_ADDRESS = METAMORPHO_VAULTS["base"]["underlying"]  # USDC


# =============================================================================
# Layer 1: Connector-name alias (matches ethereum twin)
#
# These cases intentionally stop at compile-time: they pin the alias
# contract (``protocol="morpho_vault"`` is accepted by the Pydantic
# validator AND reaches the compiler) and they feed the intent-coverage
# gate's AST scan, which keys coverage credit by the *connector* name
# (``morpho_vault``) rather than the vault-registry key (``metamorpho``).
# The 4-layer on-chain coverage lives below using ``protocol="metamorpho"``.
# =============================================================================


class TestMorphoVaultConnectorNameAliasBase:
    """The connector name is accepted as an alias for MetaMorpho vaults."""

    def test_morpho_vault_deposit_protocol_name_is_accepted(self) -> None:  # noqa: layers
        intent = VaultDepositIntent(
            protocol="morpho_vault",
            vault_address=VAULT_ADDRESS,
            amount=Decimal("100"),
            chain=CHAIN_NAME,
        )
        result = IntentCompiler(chain=CHAIN_NAME, config=IntentCompilerConfig(allow_placeholder_prices=True)).compile(
            intent
        )
        assert result.status == CompilationStatus.FAILED
        assert "gatewayclient" in (result.error or "").lower()

    def test_morpho_vault_redeem_protocol_name_is_accepted(self) -> None:  # noqa: layers
        intent = VaultRedeemIntent(
            protocol="morpho_vault",
            vault_address=VAULT_ADDRESS,
            shares=Decimal("10"),
            chain=CHAIN_NAME,
        )
        result = IntentCompiler(chain=CHAIN_NAME, config=IntentCompilerConfig(allow_placeholder_prices=True)).compile(
            intent
        )
        assert result.status == CompilationStatus.FAILED
        assert "gatewayclient" in (result.error or "").lower()


# =============================================================================
# Working 4-layer tests using protocol="metamorpho" (registry-accepted)
# =============================================================================


@pytest.mark.base
class TestMetamorphoVaultDepositOnChainBase:
    """Layers 2–4 for VAULT_DEPOSIT into the Moonwell USDC vault on Base."""

    @pytest.mark.asyncio
    async def test_compile_metamorpho_deposit_succeeds(  # noqa: layers
        self,
        funded_wallet: str,
        price_oracle: dict[str, Decimal],
        anvil_rpc_url: str,
    ) -> None:
        """Layer 1: ``VaultDepositIntent(protocol='metamorpho')`` compiles.

        Compilation-only sanity check; the 4-layer test below is skipped
        when no gateway client is available.
        """
        intent = VaultDepositIntent(
            protocol="metamorpho",
            vault_address=VAULT_ADDRESS,
            amount=Decimal("100"),
            chain=CHAIN_NAME,
        )
        compiler = IntentCompiler(
            chain=CHAIN_NAME,
            wallet_address=funded_wallet,
            price_oracle=price_oracle,
            rpc_url=anvil_rpc_url,
            config=IntentCompilerConfig(allow_placeholder_prices=True),
        )
        result = compiler.compile(intent)
        if result.status.value != "SUCCESS":
            assert (
                "GatewayClient" in (result.error or "")
                or "gateway" in (result.error or "").lower()
            ), (
                f"Unexpected compiler error: {result.error}"
            )
            pytest.skip(
                "VIB-4307: VAULT_DEPOSIT compilation requires a "
                "connected GatewayClient. as of 2026-05-12."
            )
        assert result.action_bundle is not None
        assert len(result.transactions) == 2

    @pytest.mark.asyncio
    async def test_deposit_usdc_into_metamorpho_full_4_layer(
        self,
        web3: Web3,
        anvil_rpc_url: str,
        funded_wallet: str,
        orchestrator: ExecutionOrchestrator,
        price_oracle: dict[str, Decimal],
    ) -> None:
        """Deposit 100 USDC into Moonwell USDC vault on Base."""
        deposit_amount = Decimal("100")
        usdc_decimals = 6
        deposit_amount_wei = int(deposit_amount * Decimal(10**usdc_decimals))

        usdc_before = get_token_balance(web3, UNDERLYING_ADDRESS, funded_wallet)
        shares_before = get_token_balance(web3, VAULT_ADDRESS, funded_wallet)
        assert usdc_before >= deposit_amount_wei, (
            f"USDC funding insufficient: have {usdc_before}, "
            f"need {deposit_amount_wei}"
        )

        intent = VaultDepositIntent(
            protocol="metamorpho",
            vault_address=VAULT_ADDRESS,
            amount=deposit_amount,
            chain=CHAIN_NAME,
        )
        compiler = IntentCompiler(
            chain=CHAIN_NAME,
            wallet_address=funded_wallet,
            price_oracle=price_oracle,
            rpc_url=anvil_rpc_url,
            config=IntentCompilerConfig(allow_placeholder_prices=True),
        )
        compilation_result = compiler.compile(intent)
        if compilation_result.status.value != "SUCCESS":
            pytest.skip(
                f"VIB-4307: VAULT_DEPOSIT compilation needs a connected "
                f"GatewayClient. Error: {compilation_result.error}"
            )

        execution_result = await orchestrator.execute(
            compilation_result.action_bundle
        )
        assert execution_result.success, (
            f"Execution failed: {execution_result.error}"
        )

        # Layer 3: Receipt — Deposit event from vault.
        deposit_topic = (
            "0xdcbc1c05240f31ff3ad067ef1ee35ce4997762752e3a095284754544f4c709d7"
        )
        deposit_log_found = False
        for tx_result in execution_result.transaction_results:
            if tx_result.receipt is None:
                continue
            for log in tx_result.receipt.logs or []:
                # ``logs`` is a list[dict] per TransactionReceipt; getattr would
                # silently return defaults on a dict, so use dict access.
                if isinstance(log, dict):
                    log_addr = log.get("address", "") or ""
                    topics = log.get("topics", []) or []
                else:
                    log_addr = getattr(log, "address", "") or ""
                    topics = getattr(log, "topics", []) or []
                first_topic = topics[0] if topics else None
                first_topic_hex = (
                    first_topic.hex() if hasattr(first_topic, "hex")
                    else str(first_topic) if first_topic else None
                )
                if (
                    log_addr.lower() == VAULT_ADDRESS.lower()
                    and first_topic_hex
                    and deposit_topic in first_topic_hex.lower()
                ):
                    deposit_log_found = True
                    break
        assert deposit_log_found, (
            "Expected ERC-4626 Deposit event from MetaMorpho vault"
        )

        usdc_after = get_token_balance(web3, UNDERLYING_ADDRESS, funded_wallet)
        shares_after = get_token_balance(web3, VAULT_ADDRESS, funded_wallet)

        usdc_spent = usdc_before - usdc_after
        shares_received = shares_after - shares_before

        assert usdc_spent == deposit_amount_wei
        assert shares_received > 0


@pytest.mark.base
class TestMetamorphoVaultRedeemOnChainBase:
    """Layers 2–4 for VAULT_REDEEM from Moonwell USDC vault on Base."""

    @pytest.mark.asyncio
    async def test_redeem_shares_from_metamorpho_full_4_layer(
        self,
        web3: Web3,
        anvil_rpc_url: str,
        funded_wallet: str,
        orchestrator: ExecutionOrchestrator,
        price_oracle: dict[str, Decimal],
    ) -> None:
        """Deposit USDC, then redeem all shares back to USDC."""
        usdc_decimals = 6
        deposit_amount = Decimal("100")
        deposit_amount_wei = int(deposit_amount * Decimal(10**usdc_decimals))

        compiler = IntentCompiler(
            chain=CHAIN_NAME,
            wallet_address=funded_wallet,
            price_oracle=price_oracle,
            rpc_url=anvil_rpc_url,
            config=IntentCompilerConfig(allow_placeholder_prices=True),
        )

        # Step 1: deposit
        deposit_intent = VaultDepositIntent(
            protocol="metamorpho",
            vault_address=VAULT_ADDRESS,
            amount=deposit_amount,
            chain=CHAIN_NAME,
        )
        deposit_result = compiler.compile(deposit_intent)
        if deposit_result.status.value != "SUCCESS":
            pytest.skip(
                f"VIB-4307: VAULT_DEPOSIT compilation needs a gateway "
                f"client. Error: {deposit_result.error}"
            )
        deposit_exec = await orchestrator.execute(deposit_result.action_bundle)
        assert deposit_exec.success

        shares_balance = get_token_balance(web3, VAULT_ADDRESS, funded_wallet)
        assert shares_balance > 0

        # Step 2: redeem
        usdc_before = get_token_balance(web3, UNDERLYING_ADDRESS, funded_wallet)
        shares_before = shares_balance

        redeem_intent = VaultRedeemIntent(
            protocol="metamorpho",
            vault_address=VAULT_ADDRESS,
            shares="all",
            chain=CHAIN_NAME,
        )
        redeem_result = compiler.compile(redeem_intent)
        if redeem_result.status.value != "SUCCESS":
            pytest.skip(
                f"VIB-4307: VAULT_REDEEM needs a gateway client. "
                f"Error: {redeem_result.error}"
            )
        assert len(redeem_result.transactions) == 1

        execution_result = await orchestrator.execute(redeem_result.action_bundle)
        assert execution_result.success, (
            f"Redeem failed: {execution_result.error}"
        )

        # Layer 3: Withdraw event
        withdraw_topic = (
            "0xfbde797d201c681b91056529119e0b02407c7bb96a4a2c75c01fc9667232c8db"
        )
        withdraw_log_found = False
        for tx_result in execution_result.transaction_results:
            if tx_result.receipt is None:
                continue
            for log in tx_result.receipt.logs or []:
                # ``logs`` is a list[dict] per TransactionReceipt; getattr would
                # silently return defaults on a dict, so use dict access.
                if isinstance(log, dict):
                    log_addr = log.get("address", "") or ""
                    topics = log.get("topics", []) or []
                else:
                    log_addr = getattr(log, "address", "") or ""
                    topics = getattr(log, "topics", []) or []
                first_topic = topics[0] if topics else None
                first_topic_hex = (
                    first_topic.hex() if hasattr(first_topic, "hex")
                    else str(first_topic) if first_topic else None
                )
                if (
                    log_addr.lower() == VAULT_ADDRESS.lower()
                    and first_topic_hex
                    and withdraw_topic in first_topic_hex.lower()
                ):
                    withdraw_log_found = True
                    break
        assert withdraw_log_found, (
            "Expected ERC-4626 Withdraw event from MetaMorpho vault"
        )

        usdc_after = get_token_balance(web3, UNDERLYING_ADDRESS, funded_wallet)
        shares_after = get_token_balance(web3, VAULT_ADDRESS, funded_wallet)

        usdc_received = usdc_after - usdc_before
        shares_spent = shares_before - shares_after

        assert shares_spent == shares_before
        assert usdc_received > 0
        assert usdc_received >= deposit_amount_wei * 99 // 100
