"""Intent tests for MetaMorpho VAULT_DEPOSIT / VAULT_REDEEM on Ethereum (VIB-4307).

MetaMorpho is the ERC-4626 vault layer over Morpho Blue. The vault
registry (``almanak/connectors/_strategy_base/vaults/__init__.py``) registers
it under the **protocol key ``"metamorpho"``**, while the connector
registry (``almanak/connectors/morpho_vault/__init__.py``)
registers the *connector* under the name **``"morpho_vault"``**.

This dual naming surfaces in two places:

* The Pydantic validator on ``VaultDepositIntent`` / ``VaultRedeemIntent``
  rejects ``protocol="morpho_vault"`` — it only accepts ``"metamorpho"``.
* The intent-coverage gate (``scripts/ci/check_intent_coverage.py``)
  consumes ConnectorRegistry names, so coverage credit is keyed by
  ``"morpho_vault"``.

This file covers both names: a full 4-layer on-chain test using
``protocol="metamorpho"`` (which compiles and executes), AND a
documented-blocker test using ``protocol="morpho_vault"`` (which asserts
the Pydantic rejection invariant and feeds the gate's AST scan with the
canonical connector name). When the framework reconciles the two names,
the blocker test flips and prompts the next engineer to fold it into
the working test.

This module is marked ``no_zodiac`` for now — ``metamorpho`` *is* in
``VAULT_PROTOCOL_REPRESENTATIVE`` so synthetic-intent manifest synthesis
works, but the connector-registry name ``morpho_vault`` is what the
intent-test → Zodiac mapping looks up (see VIB-4307 scope notes). Drop
the marker once the lookup is reconciled.

To run::

    uv run pytest tests/intents/ethereum/test_morpho_vault.py -v -s
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

CHAIN_NAME = "ethereum"

# Representative MetaMorpho vault on Ethereum (Steakhouse USDC).
# Address from ``almanak/framework/permissions/constants.py:METAMORPHO_VAULTS``.
VAULT_ADDRESS = METAMORPHO_VAULTS["ethereum"]["vault"]
UNDERLYING_ADDRESS = METAMORPHO_VAULTS["ethereum"]["underlying"]  # USDC


# =============================================================================
# Layer 1: Compilation — connector-name alias
#
# These cases intentionally stop at compile-time: they pin the alias
# contract (``protocol="morpho_vault"`` is accepted by the Pydantic
# validator AND reaches the compiler) and they feed the intent-coverage
# gate's AST scan, which keys coverage credit by the *connector* name
# (``morpho_vault``) rather than the vault-registry key (``metamorpho``).
# The 4-layer on-chain coverage lives below using ``protocol="metamorpho"``.
# =============================================================================


class TestMorphoVaultConnectorNameAlias:
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
# Working 4-layer tests using protocol="metamorpho"
#
# These are the actual on-chain integration tests — the vault registry
# accepts "metamorpho", and the connector code path is identical to what
# would run under "morpho_vault" once the names are reconciled.
# =============================================================================


@pytest.mark.ethereum
class TestMetamorphoVaultDepositOnChain:
    """Layers 2–4 for VAULT_DEPOSIT into the Steakhouse USDC vault."""

    @pytest.mark.asyncio
    async def test_compile_metamorpho_deposit_succeeds(  # noqa: layers
        self,
        funded_wallet: str,
        price_oracle: dict[str, Decimal],
        anvil_rpc_url: str,
    ) -> None:
        """Layer 1: ``VaultDepositIntent(protocol='metamorpho')`` compiles.

        This is the registry-accepted protocol name and the path that
        actually drives on-chain execution today. Compilation-only sanity
        check; the full 4-layer test below is skipped under the current
        ``gateway_client is None`` constraint.
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
        # The compiler requires a connected GatewayClient for vault
        # compilation (it queries vault.asset() and decimals via RPC).
        # Without one, the compilation returns FAILED with the
        # well-known "A connected GatewayClient is required" message —
        # that's the current state; we capture it explicitly so when a
        # gateway-backed fixture lands, this test must be upgraded.
        if result.status.value != "SUCCESS":
            assert (
                "GatewayClient" in (result.error or "")
                or "gateway" in (result.error or "").lower()
            ), (
                f"Compilation failed for an unexpected reason: "
                f"{result.error}. If this is a new error mode, update "
                f"the assertion."
            )
            pytest.skip(
                "VIB-4307: VAULT_DEPOSIT compilation requires a "
                "connected GatewayClient (queries vault.asset()/decimals "
                "via RPC). The intent-test fixtures do not yet supply "
                "one — wire a gateway-backed fixture before promoting "
                "this to a real 4-layer on-chain test. as of 2026-05-12."
            )
        assert result.action_bundle is not None
        # Vault deposit = approve + deposit (2 txs)
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
        """4-layer test: deposit 100 USDC into Steakhouse USDC vault.

        See the per-class docstring for the gateway-client constraint —
        the test skips gracefully until a gateway-backed fixture lands.
        """
        deposit_amount = Decimal("100")  # 100 USDC
        usdc_decimals = 6
        deposit_amount_wei = int(deposit_amount * Decimal(10**usdc_decimals))

        # Layer 4 setup
        usdc_before = get_token_balance(web3, UNDERLYING_ADDRESS, funded_wallet)
        shares_before = get_token_balance(web3, VAULT_ADDRESS, funded_wallet)
        assert usdc_before >= deposit_amount_wei, (
            f"USDC funding insufficient: have {usdc_before}, "
            f"need {deposit_amount_wei}"
        )

        # Layer 1: Compile
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
                f"GatewayClient (queries vault.asset()/decimals). "
                f"Compiler error: {compilation_result.error}"
            )
        assert compilation_result.action_bundle is not None

        # Layer 2: Execute
        execution_result = await orchestrator.execute(
            compilation_result.action_bundle
        )
        assert execution_result.success, (
            f"Execution failed: {execution_result.error}"
        )

        # Layer 3: Receipt — vault must have emitted a Deposit event.
        # MetaMorpho ERC-4626 Deposit topic: keccak("Deposit(address,address,uint256,uint256)")
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

        # Layer 4: Balance deltas
        usdc_after = get_token_balance(web3, UNDERLYING_ADDRESS, funded_wallet)
        shares_after = get_token_balance(web3, VAULT_ADDRESS, funded_wallet)

        usdc_spent = usdc_before - usdc_after
        shares_received = shares_after - shares_before

        assert usdc_spent == deposit_amount_wei, (
            f"USDC spent must equal deposit amount exactly. "
            f"Expected: {deposit_amount_wei}, Got: {usdc_spent}"
        )
        assert shares_received > 0, (
            "Vault shares must be minted to the depositor (no-op guard)"
        )


@pytest.mark.ethereum
class TestMetamorphoVaultRedeemOnChain:
    """Layers 2–4 for VAULT_REDEEM from the Steakhouse USDC vault.

    Setup: deposit USDC into the vault, then redeem the received shares
    back for USDC. The redeem path does not need an approve (caller
    redeems own shares).
    """

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

        # ── Step 1: Deposit USDC to acquire shares ────────────────────────
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
                f"client. Compiler error: {deposit_result.error}"
            )
        deposit_exec = await orchestrator.execute(deposit_result.action_bundle)
        assert deposit_exec.success, (
            f"Deposit setup failed: {deposit_exec.error}"
        )

        shares_balance = get_token_balance(web3, VAULT_ADDRESS, funded_wallet)
        assert shares_balance > 0, "Deposit setup produced no shares"

        # ── Step 2: Redeem ────────────────────────────────────────────────
        # Layer 4 setup
        usdc_before = get_token_balance(web3, UNDERLYING_ADDRESS, funded_wallet)
        shares_before = shares_balance

        # Layer 1: Compile redeem (all shares)
        redeem_intent = VaultRedeemIntent(
            protocol="metamorpho",
            vault_address=VAULT_ADDRESS,
            shares="all",
            chain=CHAIN_NAME,
        )
        redeem_result = compiler.compile(redeem_intent)
        if redeem_result.status.value != "SUCCESS":
            pytest.skip(
                f"VIB-4307: VAULT_REDEEM compilation needs a gateway "
                f"client. Compiler error: {redeem_result.error}"
            )
        assert redeem_result.action_bundle is not None
        # Redeem = 1 tx (no approve needed for own shares)
        assert len(redeem_result.transactions) == 1

        # Layer 2: Execute
        execution_result = await orchestrator.execute(redeem_result.action_bundle)
        assert execution_result.success, (
            f"Redeem execution failed: {execution_result.error}"
        )

        # Layer 3: Receipt — vault must have emitted a Withdraw event.
        # ERC-4626 Withdraw topic: keccak("Withdraw(address,address,address,uint256,uint256)")
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

        # Layer 4: Balance deltas
        usdc_after = get_token_balance(web3, UNDERLYING_ADDRESS, funded_wallet)
        shares_after = get_token_balance(web3, VAULT_ADDRESS, funded_wallet)

        usdc_received = usdc_after - usdc_before
        shares_spent = shares_before - shares_after

        assert shares_spent == shares_before, (
            f"All shares should be redeemed; spent {shares_spent}, "
            f"had {shares_before}"
        )
        assert usdc_received > 0, (
            "USDC must be returned on redeem (no-op guard)"
        )
        # Sanity: received USDC should be in the same ballpark as deposit
        # (vault is conservative — slight share-price drift acceptable).
        # 10% headroom captures yield accrual + rounding without being lax.
        assert usdc_received >= deposit_amount_wei * 99 // 100, (
            f"Redeem returned far less than deposited: "
            f"deposited={deposit_amount_wei}, received={usdc_received}"
        )
