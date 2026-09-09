"""Intent tests for Morpho Vault V2 VAULT_DEPOSIT / VAULT_REDEEM on Base.

Twin of ``test_morpho_vault.py`` (MetaMorpho v1, Moonwell USDC) against a
**Vault V2** — Steakhouse Prime USDC (``steakUSDC``), the largest USDC vault
on Base and the vault behind session be5a3567 / ALM-10044.

What V2 changes, and what these tests pin:

* every ERC-4626 ``max*`` view returns 0 by design, so redeem-all must size
  from ``balanceOf`` (the v1 ``maxRedeem`` path reported "No shares to
  redeem" for a wallet that had just deposited — ALM-10044);
* withdrawals are served from idle assets + one liquidity adapter and revert
  when short, so the compiler simulates the redeem before building it;
* the connector fingerprints the generation on-chain (``adaptersLength()``),
  never from the symbol.

The vault was created at Base block 37,404,640 (2025-10-27); on an older
fork pin the address has no code and the on-chain tests skip.

To run::

    uv run pytest tests/intents/base/test_morpho_vault_v2.py -v -s
"""

from decimal import Decimal
from typing import Any

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
from tests.intents.conftest import get_token_balance

pytestmark = [
    pytest.mark.no_zodiac(
        reason=(
            "VIB-4307: morpho_vault vault intents are covered directly; "
            "Zodiac vault permission synthesis remains outside this test. "
            "The V2 vault is registered in METAMORPHO_PERMISSION_VAULTS for discovery."
        )
    ),
    pytest.mark.intent(IntentType.VAULT_DEPOSIT, IntentType.VAULT_REDEEM),
]

CHAIN_NAME = "base"
# Steakhouse Prime USDC — Morpho Vault V2 on Base (curator: Steakhouse Financial).
VAULT_ADDRESS = "0xbeef0e0834849acc03f0089f01f4f1eeb06873c9"
UNDERLYING_ADDRESS = "0x833589fCD6eDb6E08f4c7C32D4f71b54bdA02913"  # USDC
VAULT_CREATION_BLOCK = 37_404_640

DEPOSIT_TOPIC = "0xdcbc1c05240f31ff3ad067ef1ee35ce4997762752e3a095284754544f4c709d7"
WITHDRAW_TOPIC = "0xfbde797d201c681b91056529119e0b02407c7bb96a4a2c75c01fc9667232c8db"


def _skip_unless_vault_exists(web3: Web3) -> None:
    if len(web3.eth.get_code(Web3.to_checksum_address(VAULT_ADDRESS))) == 0:
        pytest.skip(
            f"Vault V2 {VAULT_ADDRESS} has no code at this fork block "
            f"(created at block {VAULT_CREATION_BLOCK}); pin ANVIL_FORK_BLOCK_BASE later."
        )


def _topic_hex(value) -> str:
    if isinstance(value, (bytes, bytearray)):
        return Web3.to_hex(value).lower()
    return str(value).lower()


def _receipt_dict(execution_result, topic: str) -> dict:
    """The executed tx that emitted ``topic`` from the vault, as the receipt dict the protocol parser reads."""
    for tx_result in execution_result.transaction_results:
        receipt_logs = getattr(tx_result.receipt, "logs", None) if tx_result.receipt is not None else None
        logs = list(tx_result.logs or receipt_logs or [])
        for log in logs:
            log_addr = log.get("address", "") if isinstance(log, dict) else getattr(log, "address", "")
            topics = (log.get("topics", []) if isinstance(log, dict) else getattr(log, "topics", [])) or []
            if (
                topics
                and str(log_addr).lower() == VAULT_ADDRESS.lower()
                and topic.removeprefix("0x") in _topic_hex(topics[0])
            ):
                return {
                    "transactionHash": tx_result.tx_hash,
                    "status": 1,
                    "logs": [
                        {
                            "address": (
                                entry.get("address") if isinstance(entry, dict) else getattr(entry, "address", "")
                            ),
                            "topics": [
                                _topic_hex(t)
                                for t in (
                                    (entry.get("topics") if isinstance(entry, dict) else getattr(entry, "topics", None))
                                    or []
                                )
                            ],
                            "data": _topic_hex(
                                entry.get("data", "0x") if isinstance(entry, dict) else getattr(entry, "data", "0x")
                            ),
                        }
                        for entry in logs
                    ],
                }
    return {"logs": []}


def _has_log(execution_result, topic: str) -> bool:
    """True if any executed tx emitted ``topic`` from the vault.

    ``TransactionResult`` carries the decoded logs both at the top level
    (``logs``) and on the receipt; read whichever is populated.
    """
    for tx_result in execution_result.transaction_results:
        receipt_logs = getattr(tx_result.receipt, "logs", None) if tx_result.receipt is not None else None
        for log in tx_result.logs or receipt_logs or []:
            if isinstance(log, dict):
                log_addr = log.get("address", "") or ""
                topics = log.get("topics", []) or []
            else:
                log_addr = getattr(log, "address", "") or ""
                topics = getattr(log, "topics", []) or []
            if not topics or str(log_addr).lower() != VAULT_ADDRESS.lower():
                continue
            if topic.removeprefix("0x") in _topic_hex(topics[0]):
                return True
    return False


def _compiler(
    funded_wallet: str, price_oracle: dict[str, Decimal], anvil_rpc_url: str, gateway_client: Any
) -> IntentCompiler:
    """Compiler bound to the fork's gateway-shaped eth_call adapter.

    The vault compiler needs a connected ``GatewayClient`` (``rpc.Call``) for
    its on-chain reads and for the V2 redeem simulation, which is an eth_call
    sent ``from`` the owner. ``AnvilEthCallAdapter`` forwards raw params to
    the fork provider, so the ``from`` field is honoured.
    """
    return IntentCompiler(
        chain=CHAIN_NAME,
        wallet_address=funded_wallet,
        price_oracle=price_oracle,
        rpc_url=anvil_rpc_url,
        gateway_client=gateway_client,
        config=IntentCompilerConfig(allow_placeholder_prices=True),
    )


@pytest.mark.base
class TestMorphoVaultV2GenerationDetection:
    @pytest.mark.asyncio
    async def test_vault_is_fingerprinted_as_v2_on_chain(  # noqa: layers
        self, web3: Web3, funded_wallet: str, anvil_eth_call_adapter: Any
    ) -> None:
        """Layer 1: the SDK reads ``adaptersLength()`` and reports V2; v1 reads revert."""
        _skip_unless_vault_exists(web3)
        from almanak.connectors.morpho_vault.sdk import VAULT_VERSION_V2, MetaMorphoSDK, RPCError

        sdk = MetaMorphoSDK(anvil_eth_call_adapter, CHAIN_NAME)
        assert sdk.detect_vault_version(VAULT_ADDRESS) == VAULT_VERSION_V2
        assert sdk.get_max_redeem(VAULT_ADDRESS, funded_wallet) == 0  # V2: by design, even when funded
        with pytest.raises(RPCError):
            sdk.get_fee(VAULT_ADDRESS)  # v1-only selector reverts on V2
        info = sdk.get_vault_info(VAULT_ADDRESS)
        assert info.vault_version == VAULT_VERSION_V2
        assert info.asset.lower() == UNDERLYING_ADDRESS.lower()
        assert info.adapters and info.liquidity_adapter in info.adapters
        assert info.timelock == 0


@pytest.mark.base
class TestMorphoVaultV2DepositRedeemOnChainBase:
    """Layers 2-4: deposit USDC into steakUSDC (V2), then redeem ALL shares back."""

    @pytest.mark.asyncio
    async def test_deposit_then_redeem_all_full_4_layer(
        self,
        web3: Web3,
        anvil_rpc_url: str,
        funded_wallet: str,
        orchestrator: ExecutionOrchestrator,
        price_oracle: dict[str, Decimal],
        anvil_eth_call_adapter: Any,
    ) -> None:
        _skip_unless_vault_exists(web3)
        usdc_decimals = 6
        deposit_amount = Decimal("100")
        deposit_amount_wei = int(deposit_amount * Decimal(10**usdc_decimals))
        compiler = _compiler(funded_wallet, price_oracle, anvil_rpc_url, anvil_eth_call_adapter)

        usdc_start = get_token_balance(web3, UNDERLYING_ADDRESS, funded_wallet)
        assert usdc_start >= deposit_amount_wei, f"USDC funding insufficient: have {usdc_start}"
        shares_start = get_token_balance(web3, VAULT_ADDRESS, funded_wallet)

        # Deposit.
        deposit_result = compiler.compile(
            VaultDepositIntent(
                protocol="metamorpho", vault_address=VAULT_ADDRESS, amount=deposit_amount, chain=CHAIN_NAME
            )
        )
        assert deposit_result.status == CompilationStatus.SUCCESS, deposit_result.error
        assert deposit_result.action_bundle.metadata["vault_version"] == "v2"
        deposit_exec = await orchestrator.execute(deposit_result.action_bundle)
        assert deposit_exec.success, f"Deposit failed: {deposit_exec.error}"
        assert _has_log(deposit_exec, DEPOSIT_TOPIC), "Expected ERC-4626 Deposit event from the V2 vault"

        shares_after_deposit = get_token_balance(web3, VAULT_ADDRESS, funded_wallet)
        assert shares_after_deposit > shares_start
        # Layer 3 through the protocol parser, not topic matching: the enricher's
        # deposit extraction must read the V2 vault's Deposit event correctly.
        from almanak.connectors.morpho_vault import MetaMorphoReceiptParser

        deposit_data = MetaMorphoReceiptParser().extract_deposit_data(_receipt_dict(deposit_exec, DEPOSIT_TOPIC))
        assert deposit_data is not None, "receipt parser did not extract the V2 Deposit event"
        assert deposit_data["assets"] == deposit_amount_wei
        assert deposit_data["shares"] == shares_after_deposit - shares_start
        assert get_token_balance(web3, UNDERLYING_ADDRESS, funded_wallet) == usdc_start - deposit_amount_wei

        # Redeem ALL: the path that failed on V2 before generation-aware sizing.
        usdc_before_redeem = get_token_balance(web3, UNDERLYING_ADDRESS, funded_wallet)
        redeem_result = compiler.compile(
            VaultRedeemIntent(protocol="metamorpho", vault_address=VAULT_ADDRESS, shares="all", chain=CHAIN_NAME)
        )
        assert redeem_result.status == CompilationStatus.SUCCESS, (
            f"redeem-all must compile on V2 (maxRedeem is 0 by design; sizing is balanceOf): {redeem_result.error}"
        )
        assert redeem_result.action_bundle.metadata["vault_version"] == "v2"
        assert int(redeem_result.action_bundle.metadata["shares_wei"]) == shares_after_deposit
        assert len(redeem_result.transactions) == 1

        redeem_exec = await orchestrator.execute(redeem_result.action_bundle)
        assert redeem_exec.success, f"Redeem failed: {redeem_exec.error}"
        assert _has_log(redeem_exec, WITHDRAW_TOPIC), "Expected ERC-4626 Withdraw event from the V2 vault"
        redeem_data = MetaMorphoReceiptParser().extract_redeem_data(_receipt_dict(redeem_exec, WITHDRAW_TOPIC))
        assert redeem_data is not None, "receipt parser did not extract the V2 Withdraw event"
        assert redeem_data["shares_burned"] == shares_after_deposit
        assert redeem_data["assets_received"] > 0

        shares_after_redeem = get_token_balance(web3, VAULT_ADDRESS, funded_wallet)
        usdc_after_redeem = get_token_balance(web3, UNDERLYING_ADDRESS, funded_wallet)
        # V2 redeem(balanceOf) is exact — no v1-style rounding dust may remain.
        assert shares_after_redeem == 0, f"leftover shares after redeem-all: {shares_after_redeem}"
        usdc_received = usdc_after_redeem - usdc_before_redeem
        assert usdc_received > 0
        assert usdc_received >= deposit_amount_wei * 99 // 100

    @pytest.mark.asyncio
    async def test_redeem_all_with_no_position_reports_no_shares(  # noqa: layers
        self,
        web3: Web3,
        anvil_rpc_url: str,
        funded_wallet: str,
        price_oracle: dict[str, Decimal],
        anvil_eth_call_adapter: Any,
    ) -> None:
        """An empty wallet is 'No shares to redeem', not a simulation revert."""
        _skip_unless_vault_exists(web3)
        if get_token_balance(web3, VAULT_ADDRESS, funded_wallet) != 0:
            pytest.skip("wallet already holds steakUSDC shares on this fork")
        compiler = _compiler(funded_wallet, price_oracle, anvil_rpc_url, anvil_eth_call_adapter)
        result = compiler.compile(
            VaultRedeemIntent(protocol="metamorpho", vault_address=VAULT_ADDRESS, shares="all", chain=CHAIN_NAME)
        )
        assert result.status == CompilationStatus.FAILED
        assert result.error == "No shares to redeem"
