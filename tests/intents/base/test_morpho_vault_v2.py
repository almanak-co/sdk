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

_NO_ZODIAC = pytest.mark.no_zodiac(
    reason=(
        "VIB-4307: morpho_vault vault intents are covered directly; "
        "Zodiac vault permission synthesis remains outside this test. "
        "The V2 vault is registered in METAMORPHO_PERMISSION_VAULTS "
        "(deposit/redeem/forceDeallocate)."
    )
)
pytestmark = [pytest.mark.intent(IntentType.VAULT_DEPOSIT, IntentType.VAULT_REDEEM)]

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
    if isinstance(value, bytes | bytearray):
        return Web3.to_hex(value).lower()
    return str(value).lower()


def _receipt_dict(execution_result, topic: str, *, last: bool = False) -> dict:
    """The executed tx that emitted ``topic`` from the vault, as the receipt dict the protocol parser reads.

    ``last=True`` picks the LAST matching transaction: a forced-exit bundle
    emits a Withdraw from the ``forceDeallocate`` leg too (the vault books the
    penalty burn as a withdrawal), so the trailing redeem is the last one.
    """
    results = list(execution_result.transaction_results)
    for tx_result in reversed(results) if last else results:
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


def _receipt_dict_for_tx(tx_result) -> dict:
    """One executed tx's receipt as the dict the parser/enricher read (all logs, no filtering)."""
    receipt_logs = getattr(tx_result.receipt, "logs", None) if tx_result.receipt is not None else None
    logs = list(tx_result.logs or receipt_logs or [])
    return {
        "transactionHash": tx_result.tx_hash,
        "status": 1,
        "logs": [
            {
                "address": entry.get("address") if isinstance(entry, dict) else getattr(entry, "address", ""),
                "topics": [
                    _topic_hex(t)
                    for t in (
                        (entry.get("topics") if isinstance(entry, dict) else getattr(entry, "topics", None)) or []
                    )
                ],
                "data": _topic_hex(
                    entry.get("data", "0x") if isinstance(entry, dict) else getattr(entry, "data", "0x")
                ),
            }
            for entry in logs
        ],
    }


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
@_NO_ZODIAC
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
@_NO_ZODIAC
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


# Forced exit (opt-in). A real shortfall is created by draining the vault's
# liquidity market: a second account is dealt cbBTC, supplies it as collateral
# on that Morpho Blue market and borrows nearly all of its free USDC. The vault's
# other markets still hold liquidity, so a redeem-all with the opt-in covers the
# shortfall via ``forceDeallocate`` and pays the (tiny) penalty; without the
# opt-in the same redeem fails closed at compile time.

MORPHO_BLUE = "0xBBBBBbbBBb9cC5e90e3b3Af64bdAF62C37EEFFCb"
CBBTC = "0xcbB7C0000aB88B473b1f5aFd9ef808440eed33Bf"
BORROWER = "0x70997970C51812dc3A010C7d01b50e0d17dc79C8"  # Anvil default account #1


def _drain_liquidity_market(web3: Web3, sdk) -> tuple[int, int]:
    """Borrow (almost) all free USDC out of the vault's liquidity market. Returns (available_before, available_after)."""
    from almanak.connectors.morpho_vault.sdk import _decode_words

    liquidity_data = sdk.get_v2_liquidity_data(VAULT_ADDRESS)
    assert liquidity_data, "V2 vault has no liquidityData()"
    params_words = _decode_words(liquidity_data)
    loan_token = "0x" + f"{params_words[0]:064x}"[-40:]
    collateral_token = "0x" + f"{params_words[1]:064x}"[-40:]
    assert loan_token.lower() == UNDERLYING_ADDRESS.lower()
    assert collateral_token.lower() == CBBTC.lower(), f"liquidity market collateral changed: {collateral_token}"
    market_id = Web3.keccak(hexstr=liquidity_data).hex()
    market_id = market_id if market_id.startswith("0x") else "0x" + market_id
    adapter = sdk.get_v2_liquidity_adapter(VAULT_ADDRESS)
    morpho = sdk.get_v2_adapter_morpho(adapter)
    assert morpho.lower() == MORPHO_BLUE.lower()
    _, _, available_before = sdk.get_morpho_blue_market_liquidity(morpho, market_id)

    # Source collateral: Anvil cannot locate cbBTC's balance slot (proxy
    # storage), so instead move cbBTC out of Morpho Blue itself — the largest
    # holder on Base — under auto-impersonation, then supply it straight back
    # as the borrower's collateral below. Morpho's cbBTC balance is unchanged
    # once the round trip completes.
    collateral = 10_000 * 10**8  # 10,000 cbBTC — several times the LLTV requirement for the borrow below
    erc20_abi = [
        {
            "name": "approve",
            "type": "function",
            "inputs": [{"type": "address"}, {"type": "uint256"}],
            "outputs": [{"type": "bool"}],
        },
        {
            "name": "transfer",
            "type": "function",
            "inputs": [{"type": "address"}, {"type": "uint256"}],
            "outputs": [{"type": "bool"}],
        },
        {
            "name": "balanceOf",
            "type": "function",
            "inputs": [{"type": "address"}],
            "outputs": [{"type": "uint256"}],
            "stateMutability": "view",
        },
    ]
    erc20 = web3.eth.contract(address=Web3.to_checksum_address(CBBTC), abi=erc20_abi)
    morpho_cbbtc = erc20.functions.balanceOf(Web3.to_checksum_address(MORPHO_BLUE)).call()
    assert morpho_cbbtc >= collateral, f"Morpho Blue holds only {morpho_cbbtc} cbBTC wei; cannot stage {collateral}"
    # The impersonated contract has no ETH for gas on the fork; fund it (and the borrower) first.
    for account in (MORPHO_BLUE, BORROWER):
        resp = web3.provider.make_request("anvil_setBalance", [Web3.to_checksum_address(account), hex(100 * 10**18)])
        assert not resp.get("error"), f"anvil_setBalance failed: {resp}"
    tx = erc20.functions.transfer(BORROWER, collateral).transact({"from": Web3.to_checksum_address(MORPHO_BLUE)})
    assert web3.eth.wait_for_transaction_receipt(tx).status == 1, "impersonated cbBTC transfer reverted"
    # Approve, supply the collateral and borrow on Morpho Blue from the borrower (auto-impersonated).
    erc20 = web3.eth.contract(
        address=Web3.to_checksum_address(CBBTC),
        abi=[
            {
                "name": "approve",
                "type": "function",
                "inputs": [{"type": "address"}, {"type": "uint256"}],
                "outputs": [{"type": "bool"}],
            },
        ],
    )
    tx = erc20.functions.approve(Web3.to_checksum_address(MORPHO_BLUE), collateral).transact({"from": BORROWER})
    web3.eth.wait_for_transaction_receipt(tx)
    mp = (
        Web3.to_checksum_address(loan_token),
        Web3.to_checksum_address(collateral_token),
        Web3.to_checksum_address("0x" + f"{params_words[2]:064x}"[-40:]),
        Web3.to_checksum_address("0x" + f"{params_words[3]:064x}"[-40:]),
        params_words[4],
    )
    mp_type = {
        "type": "tuple",
        "components": [
            {"type": "address"},
            {"type": "address"},
            {"type": "address"},
            {"type": "address"},
            {"type": "uint256"},
        ],
    }
    blue = web3.eth.contract(
        address=Web3.to_checksum_address(MORPHO_BLUE),
        abi=[
            {
                "name": "supplyCollateral",
                "type": "function",
                "inputs": [mp_type, {"type": "uint256"}, {"type": "address"}, {"type": "bytes"}],
                "outputs": [],
            },
            {
                "name": "borrow",
                "type": "function",
                "inputs": [mp_type, {"type": "uint256"}, {"type": "uint256"}, {"type": "address"}, {"type": "address"}],
                "outputs": [{"type": "uint256"}, {"type": "uint256"}],
            },
        ],
    )
    tx = blue.functions.supplyCollateral(mp, collateral, BORROWER, b"").transact({"from": BORROWER, "gas": 1_000_000})
    assert web3.eth.wait_for_transaction_receipt(tx).status == 1, "supplyCollateral reverted"
    borrow = max(available_before - 10 * 10**6, 0)  # leave 10 USDC so the market is not exactly empty
    # Dry-run first so a revert carries its reason instead of a bare status=0.
    blue.functions.borrow(mp, borrow, 0, BORROWER, BORROWER).call({"from": BORROWER})
    tx = blue.functions.borrow(mp, borrow, 0, BORROWER, BORROWER).transact({"from": BORROWER, "gas": 2_000_000})
    receipt = web3.eth.wait_for_transaction_receipt(tx)
    assert receipt.status == 1, "borrow to drain the liquidity market reverted"
    _, _, available_after = sdk.get_morpho_blue_market_liquidity(morpho, market_id)
    return available_before, available_after


@pytest.mark.base
class TestMorphoVaultV2ForcedExitOnChainBase:
    """Forced exit runs through default-on Zodiac/Safe so forceDeallocate+redeem is one MultiSend.

    Setup drain txs are third-party fork actors (BORROWER), not the strategy
    wallet, and stay raw web3 calls. The strategy-wallet deposit/redeem legs
    go through the orchestrator.
    """
    @pytest.mark.asyncio
    async def test_drained_liquidity_market_fails_closed_then_exits_with_opt_in(
        self,
        web3: Web3,
        anvil_rpc_url: str,
        funded_wallet: str,
        orchestrator: ExecutionOrchestrator,
        price_oracle: dict[str, Decimal],
        anvil_eth_call_adapter: Any,
    ) -> None:
        _skip_unless_vault_exists(web3)
        from almanak.connectors.morpho_vault.sdk import MetaMorphoSDK

        sdk = MetaMorphoSDK(anvil_eth_call_adapter, CHAIN_NAME)
        compiler = _compiler(funded_wallet, price_oracle, anvil_rpc_url, anvil_eth_call_adapter)
        deposit_amount = Decimal("100")

        deposit_result = compiler.compile(
            VaultDepositIntent(
                protocol="metamorpho", vault_address=VAULT_ADDRESS, amount=deposit_amount, chain=CHAIN_NAME
            )
        )
        assert deposit_result.status == CompilationStatus.SUCCESS, deposit_result.error
        assert (await orchestrator.execute(deposit_result.action_bundle)).success
        shares = get_token_balance(web3, VAULT_ADDRESS, funded_wallet)
        assert shares > 0

        available_before, available_after = _drain_liquidity_market(web3, sdk)
        assert available_after < 20 * 10**6 < available_before, (available_before, available_after)

        # Without the opt-in: the redeem fails closed, nothing is sent.
        plain = compiler.compile(
            VaultRedeemIntent(protocol="metamorpho", vault_address=VAULT_ADDRESS, shares="all", chain=CHAIN_NAME)
        )
        assert plain.status == CompilationStatus.FAILED, "expected fail-closed on a drained liquidity market"
        assert "allow_force_deallocate=True" in (plain.error or "")
        assert get_token_balance(web3, VAULT_ADDRESS, funded_wallet) == shares

        # With the opt-in: forceDeallocate leg(s) + redeem, penalty within cap.
        forced = compiler.compile(
            VaultRedeemIntent(
                protocol="metamorpho",
                vault_address=VAULT_ADDRESS,
                shares="all",
                chain=CHAIN_NAME,
                allow_force_deallocate=True,
                max_force_deallocate_penalty_bps=10,
            )
        )
        assert forced.status == CompilationStatus.SUCCESS, forced.error
        meta = forced.action_bundle.metadata["force_deallocate"]
        assert int(meta["shortfall_assets"]) > 0 and meta["legs"]
        assert meta["penalty_bps"] <= 10
        assert [tx.tx_type for tx in forced.transactions][-1] == "vault_redeem"
        assert all(tx.tx_type == "vault_force_deallocate" for tx in forced.transactions[:-1])

        usdc_before = get_token_balance(web3, UNDERLYING_ADDRESS, funded_wallet)
        execution = await orchestrator.execute(forced.action_bundle)
        assert execution.success, f"forced exit failed: {execution.error}"
        assert _has_log(execution, WITHDRAW_TOPIC)
        # Layer 3 through the protocol parser: the trailing redeem's Withdraw
        # must decode to the post-penalty share count and a positive payout.
        # Layer 3 through the PRODUCTION enrichment path, not a hand-picked
        # receipt: every executed tx's receipt, in order, exactly as the
        # ResultEnricher collects them (the forceDeallocate receipt with its
        # penalty Withdraw comes first). The enricher must report the owner's
        # payout as the redemption and the penalty alongside it.
        from dataclasses import dataclass
        from dataclasses import field as _field

        from almanak.connectors.morpho_vault import MetaMorphoReceiptParser
        from almanak.framework.execution.result_enricher import ResultEnricher

        @dataclass
        class _Enriched:
            extracted_data: dict = _field(default_factory=dict)
            redeem_data: Any = None
            protocol_fees: Any = None
            extraction_warnings: list = _field(default_factory=list)

        usdc_received = get_token_balance(web3, UNDERLYING_ADDRESS, funded_wallet) - usdc_before
        deposit_wei = int(deposit_amount * Decimal(10**6))
        receipts = [_receipt_dict_for_tx(tx_result) for tx_result in execution.transaction_results]
        assert len(receipts) == len(forced.transactions)
        enriched = _Enriched()
        ResultEnricher(live_mode=False)._extract_field(
            result=enriched,  # type: ignore[arg-type]
            parser=MetaMorphoReceiptParser(),
            receipts=receipts,
            field="redeem_data",
            intent_type="VAULT_REDEEM",
            protocol="metamorpho",
        )
        redeem_data = enriched.extracted_data.get("redeem_data")
        assert redeem_data is not None, "enricher extracted no redeem data from the forced-exit bundle"
        assert redeem_data["shares_burned"] == int(meta["redeem_shares_after_penalty"])
        assert redeem_data["assets_received"] > 0
        # The forceDeallocate leg's penalty is reported separately, never as the payout.
        assert 0 < redeem_data["penalty_shares"] < redeem_data["shares_burned"]
        assert redeem_data["penalty_shares"] <= int(meta["total_penalty_shares"]) + max(
            1, int(meta["total_penalty_shares"]) // 1000
        )
        assert redeem_data["assets_received"] == usdc_received
        assert usdc_received >= deposit_wei * 999 // 1000, "forced exit returned less than 99.9% of the deposit"
        # Only the wei-of-share cushion may remain (worth ~nothing).
        leftover = get_token_balance(web3, VAULT_ADDRESS, funded_wallet)
        assert leftover <= 10**6 + int(meta["total_penalty_shares"]), f"unexpected leftover shares: {leftover}"
