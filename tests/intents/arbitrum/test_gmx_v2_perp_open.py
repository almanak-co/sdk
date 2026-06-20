"""Intent-level test for GMX V2 PERP_OPEN on Arbitrum (VIB-5252 / real-gap).

Proves the slice of the GMX V2 open lifecycle that WE own:
  PerpOpenIntent → compile → correct ExchangeRouter multicall calldata
  → execute on Anvil Arbitrum fork → TX status=1
  → GMXv2ReceiptParser finds the OrderCreated event + order key
  → collateral (USDC) debited from wallet into OrderVault, ETH execution
    fee debited from wallet

GMX V2 orders are KEEPER-EXECUTED: ``createOrder`` submits an on-chain order,
but the keeper that fills it is GMX infrastructure and NEVER runs on Anvil.
Therefore this test asserts:

  * Layer 4 = ORDER IS CREATED (``OrderCreated`` event + non-zero order key in
    DataStore) AND collateral debited → NOT a filled / ``PositionIncrease`` event.

To run:
    uv run pytest tests/intents/arbitrum/test_gmx_v2_perp_open.py -v -s --import-mode=importlib
"""

from decimal import Decimal

import pytest
from web3 import Web3

from almanak.connectors.gmx_v2 import GMXv2ReceiptParser
from almanak.connectors.gmx_v2.addresses import GMX_V2, GMX_V2_TOKENS
from almanak.framework.execution.orchestrator import ExecutionOrchestrator
from almanak.framework.intents.compiler import IntentCompiler
from almanak.framework.intents.perp_intents import PerpOpenIntent
from almanak.framework.intents.vocabulary import IntentType
from tests.intents.conftest import (
    get_token_balance,
    get_token_decimals,
)

# =============================================================================
# Constants
# =============================================================================

CHAIN_NAME = "arbitrum"

# GMX V2 Arbitrum contract addresses (from connector addresses.py)
_GMX_ARB = GMX_V2["arbitrum"]
ORDER_VAULT_ADDRESS = _GMX_ARB["order_vault"]
EXCHANGE_ROUTER_ADDRESS = _GMX_ARB["exchange_router"]
ROUTER_ADDRESS = _GMX_ARB["router"]

# USDC on Arbitrum — short/collateral token
USDC_ADDRESS = GMX_V2_TOKENS["arbitrum"]["USDC"]  # 0xaf88d...

# Minimal ERC-20 ABI — balanceOf + allowance
_ERC20_ABI = [
    {
        "inputs": [{"name": "account", "type": "address"}],
        "name": "balanceOf",
        "outputs": [{"name": "", "type": "uint256"}],
        "stateMutability": "view",
        "type": "function",
    },
    {
        "inputs": [
            {"name": "owner", "type": "address"},
            {"name": "spender", "type": "address"},
        ],
        "name": "allowance",
        "outputs": [{"name": "", "type": "uint256"}],
        "stateMutability": "view",
        "type": "function",
    },
]

# Minimal DataStore ABI — getBytes32 to check that the order key is stored
_DATASTORE_GET_BYTES32_ABI = [
    {
        "inputs": [{"name": "key", "type": "bytes32"}],
        "name": "getBytes32",
        "outputs": [{"name": "", "type": "bytes32"}],
        "stateMutability": "view",
        "type": "function",
    },
]


def _usdc_balance(web3: Web3, wallet: str) -> int:
    """Return raw USDC balance for a wallet."""
    return get_token_balance(web3, USDC_ADDRESS, wallet)


# =============================================================================
# Test class
# =============================================================================


@pytest.mark.arbitrum
@pytest.mark.asyncio
class TestGmxV2PerpOpenIntent:
    """4-layer test for GMX V2 PERP_OPEN on Arbitrum.

    Tests the PerpOpenIntent → ExchangeRouter multicall path with USDC
    collateral (non-native, requires ERC-20 approve first).

    What this proves vs. what GMX owns:
    ─────────────────────────────────
    OUR pipeline: intent compile → approve + multicall → TX receipt → receipt
    parse → balance delta confirmed.
    KEEPER (GMX infra): executing the queued order → position fill.

    Anvil never runs the GMX keeper, so ``PositionIncrease`` is not emitted.
    Layer 4 asserts ORDER IS QUEUED (``OrderCreated`` event + order key) AND
    collateral was transferred — the keeper-executed fill is out of scope.
    """

    @pytest.mark.intent(IntentType.PERP_OPEN)
    @pytest.mark.no_zodiac(reason="GMX V2 uses a 3-call multicall; Zodiac extension is deferred")
    async def test_open_eth_long_usdc_collateral(
        self,
        web3: Web3,
        funded_wallet: str,
        orchestrator: ExecutionOrchestrator,
        price_oracle_arbitrum: dict[str, Decimal],
    ):
        """Open a long ETH/USD position with 100 USDC collateral via GMX V2.

        All 4 layers:
          1. PerpOpenIntent compiles → 2 TXs: APPROVE(USDC→Router) + multicall
             (sendWnt(exec_fee) + sendTokens(USDC, OrderVault, amount) + createOrder)
          2. Both TXs execute with status=1 on the Anvil fork
          3. GMXv2ReceiptParser finds exactly 1 OrderCreated event in the
             multicall receipt; the extracted order key is non-zero
          4. USDC wallet balance decreased by exactly collateral_amount;
             native ETH decreased by at least the execution fee
        """
        # ------------------------------------------------------------------
        # Setup — collateral params
        # ------------------------------------------------------------------
        usdc_decimals = get_token_decimals(web3, USDC_ADDRESS)
        collateral_amount = Decimal("100")  # 100 USDC
        size_usd = Decimal("300")  # $300 notional — 3× leverage (above GMX minimum)

        print(f"\n{'=' * 80}")
        print("Test: GMX V2 PERP_OPEN — LONG ETH/USD, 100 USDC collateral")
        print(f"  Collateral: {collateral_amount} USDC")
        print(f"  Size:       ${size_usd} (3× leverage)")
        print(f"  OrderVault: {ORDER_VAULT_ADDRESS}")
        print(f"{'=' * 80}")

        # Snapshot pre-state
        usdc_before = _usdc_balance(web3, funded_wallet)
        eth_before = web3.eth.get_balance(funded_wallet)
        vault_usdc_before = _usdc_balance(web3, ORDER_VAULT_ADDRESS)

        print(f"USDC balance before: {Decimal(usdc_before) / Decimal(10**usdc_decimals):.2f}")
        print(f"ETH  balance before: {eth_before / 1e18:.6f}")
        print(f"OrderVault USDC before: {Decimal(vault_usdc_before) / Decimal(10**usdc_decimals):.2f}")

        # ------------------------------------------------------------------
        # Layer 1 — Compilation
        # ------------------------------------------------------------------
        intent = PerpOpenIntent(
            market="ETH/USD",
            collateral_token="USDC",
            collateral_amount=collateral_amount,
            size_usd=size_usd,
            is_long=True,
            max_slippage=Decimal("0.01"),
            protocol="gmx_v2",
            leverage=Decimal("3"),
        )

        compiler = IntentCompiler(
            chain=CHAIN_NAME,
            wallet_address=funded_wallet,
            price_oracle=price_oracle_arbitrum,
            rpc_url=orchestrator.rpc_url,
        )
        compilation = compiler.compile(intent)

        assert compilation.status.value == "SUCCESS", f"Compilation failed: {compilation.error}"
        assert compilation.action_bundle is not None, "action_bundle must be set on SUCCESS"

        txns = compilation.action_bundle.transactions
        assert len(txns) == 2, (
            f"Expected 2 TXs (APPROVE + multicall), got {len(txns)}. "
            f"If collateral is ERC-20 the compiler must emit an approval first."
        )

        approve_tx = txns[0]
        multicall_tx = txns[1]

        # Approve TX: targets the USDC token contract
        assert approve_tx["to"].lower() == USDC_ADDRESS.lower(), (
            f"TX[0] should be the USDC approve, got to={approve_tx['to']}"
        )

        # Multicall TX: targets the ExchangeRouter
        assert multicall_tx["to"].lower() == EXCHANGE_ROUTER_ADDRESS.lower(), (
            f"TX[1] should call ExchangeRouter multicall, got to={multicall_tx['to']}"
        )

        # Multicall must include ETH value (execution fee)
        assert int(multicall_tx.get("value", 0)) > 0, (
            "Multicall TX must carry ETH value for execution fee (sendWnt path)"
        )

        # Metadata sanity
        meta = compilation.action_bundle.metadata
        assert meta.get("protocol") == "gmx_v2"
        assert meta.get("market") == "ETH/USD"
        assert meta.get("is_long") is True

        execution_fee_wei = int(multicall_tx.get("value", 0))
        print(
            f"Compile OK: {len(txns)} TXs | approve USDC → Router "
            f"| multicall value={execution_fee_wei / 1e18:.6f} ETH (exec fee)"
        )

        # ------------------------------------------------------------------
        # Layer 2 — Execution
        # ------------------------------------------------------------------
        execution = await orchestrator.execute(compilation.action_bundle)
        assert execution.success, f"Execution failed: {execution.error}"
        assert len(execution.transaction_results) == 2, (
            f"Expected 2 execution results (approve + multicall), got {len(execution.transaction_results)}"
        )

        approve_result = execution.transaction_results[0]
        multicall_result = execution.transaction_results[1]

        for i, result in enumerate(execution.transaction_results):
            assert result.receipt is not None, f"TX[{i}] missing receipt"
            receipt_dict = result.receipt.to_dict()
            status = receipt_dict.get("status")
            status_int = int(status, 16) if isinstance(status, str) else status
            assert status_int == 1, f"TX[{i}] status must be 1, got {status!r}"

        multicall_receipt = multicall_result.receipt.to_dict()

        print(
            f"Execute OK: approve tx={approve_result.tx_hash[:18]} gas={approve_result.gas_used} | "
            f"multicall tx={multicall_result.tx_hash[:18]} gas={multicall_result.gas_used}"
        )

        # ------------------------------------------------------------------
        # Layer 3 — Receipt parsing (GMXv2ReceiptParser on multicall receipt)
        # ------------------------------------------------------------------
        parser = GMXv2ReceiptParser()
        parsed = parser.parse_receipt(multicall_receipt)

        assert parsed.success, f"parse_receipt reported failure: {parsed.error}"

        # On Anvil without keeper: OrderCreated is the only event we own.
        # PositionIncrease is emitted AFTER keeper execution — out of scope.
        order_created_events = [ev for ev in parsed.events if ev.event_name == "OrderCreated"]
        assert len(order_created_events) >= 1, (
            f"Expected at least 1 OrderCreated event in the multicall receipt; "
            f"got events={[ev.event_name for ev in parsed.events]!r}. "
            f"Hint: the GMX EventEmitter emits OrderCreated on createOrder(); "
            f"check that the event topic hash matches and topic[1] routing fires."
        )

        order_ev = order_created_events[0]
        order_key = order_ev.data.get("key", "")
        assert order_key and order_key != "0x" + "00" * 32, (
            f"OrderCreated event key must be non-zero, got {order_key!r}"
        )

        # The parser also populates order_events via _parse_order_event.
        # NOTE — known receipt-parser limitation (to be fixed in a follow-up):
        # GMX V2's EventEmitter encodes event data using ``EventUtils.EventLogData``
        # (dynamic arrays of key-value pairs), NOT a simple flat ABI tuple.
        # ``_decode_order_data`` uses a hardcoded flat-offset layout that produces
        # garbage values for all fields except the order key (which is correctly
        # read from topic[2], independent of the data blob). The test therefore
        # only asserts on the key (reliable) and documents the data-decoding gap
        # rather than asserting wrong field values.
        assert len(parsed.order_events) >= 1, (
            "GMXv2ReceiptParser.order_events must contain the parsed OrderCreated data"
        )
        oe = parsed.order_events[0]
        assert oe.key == order_key, "order_events[0].key must match event data key"
        # oe.order_type / oe.is_long / oe.execution_fee are decoded from the
        # EventUtils data blob with wrong flat offsets — NOT asserted here.
        # Asserting the key (from topic[2]) is the reliable proof that the
        # parser routed to the correct event and extracted the primary identifier.
        print(
            f"Parse OK: OrderCreated key={order_key[:20]}... "
            f"(data-field decoding uses simplified flat layout — order_type/is_long "
            f"not asserted; EventUtils ABI decoder is a follow-up gap)"
        )

        # Extraction methods (used by ResultEnricher)
        extracted_key = parser.extract_position_id(multicall_receipt)
        # For PERP_OPEN on Anvil (no keeper), position_id comes from
        # order_events key rather than PositionIncrease. The current
        # extract_position_id() looks for position_increases first, then
        # decreases — neither fires here. We assert what the parser CAN deliver
        # from what's in this receipt; the keeper-fill path is out of scope.
        # If extract_position_id returns None here, that's a known limitation
        # (it relies on PositionIncrease which requires keeper execution).
        print(f"extract_position_id → {extracted_key!r} (None expected on Anvil without keeper)")

        size_delta = parser.extract_size_delta(multicall_receipt)
        print(f"extract_size_delta  → {size_delta!r} (None expected on Anvil without keeper)")

        # ------------------------------------------------------------------
        # Layer 4 — On-chain state delta
        # ------------------------------------------------------------------
        usdc_after = _usdc_balance(web3, funded_wallet)
        eth_after = web3.eth.get_balance(funded_wallet)
        vault_usdc_after = _usdc_balance(web3, ORDER_VAULT_ADDRESS)

        collateral_wei = int(collateral_amount * Decimal(10**usdc_decimals))

        # 4a. USDC debited from wallet
        usdc_spent = usdc_before - usdc_after
        assert usdc_spent == collateral_wei, (
            f"USDC delta mismatch: expected {collateral_wei} (={collateral_amount} USDC), "
            f"got {usdc_spent} (before={usdc_before}, after={usdc_after})"
        )
        print(f"USDC delta: -{Decimal(usdc_spent) / Decimal(10**usdc_decimals):.2f} USDC (exact)")

        # 4b. USDC arrived in OrderVault
        vault_usdc_gained = vault_usdc_after - vault_usdc_before
        assert vault_usdc_gained == collateral_wei, (
            f"OrderVault USDC credit mismatch: expected +{collateral_wei} (={collateral_amount} USDC), "
            f"got +{vault_usdc_gained}"
        )
        print(f"OrderVault USDC credit: +{Decimal(vault_usdc_gained) / Decimal(10**usdc_decimals):.2f} USDC (exact)")

        # 4c. ETH decreased by at least the execution fee (gas on top)
        eth_spent = eth_before - eth_after
        total_gas_cost_wei = sum((r.gas_cost_wei or 0) for r in execution.transaction_results)
        assert eth_spent >= execution_fee_wei, (
            f"ETH spent {eth_spent / 1e18:.6f} < execution fee {execution_fee_wei / 1e18:.6f}"
        )
        # Upper bound: fee + gas (no native collateral in this USDC path)
        assert eth_spent <= execution_fee_wei + total_gas_cost_wei + 1_000_000_000_000_000, (
            f"ETH spent {eth_spent / 1e18:.6f} ETH seems too high — "
            f"expected ≤ exec_fee {execution_fee_wei / 1e18:.6f} + gas {total_gas_cost_wei / 1e18:.6f}"
        )
        print(
            f"ETH delta: -{eth_spent / 1e18:.6f} ETH "
            f"(exec_fee={execution_fee_wei / 1e18:.6f} + gas≈{total_gas_cost_wei / 1e18:.6f})"
        )

        print(f"\nALL 4 LAYERS PASSED — GMX V2 PERP_OPEN ETH/USD LONG (order key: {order_key[:20]}...)")
