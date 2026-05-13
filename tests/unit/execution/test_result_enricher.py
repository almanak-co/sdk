"""Tests for ResultEnricher - swap_amounts enrichment pipeline.

Verifies that the enrichment pipeline correctly extracts swap_amounts
from transaction receipts for various protocols (Enso, SushiSwap V3,
Uniswap V3) when running through the gateway execution path.

Covers:
- VIB-544: Enso swap_amounts not enriched (missing from_address in gateway receipts)
- VIB-624: SushiSwap V3 swap_amounts not enriched
- VIB-546: Enrichment diagnostic logging
"""

from dataclasses import dataclass, field
from decimal import Decimal
from typing import Any

from almanak.framework.execution.extracted_data import SwapAmounts
from almanak.framework.execution.result_enricher import ResultEnricher

# ---------------------------------------------------------------------------
# Minimal stubs for ExecutionResult / TransactionResult / TransactionReceipt
# ---------------------------------------------------------------------------

@dataclass
class _FakeReceipt:
    """Mimics TransactionReceipt.to_dict() for enricher consumption."""
    tx_hash: str = "0xabc123"
    block_number: int = 100
    block_hash: str = "0xblock"
    gas_used: int = 200000
    effective_gas_price: int = 1000000000
    status: int = 1
    logs: list = field(default_factory=list)
    from_address: str | None = None
    to_address: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "tx_hash": self.tx_hash,
            "block_number": self.block_number,
            "block_hash": self.block_hash,
            "gas_used": self.gas_used,
            "effective_gas_price": str(self.effective_gas_price),
            "status": self.status,
            "logs": self.logs,
            "contract_address": None,
            "from_address": self.from_address,
            "to_address": self.to_address,
        }


@dataclass
class _FakeTxResult:
    success: bool = True
    tx_hash: str = "0xabc123"
    receipt: _FakeReceipt | None = None
    gas_used: int = 200000


@dataclass
class _FakeExecResult:
    success: bool = True
    transaction_results: list = field(default_factory=list)
    position_id: int | None = None
    swap_amounts: SwapAmounts | None = None
    lp_close_data: Any = None
    extracted_data: dict = field(default_factory=dict)
    extraction_warnings: list = field(default_factory=list)


@dataclass
class _FakeContext:
    chain: str = "arbitrum"
    protocol: str | None = None


@dataclass
class _FakeIntent:
    intent_type: str = "SWAP"
    protocol: str | None = None


# ---------------------------------------------------------------------------
# ERC-20 Transfer event topic
# ---------------------------------------------------------------------------
TRANSFER_TOPIC = "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"

# Uniswap V3 / SushiSwap V3 Swap event topic
SWAP_TOPIC = "0xc42079f94a6350d7e6235f29174924f928cc2ac818eb64fed8004e115fbcca67"


def _make_transfer_log(
    token_address: str,
    from_addr: str,
    to_addr: str,
    amount: int,
) -> dict:
    """Build a minimal ERC-20 Transfer log entry."""
    from_topic = "0x" + from_addr.lower().replace("0x", "").zfill(64)
    to_topic = "0x" + to_addr.lower().replace("0x", "").zfill(64)
    data = "0x" + hex(amount)[2:].zfill(64)
    return {
        "address": token_address,
        "topics": [TRANSFER_TOPIC, from_topic, to_topic],
        "data": data,
        "logIndex": 0,
    }


def _make_swap_log(
    pool_address: str,
    sender: str,
    recipient: str,
    amount0: int,
    amount1: int,
    sqrt_price_x96: int = 2**96,
    liquidity: int = 10**18,
    tick: int = 0,
) -> dict:
    """Build a minimal Uniswap V3 / SushiSwap V3 Swap log entry."""
    sender_topic = "0x" + sender.lower().replace("0x", "").zfill(64)
    recipient_topic = "0x" + recipient.lower().replace("0x", "").zfill(64)

    def _int256_hex(val: int) -> str:
        if val >= 0:
            return hex(val)[2:].zfill(64)
        return hex((1 << 256) + val)[2:].zfill(64)

    data = "0x" + (
        _int256_hex(amount0)
        + _int256_hex(amount1)
        + hex(sqrt_price_x96)[2:].zfill(64)
        + hex(liquidity)[2:].zfill(64)
        + _int256_hex(tick)
    )
    return {
        "address": pool_address,
        "topics": [SWAP_TOPIC, sender_topic, recipient_topic],
        "data": data,
        "logIndex": 0,
    }


# ===========================================================================
# Tests
# ===========================================================================


class TestEnsoSwapEnrichment:
    """VIB-544: Enso swap_amounts enrichment via Transfer events."""

    WALLET = "0x1234567890abcdef1234567890abcdef12345678"
    USDC_ADDR = "0xaf88d065e77c8cC2239327C5EDb3A432268e5831"
    WETH_ADDR = "0x82af49447d8a07e3bd95bd0d56f35241523fbab1"

    def _make_enso_receipt(self, from_address: str | None = None) -> _FakeReceipt:
        """Build a receipt with Transfer logs (USDC out, WETH in)."""
        logs = [
            _make_transfer_log(self.USDC_ADDR, self.WALLET, "0xEnsoRouter", 50_000_000),
            _make_transfer_log(self.WETH_ADDR, "0xEnsoRouter", self.WALLET, 24_000_000_000_000_000),
        ]
        return _FakeReceipt(
            status=1,
            logs=logs,
            from_address=from_address,
        )

    def test_enso_enrichment_with_from_address(self):
        """Swap amounts extracted when from_address is present."""
        receipt = self._make_enso_receipt(from_address=self.WALLET)
        result = _FakeExecResult(
            transaction_results=[_FakeTxResult(receipt=receipt)],
        )
        intent = _FakeIntent(protocol="enso")
        context = _FakeContext(chain="arbitrum", protocol="enso")

        enricher = ResultEnricher()
        enriched = enricher.enrich(result, intent, context)

        assert enriched.swap_amounts is not None, "swap_amounts should be populated"
        assert enriched.swap_amounts.amount_out > 0

    def test_enso_enrichment_without_from_address_fails(self):
        """Without from_address, Enso parser cannot determine wallet direction."""
        receipt = self._make_enso_receipt(from_address=None)
        result = _FakeExecResult(
            transaction_results=[_FakeTxResult(receipt=receipt)],
        )
        intent = _FakeIntent(protocol="enso")
        context = _FakeContext(chain="arbitrum", protocol="enso")

        enricher = ResultEnricher()
        enriched = enricher.enrich(result, intent, context)

        # This documents the pre-fix behavior: no from_address -> no swap_amounts
        assert enriched.swap_amounts is None


class TestSushiSwapV3SwapEnrichment:
    """VIB-624 / VIB-1437: SushiSwap V3 swap_amounts enrichment via Swap events."""

    POOL_BASE = "0x1234000000000000000000000000000000000001"
    POOL_OPTIMISM = "0xabcdef1234567890abcdef1234567890abcdef12"
    ROUTER_BASE = "0x2626664c2603336E57B271c5C0b26F421741e481"
    ROUTER_OPTIMISM = "0x8516944E89f296eb6473d79aED1Ba12088016c9e"
    WALLET = "0xabcdef0000000000000000000000000000000001"
    USDC = "0x0b2c639c533813f4aa9d7837caf62653d097ff85"  # USDC on Optimism

    def test_sushiswap_v3_swap_enrichment(self):
        """SushiSwap V3 swap_amounts extracted from Swap event on Base."""
        usdc_base = "0x833589fCD6eDb6E08f4c7C32D4f71b54bdA02913"
        weth_base = "0x4200000000000000000000000000000000000006"
        # amount0 > 0 means user paid token0, amount1 < 0 means user received token1
        swap_log = _make_swap_log(
            pool_address=self.POOL_BASE,
            sender=self.ROUTER_BASE,
            recipient=self.WALLET,
            amount0=50_000_000,       # 50 USDC in (6 decimals)
            amount1=-24_000_000_000_000_000,  # ~0.024 WETH out
        )
        transfer_out = _make_transfer_log(usdc_base, self.WALLET, self.ROUTER_BASE, 50_000_000)
        transfer_in = _make_transfer_log(weth_base, self.ROUTER_BASE, self.WALLET, 24_000_000_000_000_000)
        receipt = _FakeReceipt(status=1, logs=[transfer_out, swap_log, transfer_in], from_address=self.WALLET)
        result = _FakeExecResult(
            transaction_results=[_FakeTxResult(receipt=receipt)],
        )
        intent = _FakeIntent(protocol="sushiswap_v3")
        context = _FakeContext(chain="base", protocol="sushiswap_v3")

        enricher = ResultEnricher()
        enriched = enricher.enrich(result, intent, context)

        assert enriched.swap_amounts is not None, "swap_amounts should be populated for SushiSwap V3"
        assert enriched.swap_amounts.amount_in > 0
        assert enriched.swap_amounts.amount_out > 0

    def test_sushiswap_v3_swap_enrichment_optimism(self):
        """VIB-1437: SushiSwap V3 swap_amounts extracted from Swap event on Optimism."""
        usdc_op = "0x0b2C639c533813f4Aa9D7837CAf62653d097Ff85"
        weth_op = "0x4200000000000000000000000000000000000006"
        swap_log = _make_swap_log(
            pool_address=self.POOL_OPTIMISM,
            sender=self.ROUTER_OPTIMISM,
            recipient=self.WALLET,
            amount0=500_000_000,      # 500 USDC in (6 decimals)
            amount1=-180_000_000_000_000_000,  # ~0.18 WETH out
        )
        transfer_out = _make_transfer_log(usdc_op, self.WALLET, self.ROUTER_OPTIMISM, 500_000_000)
        transfer_in = _make_transfer_log(weth_op, self.ROUTER_OPTIMISM, self.WALLET, 180_000_000_000_000_000)
        receipt = _FakeReceipt(status=1, logs=[transfer_out, swap_log, transfer_in], gas_used=22796, from_address=self.WALLET)
        result = _FakeExecResult(
            transaction_results=[_FakeTxResult(receipt=receipt)],
        )
        intent = _FakeIntent(protocol="sushiswap_v3")
        context = _FakeContext(chain="optimism", protocol="sushiswap_v3")

        enricher = ResultEnricher()
        enriched = enricher.enrich(result, intent, context)

        assert enriched.swap_amounts is not None, (
            "swap_amounts should be populated for SushiSwap V3 on Optimism (VIB-1437)"
        )
        assert enriched.swap_amounts.amount_in == 500_000_000
        assert enriched.swap_amounts.amount_out == 180_000_000_000_000_000

    def test_sushiswap_v3_multi_tx_bundle_optimism(self):
        """VIB-1437: swap_amounts extracted from 2nd TX (swap) in approve+swap bundle on Optimism.

        This is the exact multi-TX bundle scenario from iter 90:
        TX 1: USDC approve (55,449 gas) -- has Approval event, NO Swap event
        TX 2: exactInputSingle swap (22,796 gas) -- has Swap event
        """
        usdc_op = "0x0b2C639c533813f4Aa9D7837CAf62653d097Ff85"
        weth_op = "0x4200000000000000000000000000000000000006"
        APPROVAL_TOPIC = "0x8c5be1e5ebec7d5bd14f71427d1e84f3dd0314c0f7b2291e5b200ac8c7c3b925"

        # TX 1: approve receipt (has Approval event, NO Swap event)
        approval_log = {
            "address": self.USDC,
            "topics": [
                APPROVAL_TOPIC,
                "0x" + self.WALLET.lower().replace("0x", "").zfill(64),
                "0x" + self.ROUTER_OPTIMISM.lower().replace("0x", "").zfill(64),
            ],
            "data": "0x" + "f" * 64,  # max uint256 approval
            "logIndex": 0,
        }
        approve_receipt = _FakeReceipt(
            tx_hash="0xapprove1",
            status=1,
            logs=[approval_log],
            gas_used=55449,
            from_address=self.WALLET,
        )

        # TX 2: swap receipt (has Swap event + Transfer events)
        swap_log = _make_swap_log(
            pool_address=self.POOL_OPTIMISM,
            sender=self.ROUTER_OPTIMISM,
            recipient=self.WALLET,
            amount0=500_000_000,      # 500 USDC in
            amount1=-180_000_000_000_000_000,  # ~0.18 WETH out
        )
        transfer_out = _make_transfer_log(usdc_op, self.WALLET, self.ROUTER_OPTIMISM, 500_000_000)
        transfer_in = _make_transfer_log(weth_op, self.ROUTER_OPTIMISM, self.WALLET, 180_000_000_000_000_000)
        swap_receipt = _FakeReceipt(
            tx_hash="0xswap2",
            status=1,
            logs=[transfer_out, swap_log, transfer_in],
            gas_used=22796,
            from_address=self.WALLET,
        )

        result = _FakeExecResult(
            transaction_results=[
                _FakeTxResult(tx_hash="0xapprove1", receipt=approve_receipt),
                _FakeTxResult(tx_hash="0xswap2", receipt=swap_receipt),
            ],
        )
        intent = _FakeIntent(protocol="sushiswap_v3")
        context = _FakeContext(chain="optimism", protocol="sushiswap_v3")

        enricher = ResultEnricher()
        enriched = enricher.enrich(result, intent, context)

        assert enriched.swap_amounts is not None, (
            "swap_amounts should be populated from the swap TX (TX 2) in a "
            "2-TX approve+swap bundle on Optimism (VIB-1437)"
        )
        assert enriched.swap_amounts.amount_in == 500_000_000
        assert enriched.swap_amounts.amount_out == 180_000_000_000_000_000

    def test_sushiswap_v3_gateway_hex_status_and_null_logs_optimism(self):
        """VIB-1437: GatewayExecutionResult with hex status and null logs still enriches swap_amounts.

        Exercises the actual gateway normalization path (hex "0x1" -> 1, logs=None -> [])
        that caused the original bug, unlike the other tests which use pre-normalized fakes.
        """
        from almanak.framework.execution.gateway_orchestrator import GatewayExecutionResult

        usdc_op = "0x0b2C639c533813f4Aa9D7837CAf62653d097Ff85"
        weth_op = "0x4200000000000000000000000000000000000006"

        swap_log = _make_swap_log(
            pool_address=self.POOL_OPTIMISM,
            sender=self.ROUTER_OPTIMISM,
            recipient=self.WALLET,
            amount0=500_000_000,
            amount1=-180_000_000_000_000_000,
        )
        transfer_out = _make_transfer_log(usdc_op, self.WALLET, self.ROUTER_OPTIMISM, 500_000_000)
        transfer_in = _make_transfer_log(weth_op, self.ROUTER_OPTIMISM, self.WALLET, 180_000_000_000_000_000)

        gw_result = GatewayExecutionResult(
            success=True,
            tx_hashes=["0xapprove1", "0xswap2"],
            total_gas_used=78_245,
            receipts=[
                {"status": "0x1", "gas_used": 55_449, "logs": None},  # approve tx: OP-style hex status + null logs
                {"status": "0x1", "gas_used": 22_796, "logs": [transfer_out, swap_log, transfer_in], "from_address": self.WALLET},  # swap tx
            ],
            execution_id="test-vib-1437",
        )

        result = _FakeExecResult(transaction_results=gw_result.transaction_results)
        intent = _FakeIntent(protocol="sushiswap_v3")
        context = _FakeContext(chain="optimism", protocol="sushiswap_v3")

        enriched = ResultEnricher().enrich(result, intent, context)

        assert enriched.swap_amounts is not None, (
            "GatewayExecutionResult with hex status '0x1' and null logs should still enrich swap_amounts (VIB-1437)"
        )
        assert enriched.swap_amounts.amount_in == 500_000_000
        assert enriched.swap_amounts.amount_out == 180_000_000_000_000_000


class TestUniswapV3SwapEnrichment:
    """Verify Uniswap V3 swap enrichment still works (regression guard)."""

    POOL = "0x88e6A0c2dDD26FEEb64F039a2c41296FcB3f5640"
    ROUTER = "0xE592427A0AEce92De3Edee1F18E0157C05861564"
    WALLET = "0xabcdef0000000000000000000000000000000002"

    def test_uniswap_v3_swap_enrichment(self):
        """Uniswap V3 swap_amounts extracted from Swap event."""
        usdc_arb = "0xaf88d065e77c8cC2239327C5EDb3A432268e5831"
        weth_arb = "0x82aF49447D8a07e3bd95BD0d56f35241523fBab1"
        swap_log = _make_swap_log(
            pool_address=self.POOL,
            sender=self.ROUTER,
            recipient=self.WALLET,
            amount0=1_000_000_000,    # 1000 USDC
            amount1=-500_000_000_000_000_000,  # ~0.5 WETH
        )
        transfer_out = _make_transfer_log(usdc_arb, self.WALLET, self.ROUTER, 1_000_000_000)
        transfer_in = _make_transfer_log(weth_arb, self.ROUTER, self.WALLET, 500_000_000_000_000_000)
        receipt = _FakeReceipt(status=1, logs=[transfer_out, swap_log, transfer_in], from_address=self.WALLET)
        result = _FakeExecResult(
            transaction_results=[_FakeTxResult(receipt=receipt)],
        )
        intent = _FakeIntent(protocol="uniswap_v3")
        context = _FakeContext(chain="arbitrum", protocol="uniswap_v3")

        enricher = ResultEnricher()
        enriched = enricher.enrich(result, intent, context)

        assert enriched.swap_amounts is not None, "swap_amounts should be populated for Uniswap V3"
        assert enriched.swap_amounts.amount_in > 0
        assert enriched.swap_amounts.amount_out > 0


class TestEnrichmentProtocolResolution:
    """Test protocol resolution from intent and context."""

    def test_protocol_from_intent(self):
        """Protocol taken from intent when available."""
        receipt = _FakeReceipt(status=1, logs=[])
        result = _FakeExecResult(
            transaction_results=[_FakeTxResult(receipt=receipt)],
        )
        intent = _FakeIntent(protocol="uniswap_v3")
        context = _FakeContext(chain="arbitrum", protocol=None)

        enricher = ResultEnricher()
        enriched = enricher.enrich(result, intent, context)
        # No crash, protocol resolved from intent
        assert enriched.success

    def test_protocol_from_context_fallback(self):
        """Protocol taken from context when intent has None."""
        receipt = _FakeReceipt(status=1, logs=[])
        result = _FakeExecResult(
            transaction_results=[_FakeTxResult(receipt=receipt)],
        )
        intent = _FakeIntent(protocol=None)
        context = _FakeContext(chain="arbitrum", protocol="enso")

        enricher = ResultEnricher()
        enriched = enricher.enrich(result, intent, context)
        assert enriched.success

    def test_no_protocol_skips_enrichment(self):
        """No protocol on intent or context skips enrichment gracefully."""
        receipt = _FakeReceipt(status=1, logs=[])
        result = _FakeExecResult(
            transaction_results=[_FakeTxResult(receipt=receipt)],
        )
        intent = _FakeIntent(protocol=None)
        context = _FakeContext(chain="arbitrum", protocol=None)

        enricher = ResultEnricher()
        enriched = enricher.enrich(result, intent, context)
        assert enriched.swap_amounts is None


class TestGatewayReceiptFromAddress:
    """Test that gateway receipt path preserves from_address for enrichment."""

    def test_gateway_receipt_includes_from_address(self):
        """GatewayExecutionResult.transaction_results passes from_address."""
        from almanak.framework.execution.gateway_orchestrator import GatewayExecutionResult

        wallet = "0x1234567890abcdef1234567890abcdef12345678"
        receipt_data = {
            "status": 1,
            "gas_used": 100000,
            "block_number": 42,
            "block_hash": "0xblockhash",
            "effective_gas_price": 1000000000,
            "from_address": wallet,
            "to_address": "0xrouter",
            "logs": [],
        }
        gw_result = GatewayExecutionResult(
            success=True,
            tx_hashes=["0xtx1"],
            total_gas_used=100000,
            receipts=[receipt_data],
            execution_id="test",
        )

        tx_results = gw_result.transaction_results
        assert len(tx_results) == 1
        assert tx_results[0].receipt is not None

        receipt_dict = tx_results[0].receipt.to_dict()
        assert receipt_dict["from_address"] == wallet
        assert receipt_dict["to_address"] == "0xrouter"

    def test_gateway_receipt_without_from_address(self):
        """from_address gracefully defaults to None when not in receipt data."""
        from almanak.framework.execution.gateway_orchestrator import GatewayExecutionResult

        receipt_data = {
            "status": 1,
            "gas_used": 100000,
            "block_number": 42,
            "block_hash": "0xblockhash",
            "logs": [],
        }
        gw_result = GatewayExecutionResult(
            success=True,
            tx_hashes=["0xtx1"],
            total_gas_used=100000,
            receipts=[receipt_data],
            execution_id="test",
        )

        tx_results = gw_result.transaction_results
        receipt_dict = tx_results[0].receipt.to_dict()
        assert receipt_dict["from_address"] is None


class TestEnrichmentDiagnosticLogging:
    """VIB-546: Verify debug logging at enrichment decision points."""

    def test_failed_execution_logs_skip(self, caplog):
        """Debug log emitted when execution failed."""
        import logging

        result = _FakeExecResult(success=False)
        intent = _FakeIntent(protocol="uniswap_v3")
        context = _FakeContext(chain="arbitrum")

        with caplog.at_level(logging.DEBUG, logger="almanak.framework.execution.result_enricher"):
            enricher = ResultEnricher()
            enricher.enrich(result, intent, context)

        assert any("execution failed" in r.message for r in caplog.records)

    def test_no_protocol_logs_skip(self, caplog):
        """Debug log emitted when protocol is None."""
        import logging

        receipt = _FakeReceipt(status=1, logs=[])
        result = _FakeExecResult(
            transaction_results=[_FakeTxResult(receipt=receipt)],
        )
        intent = _FakeIntent(protocol=None)
        context = _FakeContext(chain="arbitrum", protocol=None)

        with caplog.at_level(logging.DEBUG, logger="almanak.framework.execution.result_enricher"):
            enricher = ResultEnricher()
            enricher.enrich(result, intent, context)

        assert any("protocol=None" in r.message for r in caplog.records)


# ---------------------------------------------------------------------------
# VIB-1446: Solana lending enrichment (no longer blanket-skipped)
# ---------------------------------------------------------------------------


class TestSolanaLendingEnrichment:
    """Verify that Solana lending receipts are enriched instead of skipped."""

    def _make_solana_receipt(self, pre_balances, post_balances):
        """Build a fake Solana receipt dict (no to_dict, just a raw dict)."""
        return {
            "meta": {
                "preTokenBalances": pre_balances,
                "postTokenBalances": post_balances,
            },
            "success": True,
        }

    def test_solana_supply_enriched(self):
        """Jupiter Lend supply_amounts is populated via enrichment on Solana chain."""
        usdc_mint = "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v"
        solana_receipt = self._make_solana_receipt(
            pre_balances=[
                {"accountIndex": 0, "mint": usdc_mint, "uiTokenAmount": {"amount": "100000000", "decimals": 6}},
            ],
            post_balances=[
                {"accountIndex": 0, "mint": usdc_mint, "uiTokenAmount": {"amount": "0", "decimals": 6}},
            ],
        )
        # Use raw dict receipt (Solana receipts don't have to_dict)
        tx_result = _FakeTxResult(success=True, receipt=solana_receipt)
        # Override: receipt is a dict, so enricher's _collect_receipts handles it
        result = _FakeExecResult(transaction_results=[tx_result])
        intent = _FakeIntent(intent_type="SUPPLY", protocol="jupiter_lend")
        context = _FakeContext(chain="solana")

        enricher = ResultEnricher()
        enriched = enricher.enrich(result, intent, context)

        assert "supply_amounts" in enriched.extracted_data
        supply = enriched.extracted_data["supply_amounts"]
        assert supply is not None
        assert supply.token == usdc_mint
        assert supply.amount == Decimal("100")

    def test_solana_borrow_enriched(self):
        """Jupiter Lend borrow_amounts is populated via enrichment on Solana chain."""
        sol_mint = "So11111111111111111111111111111111111111112"
        solana_receipt = self._make_solana_receipt(
            pre_balances=[
                {"accountIndex": 0, "mint": sol_mint, "uiTokenAmount": {"amount": "0", "decimals": 9}},
            ],
            post_balances=[
                {"accountIndex": 0, "mint": sol_mint, "uiTokenAmount": {"amount": "2000000000", "decimals": 9}},
            ],
        )
        tx_result = _FakeTxResult(success=True, receipt=solana_receipt)
        result = _FakeExecResult(transaction_results=[tx_result])
        intent = _FakeIntent(intent_type="BORROW", protocol="jupiter_lend")
        context = _FakeContext(chain="solana")

        enricher = ResultEnricher()
        enriched = enricher.enrich(result, intent, context)

        assert "borrow_amounts" in enriched.extracted_data
        borrow = enriched.extracted_data["borrow_amounts"]
        assert borrow is not None
        assert borrow.token == sol_mint
        assert borrow.amount == Decimal("2")

    def test_kamino_supply_enriched(self):
        """Kamino supply_amounts is also enriched on Solana chain."""
        usdc_mint = "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v"
        solana_receipt = self._make_solana_receipt(
            pre_balances=[
                {"accountIndex": 0, "mint": usdc_mint, "uiTokenAmount": {"amount": "50000000", "decimals": 6}},
            ],
            post_balances=[
                {"accountIndex": 0, "mint": usdc_mint, "uiTokenAmount": {"amount": "0", "decimals": 6}},
            ],
        )
        tx_result = _FakeTxResult(success=True, receipt=solana_receipt)
        result = _FakeExecResult(transaction_results=[tx_result])
        intent = _FakeIntent(intent_type="SUPPLY", protocol="kamino")
        context = _FakeContext(chain="solana")

        enricher = ResultEnricher()
        enriched = enricher.enrich(result, intent, context)

        assert "supply_amounts" in enriched.extracted_data
        supply = enriched.extracted_data["supply_amounts"]
        assert supply is not None
        assert supply.amount == Decimal("50")


# ===========================================================================
# VIB-3203: expected_out threading from bundle metadata -> swap extractor
# ===========================================================================


class TestExpectedOutPlumbing:
    """Verify ``bundle_metadata["expected_output_human"]`` is threaded
    through :meth:`ResultEnricher.enrich` to the parser's ``extract_swap_amounts``
    as the ``expected_out`` kwarg — enabling realized slippage_bps computation
    (VIB-3203 Phase A)."""

    def test_expected_out_threaded_to_parser(self):
        """Enricher passes expected_out kwarg sourced from bundle metadata."""
        captured_kwargs: dict[str, Any] = {}

        class _SpyParser:
            """Minimal parser that records the kwargs it was called with."""

            def __init__(self, **_kwargs):  # accept (chain=...) from registry
                pass

            def parse_receipt(self, receipt):  # noqa: ARG002
                class _Ok:
                    success = True
                    error = None

                return _Ok()

            def extract_swap_amounts(
                self,
                receipt,  # noqa: ARG002
                *,
                expected_out: Decimal | None = None,
            ) -> SwapAmounts:
                captured_kwargs["expected_out"] = expected_out
                return SwapAmounts(
                    amount_in=100,
                    amount_out=95,
                    amount_in_decimal=Decimal("100"),
                    amount_out_decimal=Decimal("95"),
                    effective_price=Decimal("0.95"),
                    slippage_bps=(
                        int(((expected_out - Decimal("95")) / expected_out) * Decimal(10_000))
                        if expected_out and expected_out > 0
                        else None
                    ),
                    expected_out_decimal=expected_out,
                    token_in="USDC",
                    token_out="ETH",
                )

        tx_result = _FakeTxResult(success=True, receipt=_FakeReceipt(logs=[{}]))
        result = _FakeExecResult(transaction_results=[tx_result])
        intent = _FakeIntent(intent_type="SWAP", protocol="spy")
        context = _FakeContext(chain="arbitrum", protocol="spy")

        enricher = ResultEnricher(live_mode=False)
        # Inject the spy via custom registration so the registry hands it back.
        enricher.parser_registry.register("spy", _SpyParser)

        enriched = enricher.enrich(
            result,
            intent,
            context,
            bundle_metadata={"expected_output_human": "100"},
        )

        assert captured_kwargs["expected_out"] == Decimal("100")
        assert enriched.swap_amounts is not None
        # (100 - 95) / 100 * 10_000 = 500 bps
        assert enriched.swap_amounts.slippage_bps == 500
        assert enriched.swap_amounts.expected_out_decimal == Decimal("100")

    def test_missing_expected_output_leaves_slippage_none(self):
        """When bundle metadata has no expected_output_human, kwarg is not set."""
        captured_kwargs: dict[str, Any] = {}

        class _SpyParser:
            def __init__(self, **_kwargs):  # accept (chain=...) from registry
                pass

            def parse_receipt(self, receipt):  # noqa: ARG002
                class _Ok:
                    success = True
                    error = None

                return _Ok()

            def extract_swap_amounts(
                self,
                receipt,  # noqa: ARG002
                *,
                expected_out: Decimal | None = None,
            ) -> SwapAmounts:
                captured_kwargs["expected_out"] = expected_out
                return SwapAmounts(
                    amount_in=100,
                    amount_out=95,
                    amount_in_decimal=Decimal("100"),
                    amount_out_decimal=Decimal("95"),
                    effective_price=Decimal("0.95"),
                    slippage_bps=None,
                    expected_out_decimal=None,
                    token_in="USDC",
                    token_out="ETH",
                )

        tx_result = _FakeTxResult(success=True, receipt=_FakeReceipt(logs=[{}]))
        result = _FakeExecResult(transaction_results=[tx_result])
        intent = _FakeIntent(intent_type="SWAP", protocol="spy2")
        context = _FakeContext(chain="arbitrum", protocol="spy2")

        enricher = ResultEnricher(live_mode=False)
        enricher.parser_registry.register("spy2", _SpyParser)

        # Both absent-metadata and metadata-without-the-key should leave
        # expected_out at default (None).
        enricher.enrich(result, intent, context)
        assert captured_kwargs["expected_out"] is None

        enricher.enrich(result, intent, context, bundle_metadata={"other_key": "123"})
        assert captured_kwargs["expected_out"] is None

    def test_legacy_parser_without_kwarg_degrades_gracefully(self):
        """Parsers without expected_out kwarg keep working (back-compat)."""

        class _LegacyParser:
            """Mimics the pre-VIB-3203 signature: no expected_out kwarg."""

            def __init__(self, **_kwargs):  # accept (chain=...) from registry
                pass

            def parse_receipt(self, receipt):  # noqa: ARG002
                class _Ok:
                    success = True
                    error = None

                return _Ok()

            def extract_swap_amounts(self, receipt):  # noqa: ARG002
                return SwapAmounts(
                    amount_in=1,
                    amount_out=1,
                    amount_in_decimal=Decimal("1"),
                    amount_out_decimal=Decimal("1"),
                    effective_price=Decimal("1"),
                    slippage_bps=None,
                    token_in="USDC",
                    token_out="ETH",
                )

        tx_result = _FakeTxResult(success=True, receipt=_FakeReceipt(logs=[{}]))
        result = _FakeExecResult(transaction_results=[tx_result])
        intent = _FakeIntent(intent_type="SWAP", protocol="legacy")
        context = _FakeContext(chain="arbitrum", protocol="legacy")

        enricher = ResultEnricher(live_mode=False)
        enricher.parser_registry.register("legacy", _LegacyParser)

        # Pass bundle_metadata with expected_output_human — parser should NOT crash.
        enriched = enricher.enrich(
            result,
            intent,
            context,
            bundle_metadata={"expected_output_human": "100"},
        )
        assert enriched.swap_amounts is not None
        assert enriched.swap_amounts.slippage_bps is None


# ===========================================================================
# VIB-4320 — Per-protocol extraction-spec overlay
# ===========================================================================


@dataclass
class _LpExecResult:
    """ExecutionResult shape rich enough for LP_OPEN / LP_COLLECT_FEES paths.

    The base ``_FakeExecResult`` above only carries SWAP-relevant fields; the
    enricher's ``_attach_to_result`` reads ``result.<field>`` for typed fields
    (position_id, lp_close_data, bridge_data) and ``result.extracted_data``
    for everything else, including ``bin_ids``. Mirror the production
    dataclass shape so the per-protocol overlay regression tests can route
    through the real enricher path.
    """

    success: bool = True
    transaction_results: list = field(default_factory=list)
    position_id: int | None = None
    tick_lower: int | None = None
    tick_upper: int | None = None
    liquidity: int | None = None
    lp_open_data: Any = None
    lp_close_data: Any = None
    bridge_data: Any = None
    swap_amounts: SwapAmounts | None = None
    protocol_fees: Any = None
    bin_ids: list[int] | None = None
    fees0: Any = None
    fees1: Any = None
    extracted_data: dict = field(default_factory=dict)
    extraction_warnings: list = field(default_factory=list)


class _PinnedRegistry:
    """Registry stub that always returns a single pinned parser instance.

    Avoids touching the global ``_default_registry`` between tests.
    """

    def __init__(self, parser: Any) -> None:
        self._parser = parser

    def get(self, protocol, chain=None, **kwargs):  # noqa: ARG002
        return self._parser


def _bin_warning_present(warnings_: list[str], field_name: str) -> bool:
    """True iff any warning mentions ``'<field_name>'`` (SUPPORTED_EXTRACTIONS shape)."""
    needle = f"'{field_name}'"
    return any(needle in w for w in warnings_)


class TestExtractionSpecPerProtocolOverlay:
    """VIB-4320 — per-protocol extraction-spec overlay.

    Generic ``EXTRACTION_SPECS`` is protocol-neutral; TJ-V2-only fields like
    ``bin_ids`` live in ``EXTRACTION_SPECS_BY_PROTOCOL`` and are appended onto
    the merged spec only when the resolved protocol matches.
    """

    # ----- 1. Uniswap V3 LP_OPEN no longer emits the bin_ids capability warning.

    def test_uniswap_v3_lp_open_no_bin_ids_warning(self) -> None:
        from almanak.framework.connectors.uniswap_v3.receipt_parser import (
            UniswapV3ReceiptParser,
        )

        parser = UniswapV3ReceiptParser(chain="arbitrum")
        enricher = ResultEnricher(parser_registry=_PinnedRegistry(parser), live_mode=False)
        result = _LpExecResult(transaction_results=[_FakeTxResult(receipt=_FakeReceipt())])
        intent = _FakeIntent(intent_type="LP_OPEN", protocol="uniswap_v3")
        context = _FakeContext(chain="arbitrum", protocol="uniswap_v3")

        enriched = enricher.enrich(result, intent, context)

        assert not _bin_warning_present(enriched.extraction_warnings, "bin_ids"), (
            f"Unexpected bin_ids warning for uniswap_v3 LP_OPEN: "
            f"{enriched.extraction_warnings}"
        )

    # ----- 2. PancakeSwap V3 LP_OPEN no longer emits the bin_ids warning.

    def test_pancakeswap_v3_lp_open_no_bin_ids_warning(self) -> None:
        from almanak.framework.connectors.pancakeswap_v3.receipt_parser import (
            PancakeSwapV3ReceiptParser,
        )

        parser = PancakeSwapV3ReceiptParser(chain="arbitrum")
        enricher = ResultEnricher(parser_registry=_PinnedRegistry(parser), live_mode=False)
        result = _LpExecResult(transaction_results=[_FakeTxResult(receipt=_FakeReceipt())])
        intent = _FakeIntent(intent_type="LP_OPEN", protocol="pancakeswap_v3")
        context = _FakeContext(chain="arbitrum", protocol="pancakeswap_v3")

        enriched = enricher.enrich(result, intent, context)

        assert not _bin_warning_present(enriched.extraction_warnings, "bin_ids"), (
            f"Unexpected bin_ids warning for pancakeswap_v3 LP_OPEN: "
            f"{enriched.extraction_warnings}"
        )

    # ----- 3. Uniswap V3 LP_COLLECT_FEES: no bin_ids warning;
    #         fees0/fees1 warnings still fire (VIB-4344 follow-up).

    def test_uniswap_v3_lp_collect_fees_no_bin_ids_warning(self) -> None:
        from almanak.framework.connectors.uniswap_v3.receipt_parser import (
            UniswapV3ReceiptParser,
        )

        parser = UniswapV3ReceiptParser(chain="arbitrum")
        enricher = ResultEnricher(parser_registry=_PinnedRegistry(parser), live_mode=False)
        result = _LpExecResult(transaction_results=[_FakeTxResult(receipt=_FakeReceipt())])
        intent = _FakeIntent(intent_type="LP_COLLECT_FEES", protocol="uniswap_v3")
        context = _FakeContext(chain="arbitrum", protocol="uniswap_v3")

        enriched = enricher.enrich(result, intent, context)

        assert not _bin_warning_present(enriched.extraction_warnings, "bin_ids"), (
            f"Unexpected bin_ids warning for uniswap_v3 LP_COLLECT_FEES: "
            f"{enriched.extraction_warnings}"
        )
        # fees0 / fees1 are genuinely unsupported on Uniswap V3 today (VIB-4344
        # follow-up). Their SUPPORTED_EXTRACTIONS warning must still fire so
        # the fee-harvest gap stays visible until the extractors are
        # implemented; merely moving them into the overlay would silence the
        # signal without fixing the underlying gap.
        assert _bin_warning_present(enriched.extraction_warnings, "fees0"), (
            f"Expected fees0 warning still fires: {enriched.extraction_warnings}"
        )
        assert _bin_warning_present(enriched.extraction_warnings, "fees1"), (
            f"Expected fees1 warning still fires: {enriched.extraction_warnings}"
        )

    # ----- 4. PancakeSwap V3 LP_COLLECT_FEES: no bin_ids warning.

    def test_pancakeswap_v3_lp_collect_fees_no_bin_ids_warning(self) -> None:
        from almanak.framework.connectors.pancakeswap_v3.receipt_parser import (
            PancakeSwapV3ReceiptParser,
        )

        parser = PancakeSwapV3ReceiptParser(chain="arbitrum")
        enricher = ResultEnricher(parser_registry=_PinnedRegistry(parser), live_mode=False)
        result = _LpExecResult(transaction_results=[_FakeTxResult(receipt=_FakeReceipt())])
        intent = _FakeIntent(intent_type="LP_COLLECT_FEES", protocol="pancakeswap_v3")
        context = _FakeContext(chain="arbitrum", protocol="pancakeswap_v3")

        enriched = enricher.enrich(result, intent, context)

        assert not _bin_warning_present(enriched.extraction_warnings, "bin_ids"), (
            f"Unexpected bin_ids warning for pancakeswap_v3 LP_COLLECT_FEES: "
            f"{enriched.extraction_warnings}"
        )

    # ----- 5. TraderJoe V2 LP_OPEN still extracts bin_ids into extracted_data,
    #         AND the bin_ids warning does NOT fire (TJ V2 parser does not
    #         declare SUPPORTED_EXTRACTIONS — the capability check skips when
    #         the parser omits the declaration).

    def test_traderjoe_v2_lp_open_still_extracts_bin_ids_into_extracted_data(self) -> None:
        from almanak.framework.connectors.traderjoe_v2.receipt_parser import (
            EVENT_TOPICS,
            TraderJoeV2ReceiptParser,
        )

        wallet = "0x" + "11" * 20
        pool = "0x" + "22" * 20
        bin_ids = [8388607, 8388608, 8388609]

        # ABI encoding for DepositedToBins data — mirror tests/unit/connectors/
        # traderjoe_v2/test_traderjoe_v2_receipt_parser_extras.py::_bins_data
        def _uint256_hex(value: int) -> str:
            return f"{value:064x}"

        ids_offset_hex = _uint256_hex(0x40)
        amounts_offset = 0x40 + 32 + len(bin_ids) * 32
        amounts_offset_hex = _uint256_hex(amounts_offset)
        ids_len_hex = _uint256_hex(len(bin_ids))
        ids_elements = "".join(_uint256_hex(b) for b in bin_ids)
        amounts_len_hex = _uint256_hex(0)
        data_hex = (
            "0x"
            + ids_offset_hex
            + amounts_offset_hex
            + ids_len_hex
            + ids_elements
            + amounts_len_hex
        )

        topic_addr = "0x" + "00" * 12 + wallet[2:].lower()
        deposit_log = {
            "topics": [EVENT_TOPICS["DepositedToBins"], topic_addr, topic_addr],
            "address": pool,
            "data": data_hex,
            "logIndex": 0,
        }

        parser = TraderJoeV2ReceiptParser()
        enricher = ResultEnricher(parser_registry=_PinnedRegistry(parser), live_mode=False)
        result = _LpExecResult(
            transaction_results=[_FakeTxResult(receipt=_FakeReceipt(logs=[deposit_log]))]
        )
        intent = _FakeIntent(intent_type="LP_OPEN", protocol="traderjoe_v2")
        context = _FakeContext(chain="avalanche", protocol="traderjoe_v2")

        enriched = enricher.enrich(result, intent, context)

        assert enriched.extracted_data.get("bin_ids") == bin_ids, (
            f"TJ V2 bin_ids missing from extracted_data: {enriched.extracted_data}"
        )
        assert not _bin_warning_present(enriched.extraction_warnings, "bin_ids"), (
            f"Unexpected bin_ids warning for traderjoe_v2 LP_OPEN: "
            f"{enriched.extraction_warnings}"
        )

    # ----- 6. LPPositionTracker._extract_bin_ids reads the enriched result.

    def test_lp_position_tracker_captures_bin_ids_after_enrichment(self) -> None:
        from almanak.framework.connectors.traderjoe_v2.receipt_parser import (
            EVENT_TOPICS,
            TraderJoeV2ReceiptParser,
        )
        from almanak.framework.strategies.lp_position_tracker import LPPositionTracker

        wallet = "0x" + "11" * 20
        pool = "0x" + "22" * 20
        bin_ids = [8388607, 8388608, 8388609]

        def _uint256_hex(value: int) -> str:
            return f"{value:064x}"

        ids_offset_hex = _uint256_hex(0x40)
        amounts_offset_hex = _uint256_hex(0x40 + 32 + len(bin_ids) * 32)
        ids_len_hex = _uint256_hex(len(bin_ids))
        ids_elements = "".join(_uint256_hex(b) for b in bin_ids)
        amounts_len_hex = _uint256_hex(0)
        data_hex = (
            "0x"
            + ids_offset_hex
            + amounts_offset_hex
            + ids_len_hex
            + ids_elements
            + amounts_len_hex
        )
        topic_addr = "0x" + "00" * 12 + wallet[2:].lower()
        deposit_log = {
            "topics": [EVENT_TOPICS["DepositedToBins"], topic_addr, topic_addr],
            "address": pool,
            "data": data_hex,
            "logIndex": 0,
        }

        parser = TraderJoeV2ReceiptParser()
        enricher = ResultEnricher(parser_registry=_PinnedRegistry(parser), live_mode=False)
        result = _LpExecResult(
            transaction_results=[_FakeTxResult(receipt=_FakeReceipt(logs=[deposit_log]))]
        )
        intent = _FakeIntent(intent_type="LP_OPEN", protocol="traderjoe_v2")
        context = _FakeContext(chain="avalanche", protocol="traderjoe_v2")

        enriched = enricher.enrich(result, intent, context)

        captured = LPPositionTracker._extract_bin_ids(enriched)
        assert captured == bin_ids, (
            f"LPPositionTracker did not pick up bin_ids from enriched result: "
            f"captured={captured} expected={bin_ids} "
            f"extracted_data={enriched.extracted_data}"
        )

    # ----- 6b. TraderJoe V2 LP_COLLECT_FEES (nitpick from CodeRabbit on PR #2269).

    def test_traderjoe_v2_lp_collect_fees_still_extracts_bin_ids_into_extracted_data(
        self,
    ) -> None:
        """LP_COLLECT_FEES emits ``WithdrawnFromBins`` (fees are withdrawn from
        bins), so the overlay must keep ``bin_ids`` extraction wired for this
        intent type the same way LP_OPEN does."""
        from almanak.framework.connectors.traderjoe_v2.receipt_parser import (
            EVENT_TOPICS,
            TraderJoeV2ReceiptParser,
        )

        wallet = "0x" + "11" * 20
        pool = "0x" + "22" * 20
        bin_ids = [8388607, 8388608, 8388609]

        def _uint256_hex(value: int) -> str:
            return f"{value:064x}"

        ids_offset_hex = _uint256_hex(0x40)
        amounts_offset_hex = _uint256_hex(0x40 + 32 + len(bin_ids) * 32)
        ids_len_hex = _uint256_hex(len(bin_ids))
        ids_elements = "".join(_uint256_hex(b) for b in bin_ids)
        amounts_len_hex = _uint256_hex(0)
        data_hex = (
            "0x"
            + ids_offset_hex
            + amounts_offset_hex
            + ids_len_hex
            + ids_elements
            + amounts_len_hex
        )
        topic_addr = "0x" + "00" * 12 + wallet[2:].lower()
        withdraw_log = {
            "topics": [EVENT_TOPICS["WithdrawnFromBins"], topic_addr, topic_addr],
            "address": pool,
            "data": data_hex,
            "logIndex": 0,
        }

        parser = TraderJoeV2ReceiptParser()
        enricher = ResultEnricher(parser_registry=_PinnedRegistry(parser), live_mode=False)
        result = _LpExecResult(
            transaction_results=[_FakeTxResult(receipt=_FakeReceipt(logs=[withdraw_log]))]
        )
        intent = _FakeIntent(intent_type="LP_COLLECT_FEES", protocol="traderjoe_v2")
        context = _FakeContext(chain="avalanche", protocol="traderjoe_v2")

        enriched = enricher.enrich(result, intent, context)

        assert enriched.extracted_data.get("bin_ids") == bin_ids, (
            f"TJ V2 bin_ids missing from extracted_data on LP_COLLECT_FEES: "
            f"{enriched.extracted_data}"
        )
        assert not _bin_warning_present(enriched.extraction_warnings, "bin_ids"), (
            f"Unexpected bin_ids warning for traderjoe_v2 LP_COLLECT_FEES: "
            f"{enriched.extraction_warnings}"
        )

    # ----- 6c. Protocol aliases must canonicalise into the overlay (Codex P2 fix).

    def test_traderjoe_v2_alias_normalized_for_overlay_lookup(self) -> None:
        """``ReceiptParserRegistry.get`` normalises aliases like
        ``trader-joe-v2`` to ``traderjoe_v2``. The overlay lookup must do the
        same — otherwise an aliased intent gets the base spec, ``extract_bin_ids``
        is never invoked, and downstream LP close/fee collection cannot reuse
        the captured bins. Regression guard for the Codex P2 finding on PR #2269.
        """
        from almanak.framework.connectors.traderjoe_v2.receipt_parser import (
            EVENT_TOPICS,
            TraderJoeV2ReceiptParser,
        )

        wallet = "0x" + "11" * 20
        pool = "0x" + "22" * 20
        bin_ids = [4242, 4243]

        def _uint256_hex(value: int) -> str:
            return f"{value:064x}"

        ids_offset_hex = _uint256_hex(0x40)
        amounts_offset_hex = _uint256_hex(0x40 + 32 + len(bin_ids) * 32)
        ids_len_hex = _uint256_hex(len(bin_ids))
        ids_elements = "".join(_uint256_hex(b) for b in bin_ids)
        amounts_len_hex = _uint256_hex(0)
        data_hex = (
            "0x"
            + ids_offset_hex
            + amounts_offset_hex
            + ids_len_hex
            + ids_elements
            + amounts_len_hex
        )
        topic_addr = "0x" + "00" * 12 + wallet[2:].lower()
        deposit_log = {
            "topics": [EVENT_TOPICS["DepositedToBins"], topic_addr, topic_addr],
            "address": pool,
            "data": data_hex,
            "logIndex": 0,
        }

        parser = TraderJoeV2ReceiptParser()
        enricher = ResultEnricher(parser_registry=_PinnedRegistry(parser), live_mode=False)
        result = _LpExecResult(
            transaction_results=[_FakeTxResult(receipt=_FakeReceipt(logs=[deposit_log]))]
        )
        # Use a non-canonical alias on both intent and context.
        intent = _FakeIntent(intent_type="LP_OPEN", protocol="trader-joe-v2")
        context = _FakeContext(chain="avalanche", protocol="trader-joe-v2")

        enriched = enricher.enrich(result, intent, context)

        assert enriched.extracted_data.get("bin_ids") == bin_ids, (
            "Aliased TraderJoe V2 protocol must still flow through the overlay "
            f"and populate bin_ids; got: {enriched.extracted_data}"
        )
        assert not _bin_warning_present(enriched.extraction_warnings, "bin_ids"), (
            f"Unexpected bin_ids warning for aliased traderjoe_v2: "
            f"{enriched.extraction_warnings}"
        )

    # ----- 7. Pure unit test of _merge_spec_with_overlay.

    def test_overlay_merge_dedup_and_order(self) -> None:
        # (a) unknown protocol returns base unchanged.
        base = list(ResultEnricher.EXTRACTION_SPECS["LP_OPEN"])
        merged = ResultEnricher._merge_spec_with_overlay("LP_OPEN", "no_such_protocol")
        assert merged == base
        # (b) overlay fields append at the tail.
        merged_tj = ResultEnricher._merge_spec_with_overlay("LP_OPEN", "traderjoe_v2")
        assert merged_tj[: len(base)] == base, "base fields must come first"
        assert merged_tj[-1] == "bin_ids", "overlay field appended at tail"
        # (c) duplicates collapse — overlay containing a field already in base
        # must not duplicate it. Drive this by temporarily extending the
        # overlay class attribute and restoring it.
        saved = ResultEnricher.EXTRACTION_SPECS_BY_PROTOCOL.get("__test__", None)
        try:
            ResultEnricher.EXTRACTION_SPECS_BY_PROTOCOL["__test__"] = {
                # ``position_id`` is already in base LP_OPEN spec — must dedup.
                "LP_OPEN": ["position_id", "bin_ids", "position_id"],
            }
            merged_dedup = ResultEnricher._merge_spec_with_overlay("LP_OPEN", "__test__")
            assert merged_dedup.count("position_id") == 1
            assert merged_dedup.count("bin_ids") == 1
            # base ordering must be preserved verbatim
            assert merged_dedup[: len(base)] == base
        finally:
            if saved is None:
                ResultEnricher.EXTRACTION_SPECS_BY_PROTOCOL.pop("__test__", None)
            else:
                ResultEnricher.EXTRACTION_SPECS_BY_PROTOCOL["__test__"] = saved
        # (d) protocol=None returns base.
        merged_none = ResultEnricher._merge_spec_with_overlay("LP_OPEN", None)
        assert merged_none == base

    # ----- 8. Forward-compat guard: a parser that declares
    #         SUPPORTED_EXTRACTIONS with Uniswap-V3-style fields under
    #         protocol="sushiswap_v3" must not emit any bin_ids / fees0 /
    #         fees1 warning once the per-protocol overlay is in place. If
    #         SushiSwap V3 (or any V3 fork) standardises on
    #         SUPPORTED_EXTRACTIONS later, this catches a regression where
    #         the generic spec silently reintroduces bin_ids.

    def test_sushiswap_v3_lp_open_no_warning_when_supported_extractions_declared(self) -> None:
        class _SyntheticV3Parser:
            """A SushiSwap-V3-style parser that DOES declare SUPPORTED_EXTRACTIONS.

            Mirrors the Uniswap V3 declaration shape so we can assert the
            overlay still suppresses bin_ids for protocols that have not
            implemented it.
            """

            SUPPORTED_EXTRACTIONS: frozenset[str] = frozenset(
                {
                    "position_id",
                    "swap_amounts",
                    "tick_lower",
                    "tick_upper",
                    "liquidity",
                    "lp_open_data",
                    "lp_close_data",
                    "protocol_fees",
                }
            )

            def __init__(self, **_kwargs: Any) -> None:
                pass

            def parse_receipt(self, receipt: Any) -> Any:  # noqa: ARG002
                class _Ok:
                    success = True
                    error = None

                return _Ok()

        parser = _SyntheticV3Parser()
        enricher = ResultEnricher(parser_registry=_PinnedRegistry(parser), live_mode=False)
        result = _LpExecResult(transaction_results=[_FakeTxResult(receipt=_FakeReceipt())])
        intent = _FakeIntent(intent_type="LP_OPEN", protocol="sushiswap_v3")
        context = _FakeContext(chain="arbitrum", protocol="sushiswap_v3")

        enriched = enricher.enrich(result, intent, context)

        # Spec is the generic LP_OPEN — overlay only adds ``bin_ids`` for
        # ``traderjoe_v2``. The synthetic V3 parser declares everything in
        # base spec, so no SUPPORTED_EXTRACTIONS warning for any field.
        for field_name in ("bin_ids", "fees0", "fees1"):
            assert not _bin_warning_present(enriched.extraction_warnings, field_name), (
                f"Unexpected {field_name!r} warning for sushiswap_v3 LP_OPEN: "
                f"{enriched.extraction_warnings}"
            )
