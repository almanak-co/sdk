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
