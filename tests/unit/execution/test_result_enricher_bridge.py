"""Tests for ResultEnricher — BRIDGE intent enrichment (VIB-3226).

Covers:
- ResultEnricher attaches typed ``bridge_data`` on successful BRIDGE intents.
- Bridge adapter name threads through ``ActionBundle.metadata["bridge"]``
  when ``intent.protocol`` is None (default for BridgeIntent).
- Across receipt parser extracts ``BridgeData`` from V3FundsDeposited + the
  wallet Transfer fallback.
- Stargate receipt parser extracts ``BridgeData`` from OFTSent + the wallet
  Transfer fallback.
- Missing bridge event -> enricher does not crash, leaves ``bridge_data=None``.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from decimal import Decimal
from typing import Any

from almanak.framework.connectors.across.adapter import ACROSS_SPOKE_POOL_ADDRESSES
from almanak.framework.connectors.across.receipt_parser import (
    AcrossReceiptParser,
    V3_FUNDS_DEPOSITED_TOPIC,
)
from almanak.framework.connectors.stargate.adapter import STARGATE_ROUTER_ADDRESSES
from almanak.framework.connectors.stargate.receipt_parser import (
    OFT_SENT_TOPIC,
    StargateReceiptParser,
)
from almanak.framework.connectors.lifi.receipt_parser import LiFiReceiptParser
from almanak.framework.execution.extracted_data import BridgeData
from almanak.framework.execution.result_enricher import ResultEnricher

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

TRANSFER_TOPIC = "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"
WALLET = "0x1234567890abcdef1234567890abcdef12345678"
USDC_BASE = "0x833589fCD6eDb6E08f4c7C32D4f71b54bdA02913"
USDC_ARBITRUM = "0xaf88d065e77c8cC2239327C5EDb3A432268e5831"


# ---------------------------------------------------------------------------
# Fakes mirroring the existing result enricher test fixtures
# ---------------------------------------------------------------------------


@dataclass
class _FakeReceipt:
    tx_hash: str = "0xbridge1"
    block_number: int = 100
    block_hash: str = "0xblock"
    gas_used: int = 250_000
    effective_gas_price: int = 1_000_000_000
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
    tx_hash: str = "0xbridge1"
    receipt: _FakeReceipt | None = None
    gas_used: int = 250_000


@dataclass
class _FakeExecResult:
    success: bool = True
    transaction_results: list = field(default_factory=list)
    position_id: int | None = None
    swap_amounts: Any = None
    lp_close_data: Any = None
    bridge_data: BridgeData | None = None
    extracted_data: dict = field(default_factory=dict)
    extraction_warnings: list = field(default_factory=list)


@dataclass
class _FakeContext:
    chain: str = "base"
    protocol: str | None = None


@dataclass
class _FakeIntent:
    intent_type: str = "BRIDGE"
    protocol: str | None = None


# ---------------------------------------------------------------------------
# Log builders
# ---------------------------------------------------------------------------


def _transfer_log(token_address: str, from_addr: str, to_addr: str, amount: int) -> dict:
    from_topic = "0x" + from_addr.lower().replace("0x", "").zfill(64)
    to_topic = "0x" + to_addr.lower().replace("0x", "").zfill(64)
    data = "0x" + hex(amount)[2:].zfill(64)
    return {
        "address": token_address,
        "topics": [TRANSFER_TOPIC, from_topic, to_topic],
        "data": data,
        "logIndex": 0,
    }


def _v3_funds_deposited_log(
    *,
    spoke_pool: str,
    input_token: str,
    output_token: str,
    input_amount: int,
    output_amount: int,
    destination_chain_id: int,
    depositor: str = WALLET,
    deposit_id: int = 1,
) -> dict:
    """Craft a minimal V3FundsDeposited log.

    Indexed topics: destinationChainId (uint256), depositId (uint32), depositor (address).
    Data layout (first 4 words read by the parser):
      inputToken (32), outputToken (32), inputAmount (32), outputAmount (32),
    followed by other uint32/address fields we do not decode here.
    """

    def _addr_topic(addr: str) -> str:
        return "0x" + addr.lower().replace("0x", "").zfill(64)

    def _u256_hex(val: int) -> str:
        return hex(val)[2:].zfill(64)

    topics = [
        V3_FUNDS_DEPOSITED_TOPIC,
        "0x" + _u256_hex(destination_chain_id),
        "0x" + _u256_hex(deposit_id),
        _addr_topic(depositor),
    ]

    # Pack the 4 leading data words the parser reads, plus padding for realism.
    data_hex = (
        _addr_topic(input_token)[2:]  # inputToken
        + _addr_topic(output_token)[2:]  # outputToken
        + _u256_hex(input_amount)  # inputAmount
        + _u256_hex(output_amount)  # outputAmount
        + _u256_hex(0)  # quoteTimestamp
        + _u256_hex(0)  # fillDeadline
        + _u256_hex(0)  # exclusivityDeadline
        + _addr_topic(WALLET)[2:]  # recipient
        + _addr_topic("0x" + "0" * 40)[2:]  # exclusiveRelayer
        + _u256_hex(0)  # message offset / length marker
    )

    return {
        "address": spoke_pool,
        "topics": topics,
        "data": "0x" + data_hex,
        "logIndex": 0,
    }


def _oft_sent_log(
    *,
    pool_address: str,
    dst_eid: int,
    amount_sent: int,
    amount_received: int,
) -> dict:
    """Craft a minimal OFTSent log.

    Indexed: guid (bytes32), fromAddress (address).
    Data: dstEid (uint32 right-padded), amountSentLD, amountReceivedLD.
    """

    def _u256_hex(val: int) -> str:
        return hex(val)[2:].zfill(64)

    topics = [
        OFT_SENT_TOPIC,
        "0x" + _u256_hex(42),  # guid (any bytes32)
        "0x" + WALLET.lower().replace("0x", "").zfill(64),  # fromAddress
    ]
    data_hex = _u256_hex(dst_eid) + _u256_hex(amount_sent) + _u256_hex(amount_received)
    return {
        "address": pool_address,
        "topics": topics,
        "data": "0x" + data_hex,
        "logIndex": 0,
    }


# ---------------------------------------------------------------------------
# Across parser tests
# ---------------------------------------------------------------------------


class TestAcrossReceiptParser:
    """Across receipt parser extracts BridgeData from V3FundsDeposited events."""

    def test_extract_bridge_data_from_v3_funds_deposited(self):
        """Standard Across deposit: V3FundsDeposited log gives input amount + chain ids."""
        # Base SpokePool for from_chain="base"
        spoke_base = ACROSS_SPOKE_POOL_ADDRESSES[8453]
        deposit_log = _v3_funds_deposited_log(
            spoke_pool=spoke_base,
            input_token=USDC_BASE,
            output_token=USDC_ARBITRUM,
            input_amount=1_000_000_000,  # 1000 USDC (6 decimals)
            output_amount=999_500_000,
            destination_chain_id=42161,  # Arbitrum EVM chain id
        )
        transfer = _transfer_log(USDC_BASE, WALLET, spoke_base, 1_000_000_000)
        receipt = {
            "status": 1,
            "transactionHash": "0xabc",
            "logs": [transfer, deposit_log],
            "from": WALLET,
        }

        parser = AcrossReceiptParser(chain="base")
        bd = parser.extract_bridge_data(
            receipt,
            from_chain="base",
            to_chain="arbitrum",
            token="USDC",
            bridge="Across",
        )

        assert bd is not None, "Across parser should return BridgeData for a standard deposit"
        assert isinstance(bd, BridgeData)
        assert bd.source_chain == "base"
        assert bd.destination_chain == "arbitrum"
        assert bd.token_symbol == "USDC"
        assert bd.bridge_name == "across"
        assert bd.amount_sent_raw == 1_000_000_000
        assert bd.amount_sent == Decimal("1000")
        assert bd.source_token_address == USDC_BASE.lower()
        assert bd.destination_token_address == USDC_ARBITRUM.lower()
        assert bd.destination_tx_hash is None  # async settlement
        assert bd.source_tx_hash == "0xabc"

    def test_extract_bridge_data_wallet_transfer_fallback(self):
        """When V3FundsDeposited is absent, the wallet->spoke Transfer is still enough."""
        spoke_base = ACROSS_SPOKE_POOL_ADDRESSES[8453]
        transfer = _transfer_log(USDC_BASE, WALLET, spoke_base, 250_000_000)  # 250 USDC
        receipt = {
            "status": 1,
            "transactionHash": "0xdef",
            "logs": [transfer],
            "from": WALLET,
        }

        parser = AcrossReceiptParser(chain="base")
        bd = parser.extract_bridge_data(
            receipt,
            from_chain="base",
            to_chain="arbitrum",
            token="USDC",
        )

        assert bd is not None, "Transfer-only fallback should still yield BridgeData"
        assert bd.amount_sent == Decimal("250")
        assert bd.amount_sent_raw == 250_000_000
        assert bd.source_token_address == USDC_BASE.lower()

    def test_missing_bridge_events_returns_none(self):
        """Receipt with no deposit event and no wallet->spoke Transfer -> None."""
        receipt = {"status": 1, "transactionHash": "0xnope", "logs": [], "from": WALLET}
        parser = AcrossReceiptParser(chain="base")
        bd = parser.extract_bridge_data(
            receipt,
            from_chain="base",
            to_chain="arbitrum",
            token="USDC",
        )
        assert bd is None, "Empty receipt must not fabricate BridgeData"


# ---------------------------------------------------------------------------
# Stargate parser tests
# ---------------------------------------------------------------------------


class TestStargateReceiptParser:
    """Stargate receipt parser extracts BridgeData from OFTSent events."""

    def test_extract_bridge_data_from_oft_sent(self):
        # Base USDC Stargate pool
        base_pool = STARGATE_ROUTER_ADDRESSES[8453]["USDC"]
        oft_log = _oft_sent_log(
            pool_address=base_pool,
            dst_eid=30110,  # Arbitrum LayerZero EID
            amount_sent=500_000_000,  # 500 USDC (6 decimals)
            amount_received=498_000_000,
        )
        transfer = _transfer_log(USDC_BASE, WALLET, base_pool, 500_000_000)
        receipt = {
            "status": 1,
            "transactionHash": "0xstar",
            "logs": [transfer, oft_log],
            "from": WALLET,
        }

        parser = StargateReceiptParser(chain="base")
        bd = parser.extract_bridge_data(
            receipt,
            from_chain="base",
            to_chain="arbitrum",
            token="USDC",
            bridge="Stargate",
        )

        assert bd is not None
        assert bd.bridge_name == "stargate"
        assert bd.source_chain == "base"
        assert bd.destination_chain == "arbitrum"
        assert bd.token_symbol == "USDC"
        assert bd.amount_sent == Decimal("500")
        assert bd.amount_sent_raw == 500_000_000

    def test_extract_bridge_data_no_oft_sent_returns_none(self):
        """No OFTSent and no wallet->pool Transfer -> None (not a Stargate receipt)."""
        receipt = {"status": 1, "transactionHash": "0xnone", "logs": [], "from": WALLET}
        parser = StargateReceiptParser(chain="base")
        bd = parser.extract_bridge_data(
            receipt,
            from_chain="base",
            to_chain="arbitrum",
            token="USDC",
        )
        assert bd is None


# ---------------------------------------------------------------------------
# End-to-end enricher tests
# ---------------------------------------------------------------------------


class TestResultEnricherBridge:
    """ResultEnricher wires BRIDGE intents through the bridge parser."""

    def test_enricher_populates_bridge_data_from_across_metadata(self):
        """Metadata ``bridge: Across`` resolves to the AcrossReceiptParser."""
        spoke_base = ACROSS_SPOKE_POOL_ADDRESSES[8453]
        deposit_log = _v3_funds_deposited_log(
            spoke_pool=spoke_base,
            input_token=USDC_BASE,
            output_token=USDC_ARBITRUM,
            input_amount=1_000_000_000,
            output_amount=999_500_000,
            destination_chain_id=42161,
        )
        transfer = _transfer_log(USDC_BASE, WALLET, spoke_base, 1_000_000_000)
        receipt = _FakeReceipt(status=1, logs=[transfer, deposit_log], from_address=WALLET)

        result = _FakeExecResult(transaction_results=[_FakeTxResult(receipt=receipt)])
        intent = _FakeIntent(intent_type="BRIDGE", protocol=None)
        context = _FakeContext(chain="base", protocol=None)

        # live_mode=False so a missing follow-up field does not raise.
        enricher = ResultEnricher(live_mode=False)
        bundle_metadata = {
            "from_chain": "base",
            "to_chain": "arbitrum",
            "token": "USDC",
            "bridge": "Across",  # <-- resolved by the compiler, mimics real flow
            "amount": "1000",
        }
        enriched = enricher.enrich(result, intent, context, bundle_metadata=bundle_metadata)

        assert enriched.bridge_data is not None, "bridge_data should be populated for BRIDGE intents"
        assert isinstance(enriched.bridge_data, BridgeData)
        assert enriched.bridge_data.bridge_name == "across"
        assert enriched.bridge_data.amount_sent == Decimal("1000")
        assert enriched.bridge_data.source_chain == "base"
        assert enriched.bridge_data.destination_chain == "arbitrum"
        # Also exposed via extracted_data for legacy consumers.
        assert "bridge_data" in enriched.extracted_data

    def test_enricher_leaves_bridge_data_none_when_no_events(self):
        """Enricher does not crash on a bridge receipt missing both events."""
        receipt = _FakeReceipt(status=1, logs=[], from_address=WALLET)
        result = _FakeExecResult(transaction_results=[_FakeTxResult(receipt=receipt)])
        intent = _FakeIntent(intent_type="BRIDGE", protocol=None)
        context = _FakeContext(chain="base", protocol=None)

        enricher = ResultEnricher(live_mode=False)
        bundle_metadata = {
            "from_chain": "base",
            "to_chain": "arbitrum",
            "token": "USDC",
            "bridge": "Across",
        }
        enriched = enricher.enrich(result, intent, context, bundle_metadata=bundle_metadata)

        assert enriched.bridge_data is None
        # Enricher should not have raised; warnings list may be populated.
        assert isinstance(enriched.extraction_warnings, list)

    def test_enricher_skips_bridge_without_protocol_resolution(self):
        """Missing bundle metadata + missing context.protocol -> skip without crash."""
        receipt = _FakeReceipt(status=1, logs=[], from_address=WALLET)
        result = _FakeExecResult(transaction_results=[_FakeTxResult(receipt=receipt)])
        intent = _FakeIntent(intent_type="BRIDGE", protocol=None)
        context = _FakeContext(chain="base", protocol=None)

        enricher = ResultEnricher(live_mode=False)
        enriched = enricher.enrich(result, intent, context, bundle_metadata=None)

        # No bridge info resolvable -> bridge_data stays None, no exception
        assert enriched.bridge_data is None


# ---------------------------------------------------------------------------
# LiFi parser tests (VIB-3226 CodeRabbit audit round 2)
# ---------------------------------------------------------------------------


class TestLiFiReceiptParserBridgeData:
    """LiFi ``extract_bridge_data`` behaviour across success and edge cases.

    CodeRabbit audit (2026-04-21, round 2) asked for unit coverage on the new
    ``LiFiReceiptParser.extract_bridge_data`` method. These tests cover the
    success path plus the documented edge cases: reverted tx, missing
    transfers, missing chain hints, unknown decimals, malformed
    expected_amount_out. Intent / on-chain tests remain out of scope per the
    PR description (``tests/intents/{chain}/test_lifi_bridge.py`` is a
    separate ticket).
    """

    def _success_receipt(self, amount: int = 1_000_000_000) -> dict:
        """Build a LiFi-style receipt: wallet sends USDC_BASE to a Diamond proxy.

        LiFi does not emit a single distinctive deposit event like Across or
        Stargate — the parser infers the bridge amount from the first
        wallet-outgoing ERC-20 Transfer. So a single Transfer log is
        sufficient for the success case.
        """
        lifi_diamond = "0x1231deb6f5749ef6ce6943a275a1d3e7486f4eae"  # lower-case
        transfer = _transfer_log(USDC_BASE, WALLET, lifi_diamond, amount)
        return {
            "status": 1,
            "transactionHash": "0xlifi-success",
            "logs": [transfer],
            "from": WALLET,
        }

    def test_extract_bridge_data_success(self):
        receipt = self._success_receipt(amount=1_000_000_000)  # 1000 USDC
        parser = LiFiReceiptParser(chain="base")
        bd = parser.extract_bridge_data(
            receipt,
            from_chain="base",
            to_chain="arbitrum",
            token="USDC",
            bridge="LiFi",
        )

        assert bd is not None
        assert isinstance(bd, BridgeData)
        assert bd.bridge_name == "lifi"
        assert bd.source_chain == "base"
        assert bd.destination_chain == "arbitrum"
        assert bd.token_symbol == "USDC"
        assert bd.amount_sent_raw == 1_000_000_000
        assert bd.amount_sent == Decimal("1000")
        assert bd.source_token_address == USDC_BASE.lower()
        # LiFi's deposit event does not carry destination token info.
        assert bd.destination_token_address is None
        assert bd.destination_tx_hash is None  # async settlement

    def test_extract_bridge_data_reverted_tx_returns_none(self):
        """status != 1 must short-circuit before log parsing."""
        receipt = {
            "status": 0,
            "transactionHash": "0xlifi-revert",
            "logs": [_transfer_log(USDC_BASE, WALLET, "0x1231deb6f5749ef6ce6943a275a1d3e7486f4eae", 1)],
            "from": WALLET,
        }
        parser = LiFiReceiptParser(chain="base")
        assert parser.extract_bridge_data(receipt, from_chain="base", to_chain="arbitrum", token="USDC") is None

    def test_extract_bridge_data_no_transfers_returns_none(self):
        """Empty logs -> no outgoing transfer -> None (benign)."""
        receipt = {"status": 1, "transactionHash": "0xempty", "logs": [], "from": WALLET}
        parser = LiFiReceiptParser(chain="base")
        assert parser.extract_bridge_data(receipt, from_chain="base", to_chain="arbitrum", token="USDC") is None

    def test_extract_bridge_data_missing_to_chain_returns_none(self):
        """No to_chain hint and no way to recover it -> None."""
        receipt = self._success_receipt()
        parser = LiFiReceiptParser(chain="base")
        assert parser.extract_bridge_data(receipt, from_chain="base", to_chain=None, token="USDC") is None

    def test_extract_bridge_data_missing_from_chain_still_works_via_self_chain(self):
        """Parser falls back to ``self._chain`` when from_chain is not supplied."""
        receipt = self._success_receipt()
        parser = LiFiReceiptParser(chain="base")
        bd = parser.extract_bridge_data(receipt, from_chain=None, to_chain="arbitrum", token="USDC")
        assert bd is not None
        assert bd.source_chain == "base"

    def test_extract_bridge_data_unknown_decimals_returns_none(self):
        """Unresolvable token address + no symbol fallback -> None (no 18-default lie)."""
        unknown_token = "0xdeadbeefdeadbeefdeadbeefdeadbeefdeadbeef"
        lifi_diamond = "0x1231deb6f5749ef6ce6943a275a1d3e7486f4eae"
        transfer = _transfer_log(unknown_token, WALLET, lifi_diamond, 1000)
        receipt = {"status": 1, "transactionHash": "0xunk", "logs": [transfer], "from": WALLET}
        parser = LiFiReceiptParser(chain="base")
        # token=None so the symbol-fallback path in _get_decimals has nothing to look up.
        assert parser.extract_bridge_data(receipt, from_chain="base", to_chain="arbitrum", token=None) is None

    def test_extract_bridge_data_malformed_expected_amount_ignored(self):
        """Malformed ``expected_amount_out`` must not poison the extraction."""
        receipt = self._success_receipt(amount=1_000_000)
        parser = LiFiReceiptParser(chain="base")
        bd = parser.extract_bridge_data(
            receipt,
            from_chain="base",
            to_chain="arbitrum",
            token="USDC",
            expected_amount_out="not-a-decimal",
        )
        assert bd is not None
        # Malformed value gets dropped to None rather than crashing the parser.
        assert bd.expected_amount_out is None
        assert bd.amount_sent == Decimal("1")

    def test_extract_bridge_data_expected_amount_out_decimal(self):
        receipt = self._success_receipt(amount=1_000_000)
        parser = LiFiReceiptParser(chain="base")
        bd = parser.extract_bridge_data(
            receipt,
            from_chain="base",
            to_chain="arbitrum",
            token="USDC",
            expected_amount_out="0.998",
        )
        assert bd is not None
        assert bd.expected_amount_out == Decimal("0.998")

    def test_extract_bridge_data_native_asset_uses_amount_fallback(self):
        """msg.value-funded native bridge has no ERC-20 Transfer.

        The parser falls back to the compiler-provided ``amount`` + the
        native token's decimals (via the resolver). ``source_token_address``
        is None since there's no ERC-20 source token.
        """
        # Native-asset bridge: no Transfer logs, no wallet_outgoing.
        receipt = {
            "status": 1,
            "transactionHash": "0xlifi-native",
            "logs": [],
            "from": WALLET,
        }
        parser = LiFiReceiptParser(chain="base")
        bd = parser.extract_bridge_data(
            receipt,
            from_chain="base",
            to_chain="arbitrum",
            token="ETH",
            amount=Decimal("0.5"),
            bridge="LiFi",
        )
        assert bd is not None
        assert bd.bridge_name == "lifi"
        assert bd.source_chain == "base"
        assert bd.destination_chain == "arbitrum"
        assert bd.token_symbol == "ETH"
        assert bd.amount_sent == Decimal("0.5")
        # ETH has 18 decimals on Base (resolved, not hardcoded).
        assert bd.amount_sent_raw == 500_000_000_000_000_000
        assert bd.source_token_address is None

    def test_extract_bridge_data_native_asset_without_amount_returns_none(self):
        """Native-asset path requires an ``amount`` hint; missing -> None."""
        receipt = {"status": 1, "transactionHash": "0xlifi-native-2", "logs": [], "from": WALLET}
        parser = LiFiReceiptParser(chain="base")
        assert (
            parser.extract_bridge_data(
                receipt, from_chain="base", to_chain="arbitrum", token="ETH", amount=None
            )
            is None
        )
