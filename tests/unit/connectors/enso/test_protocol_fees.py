"""Tests for Enso ``extract_protocol_fees`` (VIB-3204 — placeholder).

CodeRabbit audit fix: regression-guard for the placeholder. The Enso
parser declares ``"protocol_fees"`` in ``SUPPORTED_EXTRACTIONS`` so the
``ResultEnricher`` capability gate forwards calls, and the extractor
itself returns ``None`` until real integrator-fee extraction lands in
the follow-up VIB-3210. These tests lock that contract so a future
"helpful" refactor can't silently start emitting
``ProtocolFees(total_usd=Decimal(0))`` — which would falsely claim
zero aggregator fee on every Enso swap.
"""

from __future__ import annotations

from almanak.framework.connectors.enso.receipt_parser import EnsoReceiptParser


class TestEnsoProtocolFeesPlaceholder:
    def test_empty_receipt_returns_none(self) -> None:
        parser = EnsoReceiptParser()
        assert parser.extract_protocol_fees({}) is None

    def test_empty_logs_returns_none(self) -> None:
        parser = EnsoReceiptParser()
        assert parser.extract_protocol_fees({"logs": []}) is None

    def test_minimal_shape_returns_none(self) -> None:
        """A receipt with plausible fields but no fee data still returns None."""
        parser = EnsoReceiptParser()
        receipt = {
            "status": 1,
            "from": "0x" + "aa" * 20,
            "logs": [
                {
                    "address": "0x" + "bb" * 20,
                    "topics": ["0x" + "cd" * 32],
                    "data": "0x" + "0" * 128,
                }
            ],
        }
        assert parser.extract_protocol_fees(receipt) is None

    def test_realistic_swap_shape_returns_none(self) -> None:
        """Even a receipt that looks like a real Enso aggregator swap
        (with multiple Transfer events) must return None — the parser
        has no integrator-fee extraction yet."""
        parser = EnsoReceiptParser()
        wallet = "0x" + "aa" * 20
        receipt = {
            "status": 1,
            "from": wallet,
            "logs": [
                {
                    "address": "0x" + "cc" * 20,  # input token
                    "topics": [
                        "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef",  # Transfer
                        "0x" + "00" * 12 + wallet[2:],
                        "0x" + "00" * 12 + "dd" * 20,  # router
                    ],
                    "data": "0x" + f"{1_000_000:064x}",
                },
                {
                    "address": "0x" + "ee" * 20,  # output token
                    "topics": [
                        "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef",
                        "0x" + "00" * 12 + "dd" * 20,
                        "0x" + "00" * 12 + wallet[2:],
                    ],
                    "data": "0x" + f"{950_000:064x}",
                },
            ],
        }
        assert parser.extract_protocol_fees(receipt) is None

    def test_supported_extractions_advertises_protocol_fees(self) -> None:
        """Capability gate regression: the declaration must include
        ``"protocol_fees"`` so ``ResultEnricher._extract_field`` forwards
        the call instead of emitting a ``does not declare support``
        warning. See audit-pr-1602.md for the failure mode this guards
        against.
        """
        assert "protocol_fees" in EnsoReceiptParser.SUPPORTED_EXTRACTIONS
