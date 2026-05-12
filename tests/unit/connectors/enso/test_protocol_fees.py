"""Tests for Enso ``extract_protocol_fees`` — VIB-3210 typed-unavailable.

History:

* VIB-3204 stubbed the parser to ``return None`` until aggregator integrator-fee
  plumbing landed.
* VIB-3210 replaced the raw ``None`` with a typed
  ``ProtocolFees(unavailable_reason=...)`` so downstream attribution can
  distinguish "known unavailable" from "parser missing". Returning ``None``
  silently dropped fee accounting on every Enso swap.

These tests pin the post-VIB-3210 contract: every code path returns a
``ProtocolFees`` instance; raw ``None`` is forbidden. The measured path
(``protocol_fee_usd`` kwarg present) is covered in
``tests/unit/execution/test_aggregator_protocol_fees.py``; here we lock
the receipt-shape paths and the SUPPORTED_EXTRACTIONS contract.
"""

from __future__ import annotations

from almanak.framework.connectors.enso.receipt_parser import EnsoReceiptParser
from almanak.framework.execution.extracted_data import ProtocolFees

_UNAVAILABLE_REASON = "enso_integrator_fee_quote_metadata_unavailable"


class TestEnsoProtocolFeesUnavailable:
    def test_empty_receipt_returns_typed_unavailable(self) -> None:
        parser = EnsoReceiptParser()
        out = parser.extract_protocol_fees({})
        assert isinstance(out, ProtocolFees)
        assert out.total_usd is None
        assert out.unavailable_reason == _UNAVAILABLE_REASON

    def test_empty_logs_returns_typed_unavailable(self) -> None:
        parser = EnsoReceiptParser()
        out = parser.extract_protocol_fees({"logs": []})
        assert isinstance(out, ProtocolFees)
        assert out.total_usd is None
        assert out.unavailable_reason == _UNAVAILABLE_REASON

    def test_minimal_shape_returns_typed_unavailable(self) -> None:
        """A receipt with plausible fields but no fee data still returns
        the typed unavailable sentinel — the fee lives in quote metadata,
        not in logs."""
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
        out = parser.extract_protocol_fees(receipt)
        assert isinstance(out, ProtocolFees)
        assert out.unavailable_reason == _UNAVAILABLE_REASON

    def test_realistic_swap_shape_returns_typed_unavailable(self) -> None:
        """Even a realistic Enso aggregator swap receipt returns the typed
        unavailable sentinel — the parser does not scan logs for fees,
        because Enso fees are quote-time metadata."""
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
        out = parser.extract_protocol_fees(receipt)
        assert isinstance(out, ProtocolFees)
        assert out.unavailable_reason == _UNAVAILABLE_REASON

    def test_supported_extractions_advertises_protocol_fees(self) -> None:
        """Capability gate regression: the declaration must include
        ``"protocol_fees"`` so ``ResultEnricher._extract_field`` forwards
        the call instead of emitting a ``does not declare support``
        warning. See audit-pr-1602.md for the failure mode this guards
        against.
        """
        assert "protocol_fees" in EnsoReceiptParser.SUPPORTED_EXTRACTIONS
