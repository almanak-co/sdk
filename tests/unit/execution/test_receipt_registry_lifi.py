"""Unit tests for LiFi receipt parser registration in the receipt registry."""

from almanak.framework.connectors.lifi.receipt_parser import LiFiReceiptParser
from almanak.framework.execution.receipt_registry import get_parser


class TestLiFiReceiptRegistry:
    """Tests that LiFi is properly registered in the receipt registry."""

    def test_get_parser_returns_lifi_receipt_parser(self) -> None:
        """get_parser('lifi') returns a LiFiReceiptParser instance."""
        parser = get_parser("lifi")
        assert isinstance(parser, LiFiReceiptParser)

    def test_get_parser_caches_instance(self) -> None:
        """get_parser('lifi') returns the same cached instance on repeat calls."""
        parser1 = get_parser("lifi")
        parser2 = get_parser("lifi")
        assert parser1 is parser2
