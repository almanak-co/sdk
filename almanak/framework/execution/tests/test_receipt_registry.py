"""Unit tests for Receipt Parser Registry.

Tests the ReceiptParserRegistry class and module-level convenience functions
for retrieving and managing protocol receipt parsers.
"""

from typing import Any

import pytest

from almanak.framework.execution.receipt_registry import (
    ParserNotFoundError,
    ReceiptParserError,
    ReceiptParserRegistry,
    get_parser,
    is_parser_available,
    list_parsers,
    register_parser,
)

# =============================================================================
# Test Fixtures
# =============================================================================


class MockReceiptParser:
    """Mock receipt parser for testing custom registration."""

    def __init__(self, config_value: str = "default") -> None:
        self.config_value = config_value

    def parse_receipt(self, receipt: dict[str, Any]) -> dict[str, Any]:
        """Parse a receipt."""
        return {"success": True, "config": self.config_value}


@pytest.fixture
def registry() -> ReceiptParserRegistry:
    """Create a fresh registry for each test."""
    return ReceiptParserRegistry()


# =============================================================================
# ReceiptParserRegistry Tests
# =============================================================================


class TestReceiptParserRegistry:
    """Tests for ReceiptParserRegistry class."""

    def test_list_protocols_includes_builtins(self, registry: ReceiptParserRegistry) -> None:
        """Test that list_protocols includes all built-in parsers."""
        protocols = registry.list_protocols()

        assert "spark" in protocols
        assert "pancakeswap_v3" in protocols
        assert "lido" in protocols
        assert "ethena" in protocols

    def test_is_registered_for_builtin_protocols(self, registry: ReceiptParserRegistry) -> None:
        """Test is_registered returns True for built-in protocols."""
        assert registry.is_registered("spark")
        assert registry.is_registered("pancakeswap_v3")
        assert registry.is_registered("lido")
        assert registry.is_registered("ethena")
        assert registry.is_registered("benqi")

    def test_is_registered_case_insensitive(self, registry: ReceiptParserRegistry) -> None:
        """Test is_registered handles case insensitivity."""
        assert registry.is_registered("SPARK")
        assert registry.is_registered("Spark")
        assert registry.is_registered("PancakeSwap_V3")

    def test_is_registered_returns_false_for_unknown(self, registry: ReceiptParserRegistry) -> None:
        """Test is_registered returns False for unknown protocols."""
        assert not registry.is_registered("unknown_protocol")
        assert not registry.is_registered("nonexistent")

    def test_get_spark_parser(self, registry: ReceiptParserRegistry) -> None:
        """Test getting SparkReceiptParser."""
        parser = registry.get("spark")

        assert parser is not None
        assert hasattr(parser, "parse_receipt")

        # Verify it's cached
        parser2 = registry.get("spark")
        assert parser is parser2

    def test_get_pancakeswap_v3_parser(self, registry: ReceiptParserRegistry) -> None:
        """Test getting PancakeSwapV3ReceiptParser."""
        parser = registry.get("pancakeswap_v3")

        assert parser is not None
        assert hasattr(parser, "parse_receipt")

    def test_get_lido_parser(self, registry: ReceiptParserRegistry) -> None:
        """Test getting LidoReceiptParser."""
        parser = registry.get("lido")

        assert parser is not None
        assert hasattr(parser, "parse_receipt")

    def test_get_ethena_parser(self, registry: ReceiptParserRegistry) -> None:
        """Test getting EthenaReceiptParser."""
        parser = registry.get("ethena")

        assert parser is not None
        assert hasattr(parser, "parse_receipt")

    def test_get_benqi_parser(self, registry: ReceiptParserRegistry) -> None:
        """Test getting BenqiReceiptParser."""
        parser = registry.get("benqi")

        assert parser is not None
        assert hasattr(parser, "parse_receipt")

        # Verify it's cached
        parser2 = registry.get("benqi")
        assert parser is parser2

    def test_get_unknown_protocol_raises_error(self, registry: ReceiptParserRegistry) -> None:
        """Test that getting unknown protocol raises ValueError."""
        with pytest.raises(ValueError, match="Unknown protocol"):
            registry.get("unknown_protocol")

    def test_get_case_insensitive(self, registry: ReceiptParserRegistry) -> None:
        """Test that get handles case insensitivity."""
        parser1 = registry.get("spark")
        parser2 = registry.get("SPARK")
        parser3 = registry.get("Spark")

        assert parser1 is parser2
        assert parser2 is parser3

    def test_register_custom_parser(self, registry: ReceiptParserRegistry) -> None:
        """Test registering a custom parser."""
        registry.register("my_protocol", MockReceiptParser)

        assert registry.is_registered("my_protocol")
        assert "my_protocol" in registry.list_protocols()

        parser = registry.get("my_protocol")
        assert isinstance(parser, MockReceiptParser)

    def test_register_custom_parser_case_insensitive(self, registry: ReceiptParserRegistry) -> None:
        """Test that register handles case insensitivity."""
        registry.register("MY_PROTOCOL", MockReceiptParser)

        assert registry.is_registered("my_protocol")
        assert registry.is_registered("MY_PROTOCOL")

        parser = registry.get("MY_PROTOCOL")
        assert isinstance(parser, MockReceiptParser)

    def test_register_requires_class_not_instance(self, registry: ReceiptParserRegistry) -> None:
        """Test that register requires a class, not an instance."""
        with pytest.raises(TypeError, match="Expected a class"):
            registry.register("my_protocol", MockReceiptParser())  # type: ignore

    def test_register_overrides_existing(self, registry: ReceiptParserRegistry) -> None:
        """Test that registering again overrides existing parser."""

        class AnotherParser:
            def parse_receipt(self, receipt: dict[str, Any]) -> dict[str, Any]:
                return {"type": "another"}

        registry.register("custom", MockReceiptParser)
        parser1 = registry.get("custom")
        assert isinstance(parser1, MockReceiptParser)

        registry.register("custom", AnotherParser)
        parser2 = registry.get("custom")
        assert isinstance(parser2, AnotherParser)

    def test_unregister_custom_parser(self, registry: ReceiptParserRegistry) -> None:
        """Test unregistering a custom parser."""
        registry.register("my_protocol", MockReceiptParser)
        assert registry.is_registered("my_protocol")

        result = registry.unregister("my_protocol")

        assert result is True
        assert not registry.is_registered("my_protocol")
        assert "my_protocol" not in registry.list_protocols()

    def test_unregister_nonexistent_returns_false(self, registry: ReceiptParserRegistry) -> None:
        """Test that unregistering nonexistent parser returns False."""
        result = registry.unregister("nonexistent")
        assert result is False

    def test_clear_cache(self, registry: ReceiptParserRegistry) -> None:
        """Test clearing the parser cache."""
        parser1 = registry.get("spark")
        registry.clear_cache()
        parser2 = registry.get("spark")

        # Different instances after cache clear
        assert parser1 is not parser2

    def test_get_with_kwargs_bypasses_cache(self, registry: ReceiptParserRegistry) -> None:
        """Test that kwargs bypass the cache."""
        # Get cached instance
        parser1 = registry.get("spark")

        # Get with custom kwargs - should be new instance
        parser2 = registry.get("spark", pool_addresses={"0x123"})

        # Instances should be different
        assert parser1 is not parser2

        # Original cached instance should be unchanged
        parser3 = registry.get("spark")
        assert parser1 is parser3

    def test_custom_parser_with_kwargs(self, registry: ReceiptParserRegistry) -> None:
        """Test custom parser with constructor kwargs."""
        registry.register("custom", MockReceiptParser)

        parser = registry.get("custom", config_value="custom_config")

        assert isinstance(parser, MockReceiptParser)
        assert parser.config_value == "custom_config"


# =============================================================================
# Module-Level Function Tests
# =============================================================================


class TestModuleFunctions:
    """Tests for module-level convenience functions."""

    def test_get_parser_returns_parser(self) -> None:
        """Test get_parser returns a valid parser."""
        parser = get_parser("spark")
        assert parser is not None
        assert hasattr(parser, "parse_receipt")

    def test_get_parser_unknown_raises_error(self) -> None:
        """Test get_parser raises ValueError for unknown protocol."""
        with pytest.raises(ValueError, match="Unknown protocol"):
            get_parser("unknown_protocol")

    def test_list_parsers_returns_protocols(self) -> None:
        """Test list_parsers returns list of protocols."""
        protocols = list_parsers()

        assert isinstance(protocols, list)
        assert "spark" in protocols
        assert "pancakeswap_v3" in protocols
        assert "lido" in protocols
        assert "ethena" in protocols

    def test_is_parser_available(self) -> None:
        """Test is_parser_available returns correct values."""
        assert is_parser_available("spark")
        assert is_parser_available("lido")
        assert not is_parser_available("unknown")

    def test_register_parser_adds_to_default_registry(self) -> None:
        """Test register_parser adds to default registry."""
        # Use unique protocol name to avoid conflicts
        protocol = "test_module_register_protocol"

        try:
            register_parser(protocol, MockReceiptParser)
            assert is_parser_available(protocol)

            parser = get_parser(protocol)
            assert isinstance(parser, MockReceiptParser)
        finally:
            # Clean up (can't easily unregister from module function)
            pass


# =============================================================================
# Integration Tests
# =============================================================================


class TestParserIntegration:
    """Integration tests verifying parsers work correctly."""

    def test_spark_parser_parses_empty_receipt(self) -> None:
        """Test SparkReceiptParser handles empty receipt."""
        parser = get_parser("spark")

        result = parser.parse_receipt(
            {
                "transactionHash": "0x123",
                "blockNumber": 12345,
                "logs": [],
            }
        )

        assert result.success is True
        assert result.supplies == []
        assert result.withdraws == []

    def test_pancakeswap_v3_parser_parses_empty_receipt(self) -> None:
        """Test PancakeSwapV3ReceiptParser handles empty receipt."""
        parser = get_parser("pancakeswap_v3")

        result = parser.parse_receipt(
            {
                "transactionHash": "0x123",
                "blockNumber": 12345,
                "logs": [],
            }
        )

        assert result.success is True
        assert result.swaps == []

    def test_lido_parser_parses_empty_receipt(self) -> None:
        """Test LidoReceiptParser handles empty receipt."""
        parser = get_parser("lido")

        result = parser.parse_receipt(
            {
                "transactionHash": "0x123",
                "blockNumber": 12345,
                "logs": [],
            }
        )

        assert result.success is True
        assert result.stakes == []

    def test_benqi_parser_parses_empty_receipt(self) -> None:
        """Test BenqiReceiptParser handles empty receipt."""
        parser = get_parser("benqi")

        result = parser.parse_receipt(
            {
                "transactionHash": "0x123",
                "blockNumber": 12345,
                "logs": [],
            }
        )

        assert result.success is True
        assert result.events == []

    def test_ethena_parser_parses_empty_receipt(self) -> None:
        """Test EthenaReceiptParser handles empty receipt."""
        parser = get_parser("ethena")

        result = parser.parse_receipt(
            {
                "transactionHash": "0x123",
                "blockNumber": 12345,
                "logs": [],
            }
        )

        assert result.success is True
        assert result.stakes == []


# =============================================================================
# Exception Tests
# =============================================================================


class TestExceptions:
    """Tests for exception classes."""

    def test_parser_not_found_error(self) -> None:
        """Test ParserNotFoundError attributes."""
        error = ParserNotFoundError("unknown", ["spark", "lido"])

        assert error.protocol == "unknown"
        assert error.available == ["spark", "lido"]
        assert "unknown" in str(error)
        assert "spark" in str(error)

    def test_receipt_parser_error_is_exception(self) -> None:
        """Test ReceiptParserError is an Exception."""
        error = ReceiptParserError("test error")
        assert isinstance(error, Exception)
        assert str(error) == "test error"
