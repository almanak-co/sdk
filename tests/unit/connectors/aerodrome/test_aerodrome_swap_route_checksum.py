"""Tests for SwapRoute.to_tuple() checksum address handling.

Verifies that SwapRoute checksums all addresses to prevent web3.py rejection
which would silently cascade to zero slippage protection.
"""

from web3 import Web3

from almanak.framework.connectors.aerodrome.sdk import SwapRoute


class TestSwapRouteChecksum:
    """Verify SwapRoute.to_tuple() returns checksummed addresses."""

    def test_to_tuple_checksums_lowercase_addresses(self):
        """Lowercase addresses must be checksummed in the output tuple."""
        route = SwapRoute(
            from_token="0x0b2c639c533813f4aa9d7837caf62653d097ff85",
            to_token="0x4200000000000000000000000000000000000006",
            stable=False,
        )
        default_factory = "0x420dd381b31aef6683db6b902084cb0ffece40da"

        result = route.to_tuple(default_factory)

        # All addresses should be checksummed (mixed case)
        assert result[0] == Web3.to_checksum_address(route.from_token)
        assert result[1] == Web3.to_checksum_address(route.to_token)
        assert result[2] is False  # stable flag unchanged
        assert result[3] == Web3.to_checksum_address(default_factory)

    def test_to_tuple_preserves_already_checksummed(self):
        """Already-checksummed addresses should remain valid."""
        checksummed_in = Web3.to_checksum_address("0x0b2c639c533813f4aa9d7837caf62653d097ff85")
        checksummed_out = Web3.to_checksum_address("0x4200000000000000000000000000000000000006")
        checksummed_factory = Web3.to_checksum_address("0x420dd381b31aef6683db6b902084cb0ffece40da")

        route = SwapRoute(
            from_token=checksummed_in,
            to_token=checksummed_out,
            stable=True,
            factory=checksummed_factory,
        )

        result = route.to_tuple("0x0000000000000000000000000000000000000000")

        assert result[0] == checksummed_in
        assert result[1] == checksummed_out
        assert result[2] is True
        assert result[3] == checksummed_factory  # explicit factory used, not default

    def test_to_tuple_uses_explicit_factory_over_default(self):
        """When factory is set on the route, it should be used instead of default."""
        route = SwapRoute(
            from_token="0x0b2c639c533813f4aa9d7837caf62653d097ff85",
            to_token="0x4200000000000000000000000000000000000006",
            stable=False,
            factory="0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
        )

        result = route.to_tuple("0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb")

        assert result[3] == Web3.to_checksum_address("0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")

    def test_to_tuple_uses_default_factory_when_none(self):
        """When factory is None, default_factory should be checksummed and used."""
        route = SwapRoute(
            from_token="0x0b2c639c533813f4aa9d7837caf62653d097ff85",
            to_token="0x4200000000000000000000000000000000000006",
            stable=False,
            factory=None,
        )

        default = "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
        result = route.to_tuple(default)

        assert result[3] == Web3.to_checksum_address(default)
