"""CurveAdapter._is_native_token — per-chain registry-derived gate (VIB-4851 A1).

Production callers pass pool coin ADDRESSES (Curve marks raw native with the
0xEeee placeholder), so the placeholder arm carries the money-path behavior —
frozen verbatim here. The symbol arm is derived from ``ChainDescriptor.native``
instead of the legacy hardcoded "ETH" so a symbol caller on polygon gets
MATIC/POL right.
"""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from almanak.connectors.curve.adapter import CurveAdapter, CurveConfig

WALLET = "0x1234567890123456789012345678901234567890"
NATIVE_PLACEHOLDER = "0xEeeeeEeeeEeEeeEeEeEeeEEEeeeeEeeeeeeeEEeE"
USDC_ETHEREUM = "0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48"


def _make_adapter(chain: str) -> CurveAdapter:
    config = CurveConfig(chain=chain, wallet_address=WALLET)
    return CurveAdapter(config, token_resolver=MagicMock())


class TestPlaceholderArm:
    """Frozen verbatim — this is what production callers exercise."""

    @pytest.mark.parametrize("chain", ["ethereum", "polygon", "arbitrum"])
    def test_native_placeholder_address(self, chain: str) -> None:
        adapter = _make_adapter(chain)
        assert adapter._is_native_token(NATIVE_PLACEHOLDER) is True
        assert adapter._is_native_token(NATIVE_PLACEHOLDER.lower()) is True

    def test_erc20_address_not_native(self) -> None:
        assert _make_adapter("ethereum")._is_native_token(USDC_ETHEREUM) is False


class TestSymbolArm:
    def test_eth_on_ethereum(self) -> None:
        adapter = _make_adapter("ethereum")
        assert adapter._is_native_token("ETH") is True
        assert adapter._is_native_token("eth") is True
        assert adapter._is_native_token("USDC") is False

    def test_polygon_natives(self) -> None:
        """New vs the legacy hardcoded "ETH": polygon's gas coin is detected."""
        adapter = _make_adapter("polygon")
        assert adapter._is_native_token("MATIC") is True
        assert adapter._is_native_token("POL") is True
        assert adapter._is_native_token("ETH") is False

    def test_foreign_natives_rejected_on_ethereum(self) -> None:
        adapter = _make_adapter("ethereum")
        for foreign in ("MATIC", "POL", "AVAX", "BNB"):
            assert adapter._is_native_token(foreign) is False, foreign
