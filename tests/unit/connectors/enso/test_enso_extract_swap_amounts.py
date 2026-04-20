"""Tests for EnsoReceiptParser.extract_swap_amounts().

Verifies that the ResultEnricher-compatible extraction method correctly
parses ERC-20 Transfer events from Enso swap receipts.
"""

from decimal import Decimal
from unittest.mock import MagicMock, patch

from almanak.framework.connectors.enso.receipt_parser import (
    TRANSFER_EVENT_SIGNATURE,
    EnsoReceiptParser,
)
from almanak.framework.execution.extracted_data import SwapAmounts

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

WALLET = "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
TOKEN_IN = "0x1111111111111111111111111111111111111111"
TOKEN_OUT = "0x2222222222222222222222222222222222222222"

# ERC-20 Transfer topics: [signature, from (padded), to (padded)]
_SIG = TRANSFER_EVENT_SIGNATURE


def _pad_address(addr: str) -> str:
    """Pad an address to 32-byte topic (66 hex chars with 0x)."""
    return "0x" + addr[2:].lower().zfill(64)


def _encode_uint256(value: int) -> str:
    """Encode a uint256 value as 0x-prefixed hex data."""
    return "0x" + hex(value)[2:].zfill(64)


def _transfer_log(token: str, from_addr: str, to_addr: str, amount: int) -> dict:
    """Build a synthetic ERC-20 Transfer log entry."""
    return {
        "address": token,
        "topics": [_SIG, _pad_address(from_addr), _pad_address(to_addr)],
        "data": _encode_uint256(amount),
    }


def _make_receipt(
    wallet: str = WALLET,
    logs: list | None = None,
    status: int = 1,
) -> dict:
    """Build a minimal Ethereum receipt dict."""
    return {
        "from": wallet,
        "status": status,
        "transactionHash": "0x" + "ab" * 32,
        "logs": logs or [],
        "gasUsed": 150_000,
        "effectiveGasPrice": 30_000_000_000,
    }


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


class TestExtractSwapAmountsBasic:
    """Basic extraction from a simple one-hop swap."""

    def test_simple_swap(self):
        """Single Transfer in + single Transfer out -> valid SwapAmounts."""
        parser = EnsoReceiptParser(chain="arbitrum")
        amount_in = 1_000_000_000  # 1000 USDC (6 dec)
        amount_out = 500_000_000_000_000_000  # 0.5 WETH (18 dec)

        receipt = _make_receipt(
            logs=[
                _transfer_log(TOKEN_IN, WALLET, "0xrouter", amount_in),
                _transfer_log(TOKEN_OUT, "0xrouter", WALLET, amount_out),
            ]
        )

        with patch.object(parser, "_resolve_decimals", side_effect=[6, 18]):
            result = parser.extract_swap_amounts(receipt)

        assert result is not None
        assert isinstance(result, SwapAmounts)
        assert result.amount_in == amount_in
        assert result.amount_out == amount_out
        assert result.amount_in_decimal == Decimal("1000")
        assert result.amount_out_decimal == Decimal("0.5")
        assert result.effective_price == Decimal("0.5") / Decimal("1000")
        assert result.token_in == TOKEN_IN.lower()
        assert result.token_out == TOKEN_OUT.lower()

    def test_returns_none_for_reverted_tx(self):
        """Reverted transaction should return None."""
        parser = EnsoReceiptParser()
        receipt = _make_receipt(
            status=0,
            logs=[_transfer_log(TOKEN_IN, WALLET, "0xrouter", 100)],
        )
        assert parser.extract_swap_amounts(receipt) is None

    def test_returns_none_when_no_from(self):
        """Missing 'from' in receipt -> None."""
        parser = EnsoReceiptParser()
        receipt = _make_receipt(logs=[_transfer_log(TOKEN_OUT, "0xrouter", WALLET, 100)])
        del receipt["from"]
        assert parser.extract_swap_amounts(receipt) is None

    def test_returns_none_when_no_transfers(self):
        """No Transfer events -> None."""
        parser = EnsoReceiptParser()
        receipt = _make_receipt(logs=[])
        assert parser.extract_swap_amounts(receipt) is None

    def test_returns_none_when_no_transfer_to_wallet(self):
        """Only outgoing transfers (no incoming) -> None."""
        parser = EnsoReceiptParser()
        receipt = _make_receipt(
            logs=[_transfer_log(TOKEN_IN, WALLET, "0xrouter", 100)]
        )
        assert parser.extract_swap_amounts(receipt) is None

    def test_returns_none_when_amount_out_zero(self):
        """Transfer to wallet with amount 0 -> None."""
        parser = EnsoReceiptParser()
        receipt = _make_receipt(
            logs=[
                _transfer_log(TOKEN_IN, WALLET, "0xrouter", 100),
                _transfer_log(TOKEN_OUT, "0xrouter", WALLET, 0),
            ]
        )
        assert parser.extract_swap_amounts(receipt) is None


class TestExtractSwapAmountsMultiHop:
    """Multi-hop Enso routes produce multiple intermediate Transfer events."""

    def test_multi_hop_picks_first_in_last_out(self):
        """With multiple hops, picks first FROM-wallet and last TO-wallet."""
        parser = EnsoReceiptParser()
        intermediate = "0x3333333333333333333333333333333333333333"

        logs = [
            # Step 1: wallet sends TOKEN_IN to router
            _transfer_log(TOKEN_IN, WALLET, "0xrouter", 1000),
            # Step 2: intermediate hop (not involving wallet)
            _transfer_log(intermediate, "0xrouter", "0xpool", 500),
            # Step 3: pool sends intermediate to another pool
            _transfer_log(intermediate, "0xpool", "0xpool2", 500),
            # Step 4: final output to wallet
            _transfer_log(TOKEN_OUT, "0xpool2", WALLET, 2000),
        ]

        receipt = _make_receipt(logs=logs)

        with patch.object(parser, "_resolve_decimals", return_value=18):
            result = parser.extract_swap_amounts(receipt)

        assert result is not None
        assert result.amount_in == 1000
        assert result.amount_out == 2000
        assert result.token_in == TOKEN_IN.lower()
        assert result.token_out == TOKEN_OUT.lower()

    def test_multiple_transfers_to_wallet(self):
        """When wallet receives multiple transfers, last one is the output."""
        parser = EnsoReceiptParser()

        logs = [
            _transfer_log(TOKEN_IN, WALLET, "0xrouter", 1000),
            # wallet receives a dust refund first
            _transfer_log(TOKEN_IN, "0xrouter", WALLET, 5),
            # then the actual output
            _transfer_log(TOKEN_OUT, "0xrouter", WALLET, 9000),
        ]

        receipt = _make_receipt(logs=logs)

        with patch.object(parser, "_resolve_decimals", return_value=18):
            result = parser.extract_swap_amounts(receipt)

        assert result is not None
        assert result.amount_out == 9000
        assert result.token_out == TOKEN_OUT.lower()


class TestExtractSwapAmountsInputOnly:
    """Edge case: wallet sends tokens but output is native ETH (no Transfer TO wallet)."""

    def test_only_outgoing_transfer(self):
        """When there's only a Transfer FROM wallet but none TO, returns None."""
        parser = EnsoReceiptParser()
        receipt = _make_receipt(
            logs=[_transfer_log(TOKEN_IN, WALLET, "0xrouter", 1000)]
        )
        assert parser.extract_swap_amounts(receipt) is None


ROUTER = "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"


class TestExtractSwapAmountsBytesTopics:
    """Verify that bytes-typed topics (as returned by web3.py) are handled."""

    def test_bytes_topics(self):
        """Topics as bytes objects should be normalized correctly."""
        parser = EnsoReceiptParser()

        sig_bytes = bytes.fromhex(TRANSFER_EVENT_SIGNATURE[2:])
        from_bytes = bytes.fromhex(_pad_address(WALLET)[2:])
        to_bytes = bytes.fromhex(_pad_address(ROUTER)[2:])

        log = {
            "address": TOKEN_OUT,
            "topics": [sig_bytes, from_bytes, to_bytes],
            "data": _encode_uint256(5000),
        }
        # Also add a log TO wallet so we get a result
        to_wallet_from = bytes.fromhex(_pad_address(ROUTER)[2:])
        to_wallet_to = bytes.fromhex(_pad_address(WALLET)[2:])
        log_to = {
            "address": TOKEN_OUT,
            "topics": [sig_bytes, to_wallet_from, to_wallet_to],
            "data": _encode_uint256(4800),
        }

        receipt = _make_receipt(logs=[log, log_to])

        with patch.object(parser, "_resolve_decimals", return_value=18):
            result = parser.extract_swap_amounts(receipt)

        assert result is not None
        assert result.amount_out == 4800

    def test_bytes_from_address(self):
        """receipt['from'] as bytes should be normalized."""
        parser = EnsoReceiptParser()

        wallet_bytes = bytes.fromhex(WALLET[2:])
        receipt = _make_receipt(
            logs=[_transfer_log(TOKEN_OUT, ROUTER, WALLET, 100)]
        )
        receipt["from"] = wallet_bytes

        with patch.object(parser, "_resolve_decimals", return_value=18):
            result = parser.extract_swap_amounts(receipt)

        assert result is not None
        assert result.amount_out == 100


class TestResolveDecimals:
    """Test _resolve_decimals behavior."""

    def test_empty_address_returns_none(self):
        parser = EnsoReceiptParser()
        assert parser._resolve_decimals("") is None

    def test_resolver_failure_returns_none(self):
        """When token resolver throws, return None (not a wrong default)."""
        parser = EnsoReceiptParser(chain="arbitrum")

        mock_resolver = MagicMock()
        mock_resolver.resolve.side_effect = Exception("not found")

        with patch(
            "almanak.framework.data.tokens.get_token_resolver",
            return_value=mock_resolver,
        ):
            assert parser._resolve_decimals("0xdeadbeef") is None

    def test_resolver_success(self):
        """When resolver succeeds, return actual decimals."""
        parser = EnsoReceiptParser(chain="arbitrum")

        mock_token = MagicMock()
        mock_token.decimals = 6
        mock_resolver = MagicMock()
        mock_resolver.resolve.return_value = mock_token

        with patch(
            "almanak.framework.data.tokens.get_token_resolver",
            return_value=mock_resolver,
        ):
            assert parser._resolve_decimals("0xUSDC") == 6

        mock_resolver.resolve.assert_called_once_with("0xUSDC", "arbitrum")

    def test_chain_defaults_to_ethereum(self):
        """When no chain specified, use 'ethereum' as default."""
        parser = EnsoReceiptParser()  # no chain kwarg

        mock_token = MagicMock()
        mock_token.decimals = 8
        mock_resolver = MagicMock()
        mock_resolver.resolve.return_value = mock_token

        with patch(
            "almanak.framework.data.tokens.get_token_resolver",
            return_value=mock_resolver,
        ):
            assert parser._resolve_decimals("0xWBTC") == 8

        mock_resolver.resolve.assert_called_once_with("0xWBTC", "ethereum")


class TestExtractSwapAmountsDecimalResolutionFailure:
    """When decimals can't be resolved, extract_swap_amounts returns None."""

    def test_returns_none_when_output_decimals_unknown(self):
        """Unknown output token decimals -> None (not a wrong 10^12x amount)."""
        parser = EnsoReceiptParser(chain="arbitrum")
        receipt = _make_receipt(
            logs=[
                _transfer_log(TOKEN_IN, WALLET, "0xrouter", 1_000_000),
                _transfer_log(TOKEN_OUT, "0xrouter", WALLET, 500_000_000_000_000_000),
            ]
        )
        # decimals_in resolves fine, decimals_out fails
        with patch.object(parser, "_resolve_decimals", side_effect=[6, None]):
            result = parser.extract_swap_amounts(receipt)
        assert result is None

    def test_returns_valid_when_input_decimals_unknown(self):
        """Unknown input decimals -> amount_in_decimal is 0, but result still returned."""
        parser = EnsoReceiptParser(chain="arbitrum")
        receipt = _make_receipt(
            logs=[
                _transfer_log(TOKEN_IN, WALLET, "0xrouter", 1_000_000),
                _transfer_log(TOKEN_OUT, "0xrouter", WALLET, 500_000_000_000_000_000),
            ]
        )
        with patch.object(parser, "_resolve_decimals", side_effect=[None, 18]):
            result = parser.extract_swap_amounts(receipt)
        assert result is not None
        assert result.amount_in_decimal == Decimal(0)
        assert result.amount_out_decimal > 0


class TestSupportedExtractions:
    """Verify the SUPPORTED_EXTRACTIONS declaration."""

    def test_declares_swap_amounts(self):
        assert "swap_amounts" in EnsoReceiptParser.SUPPORTED_EXTRACTIONS

    def test_is_frozenset(self):
        assert isinstance(EnsoReceiptParser.SUPPORTED_EXTRACTIONS, frozenset)


class TestConstructorCapturesChain:
    """Verify constructor stores chain kwarg."""

    def test_chain_stored(self):
        parser = EnsoReceiptParser(chain="base")
        assert parser._chain == "base"

    def test_no_chain_is_none(self):
        parser = EnsoReceiptParser()
        assert parser._chain is None

    def test_ignores_unknown_kwargs(self):
        """Unknown kwargs should not raise."""
        parser = EnsoReceiptParser(chain="arbitrum", foo="bar")
        assert parser._chain == "arbitrum"
