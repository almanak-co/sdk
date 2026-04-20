"""Unit tests for SushiSwapV3ReceiptParser._resolve_decimals and
_extract_swap_tokens_from_transfers.

These test the two helper methods introduced in the enricher-decimals-bug fix
in isolation, complementing the integration-level tests in
tests/framework/connectors/sushiswap_v3/test_receipt_parser.py.
"""

from unittest.mock import MagicMock, patch

from almanak.framework.connectors.sushiswap_v3.receipt_parser import (
    SushiSwapV3ReceiptParser,
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

WALLET = "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
TOKEN_A = "0x1111111111111111111111111111111111111111"
TOKEN_B = "0x2222222222222222222222222222222222222222"
ROUTER = "0x3333333333333333333333333333333333333333"

TRANSFER_TOPIC = "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"


def _pad(addr: str) -> str:
    return "0x" + addr[2:].lower().zfill(64)


def _transfer_log(token: str, from_addr: str, to_addr: str, amount: int) -> dict:
    return {
        "address": token,
        "topics": [TRANSFER_TOPIC, _pad(from_addr), _pad(to_addr)],
        "data": "0x" + hex(amount)[2:].zfill(64),
    }


# ---------------------------------------------------------------------------
# _resolve_decimals
# ---------------------------------------------------------------------------


class TestResolveDecimals:
    """Unit tests for _resolve_decimals."""

    def test_empty_address_returns_none(self):
        parser = SushiSwapV3ReceiptParser(chain="arbitrum")
        assert parser._resolve_decimals("") is None

    def test_resolver_success(self):
        parser = SushiSwapV3ReceiptParser(chain="arbitrum")

        mock_token = MagicMock()
        mock_token.decimals = 6
        mock_resolver = MagicMock()
        mock_resolver.resolve.return_value = mock_token

        with patch(
            "almanak.framework.data.tokens.resolver.get_token_resolver",
            return_value=mock_resolver,
        ):
            assert parser._resolve_decimals("0xUSDC") == 6

        mock_resolver.resolve.assert_called_once_with("0xUSDC", "arbitrum")

    def test_resolver_failure_returns_none(self):
        parser = SushiSwapV3ReceiptParser(chain="arbitrum")

        mock_resolver = MagicMock()
        mock_resolver.resolve.side_effect = Exception("not found")

        with patch(
            "almanak.framework.data.tokens.resolver.get_token_resolver",
            return_value=mock_resolver,
        ):
            assert parser._resolve_decimals("0xdeadbeef") is None


# ---------------------------------------------------------------------------
# _extract_swap_tokens_from_transfers
# ---------------------------------------------------------------------------


class TestExtractSwapTokensFromTransfers:
    """Unit tests for _extract_swap_tokens_from_transfers."""

    def test_simple_swap(self):
        """Single transfer out + single transfer in -> correct token pair."""
        parser = SushiSwapV3ReceiptParser(chain="arbitrum")
        receipt = {
            "from": WALLET,
            "logs": [
                _transfer_log(TOKEN_A, WALLET, ROUTER, 1000),
                _transfer_log(TOKEN_B, ROUTER, WALLET, 2000),
            ],
        }

        token_in, token_out, amount_in, amount_out = parser._extract_swap_tokens_from_transfers(receipt)

        assert token_in == TOKEN_A.lower()
        assert token_out == TOKEN_B.lower()
        assert amount_in == 1000
        assert amount_out == 2000

    def test_missing_from_returns_empty(self):
        """No 'from' key -> empty results."""
        parser = SushiSwapV3ReceiptParser(chain="arbitrum")
        receipt = {"logs": [_transfer_log(TOKEN_A, WALLET, ROUTER, 1000)]}

        assert parser._extract_swap_tokens_from_transfers(receipt) == ("", "", 0, 0)

    def test_from_address_fallback(self):
        """Falls back to 'from_address' when 'from' is missing."""
        parser = SushiSwapV3ReceiptParser(chain="arbitrum")
        receipt = {
            "from_address": WALLET,
            "logs": [
                _transfer_log(TOKEN_A, WALLET, ROUTER, 500),
                _transfer_log(TOKEN_B, ROUTER, WALLET, 1500),
            ],
        }

        token_in, token_out, amount_in, amount_out = parser._extract_swap_tokens_from_transfers(receipt)

        assert token_in == TOKEN_A.lower()
        assert token_out == TOKEN_B.lower()

    def test_no_transfer_logs_returns_empty(self):
        """No Transfer events -> empty results."""
        parser = SushiSwapV3ReceiptParser(chain="arbitrum")
        receipt = {"from": WALLET, "logs": []}

        assert parser._extract_swap_tokens_from_transfers(receipt) == ("", "", 0, 0)

    def test_only_outgoing_transfer(self):
        """Only transfers FROM wallet, none TO -> token_out is empty."""
        parser = SushiSwapV3ReceiptParser(chain="arbitrum")
        receipt = {
            "from": WALLET,
            "logs": [_transfer_log(TOKEN_A, WALLET, ROUTER, 1000)],
        }

        token_in, token_out, amount_in, amount_out = parser._extract_swap_tokens_from_transfers(receipt)

        assert token_in == TOKEN_A.lower()
        assert amount_in == 1000
        assert token_out == ""
        assert amount_out == 0

    def test_multi_hop_picks_first_in_last_out(self):
        """With multiple transfers, picks first FROM wallet and last TO wallet."""
        parser = SushiSwapV3ReceiptParser(chain="arbitrum")
        intermediate = "0x4444444444444444444444444444444444444444"

        receipt = {
            "from": WALLET,
            "logs": [
                _transfer_log(TOKEN_A, WALLET, ROUTER, 1000),
                _transfer_log(intermediate, ROUTER, "0xpool", 500),
                _transfer_log(TOKEN_B, "0xpool", WALLET, 2000),
                _transfer_log(TOKEN_B, "0xpool2", WALLET, 3000),  # later transfer
            ],
        }

        token_in, token_out, amount_in, amount_out = parser._extract_swap_tokens_from_transfers(receipt)

        assert token_in == TOKEN_A.lower()
        assert amount_in == 1000
        assert token_out == TOKEN_B.lower()
        assert amount_out == 3000  # last transfer TO wallet

    def test_bytes_topics_handled(self):
        """HexBytes topics (from web3.py) are normalized correctly."""
        parser = SushiSwapV3ReceiptParser(chain="arbitrum")

        sig_bytes = bytes.fromhex(TRANSFER_TOPIC[2:])
        from_bytes = bytes.fromhex(_pad(WALLET)[2:])
        to_bytes = bytes.fromhex(_pad(ROUTER)[2:])
        to_wallet_from = bytes.fromhex(_pad(ROUTER)[2:])
        to_wallet_to = bytes.fromhex(_pad(WALLET)[2:])

        receipt = {
            "from": WALLET,
            "logs": [
                {
                    "address": TOKEN_A,
                    "topics": [sig_bytes, from_bytes, to_bytes],
                    "data": "0x" + hex(1000)[2:].zfill(64),
                },
                {
                    "address": TOKEN_B,
                    "topics": [sig_bytes, to_wallet_from, to_wallet_to],
                    "data": "0x" + hex(2000)[2:].zfill(64),
                },
            ],
        }

        token_in, token_out, amount_in, amount_out = parser._extract_swap_tokens_from_transfers(receipt)

        assert token_in == TOKEN_A.lower()
        assert token_out == TOKEN_B.lower()
        assert amount_in == 1000
        assert amount_out == 2000

    def test_skips_logs_with_fewer_than_3_topics(self):
        """Logs with < 3 topics (e.g. Approval) are silently skipped."""
        parser = SushiSwapV3ReceiptParser(chain="arbitrum")
        receipt = {
            "from": WALLET,
            "logs": [
                {
                    "address": TOKEN_A,
                    "topics": [TRANSFER_TOPIC, _pad(WALLET)],  # only 2 topics
                    "data": "0x" + hex(1000)[2:].zfill(64),
                },
                _transfer_log(TOKEN_B, ROUTER, WALLET, 2000),
            ],
        }

        token_in, token_out, amount_in, amount_out = parser._extract_swap_tokens_from_transfers(receipt)

        # The 2-topic log is skipped, only the 3-topic transfer TO wallet is picked up
        assert token_in == ""
        assert token_out == TOKEN_B.lower()
        assert amount_out == 2000
