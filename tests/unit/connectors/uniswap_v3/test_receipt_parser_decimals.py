"""Tests for UniswapV3ReceiptParser decimal resolution from Transfer events.

Verifies that when the parser is constructed without token info (as when used
via ResultEnricher), it resolves correct decimals from Transfer event token
addresses rather than defaulting everything to 18.
"""

from decimal import Decimal
from unittest.mock import patch

import pytest

from almanak.connectors.uniswap_v3.receipt_parser import (
    SwapEventData,
    TransferEventData,
    UniswapV3ReceiptParser,
)

# USDC on Polygon
USDC_ADDRESS = "0x3c499c542cef5e3811e1192ce70d8cc03d5c3359"
USDC_DECIMALS = 6

# WMATIC on Polygon
WMATIC_ADDRESS = "0x0d500b1d8e8ef31e21c99d1db9a6444d3adf1270"
WMATIC_DECIMALS = 18

POOL_ADDRESS = "0x1234567890abcdef1234567890abcdef12345678"
ROUTER_ADDRESS = "0xabcdefabcdefabcdefabcdefabcdefabcdefabcd"


def _make_swap_event(amount0: int, amount1: int) -> SwapEventData:
    """Create a SwapEventData for testing."""
    return SwapEventData(
        sender=ROUTER_ADDRESS,
        recipient=ROUTER_ADDRESS,
        amount0=amount0,
        amount1=amount1,
        sqrt_price_x96=0,
        liquidity=0,
        tick=0,
        pool_address=POOL_ADDRESS,
    )


def _make_transfer(from_addr: str, to_addr: str, value: int, token: str) -> TransferEventData:
    return TransferEventData(
        from_addr=from_addr,
        to_addr=to_addr,
        value=value,
        token_address=token,
    )


# Token info lookup table for mocking _resolve_token_info
_TOKEN_INFO = {
    USDC_ADDRESS: ("USDC", USDC_DECIMALS),
    WMATIC_ADDRESS: ("WMATIC", WMATIC_DECIMALS),
}


def _mock_resolve_token_info(token: str) -> tuple[str, int | None]:
    """Mock _resolve_token_info to return known token info."""
    addr = token.lower()
    if addr in _TOKEN_INFO:
        return _TOKEN_INFO[addr]
    return "", None


def _make_parser_no_tokens() -> UniswapV3ReceiptParser:
    """Create parser without token info, with mocked resolver."""
    parser = UniswapV3ReceiptParser.__new__(UniswapV3ReceiptParser)
    parser.chain = "polygon"
    parser.token0_address = None
    parser.token0_symbol = None
    parser.token0_decimals = 18
    parser.token1_address = None
    parser.token1_symbol = None
    parser.token1_decimals = 18
    parser.quoted_price = None
    parser._token0_decimals_resolved = False
    parser._token1_decimals_resolved = False
    parser._resolve_token_info = _mock_resolve_token_info
    return parser


class TestDecimalResolutionFromTransfers:
    """Test that _resolve_tokens_from_transfers fixes defaulted decimals."""

    def test_resolves_usdc_decimals_from_transfer(self):
        """When parser has no token info, resolve USDC as 6 decimals from Transfer event."""
        parser = _make_parser_no_tokens()
        parser.token0_address = USDC_ADDRESS
        parser.token1_address = WMATIC_ADDRESS

        swap_event = _make_swap_event(amount0=100_000_000, amount1=-1_079_340_000_000_000_000_000)
        transfers = [
            _make_transfer(ROUTER_ADDRESS, POOL_ADDRESS, 100_000_000, USDC_ADDRESS),
            _make_transfer(POOL_ADDRESS, ROUTER_ADDRESS, 1_079_340_000_000_000_000_000, WMATIC_ADDRESS),
        ]

        result = parser._build_swap_result(swap_event, transfers, None)

        # USDC amount should be 100, not 1E-10 (which happens with 18 decimals)
        assert result.amount_in_decimal == Decimal("100")
        # WMATIC amount should be ~1079.34
        assert result.amount_out_decimal == Decimal("1079.34")
        # Effective price ~10.79 WMATIC/USDC
        assert Decimal("10") < result.effective_price < Decimal("11")

    def test_skips_resolution_when_decimals_already_set(self):
        """When parser has correct decimals from construction, don't re-resolve."""
        parser = _make_parser_no_tokens()
        parser.token0_address = USDC_ADDRESS
        parser.token0_decimals = USDC_DECIMALS
        parser._token0_decimals_resolved = True
        parser.token1_address = WMATIC_ADDRESS
        parser.token1_decimals = WMATIC_DECIMALS
        parser._token1_decimals_resolved = True

        swap_event = _make_swap_event(amount0=100_000_000, amount1=-1_079_340_000_000_000_000_000)
        transfers = [
            _make_transfer(ROUTER_ADDRESS, POOL_ADDRESS, 100_000_000, USDC_ADDRESS),
            _make_transfer(POOL_ADDRESS, ROUTER_ADDRESS, 1_079_340_000_000_000_000_000, WMATIC_ADDRESS),
        ]

        result = parser._build_swap_result(swap_event, transfers, None)

        assert result.amount_in_decimal == Decimal("100")
        assert result.amount_out_decimal == Decimal("1079.34")

    def test_infers_token_addresses_from_transfers(self):
        """When parser has no token addresses at all, infer from Transfer events using swap direction."""
        parser = _make_parser_no_tokens()
        # No token addresses set

        # amount0 > 0 means token0 is input (sent TO pool)
        swap_event = _make_swap_event(amount0=100_000_000, amount1=-1_079_340_000_000_000_000_000)
        transfers = [
            _make_transfer(ROUTER_ADDRESS, POOL_ADDRESS, 100_000_000, USDC_ADDRESS),
            _make_transfer(POOL_ADDRESS, ROUTER_ADDRESS, 1_079_340_000_000_000_000_000, WMATIC_ADDRESS),
        ]

        result = parser._build_swap_result(swap_event, transfers, None)

        # Amounts should be correct (inferred from transfer direction)
        assert result.amount_in_decimal == Decimal("100")
        assert result.amount_out_decimal == Decimal("1079.34")
        # Parser state should NOT be mutated (inference uses local overrides)
        assert parser.token0_address is None
        assert parser.token1_address is None

    def test_cached_parser_not_corrupted_across_receipts(self):
        """A cached parser (no pre-set tokens) must produce correct results for different pools."""
        parser = _make_parser_no_tokens()

        # First receipt: USDC -> WMATIC
        swap1 = _make_swap_event(amount0=100_000_000, amount1=-1_079_340_000_000_000_000_000)
        transfers1 = [
            _make_transfer(ROUTER_ADDRESS, POOL_ADDRESS, 100_000_000, USDC_ADDRESS),
            _make_transfer(POOL_ADDRESS, ROUTER_ADDRESS, 1_079_340_000_000_000_000_000, WMATIC_ADDRESS),
        ]
        result1 = parser._build_swap_result(swap1, transfers1, None)
        assert result1.amount_in_decimal == Decimal("100")

        # Second receipt: same parser, different pool (WMATIC -> USDC, reversed)
        swap2 = _make_swap_event(amount0=-50_000_000, amount1=539_670_000_000_000_000_000)
        swap2 = SwapEventData(
            sender=ROUTER_ADDRESS,
            recipient=ROUTER_ADDRESS,
            amount0=-50_000_000,
            amount1=539_670_000_000_000_000_000,
            sqrt_price_x96=0,
            liquidity=0,
            tick=0,
            pool_address=POOL_ADDRESS,
        )
        transfers2 = [
            _make_transfer(ROUTER_ADDRESS, POOL_ADDRESS, 539_670_000_000_000_000_000, WMATIC_ADDRESS),
            _make_transfer(POOL_ADDRESS, ROUTER_ADDRESS, 50_000_000, USDC_ADDRESS),
        ]
        result2 = parser._build_swap_result(swap2, transfers2, None)
        # WMATIC in (~539.67), USDC out (50)
        assert result2.amount_in_decimal == Decimal("539.67")
        assert result2.amount_out_decimal == Decimal("50")

    def test_handles_empty_transfers_gracefully(self, caplog):
        """When no Transfer events, warns about unresolved decimals (VIB-592).

        VIB-3164 deferred: this path still falls back to the 18-decimal default
        rather than failing loud, because hard-failing on the live enricher
        path would halt accounting. The warning is the visible signal.
        """
        parser = _make_parser_no_tokens()

        swap_event = _make_swap_event(amount0=100_000_000, amount1=-1_079_340_000_000_000_000_000)

        import logging

        caplog.set_level(logging.WARNING)
        result = parser._build_swap_result(swap_event, [], None)

        # VIB-592: logs warning but still produces result (for backward compat)
        assert result is not None
        assert result.amount_in_decimal == Decimal("100000000") / Decimal(10**18)
        assert any("Token decimals unresolved after Transfer analysis" in msg for msg in caplog.messages)

    def test_handles_resolver_failure(self):
        """When token resolver fails, warns but still produces result (VIB-592)."""
        parser = _make_parser_no_tokens()
        parser.token0_address = USDC_ADDRESS
        parser.token1_address = WMATIC_ADDRESS
        parser._resolve_token_info = lambda token: ("", None)  # Simulate resolver failure

        swap_event = _make_swap_event(amount0=100_000_000, amount1=-1_079_340_000_000_000_000_000)
        transfers = [
            _make_transfer(ROUTER_ADDRESS, POOL_ADDRESS, 100_000_000, USDC_ADDRESS),
            _make_transfer(POOL_ADDRESS, ROUTER_ADDRESS, 1_079_340_000_000_000_000_000, WMATIC_ADDRESS),
        ]

        result = parser._build_swap_result(swap_event, transfers, None)

        # VIB-592: logs warning about unresolved decimals but still returns result
        assert result is not None
        assert result.amount_in > 0

    def test_resolves_reverse_direction(self):
        """Test swap in the other direction: WMATIC -> USDC (amount1 positive)."""
        parser = _make_parser_no_tokens()
        parser.token0_address = USDC_ADDRESS
        parser.token1_address = WMATIC_ADDRESS

        # Swap: WMATIC in (amount1 > 0) -> USDC out (amount0 < 0)
        swap_event = _make_swap_event(
            amount0=-100_000_000,  # USDC out: 100 USDC
            amount1=1_079_340_000_000_000_000_000,  # WMATIC in: ~1079 WMATIC
        )
        transfers = [
            _make_transfer(ROUTER_ADDRESS, POOL_ADDRESS, 1_079_340_000_000_000_000_000, WMATIC_ADDRESS),
            _make_transfer(POOL_ADDRESS, ROUTER_ADDRESS, 100_000_000, USDC_ADDRESS),
        ]

        result = parser._build_swap_result(swap_event, transfers, None)

        # WMATIC is token_in (amount1 > 0 = token1 is input)
        assert result.amount_in_decimal == Decimal("1079.34")
        # USDC is token_out
        assert result.amount_out_decimal == Decimal("100")


class TestFlagTracking:
    """Verify _token0/1_decimals_resolved tracking."""

    def test_resolved_flag_set_when_decimals_provided(self):
        """Flags are True when decimals are explicitly provided."""
        parser = _make_parser_no_tokens()
        parser.token0_decimals = 6
        parser._token0_decimals_resolved = True
        parser.token1_decimals = 18
        parser._token1_decimals_resolved = True

        assert parser._token0_decimals_resolved is True
        assert parser._token1_decimals_resolved is True

    def test_resolved_flag_false_when_defaulted(self):
        """Flags are False when decimals were defaulted to 18."""
        parser = _make_parser_no_tokens()

        assert parser._token0_decimals_resolved is False
        assert parser._token1_decimals_resolved is False


# ---------------------------------------------------------------------------
# Compiler-metadata hint tests (VIB-3164)
# ---------------------------------------------------------------------------

_USDC_META = {"address": USDC_ADDRESS, "symbol": "USDC", "decimals": USDC_DECIMALS}
_WMATIC_META = {"address": WMATIC_ADDRESS, "symbol": "WMATIC", "decimals": WMATIC_DECIMALS}


class TestCompilerMetadataHints:
    """VIB-3164: compiler-threaded token metadata resolves decimals."""

    def test_hint_resolves_six_decimal_token_when_resolver_misses(self):
        """Parser resolves USDC as 6-decimal via hint when resolver returns None."""
        parser = _make_parser_no_tokens()
        parser._resolve_token_info = lambda token: ("", None)  # resolver always misses
        swap_event = _make_swap_event(amount0=100_000_000, amount1=-1_079_340_000_000_000_000_000)
        transfers = [
            _make_transfer(ROUTER_ADDRESS, POOL_ADDRESS, 100_000_000, USDC_ADDRESS),
            _make_transfer(POOL_ADDRESS, ROUTER_ADDRESS, 1_079_340_000_000_000_000_000, WMATIC_ADDRESS),
        ]
        meta = {"token_in": _USDC_META, "token_out": _WMATIC_META}
        result = parser._build_swap_result(swap_event, transfers, None, swap_token_meta=meta)
        assert result.amount_in_decimal == Decimal("100")
        assert result.amount_out_decimal == Decimal("1079.34")
        assert result.token_in_decimals_resolved is True
        assert result.token_out_decimals_resolved is True
        # overrides-only: parser instance stays clean
        assert parser.token0_address is None

    def test_direction_fallback_when_transfers_unclassifiable(self, caplog):
        """Branch 3: empty transfers -> hint applied via direction fallback, no warning."""
        import logging

        parser = _make_parser_no_tokens()
        parser._resolve_token_info = lambda token: ("", None)
        swap_event = _make_swap_event(amount0=100_000_000, amount1=-1_079_340_000_000_000_000_000)
        meta = {"token_in": _USDC_META, "token_out": _WMATIC_META}

        caplog.set_level(logging.WARNING)
        result = parser._build_swap_result(swap_event, [], None, swap_token_meta=meta)

        assert result.amount_in_decimal == Decimal("100")
        assert result.amount_out_decimal == Decimal("1079.34")
        assert result.token_in_decimals_resolved is True
        assert result.token_out_decimals_resolved is True
        # Branch 3 resolved decimals, so the "unresolved" warning must NOT fire
        assert not any("Token decimals unresolved after Transfer analysis" in m for m in caplog.messages)

    def test_direction_fallback_skipped_for_multi_swap_receipts(self, caplog):
        """Branch 3 must not apply when single_swap=False (multi-hop)."""
        import logging

        parser = _make_parser_no_tokens()
        parser._resolve_token_info = lambda token: ("", None)
        swap_event = _make_swap_event(amount0=100_000_000, amount1=-1_079_340_000_000_000_000_000)
        meta = {"token_in": _USDC_META, "token_out": _WMATIC_META}

        caplog.set_level(logging.WARNING)
        result = parser._build_swap_result(swap_event, [], None, swap_token_meta=meta, single_swap=False)

        # Falls back to 18-decimal default (multi-hop path: don't apply compiler hint)
        assert result.amount_in_decimal == Decimal("100000000") / Decimal(10**18)
        assert result.token_in_decimals_resolved is False
        assert any("Token decimals unresolved after Transfer analysis" in m for m in caplog.messages)

    def test_direction_fallback_skipped_on_address_mismatch(self, caplog):
        """Branch 3: hint for token0 slot skipped when pre-set address differs."""
        import logging

        parser = _make_parser_no_tokens()
        # token0 is pre-set to WMATIC, but hint says token_in=USDC with token0_is_input=True
        # -> address mismatch -> hint NOT applied for token0
        parser.token0_address = WMATIC_ADDRESS
        parser._resolve_token_info = lambda token: ("", None)
        swap_event = _make_swap_event(amount0=100_000_000, amount1=-1_079_340_000_000_000_000_000)
        meta = {"token_in": _USDC_META, "token_out": _WMATIC_META}

        caplog.set_level(logging.WARNING)
        result = parser._build_swap_result(swap_event, [], None, swap_token_meta=meta)

        # token0 hint was skipped due to address mismatch -> token0 (in) unresolved
        assert result.token_in_decimals_resolved is False
        assert any("Token decimals unresolved after Transfer analysis" in m for m in caplog.messages)

    def test_constructor_decimals_win_over_hints(self):
        """When decimals are already resolved, hints are never consulted."""
        parser = _make_parser_no_tokens()
        parser.token0_decimals = 6
        parser._token0_decimals_resolved = True
        parser.token1_decimals = 18
        parser._token1_decimals_resolved = True
        # Provide contradictory hints (decimals=8); they must be ignored
        contradictory_meta = {
            "token_in": {"address": USDC_ADDRESS, "symbol": "USDC", "decimals": 8},
            "token_out": {"address": WMATIC_ADDRESS, "symbol": "WMATIC", "decimals": 8},
        }
        swap_event = _make_swap_event(amount0=100_000_000, amount1=-1_079_340_000_000_000_000_000)
        result = parser._build_swap_result(swap_event, [], None, swap_token_meta=contradictory_meta)
        # Decimals 6 and 18 from constructor win, not 8
        assert result.amount_in_decimal == Decimal("100")
        assert result.amount_out_decimal == Decimal("1079.34")
        assert result.token_in_decimals_resolved is True
        assert result.token_out_decimals_resolved is True

    def test_hint_wins_over_resolver(self):
        """Compiler hint takes precedence over TokenResolver for the same address."""
        parser = _make_parser_no_tokens()
        # Resolver claims USDC has 18 decimals (wrong); hint says 6 (correct)
        parser._resolve_token_info = lambda token: ("XXX", 18)
        swap_event = _make_swap_event(amount0=100_000_000, amount1=-1_079_340_000_000_000_000_000)
        transfers = [
            _make_transfer(ROUTER_ADDRESS, POOL_ADDRESS, 100_000_000, USDC_ADDRESS),
            _make_transfer(POOL_ADDRESS, ROUTER_ADDRESS, 1_079_340_000_000_000_000_000, WMATIC_ADDRESS),
        ]
        meta = {"token_in": _USDC_META, "token_out": _WMATIC_META}
        result = parser._build_swap_result(swap_event, transfers, None, swap_token_meta=meta)
        # Hint-first: USDC should be 6 decimals -> 100
        assert result.amount_in_decimal == Decimal("100")
        assert result.token_in_decimals_resolved is True

    def test_no_hints_no_transfers_preserves_warning_fallback(self, caplog):
        """Without hints or transfers, legacy 18-decimal fallback fires with warning."""
        import logging

        parser = _make_parser_no_tokens()
        swap_event = _make_swap_event(amount0=100_000_000, amount1=-1_079_340_000_000_000_000_000)

        caplog.set_level(logging.WARNING)
        result = parser._build_swap_result(swap_event, [], None)

        assert result is not None
        assert result.amount_in_decimal == Decimal("100000000") / Decimal(10**18)
        assert any("Token decimals unresolved after Transfer analysis" in m for m in caplog.messages)
        assert result.token_in_decimals_resolved is False
        assert result.token_out_decimals_resolved is False


class TestBuildExtractKwargsHook:
    """VIB-3164: the parser-owned enricher hook (Pendle exemplar pattern)."""

    def test_returns_swap_token_meta_from_compiler_metadata(self):
        """build_extract_kwargs maps from_token/to_token into swap_token_meta."""
        parser = _make_parser_no_tokens()
        bundle_metadata = {
            "from_token": {
                "symbol": "USDC",
                "address": USDC_ADDRESS.upper(),  # uppercase -> should be lowercased
                "decimals": 6,
                "is_native": False,
            },
            "to_token": {
                "symbol": "WMATIC",
                "address": WMATIC_ADDRESS,
                "decimals": 18,
                "is_native": False,
            },
        }
        kwargs = parser.build_extract_kwargs(field="swap_amounts", bundle_metadata=bundle_metadata)
        assert kwargs == {
            "swap_token_meta": {
                "token_in": {"address": USDC_ADDRESS, "symbol": "USDC", "decimals": 6},
                "token_out": {"address": WMATIC_ADDRESS, "symbol": "WMATIC", "decimals": 18},
            }
        }

    def test_skips_native_entries(self):
        """is_native=True entries are skipped; missing decimals also skipped."""
        parser = _make_parser_no_tokens()
        # from_token is native; to_token missing decimals
        bundle_metadata = {
            "from_token": {"symbol": "ETH", "address": USDC_ADDRESS, "decimals": 18, "is_native": True},
            "to_token": {"symbol": "WMATIC", "address": WMATIC_ADDRESS},
        }
        kwargs = parser.build_extract_kwargs(field="swap_amounts", bundle_metadata=bundle_metadata)
        assert kwargs == {}

    def test_coerces_string_decimals(self):
        """String decimals are coerced to int."""
        parser = _make_parser_no_tokens()
        bundle_metadata = {
            "from_token": {"symbol": "USDC", "address": USDC_ADDRESS, "decimals": "6", "is_native": False},
        }
        kwargs = parser.build_extract_kwargs(field="swap_amounts", bundle_metadata=bundle_metadata)
        assert "swap_token_meta" in kwargs
        assert kwargs["swap_token_meta"]["token_in"]["decimals"] == 6

    def test_returns_empty_for_other_fields(self):
        """Returns {} when field is not swap_amounts."""
        parser = _make_parser_no_tokens()
        bundle_metadata = {
            "from_token": {"symbol": "USDC", "address": USDC_ADDRESS, "decimals": 6, "is_native": False},
        }
        result = parser.build_extract_kwargs(field="position_id", bundle_metadata=bundle_metadata)
        assert result == {}

    def test_must_not_claim_framework_owned_kwarg(self):
        """expected_out must never appear in the returned dict."""
        parser = _make_parser_no_tokens()
        bundle_metadata = {
            "from_token": {"symbol": "USDC", "address": USDC_ADDRESS, "decimals": 6, "is_native": False},
            "to_token": {"symbol": "WMATIC", "address": WMATIC_ADDRESS, "decimals": 18, "is_native": False},
        }
        kwargs = parser.build_extract_kwargs(field="swap_amounts", bundle_metadata=bundle_metadata)
        assert "expected_out" not in kwargs


# ---------------------------------------------------------------------------
# End-to-end tests: receipt logs -> extract_swap_amounts_result with metadata
# ---------------------------------------------------------------------------

# Re-use the receipt-building helpers from test_vib_3203_expected_out to avoid
# duplicating the event encoding logic.

UNIV3_SWAP_TOPIC = "0xc42079f94a6350d7e6235f29174924f928cc2ac818eb64fed8004e115fbcca67"
ERC20_TRANSFER_TOPIC = "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"

# Use addresses the real TokenResolver does NOT know (synthetic)
FAKE_TOKEN_IN_ADDR = "0x" + "f1" * 20   # fake 6-decimal token
FAKE_TOKEN_OUT_ADDR = "0x" + "f2" * 20  # fake 18-decimal token
FAKE_POOL_ADDR = "0x" + "f3" * 20
FAKE_WALLET_ADDR = "0x" + "f4" * 20


def _pad32(val: int, signed: bool = False) -> str:
    if signed and val < 0:
        val += 1 << 256
    return f"{val:064x}"


def _addr_topic(addr: str) -> str:
    return "0x" + addr.lower().replace("0x", "").zfill(64)


def _int256_hex(val: int) -> str:
    if val >= 0:
        return hex(val)[2:].zfill(64)
    return hex((1 << 256) + val)[2:].zfill(64)


def _build_fake_univ3_receipt(
    pool: str,
    token_in: str,
    token_out: str,
    wallet: str,
    amount_in: int,
    amount_out: int,
) -> dict:
    """Build a minimal Uniswap V3 receipt with Transfer + Swap events.

    Convention: amount0 < 0 (pool sends out token_out), amount1 > 0 (pool
    receives token_in). Mirrors _build_univ3_swap_receipt from test_vib_3203.
    """
    swap_data = "0x" + (
        _int256_hex(-amount_out)
        + _int256_hex(amount_in)
        + _pad32(2**96)
        + _pad32(10**18)
        + _int256_hex(0)
    )
    return {
        "transactionHash": "0x" + "ee" * 32,
        "status": 1,
        "blockNumber": 100,
        "gasUsed": 200_000,
        "from": wallet,
        "logs": [
            # token_in: wallet -> pool
            {
                "address": token_in,
                "topics": [ERC20_TRANSFER_TOPIC, _addr_topic(wallet), _addr_topic(pool)],
                "data": "0x" + _pad32(amount_in),
                "logIndex": 0,
            },
            # Swap event (pool)
            {
                "address": pool,
                "topics": [UNIV3_SWAP_TOPIC, _addr_topic(wallet), _addr_topic(wallet)],
                "data": swap_data,
                "logIndex": 1,
            },
            # token_out: pool -> wallet
            {
                "address": token_out,
                "topics": [ERC20_TRANSFER_TOPIC, _addr_topic(pool), _addr_topic(wallet)],
                "data": "0x" + _pad32(amount_out),
                "logIndex": 2,
            },
        ],
    }


class TestExtractSwapAmountsWithMeta:
    """End-to-end: receipt logs -> extract_swap_amounts_result with hints."""

    def test_usdc_swap_resolved_via_threaded_metadata(self):
        """100 units of a 6-decimal token resolves to Decimal('100') via hints."""
        parser = UniswapV3ReceiptParser.__new__(UniswapV3ReceiptParser)
        from almanak.connectors.uniswap_v3.receipt_parser import EventRegistry, EVENT_TOPICS, EVENT_NAME_TO_TYPE

        parser.chain = "polygon"
        parser.token0_address = None
        parser.token0_symbol = None
        parser.token0_decimals = 18
        parser.token1_address = None
        parser.token1_symbol = None
        parser.token1_decimals = 18
        parser.quoted_price = None
        parser._token0_decimals_resolved = False
        parser._token1_decimals_resolved = False
        parser.registry = EventRegistry(EVENT_TOPICS, EVENT_NAME_TO_TYPE)
        # Resolver returns nothing — forces the test to rely on metadata
        parser._resolve_token_info = lambda token: ("", None)

        receipt = _build_fake_univ3_receipt(
            pool=FAKE_POOL_ADDR,
            token_in=FAKE_TOKEN_IN_ADDR,
            token_out=FAKE_TOKEN_OUT_ADDR,
            wallet=FAKE_WALLET_ADDR,
            amount_in=100_000_000,     # 100 units at 6 decimals
            amount_out=1_079_340_000_000_000_000_000,  # 1079.34 at 18 decimals
        )
        meta = {
            "token_in": {"address": FAKE_TOKEN_IN_ADDR, "symbol": "FAKE6", "decimals": 6},
            "token_out": {"address": FAKE_TOKEN_OUT_ADDR, "symbol": "FAKE18", "decimals": 18},
        }
        from almanak.framework.execution.extract_result import ExtractOk

        extract_result = parser.extract_swap_amounts_result(receipt, swap_token_meta=meta)
        assert isinstance(extract_result, ExtractOk)
        value = extract_result.value
        assert value.amount_in_decimal == Decimal("100")
        assert value.amount_in_decimal_resolved is True
        assert value.amount_out_decimal_resolved is True

    def test_fallback_stamps_unresolved_flags(self):
        """No transfers + no metadata -> legacy 18-default with unresolved flags."""
        parser = UniswapV3ReceiptParser.__new__(UniswapV3ReceiptParser)
        from almanak.connectors.uniswap_v3.receipt_parser import EventRegistry, EVENT_TOPICS, EVENT_NAME_TO_TYPE

        parser.chain = "polygon"
        parser.token0_address = None
        parser.token0_symbol = None
        parser.token0_decimals = 18
        parser.token1_address = None
        parser.token1_symbol = None
        parser.token1_decimals = 18
        parser.quoted_price = None
        parser._token0_decimals_resolved = False
        parser._token1_decimals_resolved = False
        parser.registry = EventRegistry(EVENT_TOPICS, EVENT_NAME_TO_TYPE)
        parser._resolve_token_info = lambda token: ("", None)

        # Build a receipt with NO Transfer logs (just Swap event)
        swap_data = "0x" + (
            _int256_hex(-1_079_340_000_000_000_000_000)
            + _int256_hex(100_000_000)
            + _pad32(2**96)
            + _pad32(10**18)
            + _int256_hex(0)
        )
        receipt = {
            "transactionHash": "0x" + "dd" * 32,
            "status": 1,
            "blockNumber": 1,
            "gasUsed": 100_000,
            "from": FAKE_WALLET_ADDR,
            "logs": [
                {
                    "address": FAKE_POOL_ADDR,
                    "topics": [UNIV3_SWAP_TOPIC, _addr_topic(FAKE_WALLET_ADDR), _addr_topic(FAKE_WALLET_ADDR)],
                    "data": swap_data,
                    "logIndex": 0,
                },
            ],
        }
        from almanak.framework.execution.extract_result import ExtractOk

        extract_result = parser.extract_swap_amounts_result(receipt)
        assert isinstance(extract_result, ExtractOk)
        value = extract_result.value
        # Legacy 18-decimal fallback: amount_in_decimal is the raw value divided by 10**18
        assert value.amount_in_decimal == Decimal("100000000") / Decimal(10**18)
        assert value.amount_in_decimal_resolved is False
        assert value.amount_out_decimal_resolved is False

    def test_expected_out_still_threads_with_meta(self):
        """expected_out and swap_token_meta coexist: slippage_bps is computed."""
        parser = UniswapV3ReceiptParser.__new__(UniswapV3ReceiptParser)
        from almanak.connectors.uniswap_v3.receipt_parser import EventRegistry, EVENT_TOPICS, EVENT_NAME_TO_TYPE

        parser.chain = "polygon"
        parser.token0_address = None
        parser.token0_symbol = None
        parser.token0_decimals = 18
        parser.token1_address = None
        parser.token1_symbol = None
        parser.token1_decimals = 18
        parser.quoted_price = None
        parser._token0_decimals_resolved = False
        parser._token1_decimals_resolved = False
        parser.registry = EventRegistry(EVENT_TOPICS, EVENT_NAME_TO_TYPE)
        parser._resolve_token_info = lambda token: ("", None)

        receipt = _build_fake_univ3_receipt(
            pool=FAKE_POOL_ADDR,
            token_in=FAKE_TOKEN_IN_ADDR,
            token_out=FAKE_TOKEN_OUT_ADDR,
            wallet=FAKE_WALLET_ADDR,
            amount_in=100_000_000,
            amount_out=1_079_340_000_000_000_000_000,
        )
        meta = {
            "token_in": {"address": FAKE_TOKEN_IN_ADDR, "symbol": "FAKE6", "decimals": 6},
            "token_out": {"address": FAKE_TOKEN_OUT_ADDR, "symbol": "FAKE18", "decimals": 18},
        }
        result = parser.extract_swap_amounts(
            receipt,
            expected_out=Decimal("1100"),
            swap_token_meta=meta,
        )
        assert result is not None
        assert result.slippage_bps is not None  # computed from expected_out
        assert result.amount_in_decimal == Decimal("100")
