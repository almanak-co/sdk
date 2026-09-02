"""Behavioral tests for PoolKey lookup while parsing V4 LP_OPEN receipts."""

from __future__ import annotations

import logging

import pytest

from almanak.connectors.uniswap_v4.addresses import UNISWAP_V4
from almanak.connectors.uniswap_v4.receipt_parser import (
    EVENT_TOPICS,
    UniswapV4ReceiptParser,
)
from almanak.connectors.uniswap_v4.sdk import PoolKey
from almanak.framework.observability.metrics import (
    FRAMEWORK_REGISTRY,
    V4LPDropReason,
)

PARSER_LOGGER = "almanak.connectors.uniswap_v4.receipt_parser"

# =============================================================================
# Fixtures
# =============================================================================

CHAIN = "base"
BASE_PM = UNISWAP_V4[CHAIN]["position_manager"]
BASE_POOL_MANAGER = UNISWAP_V4[CHAIN]["pool_manager"]
WALLET = "0x1111111111111111111111111111111111111111"
# Base WETH (0x4200...) < Base USDC (0x8335...). PoolKey sorts WETH first.
USDC_BASE = "0x833589fcd6edb6e08f4c7c32d4f71b54bda02913"
WETH_BASE = "0x4200000000000000000000000000000000000006"
assert int(WETH_BASE, 16) < int(USDC_BASE, 16), "WETH MUST sort before USDC numerically — fixture assumption"

TOKEN_ID = 4242
POOL_ID = "0x" + format(0xABCD, "064x")
TICK_LOWER = -887220
TICK_UPPER = -100
LIQUIDITY_DELTA = 10**15
USDC_AMOUNT = 1_000_000_000  # 1000 USDC
WETH_AMOUNT = 5 * 10**17  # 0.5 WETH


def _hex32(value: int) -> str:
    return "0x" + format(value, "064x")


def _build_modify_liquidity_log(
    *,
    pool_id: str,
    sender: str,
    tick_lower: int,
    tick_upper: int,
    liquidity_delta: int,
    salt: str,
    pool_manager: str,
) -> dict:
    sender_padded = "0x" + sender.lower().replace("0x", "").zfill(64)
    if liquidity_delta < 0:
        liquidity_delta_bytes = (1 << 256) + liquidity_delta
    else:
        liquidity_delta_bytes = liquidity_delta
    data = (
        "0x"
        + format(
            (1 << 256) + tick_lower if tick_lower < 0 else tick_lower,
            "064x",
        )
        + format(
            (1 << 256) + tick_upper if tick_upper < 0 else tick_upper,
            "064x",
        )
        + format(liquidity_delta_bytes, "064x")
        + salt.lower().replace("0x", "").zfill(64)
    )
    return {
        "address": pool_manager,
        "topics": [
            EVENT_TOPICS["ModifyLiquidity"],
            pool_id,
            sender_padded,
        ],
        "data": data,
    }


def _build_erc721_mint_log(*, position_manager: str, wallet: str, token_id: int) -> dict:
    return {
        "address": position_manager,
        "topics": [
            EVENT_TOPICS["Transfer"],
            "0x" + "0" * 64,
            "0x" + wallet.lower().replace("0x", "").zfill(64),
            "0x" + format(token_id, "064x"),
        ],
        "data": "0x",
    }


def _build_erc20_transfer_log(*, token: str, from_addr: str, to_addr: str, amount: int) -> dict:
    return {
        "address": token,
        "topics": [
            EVENT_TOPICS["Transfer"],
            "0x" + from_addr.lower().replace("0x", "").zfill(64),
            "0x" + to_addr.lower().replace("0x", "").zfill(64),
        ],
        "data": _hex32(amount),
    }


def _single_sided_receipt(*, token: str, amount: int, tx_hash: str = "0xsinglesided") -> dict:
    """A V4 LP_OPEN receipt where exactly one currency is deposited."""
    salt = _hex32(TOKEN_ID)
    return {
        "transactionHash": tx_hash,
        "logs": [
            _build_modify_liquidity_log(
                pool_id=POOL_ID,
                sender=BASE_PM,
                tick_lower=TICK_LOWER,
                tick_upper=TICK_UPPER,
                liquidity_delta=LIQUIDITY_DELTA,
                salt=salt,
                pool_manager=BASE_POOL_MANAGER,
            ),
            _build_erc721_mint_log(
                position_manager=BASE_PM,
                wallet=WALLET,
                token_id=TOKEN_ID,
            ),
            _build_erc20_transfer_log(
                token=token,
                from_addr=WALLET,
                to_addr=BASE_POOL_MANAGER,
                amount=amount,
            ),
        ],
    }


def _two_sided_receipt(tx_hash: str = "0xtwosided") -> dict:
    """A canonical V4 LP_OPEN with both currencies deposited."""
    salt = _hex32(TOKEN_ID)
    return {
        "transactionHash": tx_hash,
        "logs": [
            _build_modify_liquidity_log(
                pool_id=POOL_ID,
                sender=BASE_PM,
                tick_lower=TICK_LOWER,
                tick_upper=TICK_UPPER,
                liquidity_delta=LIQUIDITY_DELTA,
                salt=salt,
                pool_manager=BASE_POOL_MANAGER,
            ),
            _build_erc721_mint_log(
                position_manager=BASE_PM,
                wallet=WALLET,
                token_id=TOKEN_ID,
            ),
            _build_erc20_transfer_log(
                token=USDC_BASE,
                from_addr=WALLET,
                to_addr=BASE_POOL_MANAGER,
                amount=USDC_AMOUNT,
            ),
            _build_erc20_transfer_log(
                token=WETH_BASE,
                from_addr=WALLET,
                to_addr=BASE_POOL_MANAGER,
                amount=WETH_AMOUNT,
            ),
        ],
    }


def _make_pool_key() -> PoolKey:
    """Canonical Base USDC/WETH PoolKey (WETH sorts first)."""
    pool_key = PoolKey(currency0=USDC_BASE, currency1=WETH_BASE, fee=500, tick_spacing=10)
    assert pool_key.currency0.lower() == WETH_BASE.lower()
    assert pool_key.currency1.lower() == USDC_BASE.lower()
    return pool_key


def _counter_value(chain: str, reason: V4LPDropReason, outcome: str) -> float:
    value = FRAMEWORK_REGISTRY.get_sample_value(
        "v4_lp_parser_drops_total",
        {"chain": chain, "reason": reason.value, "outcome": outcome},
    )
    return value if value is not None else 0.0


class TestTwoSidedDoesNotCallLookup:
    """Regression: the canonical two-sided mint MUST NOT call the lookup. The
    predicate is ``amount0 is not None and amount1 is None`` — both-non-None
    skips the new branch entirely."""

    def test_two_sided_lookup_not_called(self) -> None:
        call_count = {"n": 0}

        def _spy_lookup(pid: str, chain: str) -> PoolKey:
            call_count["n"] += 1
            return _make_pool_key()

        parser = UniswapV4ReceiptParser(chain=CHAIN, pool_key_lookup=_spy_lookup)
        data = parser.extract_lp_open_data(_two_sided_receipt())

        assert data is not None
        assert call_count["n"] == 0, "two-sided open must not invoke the gateway PoolKey lookup"
        assert data.amount0 == WETH_AMOUNT  # WETH sorts first
        assert data.amount1 == USDC_AMOUNT
        assert data.currency0 == WETH_BASE.lower()
        assert data.currency1 == USDC_BASE.lower()

    def test_two_sided_lookup_raise_still_succeeds(self) -> None:
        """Even an injected lookup that ALWAYS raises does not affect the
        two-sided path because the lookup is never invoked."""

        def _raises(pid: str, chain: str) -> PoolKey:
            raise RuntimeError("lookup must not be called on two-sided open")

        parser = UniswapV4ReceiptParser(chain=CHAIN, pool_key_lookup=_raises)
        data = parser.extract_lp_open_data(_two_sided_receipt())

        assert data is not None
        assert data.amount0 == WETH_AMOUNT
        assert data.amount1 == USDC_AMOUNT


class TestSingleSidedLookupSuccess:
    """When only one currency is observed AND the PoolKey lookup succeeds, the
    parser stamps the missing leg as measured zero (``Decimal("0")`` semantics;
    int field = 0) and populates both currency addresses from the PoolKey."""

    def test_single_sided_currency0_observed(self) -> None:
        """WETH (currency0 in PoolKey order) is the only currency observed."""
        receipt = _single_sided_receipt(token=WETH_BASE, amount=WETH_AMOUNT)
        parser = UniswapV4ReceiptParser(chain=CHAIN, pool_key_lookup=lambda pid, chain: _make_pool_key())

        data = parser.extract_lp_open_data(receipt)

        assert data is not None
        assert data.amount0 == WETH_AMOUNT
        assert data.amount1 == 0
        assert data.currency0 == WETH_BASE.lower()
        assert data.currency1 == USDC_BASE.lower()

    def test_single_sided_currency1_observed(self) -> None:
        """USDC (currency1 in PoolKey order) is the only currency observed."""
        receipt = _single_sided_receipt(token=USDC_BASE, amount=USDC_AMOUNT)
        parser = UniswapV4ReceiptParser(chain=CHAIN, pool_key_lookup=lambda pid, chain: _make_pool_key())

        data = parser.extract_lp_open_data(receipt)

        assert data is not None
        assert data.amount0 == 0
        assert data.amount1 == USDC_AMOUNT
        assert data.currency0 == WETH_BASE.lower()
        assert data.currency1 == USDC_BASE.lower()

    def test_measured_zero_is_int_zero_not_none(self) -> None:
        """The Empty != Zero invariant: 0 means measured zero, None means
        unmeasured. After lookup success the unobserved leg is 0, NOT None.
        """
        receipt = _single_sided_receipt(token=WETH_BASE, amount=WETH_AMOUNT)
        parser = UniswapV4ReceiptParser(chain=CHAIN, pool_key_lookup=lambda pid, chain: _make_pool_key())
        data = parser.extract_lp_open_data(receipt)
        assert data is not None
        assert data.amount1 is not None
        assert data.amount1 == 0

    def test_lookup_called_with_canonical_args(self) -> None:
        """The lookup is called with (pool_id_hex_lowercase, chain)."""
        captured: dict[str, str] = {}

        def _capturing_lookup(pid: str, chain: str) -> PoolKey:
            captured["pool_id"] = pid
            captured["chain"] = chain
            return _make_pool_key()

        parser = UniswapV4ReceiptParser(chain=CHAIN, pool_key_lookup=_capturing_lookup)
        receipt = _single_sided_receipt(token=WETH_BASE, amount=WETH_AMOUNT)
        parser.extract_lp_open_data(receipt)

        assert captured["pool_id"] == POOL_ID.lower()
        assert captured["chain"] == CHAIN


class TestSingleSidedLookupNotFound:
    def test_drops_with_warning_and_counter(self, caplog: pytest.LogCaptureFixture) -> None:
        reason = V4LPDropReason.POOL_KEY_NOT_FOUND
        before = _counter_value(CHAIN, reason, "drop")

        tx_hash = "0xsinglesided_notfound"
        receipt = _single_sided_receipt(token=USDC_BASE, amount=USDC_AMOUNT, tx_hash=tx_hash)
        parser = UniswapV4ReceiptParser(chain=CHAIN, pool_key_lookup=lambda pid, chain: None)

        with caplog.at_level(logging.WARNING, logger=PARSER_LOGGER):
            data = parser.extract_lp_open_data(receipt)

        assert data is None, "NOT_FOUND must drop rather than return ambiguous LPOpenData"
        joined = " ".join(rec.message for rec in caplog.records)
        assert "pool_key_not_found" in joined
        assert tx_hash in joined
        assert POOL_ID.lower() in joined.lower()
        assert _counter_value(CHAIN, reason, "drop") == before + 1.0


class TestSingleSidedLookupRaises:
    def test_drops_with_warning_and_counter(self, caplog: pytest.LogCaptureFixture) -> None:
        reason = V4LPDropReason.POOL_KEY_LOOKUP_ERROR
        before = _counter_value(CHAIN, reason, "drop")

        def _raises(pid: str, chain: str) -> PoolKey:
            raise RuntimeError("gateway transport unavailable")

        tx_hash = "0xsinglesided_unavailable"
        receipt = _single_sided_receipt(token=USDC_BASE, amount=USDC_AMOUNT, tx_hash=tx_hash)
        parser = UniswapV4ReceiptParser(chain=CHAIN, pool_key_lookup=_raises)

        with caplog.at_level(logging.WARNING, logger=PARSER_LOGGER):
            data = parser.extract_lp_open_data(receipt)

        assert data is None
        joined = " ".join(rec.message for rec in caplog.records)
        assert "pool_key_lookup_error" in joined
        assert tx_hash in joined
        assert _counter_value(CHAIN, reason, "drop") == before + 1.0


class TestSingleSidedMissingLookup:
    """Paper / dry_run / unit mode: no gateway means no lookup callable. The
    parser MUST drop fail-loud rather than emit ambiguous attribution."""

    def test_drops_with_warning_and_counter(self, caplog: pytest.LogCaptureFixture) -> None:
        reason = V4LPDropReason.MISSING_POOL_KEY_LOOKUP
        before = _counter_value(CHAIN, reason, "drop")

        tx_hash = "0xsinglesided_nolookup"
        receipt = _single_sided_receipt(token=WETH_BASE, amount=WETH_AMOUNT, tx_hash=tx_hash)
        parser = UniswapV4ReceiptParser(chain=CHAIN)  # NO pool_key_lookup

        with caplog.at_level(logging.WARNING, logger=PARSER_LOGGER):
            data = parser.extract_lp_open_data(receipt)

        assert data is None
        joined = " ".join(rec.message for rec in caplog.records)
        assert "missing_pool_key_lookup" in joined
        assert tx_hash in joined
        assert _counter_value(CHAIN, reason, "drop") == before + 1.0


class TestSingleSidedNativeCurrencyLeftUnmeasured:
    """A native deposit emits no Transfer, so its unobserved leg remains None."""

    def test_native_currency0_leaves_native_leg_none(self, caplog: pytest.LogCaptureFixture) -> None:
        from almanak.connectors.uniswap_v4.sdk import NATIVE_CURRENCY

        native_pool_key = PoolKey(currency0=NATIVE_CURRENCY, currency1=WETH_BASE, fee=3000, tick_spacing=60)

        tx_hash = "0xsinglesided_native"
        receipt = _single_sided_receipt(token=WETH_BASE, amount=WETH_AMOUNT, tx_hash=tx_hash)
        parser = UniswapV4ReceiptParser(chain=CHAIN, pool_key_lookup=lambda pid, chain: native_pool_key)

        with caplog.at_level(logging.WARNING, logger=PARSER_LOGGER):
            data = parser.extract_lp_open_data(receipt)

        assert data is not None, "native-ETH single-sided open must not be dropped"
        assert data.currency0 == NATIVE_CURRENCY
        assert data.currency1 == WETH_BASE
        assert data.amount0 is None, "native leg MUST be None (unmeasured), never a fabricated zero"
        assert data.amount1 == WETH_AMOUNT, "observed ERC-20 leg must be measured on its PoolKey leg"


class TestSingleSidedTransferSetMismatch:
    """A currency outside the PoolKey cannot be attributed and must be dropped."""

    def test_observed_currency_outside_pool_key_drops(self, caplog: pytest.LogCaptureFixture) -> None:
        reason = V4LPDropReason.TRANSFER_SET_MISMATCH
        before = _counter_value(CHAIN, reason, "drop")

        STRANGER = "0xc0ffee0000000000000000000000000000000000"
        tx_hash = "0xsinglesided_mismatch"
        receipt = _single_sided_receipt(token=STRANGER, amount=12345, tx_hash=tx_hash)
        parser = UniswapV4ReceiptParser(chain=CHAIN, pool_key_lookup=lambda pid, chain: _make_pool_key())

        with caplog.at_level(logging.WARNING, logger=PARSER_LOGGER):
            data = parser.extract_lp_open_data(receipt)

        assert data is None
        joined = " ".join(rec.message for rec in caplog.records)
        assert "transfer_set_mismatch" in joined
        assert _counter_value(CHAIN, reason, "drop") == before + 1.0
