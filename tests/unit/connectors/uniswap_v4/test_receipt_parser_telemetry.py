"""T11 (VIB-4480): V4 LP parser drop-path telemetry.

Every V4 LP receipt-parser drop path is required to:

1. Emit a structured WARNING containing ``pool_id``, ``tx_hash``,
   ``outcome``, ``reason``, ``chain`` (the operator contract).
2. Increment ``v4_lp_parser_drops_total{chain,reason,outcome}`` exactly once
   per drop event.

This file covers one test per drop reason in :class:`V4LPDropReason`. The
counter is read off ``FRAMEWORK_REGISTRY`` via ``get_sample_value`` and
diffed against a baseline snapshot taken inside the test so parallel test
workers do not race each other.
"""

from __future__ import annotations

import logging

import pytest

from almanak.framework.connectors.uniswap_v4.adapter import UniswapV4UnsupportedPoolError
from almanak.framework.connectors.uniswap_v4.receipt_parser import (
    EVENT_TOPICS,
    UniswapV4ReceiptParser,
)
from almanak.framework.connectors.uniswap_v4.sdk import (
    NATIVE_CURRENCY,
    POSITION_MANAGER_ADDRESS_SET,
    PoolKey,
    _pad_int24,
    _pad_uint,
)
from almanak.framework.observability.metrics import (
    FRAMEWORK_REGISTRY,
    V4_LP_PARSER_DROPS_TOTAL,
    V4LPDropReason,
)

PARSER_LOGGER = "almanak.framework.connectors.uniswap_v4.receipt_parser"

CHAIN = "arbitrum"
POOL_MANAGER = "0x000000000004444c5dc75cB358380D2e3dE08A90"
POSITION_MANAGER = next(iter(POSITION_MANAGER_ADDRESS_SET))
WALLET = "0x1234567890abcdef1234567890abcdef12345678"
USDC = "0xaf88d065e77c8cc2239327c5edb3a432268e5831"
WETH = "0x82af49447d8a07e3bd95bd0d56f35241523fbab1"
POOL_ID_HEX = "0x" + "de" * 32


# =============================================================================
# Counter helpers
# =============================================================================


def _counter_value(chain: str, reason: V4LPDropReason, outcome: str) -> float:
    """Return the current counter sample for (chain, reason, outcome), or 0.0."""
    value = FRAMEWORK_REGISTRY.get_sample_value(
        "v4_lp_parser_drops_total",
        {"chain": chain, "reason": reason.value, "outcome": outcome},
    )
    return value if value is not None else 0.0


# =============================================================================
# Receipt builders
# =============================================================================


def _modify_liquidity_log(
    *,
    sender: str,
    liquidity_delta: int,
    salt: str = "0x" + "00" * 32,
    pool_id: str = POOL_ID_HEX,
    pool_manager: str = POOL_MANAGER,
) -> dict:
    if liquidity_delta < 0:
        ld_bytes = (1 << 256) + liquidity_delta
    else:
        ld_bytes = liquidity_delta
    data_hex = (
        "0x"
        + _pad_int24(-60000)
        + _pad_int24(60000)
        + _pad_uint(ld_bytes)
        + salt.lower().replace("0x", "").zfill(64)
    )
    return {
        "address": pool_manager,
        "topics": [
            EVENT_TOPICS["ModifyLiquidity"],
            pool_id,
            "0x" + "00" * 12 + sender.lower().replace("0x", ""),
        ],
        "data": data_hex,
    }


def _transfer_log(*, token: str, from_addr: str, to_addr: str, amount: int) -> dict:
    return {
        "address": token,
        "topics": [
            EVENT_TOPICS["Transfer"],
            "0x" + "00" * 12 + from_addr.lower().replace("0x", ""),
            "0x" + "00" * 12 + to_addr.lower().replace("0x", ""),
        ],
        "data": "0x" + _pad_uint(amount),
    }


def _erc721_mint_log(*, position_manager: str, wallet: str, token_id: int) -> dict:
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


def _make_parser(pool_key_lookup=None) -> UniswapV4ReceiptParser:
    return UniswapV4ReceiptParser(
        chain=CHAIN,
        pool_manager_address=POOL_MANAGER,
        position_manager_address=POSITION_MANAGER,
        pool_key_lookup=pool_key_lookup,
    )


def _assert_warning_contract(caplog: pytest.LogCaptureFixture, reason: V4LPDropReason, *, outcome: str, tx: str) -> None:
    """Every drop WARNING must carry the canonical telemetry fields."""
    warnings = [rec.message for rec in caplog.records if rec.levelno >= logging.WARNING]
    assert warnings, "expected at least one WARNING log record"
    joined = " ".join(warnings)
    assert f"reason={reason.value}" in joined
    assert f"outcome={outcome}" in joined
    assert f"chain={CHAIN}" in joined
    assert f"tx={tx}" in joined
    assert "pool_id=" in joined


# =============================================================================
# extract_lp_open_data drop paths (T05)
# =============================================================================


def test_non_position_manager_sender_emits_warning_and_counter(caplog: pytest.LogCaptureFixture):
    reason = V4LPDropReason.NON_POSITION_MANAGER_SENDER
    before = _counter_value(CHAIN, reason, "drop")
    parser = _make_parser()
    tx = "0xnonpm"
    receipt = {
        "transactionHash": tx,
        "logs": [
            _modify_liquidity_log(
                sender="0x000000000000000000000000000000000000beef",  # NOT in allowlist
                liquidity_delta=10**15,
                salt="0x" + format(7, "064x"),
            ),
        ],
    }
    with caplog.at_level(logging.WARNING, logger=PARSER_LOGGER):
        result = parser.extract_lp_open_data(receipt)
    assert result is None
    _assert_warning_contract(caplog, reason, outcome="drop", tx=tx)
    assert _counter_value(CHAIN, reason, "drop") == before + 1.0


def test_missing_position_id_emits_warning_and_counter(caplog: pytest.LogCaptureFixture):
    """ModifyLiquidity mint with allowlisted sender but no ERC-721 mint Transfer."""
    reason = V4LPDropReason.MISSING_POSITION_ID
    before = _counter_value(CHAIN, reason, "drop")
    parser = _make_parser()
    tx = "0xnoposid"
    # Allowlisted sender, no ERC-721 mint Transfer at all
    receipt = {
        "transactionHash": tx,
        "logs": [
            _modify_liquidity_log(
                sender=POSITION_MANAGER,
                liquidity_delta=10**15,
                salt="0x" + format(7, "064x"),
            ),
        ],
    }
    with caplog.at_level(logging.WARNING, logger=PARSER_LOGGER):
        result = parser.extract_lp_open_data(receipt)
    assert result is None
    _assert_warning_contract(caplog, reason, outcome="drop", tx=tx)
    assert _counter_value(CHAIN, reason, "drop") == before + 1.0


def test_salt_tokenid_mismatch_emits_warning_and_counter(caplog: pytest.LogCaptureFixture):
    reason = V4LPDropReason.SALT_TOKENID_MISMATCH
    before = _counter_value(CHAIN, reason, "drop")
    parser = _make_parser()
    tx = "0xsaltmismatch"
    token_id = 4242
    # salt deliberately != bytes32(tokenId)
    wrong_salt = "0x" + format(9999, "064x")
    receipt = {
        "transactionHash": tx,
        "logs": [
            _modify_liquidity_log(
                sender=POSITION_MANAGER,
                liquidity_delta=10**15,
                salt=wrong_salt,
            ),
            _erc721_mint_log(position_manager=POSITION_MANAGER, wallet=WALLET, token_id=token_id),
        ],
    }
    with caplog.at_level(logging.WARNING, logger=PARSER_LOGGER):
        result = parser.extract_lp_open_data(receipt)
    assert result is None
    _assert_warning_contract(caplog, reason, outcome="drop", tx=tx)
    assert _counter_value(CHAIN, reason, "drop") == before + 1.0


# =============================================================================
# extract_lp_close_data drop paths (T07)
# =============================================================================


def _burn_log() -> dict:
    return _modify_liquidity_log(sender=POSITION_MANAGER, liquidity_delta=-500_000)


def test_missing_pool_key_lookup_emits_warning_and_counter(caplog: pytest.LogCaptureFixture):
    reason = V4LPDropReason.MISSING_POOL_KEY_LOOKUP
    before = _counter_value(CHAIN, reason, "drop")
    parser = _make_parser(pool_key_lookup=None)  # explicitly missing
    tx = "0xnolookup"
    receipt = {"transactionHash": tx, "logs": [_burn_log()]}
    with caplog.at_level(logging.WARNING, logger=PARSER_LOGGER):
        result = parser.extract_lp_close_data(receipt)
    assert result is None
    _assert_warning_contract(caplog, reason, outcome="drop", tx=tx)
    assert _counter_value(CHAIN, reason, "drop") == before + 1.0


def test_pool_key_not_found_emits_warning_and_counter(caplog: pytest.LogCaptureFixture):
    reason = V4LPDropReason.POOL_KEY_NOT_FOUND
    before = _counter_value(CHAIN, reason, "drop")
    parser = _make_parser(pool_key_lookup=lambda pid, chain: None)
    tx = "0xnotfound"
    receipt = {"transactionHash": tx, "logs": [_burn_log()]}
    with caplog.at_level(logging.WARNING, logger=PARSER_LOGGER):
        result = parser.extract_lp_close_data(receipt)
    assert result is None
    _assert_warning_contract(caplog, reason, outcome="drop", tx=tx)
    assert _counter_value(CHAIN, reason, "drop") == before + 1.0


def test_pool_key_lookup_error_emits_warning_and_counter(caplog: pytest.LogCaptureFixture):
    reason = V4LPDropReason.POOL_KEY_LOOKUP_ERROR
    before = _counter_value(CHAIN, reason, "drop")

    def _raises(pid: str, chain: str):
        raise RuntimeError("gateway transport blew up")

    parser = _make_parser(pool_key_lookup=_raises)
    tx = "0xlookupraises"
    receipt = {"transactionHash": tx, "logs": [_burn_log()]}
    with caplog.at_level(logging.WARNING, logger=PARSER_LOGGER):
        result = parser.extract_lp_close_data(receipt)
    assert result is None
    _assert_warning_contract(caplog, reason, outcome="drop", tx=tx)
    assert _counter_value(CHAIN, reason, "drop") == before + 1.0


def test_native_currency_unsupported_emits_warning_and_counter(caplog: pytest.LogCaptureFixture):
    """Native-ETH currency0 raises UniswapV4UnsupportedPoolError; counter increments BEFORE the raise."""
    reason = V4LPDropReason.NATIVE_CURRENCY_UNSUPPORTED
    before = _counter_value(CHAIN, reason, "raise")
    native_pool_key = PoolKey(currency0=NATIVE_CURRENCY, currency1=WETH, fee=3000, tick_spacing=60)
    parser = _make_parser(pool_key_lookup=lambda pid, chain: native_pool_key)
    tx = "0xnative"
    receipt = {"transactionHash": tx, "logs": [_burn_log()]}
    with caplog.at_level(logging.WARNING, logger=PARSER_LOGGER):
        with pytest.raises(UniswapV4UnsupportedPoolError):
            parser.extract_lp_close_data(receipt)
    _assert_warning_contract(caplog, reason, outcome="raise", tx=tx)
    assert _counter_value(CHAIN, reason, "raise") == before + 1.0


def test_transfer_set_mismatch_emits_warning_and_counter(caplog: pytest.LogCaptureFixture):
    """VIB-4426 P1 #3 — `transfer_set_mismatch` now only fires when the
    observed token is OUTSIDE the PoolKey currency set (i.e. a true
    mis-attribution: parser saw a transfer for some unrelated token).
    A single-sided close where the observed token IS a PoolKey currency
    is legitimate (out-of-range CL position) and returns a measured-zero
    on the missing leg.
    """
    from tests.unit.connectors.uniswap_v4.test_receipt_parser_telemetry import _make_parser as _mp  # noqa: F401

    reason = V4LPDropReason.TRANSFER_SET_MISMATCH
    before = _counter_value(CHAIN, reason, "drop")
    pool_key = PoolKey(currency0=USDC, currency1=WETH, fee=500, tick_spacing=10)
    parser = _make_parser(pool_key_lookup=lambda pid, chain: pool_key)
    tx = "0xtransfermismatch"
    # Transfer an UNRELATED token from PoolManager — not in {currency0, currency1}.
    unrelated = "0x" + "ee" * 20
    receipt = {
        "transactionHash": tx,
        "logs": [
            _burn_log(),
            _transfer_log(token=unrelated, from_addr=POOL_MANAGER, to_addr=WALLET, amount=999),
        ],
    }
    with caplog.at_level(logging.WARNING, logger=PARSER_LOGGER):
        result = parser.extract_lp_close_data(receipt)
    assert result is None, "out-of-PoolKey-set token observation IS a real mismatch"
    _assert_warning_contract(caplog, reason, outcome="drop", tx=tx)
    assert _counter_value(CHAIN, reason, "drop") == before + 1.0


# =============================================================================
# Counter shape sanity (catches accidental label drift)
# =============================================================================


def test_counter_has_canonical_label_set():
    """Hard-code the label set so changing it requires updating both the
    counter declaration AND this test — protects dashboards from silent
    relabelling."""
    metric = V4_LP_PARSER_DROPS_TOTAL._metrics  # type: ignore[attr-defined]
    # Trigger an increment so at least one labeled child exists.
    V4_LP_PARSER_DROPS_TOTAL.labels(chain="__test__", reason="non_position_manager_sender", outcome="drop").inc(0)
    assert any(("__test__", "non_position_manager_sender", "drop") == key for key in metric)
