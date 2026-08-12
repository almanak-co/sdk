"""VIB-3876 — anvil_funding config schema validation.

Pins the contract that ``_normalize_anvil_funding`` accepts well-formed flat
or per-chain address funding dicts and rejects malformed values with a warning
+ safe fallback. Without this, a user-authored config with malformed
``anvil_funding`` (list, string, int, dict-of-bools) would propagate to
``ManagedGateway._anvil_funding`` and crash mid-startup inside
``_fund_anvil_wallets()`` on ``.items()``.

Both ``cli/teardown.py`` (post VIB-3819) and ``cli/run_helpers.py`` consume
this — the helper lives in ``run_helpers.py`` so both call sites import the
same normalization.
"""

from __future__ import annotations

import logging

from almanak.framework.cli.run_helpers import _normalize_anvil_funding

WETH = "0x82af49447d8a07e3bd95bd0d56f35241523fbab1"
USDC = "0xaf88d065e77c8cc2239327c5edb3a432268e5831"
WSTETH = "0x5979d7b546e38e414f7e9822514be443a4800529"
WBTC = "0x2f2a2543b76a4166549f7aab2e75bef0aefc5b0f"
DAI = "0xda10009cbd5d07dd0cecc66161fc93d7c9000da1"


def test_well_formed_dict_passes_through() -> None:
    """Canonical case: ERC-20 addresses plus the native gas exception."""
    raw = {WETH: 1, USDC: 1000, "ETH": 0.5}
    assert _normalize_anvil_funding(raw) == raw


def test_string_amount_preserved() -> None:
    """String amounts allowed for high-precision Decimal values (e.g. wstETH)."""
    raw = {WSTETH: "1.234567890123456789"}
    assert _normalize_anvil_funding(raw) == raw


def test_per_chain_address_sections_pass_through() -> None:
    raw = {"arbitrum": {USDC: 1000}, "base": {"0x833589fcd6edb6e08f4c7c32d4f71b54bda02913": 500}}
    assert _normalize_anvil_funding(raw) == raw


def test_empty_dict_returns_empty_silently(caplog) -> None:
    """Empty dict is a valid no-op — no warning. Pin the silence so a future
    regression that starts logging on ``{}`` / ``None`` is caught (CodeRabbit
    P_minor: tests/unit assertions must be specific)."""
    with caplog.at_level(logging.WARNING, logger="almanak.framework.cli.run_helpers"):
        assert _normalize_anvil_funding({}) == {}
        assert _normalize_anvil_funding(None) == {}
    assert not caplog.records, f"Expected silent path, got logs: {[r.message for r in caplog.records]}"


def test_list_value_rejected_with_warning(caplog) -> None:
    """``anvil_funding: [WETH, USDC]`` (list, not dict) → empty + warning."""
    with caplog.at_level(logging.WARNING, logger="almanak.framework.cli.run_helpers"):
        result = _normalize_anvil_funding(["WETH", "USDC"])
    assert result == {}
    assert any("malformed anvil_funding" in r.message for r in caplog.records), (
        f"Expected warning about malformed anvil_funding, got: {[r.message for r in caplog.records]}"
    )


def test_string_value_rejected_with_warning(caplog) -> None:
    """``anvil_funding: WETH`` (string, not dict) → empty + warning."""
    with caplog.at_level(logging.WARNING, logger="almanak.framework.cli.run_helpers"):
        result = _normalize_anvil_funding("WETH")
    assert result == {}
    assert any("malformed anvil_funding" in r.message for r in caplog.records)


def test_int_value_rejected_with_warning(caplog) -> None:
    """``anvil_funding: 1`` (scalar int, not dict) → empty + warning."""
    with caplog.at_level(logging.WARNING, logger="almanak.framework.cli.run_helpers"):
        result = _normalize_anvil_funding(1)
    assert result == {}
    assert any("malformed anvil_funding" in r.message for r in caplog.records)


def test_non_string_keys_dropped_with_warning(caplog) -> None:
    """Non-string token symbols dropped (string keys preserved)."""
    raw = {WETH: 1, 42: 100, USDC: 1000}
    with caplog.at_level(logging.WARNING, logger="almanak.framework.cli.run_helpers"):
        result = _normalize_anvil_funding(raw)
    assert result == {WETH: 1, USDC: 1000}
    assert any("non-string key" in r.message for r in caplog.records)


def test_bool_values_dropped(caplog) -> None:
    """``True``/``False`` dropped — bool is a subclass of int, but a True
    bool was almost certainly a config typo and silently treating it as 1
    would fund the wallet with 1 token unit (wrong + confusing)."""
    raw = {WETH: True, USDC: 1000}
    with caplog.at_level(logging.WARNING, logger="almanak.framework.cli.run_helpers"):
        result = _normalize_anvil_funding(raw)
    assert result == {USDC: 1000}
    assert any(WETH in r.message and "bool" in r.message for r in caplog.records)


def test_nested_dict_value_dropped(caplog) -> None:
    """A nested dict in place of a numeric amount is dropped + warning."""
    raw = {WETH: {"amount": [1], "chain": ["base"]}, USDC: 1000}
    with caplog.at_level(logging.WARNING, logger="almanak.framework.cli.run_helpers"):
        result = _normalize_anvil_funding(raw)
    assert result == {USDC: 1000}
    assert any(WETH in r.message for r in caplog.records)


def test_bad_nested_entry_does_not_discard_valid_chain_siblings(caplog) -> None:
    """A malformed token entry drops locally, not the whole per-chain section."""
    raw = {"arbitrum": {USDC: 1000, "bad": {"amount": 1}, WETH: 2}}
    with caplog.at_level(logging.WARNING, logger="almanak.framework.cli.run_helpers"):
        result = _normalize_anvil_funding(raw)
    assert result == {"arbitrum": {USDC: 1000, WETH: 2}}
    assert any("anvil_funding[arbitrum][bad]" in r.message for r in caplog.records)


def test_list_value_in_dict_dropped(caplog) -> None:
    """A list as token amount is dropped + warning."""
    raw = {WETH: [1, 2, 3], USDC: 1000}
    with caplog.at_level(logging.WARNING, logger="almanak.framework.cli.run_helpers"):
        result = _normalize_anvil_funding(raw)
    assert result == {USDC: 1000}
    assert any(WETH in r.message for r in caplog.records)


def test_mixed_valid_and_invalid_keeps_valid(caplog) -> None:
    """Partial-malformed dict → only the valid entries pass through, rest dropped."""
    raw = {
        WETH: 1,  # valid int
        USDC: "1000",  # valid str
        "ETH": 0.5,  # valid float
        WBTC: True,  # bool (rejected)
        DAI: [1, 2],  # list (rejected)
        42: 100,  # non-string key (rejected)
    }
    with caplog.at_level(logging.WARNING, logger="almanak.framework.cli.run_helpers"):
        result = _normalize_anvil_funding(raw)
    assert result == {WETH: 1, USDC: "1000", "ETH": 0.5}
