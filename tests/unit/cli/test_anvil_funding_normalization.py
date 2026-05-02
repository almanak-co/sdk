"""VIB-3876 — anvil_funding config schema validation.

Pins the contract that ``_normalize_anvil_funding`` accepts a well-formed
``{token_symbol: amount}`` dict and rejects everything else with a warning
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


def test_well_formed_dict_passes_through() -> None:
    """Canonical case: dict of token symbol → numeric amount."""
    raw = {"WETH": 1, "USDC": 1000, "ETH": 0.5}
    assert _normalize_anvil_funding(raw) == raw


def test_string_amount_preserved() -> None:
    """String amounts allowed for high-precision Decimal values (e.g. wstETH)."""
    raw = {"wstETH": "1.234567890123456789"}
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
    raw = {"WETH": 1, 42: 100, "USDC": 1000}
    with caplog.at_level(logging.WARNING, logger="almanak.framework.cli.run_helpers"):
        result = _normalize_anvil_funding(raw)
    assert result == {"WETH": 1, "USDC": 1000}
    assert any("non-string key" in r.message for r in caplog.records)


def test_bool_values_dropped(caplog) -> None:
    """``True``/``False`` dropped — bool is a subclass of int, but a True
    bool was almost certainly a config typo and silently treating it as 1
    would fund the wallet with 1 token unit (wrong + confusing)."""
    raw = {"WETH": True, "USDC": 1000}
    with caplog.at_level(logging.WARNING, logger="almanak.framework.cli.run_helpers"):
        result = _normalize_anvil_funding(raw)
    assert result == {"USDC": 1000}
    assert any("WETH" in r.message and "bool" in r.message for r in caplog.records)


def test_nested_dict_value_dropped(caplog) -> None:
    """A nested dict in place of a numeric amount is dropped + warning."""
    raw = {"WETH": {"amount": 1, "chain": "base"}, "USDC": 1000}
    with caplog.at_level(logging.WARNING, logger="almanak.framework.cli.run_helpers"):
        result = _normalize_anvil_funding(raw)
    assert result == {"USDC": 1000}
    assert any("WETH" in r.message for r in caplog.records)


def test_list_value_in_dict_dropped(caplog) -> None:
    """A list as token amount is dropped + warning."""
    raw = {"WETH": [1, 2, 3], "USDC": 1000}
    with caplog.at_level(logging.WARNING, logger="almanak.framework.cli.run_helpers"):
        result = _normalize_anvil_funding(raw)
    assert result == {"USDC": 1000}
    assert any("WETH" in r.message for r in caplog.records)


def test_mixed_valid_and_invalid_keeps_valid(caplog) -> None:
    """Partial-malformed dict → only the valid entries pass through, rest dropped."""
    raw = {
        "WETH": 1,  # valid int
        "USDC": "1000",  # valid str
        "ETH": 0.5,  # valid float
        "WBTC": True,  # bool (rejected)
        "DAI": [1, 2],  # list (rejected)
        42: 100,  # non-string key (rejected)
        "WSTETH": {"a": 1},  # dict (rejected)
    }
    with caplog.at_level(logging.WARNING, logger="almanak.framework.cli.run_helpers"):
        result = _normalize_anvil_funding(raw)
    assert result == {"WETH": 1, "USDC": "1000", "ETH": 0.5}
