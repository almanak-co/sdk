"""Unit tests for ``cli/_solana_setup.get_orca_pool_accounts`` (VIB-3878).

Pins the contract that this helper:

- Returns an early empty list for non-Orca strategies and missing pool addresses.
- Returns ``[]`` (never raises) on malformed numeric config — fork startup
  must not be aborted by a typo in ``tick_spacing`` / ``range_pct``.
- Returns ``[]`` (never raises) when the Orca API is unreachable / 4xx / 5xx.
- On a valid 200 response, includes the vault token accounts AND derives at
  least one tick-array PDA so the local fork has the LP-range coverage it
  needs before the close-side intent runs.

The helper runs in the CLI process before the gateway exists, so the direct
HTTP call is the documented exception to the gateway-boundary rule. Mocking
``requests.get`` and ``solders.pubkey`` keeps the test fully offline.
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

from almanak.framework.cli._solana_setup import get_orca_pool_accounts


def test_non_orca_protocol_returns_empty() -> None:
    """``protocol != "orca_whirlpools"`` short-circuits without an HTTP call."""
    assert get_orca_pool_accounts({"protocol": "raydium_clmm", "pool_address": "x"}) == []
    assert get_orca_pool_accounts({}) == []


def test_missing_pool_address_returns_empty() -> None:
    """Even an Orca strategy with no pool_address returns empty (not None)."""
    assert get_orca_pool_accounts({"protocol": "orca_whirlpools"}) == []
    assert get_orca_pool_accounts({"protocol": "orca_whirlpools", "pool_address": ""}) == []


def test_malformed_tick_spacing_returns_empty(capsys) -> None:
    """A non-numeric ``tick_spacing`` must NOT raise — fork startup depends on
    this helper's "return [] on any error" contract."""
    result = get_orca_pool_accounts(
        {"protocol": "orca_whirlpools", "pool_address": "x" * 44, "tick_spacing": "bogus"}
    )
    assert result == []


def test_malformed_range_pct_returns_empty() -> None:
    """Same guard for ``range_pct`` — never crash fork startup on a config typo."""
    result = get_orca_pool_accounts(
        {"protocol": "orca_whirlpools", "pool_address": "x" * 44, "range_pct": "twenty"}
    )
    assert result == []


def test_api_404_returns_empty() -> None:
    """Non-200 Orca API response yields ``[]`` and a warning, never raises."""
    fake_resp = MagicMock(status_code=404)
    with patch("requests.get", return_value=fake_resp):
        result = get_orca_pool_accounts(
            {"protocol": "orca_whirlpools", "pool_address": "x" * 44}
        )
    assert result == []


def test_api_exception_returns_empty() -> None:
    """Network-level exception (timeout, DNS, etc.) is caught and returns ``[]``."""
    with patch("requests.get", side_effect=ConnectionError("boom")):
        result = get_orca_pool_accounts(
            {"protocol": "orca_whirlpools", "pool_address": "x" * 44}
        )
    assert result == []


def test_valid_response_includes_vaults_and_tick_arrays() -> None:
    """Successful 200 response → vault accounts + at least one tick-array PDA.

    A valid pool address (Orca SOL/USDC Whirlpool) is required because
    ``Pubkey.from_string`` validates base58-encoded ed25519 keys.
    """
    valid_pool = "HJPjoWUrhoZzkNfRpHuieeFk9WcZWjwy6PBjZ81ngndJ"
    valid_vault_a = "BVNo8ftg2LkkssnWT4ZWdtoFaevnfD6ExYeramwM27pe"
    valid_vault_b = "5KeVQQwKXCSEZWfvFdsCGYL5iSJpZxqd9hDsjTqxUjZf"
    fake_resp = MagicMock(
        status_code=200,
        json=lambda: {
            "data": {
                "tickCurrentIndex": 0,
                "tickSpacing": 64,
                "tokenVaultA": valid_vault_a,
                "tokenVaultB": valid_vault_b,
            }
        },
    )
    with patch("requests.get", return_value=fake_resp):
        result = get_orca_pool_accounts(
            {"protocol": "orca_whirlpools", "pool_address": valid_pool, "range_pct": 10}
        )
    assert valid_vault_a in result
    assert valid_vault_b in result
    # PDAs are 44-char base58 strings; we should have multiple beyond the 2 vaults.
    assert len(result) > 2, f"Expected vaults + tick-array PDAs, got {result}"


def test_orca_api_base_url_env_override(monkeypatch) -> None:
    """``ORCA_API_BASE_URL`` env var should override the default Orca endpoint."""
    captured_url: dict[str, str] = {}

    def fake_get(url: str, **_kwargs):
        captured_url["url"] = url
        m = MagicMock(status_code=404)
        return m

    monkeypatch.setenv("ORCA_API_BASE_URL", "https://orca-mock.test/v9")
    with patch("requests.get", side_effect=fake_get):
        get_orca_pool_accounts({"protocol": "orca_whirlpools", "pool_address": "x" * 44})

    assert captured_url["url"].startswith("https://orca-mock.test/v9/pools/")
