"""VIB-5573 WI-1: the generic ERC-4626 vault teardown post-condition + its
registration as a framework default for every ``ProtocolKind.VAULT`` connector.

Covers the closure rule (asset-denominated exact-0 — handles the share-dust a
clean ``redeem(maxRedeem)`` leaves behind), the three-valued outcome
(closed / residual / unmeasured), the bounded read-retry, and that the default
resolves for every vault slug (the register-by-name / lookup-by-protocol fix).
"""

from __future__ import annotations

from types import SimpleNamespace

import pytest

from almanak.connectors._strategy_base import vault_post_condition as vpc
from almanak.connectors._strategy_base.teardown_post_condition import get_teardown_post_condition
from almanak.connectors._strategy_base.vault_post_condition import (
    erc4626_vault_teardown_post_condition as vault_hook,
)

_VAULT = "0xc1256Ae5FF1cf2719D4937adb3bbCCab2E00A2Ca"
_OWNER = "0x8a8678A18e60c7eDbacC6FCBd5A08D0A6ECb21e8"


def _uint256_hex(value: int) -> str:
    return "0x" + f"{value:064x}"


class _FakeGateway:
    """Minimal gateway_client double: scripted balanceOf + convertToAssets.

    ``balances`` / ``assets`` may each be a single value or a list consumed in
    call order (to script a transient None → value read-retry). A value of
    ``"raise"`` makes that call raise.
    """

    def __init__(self, balances, assets):
        self._balances = list(balances) if isinstance(balances, list) else [balances]
        self._assets = list(assets) if isinstance(assets, list) else [assets]
        self.balance_calls = 0
        self.eth_calls = 0

    def query_erc20_balance(self, *, chain, token_address, wallet_address, block=None):
        v = self._balances[min(self.balance_calls, len(self._balances) - 1)]
        self.balance_calls += 1
        if v == "raise":
            raise RuntimeError("gateway balanceOf blip")
        return v

    def eth_call(self, *, chain, to, data, block=None):
        v = self._assets[min(self.eth_calls, len(self._assets) - 1)]
        self.eth_calls += 1
        if v == "raise":
            raise RuntimeError("gateway eth_call blip")
        # ``assets`` entries are ints (asset value) or None; encode ints to hex.
        return _uint256_hex(v) if isinstance(v, int) else v


def _position(vault=_VAULT, position_id="metamorpho_base_yield", chain="base", details=None):
    return SimpleNamespace(
        protocol="metamorpho",
        position_id=position_id,
        chain=chain,
        details={"vault_address": vault} if details is None else details,
    )


@pytest.fixture(autouse=True)
def _no_backoff(monkeypatch):
    """Zero the read-retry backoff so retry tests don't sleep."""
    monkeypatch.setattr(vpc, "_READ_BACKOFF_S", 0.0)


def test_zero_shares_is_closed():
    gw = _FakeGateway(balances=0, assets=999)
    r = vault_hook(_position(), _OWNER, gateway_client=gw)
    assert r.closed is True
    assert r.unmeasured is False
    assert gw.eth_calls == 0  # short-circuits: no convertToAssets needed


def test_share_dust_worth_zero_assets_is_closed():
    # A clean redeem(maxRedeem) leaves a few wei of shares worth 0 assets.
    gw = _FakeGateway(balances=871898627444, assets=0)
    r = vault_hook(_position(), _OWNER, gateway_client=gw)
    assert r.closed is True
    assert r.unmeasured is False


def test_erc4626_roundtrip_rounding_leftover_is_closed():
    # The E2E real-fork case: redeem(maxRedeem) left 926083973261 shares worth
    # exactly 1 wei of assets (the ERC-4626 round-trip rounding floor). Must be
    # CLOSED, not a residual — else VIB-5572 latches entry on every clean
    # vault teardown.
    gw = _FakeGateway(balances=926083973261, assets=1)
    r = vault_hook(_position(), _OWNER, gateway_client=gw)
    assert r.closed is True
    assert r.unmeasured is False


def test_dust_floor_boundary():
    from almanak.connectors._strategy_base.vault_post_condition import _VAULT_ASSET_DUST_WEI

    # At the floor → closed; one wei above → measured residual (FAILED).
    at_floor = vault_hook(_position(), _OWNER, gateway_client=_FakeGateway(1_000, _VAULT_ASSET_DUST_WEI))
    assert at_floor.closed is True
    above = vault_hook(_position(), _OWNER, gateway_client=_FakeGateway(1_000, _VAULT_ASSET_DUST_WEI + 1))
    assert above.closed is False
    assert above.unmeasured is False
    assert above.residual["assets"] == _VAULT_ASSET_DUST_WEI + 1


def test_positive_asset_residual_is_measured_open():
    gw = _FakeGateway(balances=2963669482273198015, assets=3200145)
    r = vault_hook(_position(), _OWNER, gateway_client=gw)
    assert r.closed is False
    assert r.unmeasured is False  # MEASURED residual → FAILED, not unmeasured
    assert r.residual["assets"] == 3200145
    assert r.residual["shares"] == 2963669482273198015
    assert r.residual["vault_address"] == _VAULT


def test_missing_gateway_client_is_unmeasured():
    r = vault_hook(_position(), _OWNER, gateway_client=None)
    assert r.closed is False
    assert r.unmeasured is True


def test_missing_chain_is_unmeasured():
    pos = _position()
    pos.chain = ""
    r = vault_hook(pos, _OWNER, gateway_client=_FakeGateway(0, 0))
    assert r.unmeasured is True


def test_missing_vault_address_is_unmeasured():
    pos = _position(details={})  # no vault_address, position_id is not an address
    r = vault_hook(pos, _OWNER, gateway_client=_FakeGateway(0, 0))
    assert r.unmeasured is True


def test_balance_read_fault_is_unmeasured_not_residual():
    # balanceOf returns None on every attempt → cannot measure → unmeasured,
    # NEVER a fabricated residual (Empty ≠ Zero).
    gw = _FakeGateway(balances=None, assets=0)
    r = vault_hook(_position(), _OWNER, gateway_client=gw)
    assert r.unmeasured is True
    assert r.closed is False


def test_convert_to_assets_fault_is_unmeasured():
    # Shares present but convertToAssets unreadable (fault or non-ERC4626 vault)
    # → unmeasured, not a fabricated residual.
    gw = _FakeGateway(balances=123, assets=None)
    r = vault_hook(_position(), _OWNER, gateway_client=gw)
    assert r.unmeasured is True
    assert r.closed is False


def test_malformed_abi_return_is_unmeasured_not_fabricated_residual():
    # A short / revert-shaped eth_call return (not a full 32-byte ABI word) must
    # NOT be parsed as a positive residual (which would fabricate a FAILED). It is
    # a read fault → unmeasured (VIB-5573, CodeRabbit/Gemini).
    gw = _FakeGateway(balances=123, assets="0xdeadbeef")  # 8 hex chars, not 64
    r = vault_hook(_position(), _OWNER, gateway_client=gw)
    assert r.unmeasured is True
    assert r.closed is False


def test_invalid_vault_address_is_unmeasured():
    # A malformed vault address is rejected BEFORE any on-chain read → unmeasured
    # (no wasted retries on doomed gateway calls) (VIB-5573, Gemini).
    pos = _position(details={"vault_address": "not-a-real-address"})
    r = vault_hook(pos, _OWNER, gateway_client=_FakeGateway(0, 0))
    assert r.unmeasured is True
    assert r.closed is False


def test_balance_read_retry_recovers_from_transient_none():
    # First balanceOf blips (None), retry returns the real value → measured.
    gw = _FakeGateway(balances=[None, 2963669482273198015], assets=3200145)
    r = vault_hook(_position(), _OWNER, gateway_client=gw)
    assert r.unmeasured is False
    assert r.closed is False
    assert r.residual["assets"] == 3200145
    assert gw.balance_calls == 2  # proves the retry happened


def test_vault_address_from_position_id_fallback():
    # No details vault_address, but position_id is an address → used as vault.
    pos = _position(position_id=_VAULT, details={})
    gw = _FakeGateway(balances=0, assets=0)
    r = vault_hook(pos, _OWNER, gateway_client=gw)
    assert r.closed is True
    assert r.unmeasured is False


@pytest.mark.parametrize("slug", ["metamorpho", "morpho_vault", "beefy", "yearn", "lagoon"])
def test_default_registered_for_every_vault_slug(slug):
    # The register-by-name / lookup-by-protocol fix: every vault connector slug
    # (incl. metamorpho, which is NOT the connector folder name morpho_vault)
    # resolves to the ERC-4626 default.
    import almanak.framework.teardown.post_conditions  # noqa: F401  (triggers registration)

    assert get_teardown_post_condition(slug) is vault_hook


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
