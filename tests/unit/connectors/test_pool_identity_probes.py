"""Connector identity probes for resolve_pool_address (ALM-3368).

Scripted-transport tests. The anti-spoof property under test: a probe claims
an address only when its own factory/registry acknowledges it — matching the
ABI shape alone never yields ``verified``.
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from almanak.connectors._strategy_base.pool_identity_base import (
    FEE_SELECTOR,
    STABLE_SELECTOR,
    TICK_SPACING_SELECTOR,
    TOKEN0_SELECTOR,
    TOKEN1_SELECTOR,
    identify_clamm_pool,
    identify_erc20,
)
from almanak.connectors._strategy_base.pool_reader import PoolDiscriminatorKind, PoolReaderSpec
from almanak.connectors._strategy_base.v3_pool_abi import encode_get_pool

WETH = "0x4200000000000000000000000000000000000006"
USDC = "0x833589fcd6edb6e08f4c7c32d4f71b54bda02913"
POOL = "0x0b1c2dcbbfa744ebd3fc17ff1a96a1e1eb4b2d69"
FACTORY = "0x33128a8fc17869897dce68ed026d694621f6fdfd"
SECOND_FACTORY = "0x1111111111111111111111111111111111111111"


def _word(value: int) -> bytes:
    return value.to_bytes(32, "big")


def _addr(address: str) -> bytes:
    return bytes(12) + bytes.fromhex(address.removeprefix("0x"))


def _spec(**overrides) -> PoolReaderSpec:
    defaults = {
        "protocol": "uniswap_v3",
        "factory_addresses": {"base": FACTORY},
        "reader_kind": "v3_slot0",
    }
    defaults.update(overrides)
    return PoolReaderSpec(**defaults)


def _patch_calls(script: dict[tuple[str, str], bytes]):
    def fake_eth_call(rpc_url, to, data, timeout=10.0, *, chain=None, gateway_client=None, **kw):
        return script.get((to.lower(), data))

    return patch("almanak.connectors._strategy_base.pool_identity_base.eth_call", side_effect=fake_eth_call)


def _v3_pool_script(acknowledged: str | None = POOL) -> dict:
    script = {
        (POOL, TOKEN0_SELECTOR): _addr(WETH),
        (POOL, TOKEN1_SELECTOR): _addr(USDC),
        (POOL, FEE_SELECTOR): _word(10000),
        (POOL, TICK_SPACING_SELECTOR): _word(200),
    }
    if acknowledged is not None:
        script[(FACTORY, encode_get_pool("0x1698ee82", WETH, USDC, 10000))] = _addr(acknowledged)
    return script


def test_clamm_probe_verified_when_factory_acknowledges():
    spec = _spec(get_pool_selector="0x1698ee82")
    with _patch_calls(_v3_pool_script()):
        payload = identify_clamm_pool(spec, "base", POOL)
    assert payload is not None
    assert payload["protocol"] == "uniswap_v3"
    assert payload["fee_tier"] == 10000
    assert payload["factory_verified"] == "verified"


def test_clamm_probe_abstains_without_factories():
    spec = _spec(factory_addresses={})
    with _patch_calls({}) as eth_call:
        assert identify_clamm_pool(spec, "base", POOL) is None
    eth_call.assert_not_called()


def test_clamm_probe_checks_every_factory_generation_until_one_acknowledges():
    selector = "0x1698ee82"
    spec = _spec(
        factory_addresses={},
        factory_generations={"base": (FACTORY, SECOND_FACTORY)},
        get_pool_selector=selector,
    )
    get_pool_data = encode_get_pool(selector, WETH, USDC, 10000)
    script = _v3_pool_script(acknowledged="0x" + "99" * 20)
    script[(SECOND_FACTORY, get_pool_data)] = _addr(POOL)

    with _patch_calls(script) as eth_call:
        payload = identify_clamm_pool(spec, "base", POOL)

    factory_calls = [call.args[1].lower() for call in eth_call.call_args_list if call.args[2] == get_pool_data]
    assert factory_calls == [FACTORY, SECOND_FACTORY]
    assert payload is not None
    assert payload["factory"] == SECOND_FACTORY
    assert payload["factory_verified"] == "verified"


def test_clamm_probe_abstains_when_factory_disowns():
    spec = _spec(get_pool_selector="0x1698ee82")
    with _patch_calls(_v3_pool_script(acknowledged="0x" + "99" * 20)):
        assert identify_clamm_pool(spec, "base", POOL) is None


def test_clamm_probe_abstains_on_solidly_shape():
    spec = _spec(get_pool_selector="0x1698ee82")
    script = _v3_pool_script() | {(POOL, STABLE_SELECTOR): _word(0)}
    with _patch_calls(script):
        assert identify_clamm_pool(spec, "base", POOL) is None


def test_clamm_probe_uses_tick_spacing_for_slipstream_family():
    spec = _spec(
        protocol="aerodrome_slipstream",
        get_pool_selector="0x28af8d0b",
        discriminator_kind=PoolDiscriminatorKind.TICK_SPACING,
    )
    script = _v3_pool_script(acknowledged=None)
    script[(FACTORY, encode_get_pool("0x28af8d0b", WETH, USDC, 200))] = _addr(POOL)
    with _patch_calls(script):
        payload = identify_clamm_pool(spec, "base", POOL)
    assert payload is not None
    assert payload["protocol"] == "aerodrome_slipstream"
    assert payload["factory_verified"] == "verified"


def test_erc20_fallback_classifies_non_pool():
    from almanak.connectors._strategy_base.pool_identity_base import (
        DECIMALS_SELECTOR,
        TOTAL_SUPPLY_SELECTOR,
    )

    script = {
        (WETH, DECIMALS_SELECTOR): _word(18),
        (WETH, TOTAL_SUPPLY_SELECTOR): _word(10**24),
    }
    with _patch_calls(script):
        payload = identify_erc20("base", WETH)
    assert payload is not None
    assert payload["kind"] == "erc20"
    assert payload["decimals"] == 18
    assert "pool_address" not in payload
    assert any(f"uv run almanak ax -c base dex-pools {WETH}" in note for note in payload["notes"])
    assert any("then verify the exact pool before execution" in note for note in payload["notes"])


def test_curve_probe_wraps_meta_registry_resolution():
    from almanak.connectors.curve.pool_identity import identify_pool_payload

    meta = MagicMock(
        address="0x11c1fbd4b3de66bc0565779b35171a6cf3e71f59",
        lp_token="0x98244d93d42b42ab3e3a4d12a5dc0b3e7f8f32f9",
        coin_addresses=[WETH, "0x2ae3f1ec7f1f5012cfeab0185bfc7aa3cf0dec22"],
        coin_symbols=["WETH", "CBETH"],
        coin_decimals=[18, 18],
        pool_type="cryptoswap",
        is_metapool=False,
    )
    with patch("almanak.connectors.curve.pool_resolver.resolve_pool_metadata", return_value=meta):
        payload = identify_pool_payload(MagicMock(protocol="curve"), "base", meta.address)
    assert payload is not None
    assert payload["factory_verified"] == "verified"
    assert payload["identified_via"] == "meta_registry"
    assert payload["lp_token"] == meta.lp_token
    assert any("separate contracts" in n for n in payload["notes"])

    # Definitive None (registry confirmed non-membership) → honest abstain;
    # non-definitive None (transport blip) → raise so the executor does not
    # let the ERC-20 fallback misreport a Curve pool as a plain token.
    with patch("almanak.connectors.curve.pool_resolver.resolve_pool_metadata", return_value=None):
        with patch("almanak.connectors.curve.pool_resolver.resolution_is_definitive", return_value=True):
            assert identify_pool_payload(MagicMock(protocol="curve"), "base", meta.address) is None
        with patch("almanak.connectors.curve.pool_resolver.resolution_is_definitive", return_value=False):
            with pytest.raises(RuntimeError, match="indeterminate"):
                identify_pool_payload(MagicMock(protocol="curve"), "base", meta.address)


def test_solidly_probe_requires_factory_acknowledgment():
    from almanak.connectors._strategy_base.pool_validation_base import PoolValidationReason, PoolValidationResult
    from almanak.connectors.aerodrome.pool_identity import identify_pool_payload

    aero_pool = "0x6cdcb1c4a4d1c3c6d054b27ac5b77e89eafb971d"
    script = {
        (aero_pool, TOKEN0_SELECTOR): _addr(USDC),
        (aero_pool, TOKEN1_SELECTOR): _addr(WETH),
        (aero_pool, STABLE_SELECTOR): _word(0),
    }
    confirmed = PoolValidationResult(exists=True, reason=PoolValidationReason.CONFIRMED, pool_address=aero_pool)
    disowned = PoolValidationResult(exists=True, reason=PoolValidationReason.CONFIRMED, pool_address="0x" + "88" * 20)
    with (
        _patch_calls(script),
        patch("almanak.connectors.aerodrome.pool_identity.validate_aerodrome_pool", return_value=confirmed),
    ):
        payload = identify_pool_payload(MagicMock(protocol="aerodrome"), "base", aero_pool)
    assert payload is not None
    assert payload["family"] == "solidly"
    assert payload["stable"] is False
    assert payload["factory_verified"] == "verified"

    with (
        _patch_calls(script),
        patch("almanak.connectors.aerodrome.pool_identity.validate_aerodrome_pool", return_value=disowned),
    ):
        assert identify_pool_payload(MagicMock(protocol="aerodrome"), "base", aero_pool) is None


def test_v4_probe_state_view_classification():
    from almanak.connectors._strategy_base.v4_pool_abi import encode_get_slot0
    from almanak.connectors.uniswap_v4.pool_identity import identify_pool_payload

    state_view = "0x" + "aa" * 20
    pool_id = "0x" + "e0" * 32
    spec = MagicMock(protocol="uniswap_v4", factory_addresses={"base": state_view})

    def _slot0_response(sqrt_price: int, tick: int) -> bytes:
        return _word(sqrt_price) + _word(tick) + _word(0) + _word(0)

    def _patch_v4(response: bytes | None):
        def fake_eth_call(rpc_url, to, data, timeout=10.0, *, chain=None, gateway_client=None, **kw):
            assert to == state_view
            assert data == encode_get_slot0(pool_id)
            return response

        return patch("almanak.connectors._strategy_base.pool_validation_base.eth_call", side_effect=fake_eth_call)

    # Initialized pool id: nonzero sqrtPriceX96 → verified, live tick.
    with _patch_v4(_slot0_response(2**96, 100)):
        payload = identify_pool_payload(spec, "base", pool_id)
    assert payload is not None
    assert payload["kind"] == "pool_id"
    assert payload["factory_verified"] == "verified"
    assert payload["tick"] == 100

    # Uninitialized: StateView returns zeroes → claimed as pool_id shape, mismatch.
    with _patch_v4(_slot0_response(0, 0)):
        payload = identify_pool_payload(spec, "base", pool_id)
    assert payload is not None
    assert payload["factory_verified"] == "mismatch"
    assert "tick" not in payload

    # Unreadable transport (eth_call yields no data) → raises for the
    # executor's probe-fault accounting; never a silent abstain.
    with _patch_v4(None):
        with pytest.raises(Exception, match="no data"):
            identify_pool_payload(spec, "base", pool_id)

    # 40-hex contract addresses and chains without a StateView abstain
    # without any read.
    assert identify_pool_payload(spec, "base", "0x" + "11" * 20) is None
    assert identify_pool_payload(spec, "mantle", pool_id) is None


# ERC-4626 vault probe: a Morpho vault share token must not be reported
# as "ERC-20 only, not usable".

VAULT = "0x" + "be" * 20
UNDERLYING = "0x" + "0c" * 20


def _string_word(text: str) -> bytes:
    raw = text.encode()
    return (32).to_bytes(32, "big") + len(raw).to_bytes(32, "big") + raw.ljust(32, b"\x00")


def _erc4626_script(*, generation: str | None) -> dict:
    from almanak.connectors._strategy_base.pool_identity_base import (
        DECIMALS_SELECTOR,
        ERC4626_ASSET_SELECTOR,
        ERC4626_TOTAL_ASSETS_SELECTOR,
        METAMORPHO_V1_WITHDRAW_QUEUE_LENGTH_SELECTOR,
        MORPHO_VAULT_V2_ADAPTERS_LENGTH_SELECTOR,
        SYMBOL_SELECTOR,
    )

    script = {
        (VAULT, ERC4626_ASSET_SELECTOR): _addr(UNDERLYING),
        (VAULT, ERC4626_TOTAL_ASSETS_SELECTOR): _word(429_847_842_881_293),
        (VAULT, SYMBOL_SELECTOR): _string_word("steakUSDC"),
        (VAULT, DECIMALS_SELECTOR): _word(18),
        (UNDERLYING, SYMBOL_SELECTOR): _string_word("USDC"),
        (UNDERLYING, DECIMALS_SELECTOR): _word(6),
    }
    if generation == "v1":
        script[(VAULT, METAMORPHO_V1_WITHDRAW_QUEUE_LENGTH_SELECTOR)] = _word(5)
    elif generation == "v2":
        script[(VAULT, MORPHO_VAULT_V2_ADAPTERS_LENGTH_SELECTOR)] = _word(1)
    return script


def test_erc4626_probe_classifies_metamorpho_v1_vault():
    from almanak.connectors._strategy_base.pool_identity_base import identify_erc4626_vault

    with _patch_calls(_erc4626_script(generation="v1")):
        payload = identify_erc4626_vault("base", VAULT)
    assert payload is not None
    assert payload["kind"] == "erc4626_vault"
    assert payload["family"] == "erc4626"
    assert payload["protocol"] == "metamorpho"
    assert payload["vault_version"] == "v1"
    assert payload["symbol"] == "steakUSDC"
    assert payload["underlying_asset"] == UNDERLYING
    assert payload["underlying_symbol"] == "USDC"
    assert payload["underlying_decimals"] == 6
    assert payload["total_assets"] == 429_847_842_881_293
    assert payload["lp_token"] == VAULT
    notes = " ".join(payload["notes"])
    assert "vault_deposit" in notes
    assert "market_id" in notes  # tells the agent NOT to look for one
    assert "MetaMorpho v1" in notes


def test_erc4626_probe_reports_morpho_vault_v2_supported_with_liquidity_caveat():
    from almanak.connectors._strategy_base.pool_identity_base import identify_erc4626_vault

    with _patch_calls(_erc4626_script(generation="v2")):
        payload = identify_erc4626_vault("base", VAULT)
    assert payload is not None
    assert payload["protocol"] == "metamorpho"
    assert payload["vault_version"] == "v2"
    notes = " ".join(payload["notes"])
    assert "Morpho Vault V2" in notes
    assert "supported by the morpho_vault connector" in notes
    assert "balanceOf" in notes  # V2 redeem sizing, since maxRedeem is 0 by design
    assert "VaultIlliquidError" in notes  # liquidity is not guaranteed; fails closed
    assert 'Intent.vault_deposit(protocol="metamorpho"' in notes  # V2 is executable on this branch


def test_erc4626_probe_reports_generic_vault_without_morpho_fingerprint():
    from almanak.connectors._strategy_base.pool_identity_base import identify_erc4626_vault

    with _patch_calls(_erc4626_script(generation=None)):
        payload = identify_erc4626_vault("base", VAULT)
    assert payload is not None
    assert payload["kind"] == "erc4626_vault"
    assert payload["protocol"] is None
    assert payload["vault_version"] is None
    # No Morpho fingerprint ⇒ no connector is named for the deposit intent (a
    # foreign ERC-4626 vault must not be routed to the MetaMorpho executor).
    notes = " ".join(payload["notes"])
    assert 'protocol="metamorpho"' not in notes
    assert "which vault connector owns it is not established" in notes


def test_erc4626_probe_abstains_on_plain_erc20():
    from almanak.connectors._strategy_base.pool_identity_base import (
        DECIMALS_SELECTOR,
        TOTAL_SUPPLY_SELECTOR,
        identify_erc4626_vault,
    )

    script = {
        (WETH, DECIMALS_SELECTOR): _word(18),
        (WETH, TOTAL_SUPPLY_SELECTOR): _word(10**24),
    }
    with _patch_calls(script):
        assert identify_erc4626_vault("base", WETH) is None


def test_erc4626_probe_abstains_when_asset_answers_but_total_assets_does_not():
    from almanak.connectors._strategy_base.pool_identity_base import (
        ERC4626_ASSET_SELECTOR,
        identify_erc4626_vault,
    )

    script = {(VAULT, ERC4626_ASSET_SELECTOR): _addr(UNDERLYING)}
    with _patch_calls(script):
        assert identify_erc4626_vault("base", VAULT) is None


def test_erc4626_probe_abstains_when_mandatory_read_reverts():
    from almanak.connectors._strategy_base.pool_identity_base import (
        ERC4626_ASSET_SELECTOR,
        identify_erc4626_vault,
        probe_call,
    )

    def fake_eth_call(rpc_url, to, data, timeout=10.0, *, chain=None, gateway_client=None, **kw):
        if data == ERC4626_ASSET_SELECTOR:
            raise ValueError("Gateway eth_call error for vault: execution reverted")
        return None

    with patch("almanak.connectors._strategy_base.pool_identity_base.eth_call", side_effect=fake_eth_call):
        assert identify_erc4626_vault("base", VAULT) is None
        assert probe_call("base", VAULT, ERC4626_ASSET_SELECTOR) is None


def test_erc4626_probe_propagates_transport_failure_on_mandatory_read():
    from almanak.connectors._strategy_base.pool_identity_base import (
        ERC4626_ASSET_SELECTOR,
        ERC4626_TOTAL_ASSETS_SELECTOR,
        identify_erc4626_vault,
        probe_call,
    )

    def fake_eth_call(rpc_url, to, data, timeout=10.0, *, chain=None, gateway_client=None, **kw):
        if data == ERC4626_ASSET_SELECTOR:
            return _addr(UNDERLYING)
        if data == ERC4626_TOTAL_ASSETS_SELECTOR:
            raise ValueError("Gateway eth_call transport error for vault: UNAVAILABLE timeout")
        return None

    with patch("almanak.connectors._strategy_base.pool_identity_base.eth_call", side_effect=fake_eth_call):
        with pytest.raises(ValueError, match="transport"):
            identify_erc4626_vault("base", VAULT)
        with pytest.raises(ValueError, match="transport"):
            probe_call("base", VAULT, ERC4626_TOTAL_ASSETS_SELECTOR)


def test_erc4626_probe_abstains_on_malformed_gateway_payload_not_transport():
    """A fake/broken gateway answer is not a downed RPC — fall through to ERC-20."""
    from almanak.connectors._strategy_base.pool_identity_base import (
        ERC4626_ASSET_SELECTOR,
        identify_erc4626_vault,
        probe_call,
    )

    def fake_eth_call(rpc_url, to, data, timeout=10.0, *, chain=None, gateway_client=None, **kw):
        raise ValueError(f"Gateway eth_call failed for {to} on {chain}: fromhex() argument must be str, not MagicMock")

    with patch("almanak.connectors._strategy_base.pool_identity_base.eth_call", side_effect=fake_eth_call):
        assert identify_erc4626_vault("base", VAULT) is None
        assert probe_call("base", VAULT, ERC4626_ASSET_SELECTOR) is None


_HTTP_TRANSPORT_MARKERS = (
    "too many requests",
    "502 bad gateway",
    "504 gateway timeout",
    "connection aborted",
    "server disconnected",
    "cannot connect to host",
    "max retries exceeded",
)


@pytest.mark.parametrize("marker", _HTTP_TRANSPORT_MARKERS)
def test_looks_like_transport_classifies_http_gateway_failures(marker):
    from almanak.connectors._strategy_base.rpc import looks_like_transport

    assert looks_like_transport(f"Gateway eth_call failed: {marker}")
    assert looks_like_transport(f"Gateway eth_call failed: {marker.upper()}")


@pytest.mark.parametrize("marker", _HTTP_TRANSPORT_MARKERS)
def test_probe_call_raises_on_http_gateway_transport_failures(marker):
    from almanak.connectors._strategy_base.pool_identity_base import (
        ERC4626_ASSET_SELECTOR,
        probe_call,
    )

    def fake_eth_call(rpc_url, to, data, timeout=10.0, *, chain=None, gateway_client=None, **kw):
        raise ValueError(f"Gateway eth_call failed: {marker.upper()}")

    with patch("almanak.connectors._strategy_base.pool_identity_base.eth_call", side_effect=fake_eth_call):
        with pytest.raises(ValueError, match="Gateway eth_call failed"):
            probe_call("base", VAULT, ERC4626_ASSET_SELECTOR)
