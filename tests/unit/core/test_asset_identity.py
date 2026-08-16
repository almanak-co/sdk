"""Writer-safe asset identity contract tests (VIB-6675)."""

from __future__ import annotations

from dataclasses import FrozenInstanceError

import pytest

from almanak.core.asset_identity import (
    ASSET_IDENTITY_SCHEMA_VERSION,
    AssetIdentity,
    AssetNamespace,
    KnownDecimals,
    NativeIdentityUnavailable,
    NativeIdentityUnavailableReason,
    UnmeasuredDecimals,
    UnmeasuredDecimalsReason,
    parse_caip19,
    resolve_native_asset_identity,
)

USDC_ARBITRUM = "0xAf88D065E77C8cC2239327C5EDb3A432268e5831"
USDC_SOLANA = "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v"


def test_evm_identity_normalizes_chain_alias_and_address_case() -> None:
    identity = AssetIdentity("arb", AssetNamespace.ERC20, USDC_ARBITRUM)

    assert identity.identity_key == (
        "arbitrum",
        AssetNamespace.ERC20,
        USDC_ARBITRUM.lower(),
    )
    assert identity.caip19 == f"eip155:42161/erc20:{USDC_ARBITRUM.lower()}"


def test_solana_identity_preserves_case_sensitive_mint() -> None:
    identity = AssetIdentity("solana", AssetNamespace.TOKEN, USDC_SOLANA)

    assert identity.asset_reference == USDC_SOLANA
    assert identity.caip19 == f"solana:5eykt4UsFv8P8NJdTREpY1vzqKqZKvdp/token:{USDC_SOLANA}"


@pytest.mark.parametrize("reference", ["z" * 44, "2" * 32])
def test_solana_identity_rejects_base58_values_that_are_not_32_bytes(reference: str) -> None:
    with pytest.raises(ValueError, match="decode to exactly 32 bytes"):
        AssetIdentity("solana", AssetNamespace.TOKEN, reference)


@pytest.mark.parametrize(
    ("chain", "namespace", "reference", "message"),
    [
        ("ethereum", AssetNamespace.ERC20, "0x1234", "40 hexadecimal"),
        ("ethereum", AssetNamespace.TOKEN, USDC_SOLANA, "requires namespace 'erc20'"),
        ("solana", AssetNamespace.ERC20, USDC_ARBITRUM, "requires namespace 'token'"),
        ("solana", AssetNamespace.TOKEN, "not-a-mint", "base58 mint"),
    ],
)
def test_identity_rejects_wrong_family_or_malformed_reference(
    chain: str,
    namespace: AssetNamespace,
    reference: str,
    message: str,
) -> None:
    with pytest.raises(ValueError, match=message):
        AssetIdentity(chain, namespace, reference)


def test_identity_rejects_raw_namespace_even_when_value_matches() -> None:
    with pytest.raises(TypeError, match="must be an AssetNamespace"):
        AssetIdentity("ethereum", "erc20", USDC_ARBITRUM)  # type: ignore[arg-type]


def test_native_identity_uses_registered_slip44_and_never_guesses() -> None:
    assert AssetIdentity.native("arbitrum") == AssetIdentity(
        "arbitrum",
        AssetNamespace.NATIVE,
        "60",
    )
    assert AssetIdentity.native("solana").caip19.endswith("/slip44:501")

    with pytest.raises(ValueError, match="must be SLIP-44 '60'"):
        AssetIdentity("arbitrum", AssetNamespace.NATIVE, "501")
    with pytest.raises(ValueError, match="no SLIP-44 coin type"):
        AssetIdentity.native("mantle")


@pytest.mark.parametrize("chain", ["xlayer", "0g", "plasma", "mantle"])
def test_missing_slip44_returns_typed_native_identity_refusal(chain: str) -> None:
    result = resolve_native_asset_identity(chain)

    assert result == NativeIdentityUnavailable(
        chain=chain,
        reason_code=NativeIdentityUnavailableReason.MISSING_SLIP44,
    )
    assert not isinstance(result, AssetIdentity)
    assert not hasattr(result, "to_wire")


def test_native_identity_refusal_cannot_be_fabricated_for_a_registered_chain() -> None:
    with pytest.raises(ValueError, match="has a registered SLIP-44"):
        NativeIdentityUnavailable("arbitrum", NativeIdentityUnavailableReason.MISSING_SLIP44)
    with pytest.raises(TypeError, match="must be a NativeIdentityUnavailableReason"):
        NativeIdentityUnavailable("mantle", "missing_slip44")  # type: ignore[arg-type]


def test_caip19_and_wire_forms_round_trip_to_the_same_identity() -> None:
    caip19 = f"eip155:42161/erc20:{USDC_ARBITRUM}"
    identity = AssetIdentity.from_caip19(caip19)
    wire = identity.to_wire()

    assert wire == {
        "schemaVersion": ASSET_IDENTITY_SCHEMA_VERSION,
        "chain": "arbitrum",
        "assetNamespace": "erc20",
        "assetReference": USDC_ARBITRUM.lower(),
    }
    assert AssetIdentity.from_wire(wire) == identity
    assert AssetIdentity.from_caip19(identity.caip19) == identity
    assert AssetIdentity.from_json(identity.to_json()) == identity
    assert identity.to_json() == (
        f'{{"assetNamespace":"erc20","assetReference":"{USDC_ARBITRUM.lower()}","chain":"arbitrum","schemaVersion":1}}'
    )


@pytest.mark.parametrize(
    "wire",
    [
        {"schemaVersion": True, "chain": "ethereum", "assetNamespace": "erc20", "assetReference": USDC_ARBITRUM},
        {"schemaVersion": 2, "chain": "ethereum", "assetNamespace": "erc20", "assetReference": USDC_ARBITRUM},
        {
            "schemaVersion": 1,
            "chain": "ethereum",
            "assetNamespace": "erc20",
            "assetReference": USDC_ARBITRUM,
            "symbol": "USDC",
        },
    ],
)
def test_wire_loader_rejects_version_or_shape_drift(wire: dict[str, object]) -> None:
    with pytest.raises(ValueError):
        AssetIdentity.from_wire(wire)


def test_caip_parser_preserves_legacy_trim_and_identity_rejects_unknown_namespaces() -> None:
    parsed = parse_caip19(f" eip155:1/erc20:{USDC_ARBITRUM} ")
    assert parsed.caip2 == "eip155:1"
    with pytest.raises(ValueError, match="Unsupported fungible asset namespace"):
        AssetIdentity.from_caip19(f"eip155:1/erc721:{USDC_ARBITRUM}")


def test_identity_is_immutable_and_has_no_display_or_amount_metadata() -> None:
    identity = AssetIdentity("arbitrum", AssetNamespace.ERC20, USDC_ARBITRUM)

    assert not hasattr(identity, "symbol")
    assert not hasattr(identity, "decimals")
    with pytest.raises(FrozenInstanceError):
        identity.chain = "ethereum"  # type: ignore[misc]


def test_decimals_are_a_closed_measured_or_unmeasured_union() -> None:
    assert KnownDecimals(6).value == 6
    assert UnmeasuredDecimals(UnmeasuredDecimalsReason.NOT_RESOLVED).reason_code is (
        UnmeasuredDecimalsReason.NOT_RESOLVED
    )
    assert not hasattr(UnmeasuredDecimals(UnmeasuredDecimalsReason.PROVIDER_UNAVAILABLE), "value")

    for invalid in (True, -1, 78, 6.0):
        with pytest.raises(ValueError, match="integer from 0 through 77"):
            KnownDecimals(invalid)  # type: ignore[arg-type]
    with pytest.raises(TypeError, match="must be an UnmeasuredDecimalsReason"):
        UnmeasuredDecimals("not_resolved")  # type: ignore[arg-type]


def test_identity_module_has_no_framework_gateway_or_connector_imports() -> None:
    source = __import__("inspect").getsource(__import__("almanak.core.asset_identity", fromlist=["*"]))

    assert "almanak.framework" not in source
    assert "almanak.gateway" not in source
    assert "almanak.connectors" not in source
