"""Branch-complete tests for runtime wallet, chain, and protocol normalization."""

from __future__ import annotations

from types import SimpleNamespace

import pytest

from almanak.config.runtime import (
    CHAIN_IDS,
    ConfigurationError,
    _derive_wallet_address,
    _normalise_multi_chains,
    _normalise_multi_protocols,
)
from almanak.core.chains import ChainRegistry
from almanak.framework.execution.config import SUPPORTED_PROTOCOLS, LocalRuntimeConfig, MultiChainRuntimeConfig

_EVM_PRIVATE_KEY = "0xac0974bec39a17e36ba4a6b4d238ff944bacb478cbed5efcae784d7bf4f2ff80"
_EVM_WALLET = "0xf39Fd6e51aad88F6F4ce6aB8827279cffFb92266"
_SOLANA_BASE58_KEY = "1GMkH3brNXiNNs1tiFZHu4yZSRrzJwxi5wB9bHFtMikjwpAW9DMZzU2Pqakc5it8X3N5vPmqdN7KF4CCUpmKhq"
_SOLANA_HEX_SEED = "000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f"
_SOLANA_WALLET = "FAe4sisG95oZ42w7buUn5qEE4TAnfTTFPiguZUHmhiF"
_ZODIAC_EOA = "0x70997970C51812dc3A010C7d01b50e0d17dc79C8"
_VALID_CHAINS = ", ".join(sorted(CHAIN_IDS))
_VALID_PROTOCOLS = ", ".join(sorted(SUPPORTED_PROTOCOLS))
_MULTI_SOLANA_REASON = (
    "Solana is not supported in the multi-chain lane "
    "(``_build_multi_chain`` is EVM-only). Use the single-chain "
    "lane (``chain='solana'``) instead, or open a ticket if "
    "Solana belongs in a multi-chain mix you're building."
)


def _zodiac_signer() -> SimpleNamespace:
    return SimpleNamespace(mode="zodiac", eoa_address=_ZODIAC_EOA)


@pytest.mark.parametrize(
    ("chain", "private_key", "safe_signer", "gateway_wallets_configured", "expected"),
    [
        pytest.param("arbitrum", _EVM_PRIVATE_KEY, None, False, _EVM_WALLET, id="evm-prefixed-key"),
        pytest.param("arbitrum", _EVM_PRIVATE_KEY[2:], None, False, _EVM_WALLET, id="evm-unprefixed-key"),
        pytest.param("", _EVM_PRIVATE_KEY, None, False, _EVM_WALLET, id="multi-chain-evm-key"),
        pytest.param("arbitrum", _EVM_PRIVATE_KEY, _zodiac_signer(), False, _EVM_WALLET, id="key-precedes-signer"),
        pytest.param("arbitrum", "", _zodiac_signer(), False, _ZODIAC_EOA, id="remote-zodiac-signer"),
        pytest.param("arbitrum", "", _zodiac_signer(), True, _ZODIAC_EOA, id="zodiac-precedes-gateway"),
        pytest.param("arbitrum", "", None, True, "", id="gateway-wallet-placeholder"),
        pytest.param("solana", _SOLANA_BASE58_KEY, None, False, _SOLANA_WALLET, id="solana-base58-keypair"),
        pytest.param("solana", _SOLANA_HEX_SEED, None, False, _SOLANA_WALLET, id="solana-legacy-hex-seed"),
    ],
)
def test_derive_wallet_address_table(
    chain: str,
    private_key: str,
    safe_signer: object | None,
    gateway_wallets_configured: bool,
    expected: str,
) -> None:
    assert (
        _derive_wallet_address(
            chain=chain,
            private_key=private_key,
            safe_signer=safe_signer,
            gateway_wallets_configured=gateway_wallets_configured,
        )
        == expected
    )


@pytest.mark.parametrize(
    ("chain", "private_key", "safe_signer", "reason"),
    [
        pytest.param("arbitrum", "", None, "Private key cannot be empty", id="missing-key"),
        pytest.param(
            "arbitrum",
            "",
            SimpleNamespace(mode="direct", eoa_address=_ZODIAC_EOA),
            "Private key cannot be empty",
            id="direct-safe-still-needs-key",
        ),
        pytest.param(
            "arbitrum",
            "0x1234",
            None,
            "Private key must be 32 bytes (64 hex characters)",
            id="short-evm-key",
        ),
        pytest.param(
            "arbitrum",
            "0X" + _EVM_PRIVATE_KEY[2:],
            None,
            "Private key must be 32 bytes (64 hex characters)",
            id="uppercase-prefix-is-not-legacy-format",
        ),
        pytest.param(
            "arbitrum",
            "0x" + "xy" * 32,
            None,
            "Invalid private key format",
            id="non-hex-evm-key",
        ),
        pytest.param(
            "arbitrum",
            "0x" + "00" * 32,
            None,
            "Invalid private key format",
            id="invalid-zero-scalar",
        ),
        pytest.param(
            "arbitrum",
            "0xfffffffffffffffffffffffffffffffebaaedce6af48a03bbfd25e8cd0364141",
            None,
            "Invalid private key format",
            id="invalid-curve-order-scalar",
        ),
        pytest.param(
            "arbitrum",
            "0x" + "ff" * 32,
            None,
            "Invalid private key format",
            id="invalid-overflow-scalar",
        ),
        pytest.param(
            "solana",
            "not-a-solana-key",
            None,
            "Invalid Solana private key (expected base58 Ed25519 keypair)",
            id="invalid-solana-key",
        ),
    ],
)
def test_derive_wallet_address_exact_errors_do_not_expose_keys(
    chain: str,
    private_key: str,
    safe_signer: object | None,
    reason: str,
) -> None:
    with pytest.raises(ConfigurationError) as exc_info:
        _derive_wallet_address(
            chain=chain,
            private_key=private_key,
            safe_signer=safe_signer,
            gateway_wallets_configured=False,
        )

    assert exc_info.value.field == "private_key"
    assert exc_info.value.reason == reason
    assert str(exc_info.value) == f"Configuration error for 'private_key': {reason}"
    if private_key:
        assert private_key not in str(exc_info.value)


@pytest.mark.parametrize("config_type", [LocalRuntimeConfig, MultiChainRuntimeConfig])
def test_direct_runtime_dataclasses_reject_zero_private_key(config_type: type) -> None:
    kwargs = {
        "private_key": "0x" + "00" * 32,
    }
    if config_type is LocalRuntimeConfig:
        kwargs.update(chain="arbitrum", rpc_url="https://arb1.arbitrum.io/rpc")
    else:
        kwargs.update(chains=["arbitrum"], protocols={"arbitrum": ["uniswap_v3"]})

    with pytest.raises(ConfigurationError) as exc_info:
        config_type(**kwargs)

    assert str(exc_info.value) == "Configuration error for 'private_key': Invalid private key format"


_CANONICAL_CHAIN_CASES = [
    pytest.param([chain.upper()], [chain], id=f"canonical-{chain}") for chain in CHAIN_IDS if chain != "solana"
]
_ALIAS_CHAIN_CASES = [
    pytest.param([alias], [canonical], id=f"alias-{alias}")
    for alias, canonical in ChainRegistry.aliases().items()
    if alias != canonical and canonical != "solana"
]


@pytest.mark.parametrize(
    ("chains", "expected"),
    [
        *_CANONICAL_CHAIN_CASES,
        *_ALIAS_CHAIN_CASES,
        pytest.param(
            [" base ", "eip155:42161", "EIP155:10"],
            ["base", "arbitrum", "optimism"],
            id="whitespace-caip-and-order",
        ),
    ],
)
def test_normalise_multi_chains_table_preserves_order(chains: list[str], expected: list[str]) -> None:
    original = list(chains)

    normalised, chain_ids = _normalise_multi_chains(chains)

    assert normalised == expected
    assert list(chain_ids) == expected
    assert chain_ids == {chain: CHAIN_IDS[chain] for chain in expected}
    assert chains == original


@pytest.mark.parametrize(
    ("chains", "reason"),
    [
        pytest.param([], "At least one chain must be specified", id="empty-list"),
        pytest.param([""], "Chain name cannot be empty", id="empty-name"),
        pytest.param(["unknown"], f"Unsupported chain 'unknown'. Valid chains: {_VALID_CHAINS}", id="unknown"),
        pytest.param([" "], f"Unsupported chain ' '. Valid chains: {_VALID_CHAINS}", id="whitespace-only"),
        pytest.param(
            ["arbitrum", "ARBITRUM"],
            "Duplicate chain 'arbitrum' in chains list",
            id="duplicate-case-insensitive",
        ),
        pytest.param(["bsc", "bnb"], "Duplicate chain 'bsc' in chains list", id="duplicate-alias"),
        pytest.param(
            ["arbitrum", "eip155:42161"],
            "Duplicate chain 'arbitrum' in chains list",
            id="duplicate-caip",
        ),
        pytest.param(["solana"], _MULTI_SOLANA_REASON, id="solana"),
        pytest.param(["sol"], _MULTI_SOLANA_REASON, id="solana-alias"),
    ],
)
def test_normalise_multi_chains_exact_errors(chains: list[str], reason: str) -> None:
    with pytest.raises(ConfigurationError) as exc_info:
        _normalise_multi_chains(chains)

    assert exc_info.value.field == "chains"
    assert exc_info.value.reason == reason
    assert str(exc_info.value) == f"Configuration error for 'chains': {reason}"


@pytest.mark.parametrize(
    ("chains", "protocols", "expected"),
    [
        pytest.param(
            ["arbitrum", "base"],
            {"BASE": ["UNISWAP_V3"], "ARBITRUM": ["AAVE_V3", "uniswap_v3"]},
            {"base": ["uniswap_v3"], "arbitrum": ["aave_v3", "uniswap_v3"]},
            id="case-and-mapping-order",
        ),
        pytest.param(["mantle"], {"MANTLE": ["UNISWAP_V3"]}, {"mantle": ["uniswap_v3"]}, id="mantle-alias"),
        pytest.param(["mantle"], {"mantle": ["Agni"]}, {"mantle": ["agni"]}, id="agni-alias"),
        pytest.param(
            ["avalanche"],
            {"avalanche": ["TRADER-JOE-V2"]},
            {"avalanche": ["trader-joe-v2"]},
            id="legacy-hyphenated-protocol",
        ),
        pytest.param(
            ["optimism"],
            {"optimism": ["VELODROME"]},
            {"optimism": ["velodrome"]},
            id="chain-scoped-velodrome-alias",
        ),
        pytest.param(
            ["arbitrum"],
            {"arbitrum": ["MORPHO"]},
            {"arbitrum": ["morpho"]},
            id="global-morpho-alias",
        ),
    ],
)
def test_normalise_multi_protocols_table_preserves_spelling_and_order(
    chains: list[str],
    protocols: dict[str, list[str]],
    expected: dict[str, list[str]],
) -> None:
    original = {chain: list(values) for chain, values in protocols.items()}

    normalised = _normalise_multi_protocols(chains, protocols)

    assert list(normalised.items()) == list(expected.items())
    assert protocols == original


@pytest.mark.parametrize(
    ("chains", "protocols", "reason"),
    [
        pytest.param(["arbitrum"], {}, "Protocols mapping cannot be empty", id="empty-mapping"),
        pytest.param(
            ["arbitrum"],
            {"base": ["uniswap_v3"]},
            "Protocol mapping for chain 'base' but chain not in configured chains: ['arbitrum']",
            id="unconfigured-chain",
        ),
        pytest.param(
            ["arbitrum"],
            {"arbitrum": []},
            "Protocol list for chain 'arbitrum' cannot be empty",
            id="empty-protocol-list",
        ),
        pytest.param(
            ["arbitrum"],
            {"arbitrum": ["unknown"]},
            f"Unknown protocol 'unknown'. Valid protocols: {_VALID_PROTOCOLS}",
            id="unknown-protocol",
        ),
        pytest.param(
            ["arbitrum"],
            {"arbitrum": [""]},
            f"Unknown protocol ''. Valid protocols: {_VALID_PROTOCOLS}",
            id="empty-protocol-name",
        ),
        pytest.param(
            ["arbitrum"],
            {"arbitrum": ["lido"]},
            "Protocol 'lido' is not available on chain 'arbitrum'. Available on: ethereum",
            id="protocol-unavailable-on-chain",
        ),
        pytest.param(
            ["arbitrum"],
            {"arbitrum": ["uniswap_v3", "UNISWAP_V3"]},
            "Duplicate protocol 'UNISWAP_V3' for chain 'arbitrum'",
            id="duplicate-protocol-case-insensitive",
        ),
        pytest.param(
            ["arbitrum", "base"],
            {"arbitrum": ["uniswap_v3"]},
            "No protocols configured for chain 'base'",
            id="missing-chain-mapping",
        ),
    ],
)
def test_normalise_multi_protocols_exact_errors(
    chains: list[str],
    protocols: dict[str, list[str]],
    reason: str,
) -> None:
    with pytest.raises(ConfigurationError) as exc_info:
        _normalise_multi_protocols(chains, protocols)

    assert exc_info.value.field == "protocols"
    assert exc_info.value.reason == reason
    assert str(exc_info.value) == f"Configuration error for 'protocols': {reason}"
