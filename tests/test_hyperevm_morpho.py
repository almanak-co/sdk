"""HyperEVM network identity and its Morpho Blue declaration (20 October 2026 acceptance)."""

from datetime import UTC, datetime

import pytest

from almanak.caip import AssetType, ChainId
from almanak.chains import HYPEREVM, evm_network
from almanak.connectors.composition import connector_from_wire
from almanak.connectors.morpho_blue.deployment import REVISIONS
from almanak.connectors.morpho_blue.networks import core_for
from almanak.connectors.support import RunMode
from almanak.gateway.evm import native_metadata
from almanak.gateway.morpho_lookup import API_CHAINS, MorphoMarkets
from almanak.gateway.onchain_lookup import NETWORKS
from almanak.gateway.providers import ALCHEMY_NETWORKS, AlchemyProvider, provider_endpoint
from almanak.gateway.roles_storage import ZEPPELIN_PROXY_HASHES

CHAIN = ChainId("eip155:999")
USDC = "0xb88339cb7199b77e23db6e890353e22632ba630f"


def test_network_pays_gas_in_hype_and_wraps_through_whype():
    network = evm_network(CHAIN)
    assert network is HYPEREVM
    assert network.native_asset == AssetType("eip155:999/slip44:2457")
    identity, metadata = native_metadata(CHAIN)
    assert identity == network.native_asset and metadata.decimals == 18
    assert network.wrapped_native == AssetType(
        "eip155:999/erc20:0x5555555555555555555555555555555555555555"
    )
    # Block 0 as Alchemy serves it, equal to block 1's parentHash.
    assert network.genesis == "0xd8fcc13b6a195b88b7b2da3722ff6cad767b13a8c1e9ffb1c73aa9d216d895f0"
    # WHYPE is a WETH9 clone, legacy receipts settle at the bid, and big blocks
    # interleave with one-second small blocks.
    assert network.family == "ethereum"
    assert not network.wrapping_emits_transfer
    assert not network.legacy_price_is_ceiling
    assert network.fee_history_reports_tips
    assert not network.fixed_block_interval


def test_alchemy_route_keeps_the_key_in_trusted_resolution(tmp_path):
    env = tmp_path / "operator.env"
    env.write_text("ALCHEMY_API_KEY=k/ey\n")
    provider = AlchemyProvider(
        kind="alchemy", network=ALCHEMY_NETWORKS[CHAIN], api_key_env="ALCHEMY_API_KEY"
    )
    assert provider_endpoint(provider, env) == (
        "https://hyperliquid-mainnet.g.alchemy.com/v2/k%2Fey"
    )


def test_morpho_core_and_revisions_reuse_the_reviewed_compilation():
    assert core_for(CHAIN) == "0x68e37de8d93d3496ae143f2e900490f6280c57cd"
    # The chain-999 core, IRM and oracle runtimes matched these entries byte for byte,
    # outside their immutables; the domain separator still binds chain 999 at runtime.
    assert REVISIONS["eip155:999"] == REVISIONS["eip155:4663"] == REVISIONS["eip155:42161"]


def test_morpho_cell_declares_every_mode_and_profile_from_its_own_boundary():
    support = connector_from_wire({"connector": "morpho_blue"}).support_for(CHAIN)
    assert support.modes == (RunMode.FORK, RunMode.PRODUCTION, RunMode.BACKTEST)
    assert support.account_profiles == ("eoa", "safe_roles")
    for action in support.actions:
        support.require(RunMode.PRODUCTION, action, "safe_roles")
    assert {h.backtest_start_at for h in support.historical} == {
        datetime(2026, 10, 20, 4, tzinfo=UTC)
    }
    assert "pinned Jovian L1 and operator fee accounting" not in support.restrictions


def test_discovery_uses_the_morpho_api_and_the_hyperevm_vendor_network():
    assert CHAIN in API_CHAINS
    MorphoMarkets(chain=CHAIN, loan=USDC)
    assert NETWORKS[CHAIN] == "hyperevm"


@pytest.mark.parametrize(
    "code_hash", ["0xa119f674d44072e257f162aee49c7aba9b7a04fe2e8c84fcefddb366ab6d40cd"]
)
def test_usdc_identity_follows_its_zeppelin_implementation_slot(code_hash):
    # HyperEVM USDC's FiatTokenProxy compilation differs from Ethereum's and Arbitrum's.
    assert code_hash in ZEPPELIN_PROXY_HASHES
