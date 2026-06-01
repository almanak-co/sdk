"""On-chain regression: BTCB on BSC must report decimals=18.

Forks BSC mainnet via the shared ``anvil_bsc`` fixture, reads ``decimals()``
straight from the BTCB contract at ``0x7130d2A12B9BCbFAe4f2634d864A1Ee1Ce3Ead9c``
via Web3, and compares it against what the SDK's :class:`TokenResolver` reports
for ``BTCB``, ``BTC`` (alias) and ``WBTC`` (legacy alias) on BSC.

Why this test exists
--------------------
For months the registry recorded ``0x7130…ead9c`` under the ``WBTC`` token with
``decimals=8`` (BTC's native precision). The contract is actually BTCB
(Binance-Peg BTC), which is 18 decimals on-chain. The mismatch produced a
silent ``10^10`` mis-scale anywhere ``raw / 10**decimals`` ran on this token —
balances, swap amounts, LP basis, accounting. The bug was latent because no
demo strategy or kitchen-loop run currently routes WBTC through BSC, but it
would have surfaced the moment a user-supplied strategy did.

This test is the on-chain truth check the registry was missing.
"""

import pytest
from web3 import Web3

from almanak.framework.data.tokens.resolver import TokenResolver
from tests.conftest_gateway import AnvilFixture

BTCB_ADDRESS = "0x7130d2A12B9BCbFAe4f2634d864A1Ee1Ce3Ead9c"

# Minimal ERC20 ABI — just decimals() and balanceOf for a sanity probe.
ERC20_ABI = [
    {
        "constant": True,
        "inputs": [],
        "name": "decimals",
        "outputs": [{"name": "", "type": "uint8"}],
        "type": "function",
    },
    {
        "constant": True,
        "inputs": [],
        "name": "symbol",
        "outputs": [{"name": "", "type": "string"}],
        "type": "function",
    },
]


@pytest.fixture
def web3_bsc(anvil_bsc: AnvilFixture) -> Web3:
    """Web3 client pointed at the BSC Anvil fork."""
    return Web3(Web3.HTTPProvider(anvil_bsc.get_rpc_url()))


@pytest.fixture
def fresh_resolver(tmp_path) -> TokenResolver:
    """Cache-isolated resolver instance so this test never reads/writes
    the user's ``~/.almanak/token_cache.json``."""
    TokenResolver.reset_instance()
    cache_file = tmp_path / "token_cache.json"
    yield TokenResolver(cache_file=str(cache_file))
    TokenResolver.reset_instance()


@pytest.mark.anvil
@pytest.mark.bsc
class TestBTCBDecimalsOnChain:
    """BTCB must read 18 on-chain AND through the SDK resolver."""

    def test_onchain_btcb_decimals_is_18(self, web3_bsc: Web3) -> None:
        """Authoritative on-chain reading."""
        contract = web3_bsc.eth.contract(
            address=Web3.to_checksum_address(BTCB_ADDRESS),
            abi=ERC20_ABI,
        )
        onchain_decimals = contract.functions.decimals().call()
        onchain_symbol = contract.functions.symbol().call()

        assert onchain_decimals == 18, (
            f"BTCB on-chain decimals returned {onchain_decimals}; the BSC "
            "BEP-20 has always been 18. If this ever changes it is a chain "
            "event, not a registry edit."
        )
        # BSC's actual BEP-20 symbol field reads "BTCB" — sanity check that
        # the address really is the Binance-Peg contract we think it is.
        assert onchain_symbol.upper() == "BTCB"

    def test_resolver_matches_onchain_for_btcb(
        self, web3_bsc: Web3, fresh_resolver: TokenResolver
    ) -> None:
        """The SDK registry must agree with the on-chain value."""
        contract = web3_bsc.eth.contract(
            address=Web3.to_checksum_address(BTCB_ADDRESS),
            abi=ERC20_ABI,
        )
        onchain_decimals = contract.functions.decimals().call()
        sdk_token = fresh_resolver.resolve("BTCB", "bsc")
        assert sdk_token.decimals == onchain_decimals
        assert sdk_token.address.lower() == BTCB_ADDRESS.lower()

    def test_resolver_btc_alias_matches_onchain(
        self, web3_bsc: Web3, fresh_resolver: TokenResolver
    ) -> None:
        """The bsc-scoped 'BTC' alias must route to the same on-chain truth."""
        contract = web3_bsc.eth.contract(
            address=Web3.to_checksum_address(BTCB_ADDRESS),
            abi=ERC20_ABI,
        )
        onchain_decimals = contract.functions.decimals().call()
        sdk_token = fresh_resolver.resolve("BTC", "bsc")
        assert sdk_token.decimals == onchain_decimals
        assert sdk_token.address.lower() == BTCB_ADDRESS.lower()

    def test_resolver_wbtc_legacy_alias_matches_onchain(
        self, web3_bsc: Web3, fresh_resolver: TokenResolver
    ) -> None:
        """Legacy 'WBTC' callers on BSC must keep resolving AND must inherit
        the correct on-chain decimals — this is the original bug surface."""
        contract = web3_bsc.eth.contract(
            address=Web3.to_checksum_address(BTCB_ADDRESS),
            abi=ERC20_ABI,
        )
        onchain_decimals = contract.functions.decimals().call()
        sdk_token = fresh_resolver.resolve("WBTC", "bsc")
        assert sdk_token.decimals == onchain_decimals
        assert sdk_token.address.lower() == BTCB_ADDRESS.lower()
