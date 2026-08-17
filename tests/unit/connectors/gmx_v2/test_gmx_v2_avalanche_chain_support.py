"""Unit tests for VIB-1720: GMX V2 Avalanche chain support.

The previous chain check at sdk.py:186 hard-rejected anything other than
arbitrum. This test pins the new behaviour: every chain in
GMX_V2_SDK_ADDRESSES (currently arbitrum + avalanche) constructs cleanly,
unlisted chains raise a helpful error mentioning the contract registry.
"""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from almanak.connectors.gmx_v2.sdk import (
    GMX_V2_SDK_ADDRESSES,
    GMXV2SDK,
)


@pytest.fixture
def stub_gateway() -> MagicMock:
    """Connected gateway so we can construct the SDK without an HTTPProvider."""
    gateway = MagicMock()
    gateway.is_connected = True
    return gateway


def test_arbitrum_remains_supported(stub_gateway: MagicMock) -> None:
    sdk = GMXV2SDK(chain="arbitrum", gateway_client=stub_gateway)
    assert sdk.chain == "arbitrum"
    assert sdk.EXCHANGE_ROUTER_ADDRESS.lower() == "0x1c3fa76e6e1088bce750f23a5bfcffa1efef6a41"


def test_avalanche_construction_succeeds(stub_gateway: MagicMock) -> None:
    """The whole point of VIB-1720: avalanche must not raise."""
    sdk = GMXV2SDK(chain="avalanche", gateway_client=stub_gateway)
    assert sdk.chain == "avalanche"
    # Compare lowercase to be tolerant of EIP-55 vs lowercase casing in the registry.
    assert sdk.EXCHANGE_ROUTER_ADDRESS.lower() == "0x8f550e53dfe96c055d5bdb267c21f268fcaf63b2"
    assert sdk.WETH_ADDRESS.lower() == "0xb31f66aa3c1e785363f0875a1b74e27b85fd66c7", (
        "WETH alias on Avalanche should resolve to WAVAX (the native wrapper)"
    )


def test_unsupported_chain_lists_supported_set(stub_gateway: MagicMock) -> None:
    with pytest.raises(ValueError) as excinfo:
        GMXV2SDK(chain="ethereum", gateway_client=stub_gateway)
    msg = str(excinfo.value)
    assert "ethereum" in msg
    # Error must guide the reader to the core-contract registry — that's the
    # actual fix path, while markets remain dynamic.
    assert "gmx_v2/addresses.py" in msg
    for chain in GMX_V2_SDK_ADDRESSES:
        assert chain in msg, f"error should list supported chain {chain!r}"


def test_sdk_address_map_has_no_static_market_aliases() -> None:
    """Market identity must never leak back into SDK deployment wiring."""
    for addresses in GMX_V2_SDK_ADDRESSES.values():
        assert all("MARKET" not in key for key in addresses)


def test_avalanche_address_map_has_required_keys() -> None:
    """Every key existing call sites read from must be present for new chains."""
    required = {
        "EXCHANGE_ROUTER",
        "ROUTER",
        "DATA_STORE",
        "ORDER_VAULT",
        "READER",
        "WETH",
    }
    for chain, addr_map in GMX_V2_SDK_ADDRESSES.items():
        missing = required - set(addr_map)
        assert not missing, f"chain={chain} missing GMX_V2_SDK_ADDRESSES keys: {missing}"


def test_construction_requires_rpc_url_or_gateway() -> None:
    with pytest.raises(ValueError, match=r"rpc_url|gateway_client"):
        GMXV2SDK(chain="avalanche")


def test_compiler_resolves_mixed_case_avalanche_collateral_keys() -> None:
    """A mixed-case venue symbol resolves through the verified market tuple.

    Address-first: the market arrives as the fixture snapshot's verified
    avalanche ETH/USD address (catalog-primed, and served by the fake dynamic
    gateway because the risk-increasing OPEN leg demands CURRENT venue
    listing). No connector-owned token mirror participates.
    """
    from decimal import Decimal
    from unittest.mock import MagicMock

    from almanak.framework.intents.compiler import IntentCompiler, IntentCompilerConfig
    from almanak.framework.intents.compiler_models import CompilationStatus
    from almanak.framework.intents.vocabulary import PerpOpenIntent
    from tests.unit.connectors.gmx_v2.market_fixtures import (
        fake_dynamic_gateway,
        market_address,
        market_record,
        prime_catalog,
    )

    verified_market = market_record("avalanche", "ETH/USD")
    expected_addr = verified_market.long_token
    prime_catalog(verified_market, chain="avalanche")

    # Minimal compiler state — bypass __init__ so we don't pull in gateways or
    # token-resolver singletons; the lookup we exercise doesn't need them.
    compiler = IntentCompiler.__new__(IntentCompiler)
    compiler.chain = "avalanche"
    compiler.wallet_address = "0x" + "1" * 40
    compiler.rpc_url = None
    compiler._approve_cache = {}
    # The OPEN leg demands CURRENT venue listing: the fake dynamic gateway is
    # what resolves the market so the compile reaches the collateral lookup.
    compiler._gateway_client = fake_dynamic_gateway("avalanche")
    # Resolver-less compilation proves the market record owns this identity;
    # neither collateral address nor decimals may come from generic fallback.
    compiler._token_resolver = None
    # VIB-6219: the compile path now derives a real acceptablePrice, so it needs
    # a price oracle. Without one the compile fails closed BEFORE reaching the
    # collateral lookup and this test would pass vacuously.
    compiler._config = IntentCompilerConfig(allow_placeholder_prices=False)
    compiler._using_placeholders = False
    compiler._placeholder_warning_logged = False
    compiler._stablecoin_fallback_logged = set()
    compiler.price_oracle = {"ETH": Decimal("3000"), "USDC": Decimal("1")}
    compiler.default_deadline_seconds = 600
    compiler.default_protocol = "gmx_v2"
    compiler._allowance_cache = {}
    # Stub _build_approve_tx so the compile can proceed past the approval step.
    compiler._build_approve_tx = lambda token_address, spender, amount: []
    compiler._get_chain_rpc_url = lambda: "http://localhost:8545"

    intent = PerpOpenIntent(
        market=market_address("avalanche", "ETH/USD"),
        collateral_token="WETH.e",  # mixed-case as a user would type
        collateral_amount=Decimal("1"),
        size_usd=Decimal("1000"),
        is_long=True,
        leverage=Decimal("10"),
        protocol="gmx_v2",
    )

    # Patch only the network-y pieces; the lookup logic itself is what we test.
    from unittest.mock import patch

    mock_sdk = MagicMock()
    mock_sdk.ROUTER_ADDRESS = "0xrouter"
    mock_sdk.WETH_ADDRESS = expected_addr
    mock_sdk.build_increase_order_multicall.return_value = MagicMock(
        to="0xrouter", value=0, data=b"0x", gas_estimate=300_000
    )
    mock_sdk.get_execution_fee.return_value = int(0.02 * 10**18)

    mock_adapter_result = MagicMock(success=True, error=None)
    mock_adapter_result.collateral_amount_usd = Decimal("1000")

    with (
        patch("almanak.connectors.gmx_v2.compiler.GMXv2Adapter") as mock_adapter_cls,
        patch("almanak.connectors.gmx_v2.compiler.GMXv2Config"),
        patch("almanak.connectors.gmx_v2.compiler.GMXV2SDK", return_value=mock_sdk),
    ):
        mock_adapter_cls.return_value.open_position.return_value = mock_adapter_result
        result = compiler.compile(intent)

    assert result.status == CompilationStatus.SUCCESS, (
        f"WETH.e collateral lookup must derive {expected_addr} from the verified market; got error: {result.error}"
    )
    # Positively pin that the resolved ADDRESS reached the order, rather than
    # only asserting the absence of one error string — the weaker form passed
    # vacuously once the compile started failing earlier for a different reason.
    #
    # Asserting the metadata symbol is not enough either (CodeRabbit): metadata
    # echoes back the INPUT `"WETH.e"`, so a regression that resolved the symbol
    # to the WRONG address would still satisfy it, and `expected_addr` would go
    # unused outside a failure message. Read the address out of the order params
    # the SDK was actually handed.
    assert result.action_bundle.metadata["collateral_token"] == "WETH.e"

    assert mock_sdk.build_increase_order_multicall.call_count == 1, (
        "the compile must have reached the order builder exactly once"
    )
    order_params = mock_sdk.build_increase_order_multicall.call_args.args[0]
    assert order_params.initial_collateral_token.lower() == expected_addr.lower(), (
        f"the order must carry the RESOLVED WETH.e address {expected_addr}, got {order_params.initial_collateral_token}"
    )


def test_native_wrapped_identity_is_derived_for_avalanche() -> None:
    """SDK native handling and verified market metadata agree on WAVAX."""
    from almanak.connectors.gmx_v2.permission_seed import permission_markets
    from almanak.core.chains import ChainRegistry
    from tests.unit.connectors.gmx_v2.market_fixtures import market_record

    wrapped = ChainRegistry.resolve("avalanche").native.wrapped_address
    avax_market = market_record("avalanche", "AVAX/USD")

    assert wrapped is not None
    assert GMX_V2_SDK_ADDRESSES["avalanche"]["WETH"].lower() == wrapped.lower()
    assert avax_market.long_token.lower() == wrapped.lower()
    assert avax_market.long_token_decimals == 18
    # The permission seed is a different ETH market and must not leak into the
    # runtime catalog merely because both paths share chain deployment wiring.
    assert permission_markets()["avalanche"].market_token.lower() != avax_market.market_token.lower()
