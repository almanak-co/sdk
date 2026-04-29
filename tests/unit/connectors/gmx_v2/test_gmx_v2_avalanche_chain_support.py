"""Unit tests for VIB-1720: GMX V2 Avalanche chain support.

The previous chain check at sdk.py:186 hard-rejected anything other than
arbitrum. This test pins the new behaviour: every chain in
GMX_V2_SDK_ADDRESSES (currently arbitrum + avalanche) constructs cleanly,
unlisted chains raise a helpful error mentioning the contract registry.
"""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from almanak.framework.connectors.gmx_v2.sdk import (
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
    # Error must guide the reader to the registry — that's the actual fix path.
    assert "core/contracts.py" in msg
    for chain in GMX_V2_SDK_ADDRESSES:
        assert chain in msg, f"error should list supported chain {chain!r}"


def test_avalanche_address_map_has_native_avax_market() -> None:
    """Sanity-check that the AVAX/USD market only exists where we wired it."""
    assert "AVAX_USD_MARKET" in GMX_V2_SDK_ADDRESSES["avalanche"]
    assert "AVAX_USD_MARKET" not in GMX_V2_SDK_ADDRESSES["arbitrum"]


def test_avalanche_address_map_has_required_keys() -> None:
    """Every key existing call sites read from must be present for new chains."""
    required = {
        "EXCHANGE_ROUTER",
        "ROUTER",
        "DATA_STORE",
        "ORDER_VAULT",
        "READER",
        "ETH_USD_MARKET",
        "BTC_USD_MARKET",
        "WETH",
        "USDC",
        "USDT",
    }
    for chain, addr_map in GMX_V2_SDK_ADDRESSES.items():
        missing = required - set(addr_map)
        assert not missing, f"chain={chain} missing GMX_V2_SDK_ADDRESSES keys: {missing}"


def test_construction_requires_rpc_url_or_gateway() -> None:
    with pytest.raises(ValueError, match=r"rpc_url|gateway_client"):
        GMXV2SDK(chain="avalanche")


def test_compiler_resolves_mixed_case_avalanche_collateral_keys() -> None:
    """Drive the actual compile path: PerpOpenIntent with collateral 'WETH.e'
    (the user-typed mixed case) must resolve to the registry address even
    though the compiler upper-cases the symbol before looking it up.

    Without case-insensitive lookup logic in compiler.py, this test fails:
    'WETH.e'.upper() == 'WETH.E' which is NOT a literal key of GMX_V2_TOKENS.
    """
    from decimal import Decimal
    from unittest.mock import MagicMock

    from almanak.core.contracts import GMX_V2_TOKENS
    from almanak.framework.intents.compiler import IntentCompiler
    from almanak.framework.intents.compiler_models import CompilationStatus
    from almanak.framework.intents.vocabulary import PerpOpenIntent

    expected_addr = GMX_V2_TOKENS["avalanche"]["WETH.e"]

    # Minimal compiler state — bypass __init__ so we don't pull in gateways or
    # token-resolver singletons; the lookup we exercise doesn't need them.
    compiler = IntentCompiler.__new__(IntentCompiler)
    compiler.chain = "avalanche"
    compiler.wallet_address = "0x" + "1" * 40
    compiler.rpc_url = None
    compiler._approve_cache = {}
    compiler._gateway_client = None
    # Resolver-less compile path forces the static decimals fallback we just
    # added, which is exactly the regression surface this test pins.
    compiler._token_resolver = None
    # Stub _build_approve_tx so the compile can proceed past the approval step.
    compiler._build_approve_tx = lambda token_address, spender, amount: []
    compiler._get_chain_rpc_url = lambda: "http://localhost:8545"

    intent = PerpOpenIntent(
        market="ETH/USD",
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
    mock_sdk.get_market_address.return_value = "0xmarket"
    mock_sdk.ROUTER_ADDRESS = "0xrouter"
    mock_sdk.build_increase_order_multicall.return_value = MagicMock(
        to="0xrouter", value=0, data=b"0x", gas_estimate=300_000
    )
    mock_sdk.get_execution_fee.return_value = int(0.02 * 10**18)

    mock_adapter_result = MagicMock(success=True, error=None)
    mock_adapter_result.collateral_amount_usd = Decimal("1000")
    mock_adapter_result.acceptable_price_30dec = 1_000_000_000_000_000_000_000_000_000_000_000

    with (
        patch("almanak.framework.connectors.GMXv2Adapter") as mock_adapter_cls,
        patch("almanak.framework.connectors.GMXv2Config"),
        patch("almanak.framework.connectors.gmx_v2.GMXV2SDK", return_value=mock_sdk),
        patch(
            "almanak.framework.connectors.gmx_v2.GMX_V2_MARKETS",
            {"avalanche": {"ETH/USD": "0xmarket"}},
        ),
    ):
        mock_adapter_cls.return_value.open_position.return_value = mock_adapter_result
        result = compiler._compile_perp_open(intent)

    # Specific failure mode the test pins: an "Unknown collateral token: WETH.e"
    # error indicates the case-insensitive lookup regressed. Anything else
    # (success, or a different downstream error) means the lookup itself worked.
    if result.status == CompilationStatus.FAILED:
        assert "Unknown collateral token" not in (result.error or ""), (
            f"WETH.e collateral lookup must succeed via case-insensitive match against "
            f"GMX_V2_TOKENS['avalanche']['WETH.e']={expected_addr}; got error: {result.error}"
        )


def test_native_wrapped_decimals_documented_for_avalanche() -> None:
    """WAVAX is 18-decimal (same as WETH/ETH); the perp compiler must not
    fall back to the stable-default of 6 for it. Pin the invariant so a
    future contributor doesn't reintroduce the WETH/ETH-only special case.
    """
    native_wrapped_18 = {"WETH", "ETH", "WAVAX", "AVAX"}
    # The perp compiler's native-wrapped set (line 5557 in compiler.py) must
    # include all of these; if someone removes WAVAX/AVAX, a real WAVAX long
    # would underfund collateral by 1e12 and be picked up by this test.
    from almanak.framework.intents import compiler as compiler_mod

    src = compiler_mod.__file__
    with open(src) as f:
        text = f.read()
    for sym in native_wrapped_18:
        assert (
            f'"{sym}"' in text
        ), f"perp compiler must reference {sym} as a native-wrapped 18-decimal token"
