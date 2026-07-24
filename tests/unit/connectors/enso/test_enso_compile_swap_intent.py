"""Tests for EnsoAdapter.compile_swap_intent.

compile_swap_intent converts a SwapIntent into an ActionBundle:

- resolves token addresses/decimals via the TokenResolver,
- converts amount (token terms) or amount_usd (via the price oracle) to wei,
- fetches a route from EnsoClient,
- optionally rewrites routeSingle calldata to safeRouteSingle,
- prepends an ERC-20 approval (skipped for the native-token sentinel),
- marks the swap transaction as deferred with route_params in metadata.

Failure semantics: PriceUnavailableError propagates (never a silent fake
price); every other failure is wrapped into an error ActionBundle with no
transactions. These tests mock the EnsoClient and TokenResolver seams,
following the pattern of test_enso_get_fresh_swap_transaction.py.
"""

from __future__ import annotations

from decimal import Decimal
from unittest.mock import MagicMock, patch

import pytest

from almanak.connectors.enso.adapter import ENSO_GAS_ESTIMATES, EnsoAdapter
from almanak.connectors.enso.client import EnsoConfig
from almanak.framework.data.tokens.exceptions import TokenResolutionError
from almanak.framework.intents.vocabulary import SwapIntent
from almanak.framework.market import PriceUnavailableError

USDC_ADDRESS = "0xaf88d065e77c8cc2239327c5edb3a432268e5831"
WETH_ADDRESS = "0x82af49447d8a07e3bd95bd0d56f35241523fbab1"
NATIVE_SENTINEL = "0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee"
ROUTER_ADDRESS = "0x8fd0038cd4b544ed4b1f8cbfcbee6de19f7ba1ab"

ROUTE_SINGLE_SELECTOR = "0xb94c3609"


@pytest.fixture
def enso_config() -> EnsoConfig:
    return EnsoConfig(
        chain="arbitrum",
        wallet_address="0x1111111111111111111111111111111111111111",
        api_key="test-api-key",  # noqa: S106 - test fixture
    )


def _make_resolver(tokens: dict[str, tuple[str, int]]) -> MagicMock:
    """Build a TokenResolver mock resolving symbol -> (address, decimals)."""
    resolver = MagicMock()

    def resolve(token: str, chain: str, **kwargs) -> MagicMock:
        if token not in tokens:
            raise TokenResolutionError(
                token=token,
                chain=str(chain),
                reason="not in test registry",
            )
        address, decimals = tokens[token]
        resolved = MagicMock()
        resolved.address = address
        resolved.decimals = decimals
        resolved.symbol = token
        return resolved

    resolver.resolve.side_effect = resolve
    return resolver


def _make_adapter(
    config: EnsoConfig,
    tokens: dict[str, tuple[str, int]] | None = None,
    prices: dict[str, Decimal] | None = None,
    use_safe_route_single: bool = False,
) -> EnsoAdapter:
    """Build an EnsoAdapter skipping heavy __init__ side effects."""
    with patch.object(EnsoAdapter, "__init__", lambda self, *a, **kw: None):
        adapter = EnsoAdapter.__new__(EnsoAdapter)
        adapter.config = config
        adapter.chain = config.chain
        adapter.wallet_address = config.wallet_address
        adapter.use_safe_route_single = use_safe_route_single
        adapter._using_placeholders = prices is None
        adapter._price_provider = prices or {}
        adapter._token_resolver = _make_resolver(
            tokens
            if tokens is not None
            else {"USDC": (USDC_ADDRESS, 6), "WETH": (WETH_ADDRESS, 18)}
        )
        adapter.client = MagicMock()
        adapter.client.get_router_address.return_value = ROUTER_ADDRESS
    return adapter


def _make_route_tx(
    to: str = ROUTER_ADDRESS,
    data: str = "0xdeadbeef",
    value: int = 0,
    gas: int | None = 180000,
    amount_out_wei: int = 500_000_000_000_000_000,
    price_impact: int = 5,
) -> MagicMock:
    """Build a RouteTransaction-like mock with the fields the adapter reads."""
    route_tx = MagicMock()
    route_tx.tx.to = to
    route_tx.tx.data = data
    route_tx.tx.value = value
    route_tx.gas = gas
    route_tx.get_amount_out_wei.return_value = amount_out_wei
    route_tx.price_impact = price_impact
    return route_tx


def _swap_intent(**overrides) -> SwapIntent:
    kwargs: dict = {
        "from_token": "USDC",
        "to_token": "WETH",
        "amount": Decimal("1000"),
    }
    kwargs.update(overrides)
    return SwapIntent(**kwargs)


class TestCompileSwapIntentHappyPath:
    def test_amount_path_builds_approve_and_deferred_swap(self, enso_config):
        adapter = _make_adapter(enso_config)
        adapter.client.get_route.return_value = _make_route_tx()

        bundle = adapter.compile_swap_intent(_swap_intent())

        assert bundle.intent_type == "SWAP"
        assert len(bundle.transactions) == 2

        approve, swap = bundle.transactions
        assert approve["tx_type"] == "approve"
        assert approve["to"] == USDC_ADDRESS
        assert swap["tx_type"] == "swap_deferred"
        assert swap["to"] == ROUTER_ADDRESS
        assert swap["data"] == "0xdeadbeef"
        assert swap["gas_estimate"] == 180000

        # 1000 USDC at 6 decimals
        kwargs = adapter.client.get_route.call_args.kwargs
        assert kwargs["amount_in"] == 1000 * 10**6
        assert isinstance(kwargs["amount_in"], int)
        # default max_slippage 0.005 -> 50 bps
        assert kwargs["slippage_bps"] == 50

    def test_metadata_carries_deferred_route_params(self, enso_config):
        adapter = _make_adapter(enso_config)
        adapter.client.get_route.return_value = _make_route_tx()

        intent = _swap_intent()
        bundle = adapter.compile_swap_intent(intent)

        meta = bundle.metadata
        assert meta["intent_id"] == intent.intent_id
        assert meta["from_token"] == "USDC"
        assert meta["to_token"] == "WETH"
        assert meta["token_in_address"] == USDC_ADDRESS
        assert meta["token_out_address"] == WETH_ADDRESS
        assert meta["amount_in"] == str(1000 * 10**6)
        assert meta["amount_out"] == str(500_000_000_000_000_000)
        assert meta["protocol"] == "enso"
        assert meta["router"] == ROUTER_ADDRESS
        assert meta["deferred_swap"] is True
        assert meta["route_params"] == {
            "token_in": USDC_ADDRESS,
            "token_out": WETH_ADDRESS,
            "amount_in": 1000 * 10**6,
            "slippage_bps": 50,
        }
        # total gas = approve estimate + route gas
        assert meta["gas_estimate"] == ENSO_GAS_ESTIMATES["approve"] + 180000

    def test_amount_usd_path_converts_via_price_oracle(self, enso_config):
        adapter = _make_adapter(enso_config, prices={"WETH": Decimal("2000")})
        adapter.client.get_route.return_value = _make_route_tx()

        bundle = adapter.compile_swap_intent(
            _swap_intent(
                from_token="WETH",
                to_token="USDC",
                amount=None,
                amount_usd=Decimal("1000"),
            )
        )

        # $1000 at $2000/WETH = 0.5 WETH = 5e17 wei
        kwargs = adapter.client.get_route.call_args.kwargs
        assert kwargs["amount_in"] == 500_000_000_000_000_000
        assert bundle.metadata["amount_in"] == str(500_000_000_000_000_000)

    def test_explicit_price_oracle_argument_overrides_default(self, enso_config):
        adapter = _make_adapter(enso_config, prices={"WETH": Decimal("1")})
        adapter.client.get_route.return_value = _make_route_tx()

        adapter.compile_swap_intent(
            _swap_intent(
                from_token="WETH",
                to_token="USDC",
                amount=None,
                amount_usd=Decimal("100"),
            ),
            price_oracle={"WETH": Decimal("4000")},
        )

        # $100 at $4000/WETH = 0.025 WETH
        kwargs = adapter.client.get_route.call_args.kwargs
        assert kwargs["amount_in"] == 25_000_000_000_000_000

    def test_native_token_input_skips_approval(self, enso_config):
        adapter = _make_adapter(
            enso_config,
            tokens={"ETH": (NATIVE_SENTINEL, 18), "USDC": (USDC_ADDRESS, 6)},
        )
        adapter.client.get_route.return_value = _make_route_tx()

        bundle = adapter.compile_swap_intent(
            _swap_intent(from_token="ETH", to_token="USDC", amount=Decimal("1"))
        )

        assert len(bundle.transactions) == 1
        assert bundle.transactions[0]["tx_type"] == "swap_deferred"
        assert bundle.metadata["gas_estimate"] == 180000

    def test_missing_route_gas_falls_back_to_default_estimate(self, enso_config):
        adapter = _make_adapter(enso_config)
        adapter.client.get_route.return_value = _make_route_tx(gas=None)

        bundle = adapter.compile_swap_intent(_swap_intent())

        swap = bundle.transactions[-1]
        assert swap["gas_estimate"] == ENSO_GAS_ESTIMATES["swap"]


class TestCompileSwapIntentSafeRouteSingle:
    def test_route_single_data_is_transformed(self, enso_config):
        adapter = _make_adapter(enso_config, use_safe_route_single=True)
        original_data = ROUTE_SINGLE_SELECTOR + "ab" * 32
        adapter.client.get_route.return_value = _make_route_tx(data=original_data)
        transform = MagicMock(return_value="0x21025a06feed")
        adapter._transform_to_safe_route_single = transform

        bundle = adapter.compile_swap_intent(_swap_intent())

        assert bundle.transactions[-1]["data"] == "0x21025a06feed"
        transform.assert_called_once_with(
            original_data=original_data,
            token_out_address=WETH_ADDRESS,
            receiver=enso_config.wallet_address,
            amount_out=500_000_000_000_000_000,
            slippage_bps=50,
        )

    def test_non_route_single_data_left_untouched(self, enso_config):
        adapter = _make_adapter(enso_config, use_safe_route_single=True)
        adapter.client.get_route.return_value = _make_route_tx(data="0xf52e33f5cafe")
        transform = MagicMock()
        adapter._transform_to_safe_route_single = transform

        bundle = adapter.compile_swap_intent(_swap_intent())

        assert bundle.transactions[-1]["data"] == "0xf52e33f5cafe"
        transform.assert_not_called()

    def test_transform_disabled_leaves_route_single_data(self, enso_config):
        adapter = _make_adapter(enso_config, use_safe_route_single=False)
        original_data = ROUTE_SINGLE_SELECTOR + "cd" * 32
        adapter.client.get_route.return_value = _make_route_tx(data=original_data)

        bundle = adapter.compile_swap_intent(_swap_intent())

        assert bundle.transactions[-1]["data"] == original_data


class TestCompileSwapIntentErrorBundles:
    def test_unresolved_amount_all_returns_error_bundle(self, enso_config):
        adapter = _make_adapter(enso_config)

        bundle = adapter.compile_swap_intent(_swap_intent(amount="all"))

        assert bundle.transactions == []
        assert "amount='all' must be resolved before compilation" in bundle.metadata["error"]
        adapter.client.get_route.assert_not_called()

    def test_missing_both_amounts_returns_error_bundle(self, enso_config):
        """SwapIntent validation forbids both amounts being None, so bypass it
        with model_construct to document the adapter's own guard."""
        adapter = _make_adapter(enso_config)
        intent = SwapIntent.model_construct(from_token="USDC", to_token="WETH")

        bundle = adapter.compile_swap_intent(intent)

        assert bundle.transactions == []
        assert "Either amount or amount_usd must be specified" in bundle.metadata["error"]

    def test_empty_input_token_address_returns_error_bundle(self, enso_config):
        adapter = _make_adapter(
            enso_config,
            tokens={"USDC": ("", 6), "WETH": (WETH_ADDRESS, 18)},
        )

        bundle = adapter.compile_swap_intent(_swap_intent())

        assert bundle.transactions == []
        assert bundle.metadata["error"] == "Unknown input token: USDC"

    def test_empty_output_token_address_returns_error_bundle(self, enso_config):
        adapter = _make_adapter(
            enso_config,
            tokens={"USDC": (USDC_ADDRESS, 6), "WETH": ("", 18)},
        )

        bundle = adapter.compile_swap_intent(_swap_intent())

        assert bundle.transactions == []
        assert bundle.metadata["error"] == "Unknown output token: WETH"

    def test_token_resolution_failure_wrapped_in_error_bundle(self, enso_config):
        adapter = _make_adapter(enso_config, tokens={"USDC": (USDC_ADDRESS, 6)})

        bundle = adapter.compile_swap_intent(_swap_intent(to_token="NOPE"))

        assert bundle.transactions == []
        assert "NOPE" in bundle.metadata["error"]
        assert bundle.metadata["intent_id"]

    def test_route_fetch_failure_wrapped_in_error_bundle(self, enso_config):
        adapter = _make_adapter(enso_config)
        adapter.client.get_route.side_effect = RuntimeError("enso api down")

        bundle = adapter.compile_swap_intent(_swap_intent())

        assert bundle.transactions == []
        assert bundle.metadata["error"] == "enso api down"


class TestCompileSwapIntentPriceSafety:
    def test_missing_price_raises_price_unavailable(self, enso_config):
        """amount_usd with no oracle price must raise, never fall back to a
        fake price or an error bundle."""
        adapter = _make_adapter(enso_config, prices={})

        with pytest.raises(PriceUnavailableError, match="USDC"):
            adapter.compile_swap_intent(
                _swap_intent(amount=None, amount_usd=Decimal("1000"))
            )

        adapter.client.get_route.assert_not_called()
