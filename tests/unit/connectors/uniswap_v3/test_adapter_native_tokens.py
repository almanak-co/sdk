"""Targeted tests for UniswapV3Adapter._is_native_token expansion.

Covers the native symbol set including 0G Chain (A0GI / 0G) so that
Jaine swaps with native input take the wrap-via-msg.value code path.
"""

from unittest.mock import MagicMock

from almanak.connectors.uniswap_v3.adapter import UniswapV3Adapter, UniswapV3Config


def _make_adapter(chain: str) -> UniswapV3Adapter:
    return UniswapV3Adapter(
        config=UniswapV3Config(
            chain=chain,
            wallet_address="0x1234567890123456789012345678901234567890",
            allow_placeholder_prices=True,
        ),
        token_resolver=MagicMock(),
    )


def test_is_native_token_eth_family() -> None:
    adapter = _make_adapter("ethereum")
    assert adapter._is_native_token("ETH") is True
    assert adapter._is_native_token("WETH") is False
    assert adapter._is_native_token("USDC") is False


def test_is_native_token_placeholder_sentinel() -> None:
    adapter = _make_adapter("ethereum")
    assert adapter._is_native_token("0xEeeeeEeeeEeEeeEeEeEeeEEEeeeeEeeeeeeeEEeE") is True


def test_is_native_token_zerog() -> None:
    adapter = _make_adapter("zerog")
    assert adapter._is_native_token("A0GI") is True
    assert adapter._is_native_token("W0G") is False
    # "0G" is NOT native: A0GI is the canonical symbol in the registry, and
    # treating "0G" as native would break resolve_for_swap at runtime.
    assert adapter._is_native_token("0G") is False


def test_uses_v1_router_zerog() -> None:
    """Jaine on 0G only speaks the V1 8-arg router ABI."""
    assert _make_adapter("zerog")._uses_v1_router() is True


def test_uses_v1_router_eth_family() -> None:
    """Canonical UniV3 chains use SwapRouter02 (7-arg, no deadline)."""
    for chain in ("ethereum", "arbitrum", "optimism", "base", "polygon"):
        assert _make_adapter(chain)._uses_v1_router() is False, chain


def test_exact_input_selector_branches_on_v1_router() -> None:
    """zerog must encode the 8-param V1 struct (selector 0x414bf389)."""
    from almanak.connectors.uniswap_v3.adapter import EXACT_INPUT_SINGLE_SELECTOR, EXACT_INPUT_SINGLE_V1_SELECTOR

    zerog = _make_adapter("zerog")
    zerog._get_token_symbol = lambda a: "X"  # type: ignore[assignment]
    zerog._get_token_decimals = lambda s: 18  # type: ignore[assignment]
    tx_zerog = zerog._build_exact_input_single_tx(
        token_in="0x1111111111111111111111111111111111111111",
        token_out="0x2222222222222222222222222222222222222222",
        fee=100,
        recipient="0x3333333333333333333333333333333333333333",
        amount_in=10**18,
        amount_out_minimum=1,
    )
    assert tx_zerog.data.startswith(EXACT_INPUT_SINGLE_V1_SELECTOR)

    base = _make_adapter("base")
    base._get_token_symbol = lambda a: "X"  # type: ignore[assignment]
    base._get_token_decimals = lambda s: 18  # type: ignore[assignment]
    tx_base = base._build_exact_input_single_tx(
        token_in="0x1111111111111111111111111111111111111111",
        token_out="0x2222222222222222222222222222222222222222",
        fee=500,
        recipient="0x3333333333333333333333333333333333333333",
        amount_in=10**18,
        amount_out_minimum=1,
    )
    assert tx_base.data.startswith(EXACT_INPUT_SINGLE_SELECTOR)
