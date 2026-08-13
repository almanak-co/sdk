"""Unit tests for ``ManagedGateway._parse_anvil_funding_for_chain`` (ALM-3269/3270).

The 2026-08-12 prod cluster's Arbitrum half: strategies keyed native ETH by
the zero address in ``anvil_funding``. Treated as an ERC-20, it failed
decimals discovery inside the funding batch and the fork refused to start —
with every other (perfectly fundable) token named in the failure list. The
parse layer must reject the zero address up front with a message that names
the one key that means "native".

``ManagedGateway.__init__`` starts real infrastructure, so these tests build
the instance via ``__new__`` and set only the attribute the parser reads.
"""

from __future__ import annotations

from decimal import Decimal

from almanak.framework.data.tokens.defaults import NATIVE_SENTINEL
from almanak.gateway.managed import ManagedGateway

ZERO_ADDRESS = "0x0000000000000000000000000000000000000000"
USDC_ARBITRUM = "0xaf88d065e77c8cc2239327c5edb3a432268e5831"


def _gateway_with_funding(funding: dict) -> ManagedGateway:
    mg = ManagedGateway.__new__(ManagedGateway)
    mg._anvil_funding = funding
    return mg


def test_zero_address_key_canonicalizes_to_native(caplog) -> None:
    """ALM-3269/3270/3288: the zero address folds into the native lane (it is
    the deploy-side surfaces' native representation, and no ERC-20 can exist
    there), with a warning recommending the sentinel."""
    import logging

    mg = _gateway_with_funding({ZERO_ADDRESS: "1.5"})

    with caplog.at_level(logging.WARNING, logger="almanak.gateway.managed"):
        _chain_native, native_amount, erc20_tokens = mg._parse_anvil_funding_for_chain("arbitrum")

    assert native_amount == Decimal("1.5")
    assert erc20_tokens == {}
    assert any(NATIVE_SENTINEL in rec.message or NATIVE_SENTINEL in str(rec.args) for rec in caplog.records)


def test_zero_address_and_sentinel_amounts_sum_into_native() -> None:
    mg = _gateway_with_funding({ZERO_ADDRESS: "1", NATIVE_SENTINEL: "2"})

    _chain_native, native_amount, erc20_tokens = mg._parse_anvil_funding_for_chain("arbitrum")

    assert native_amount == Decimal("3")
    assert erc20_tokens == {}


def test_native_sentinel_key_still_funds_native() -> None:
    mg = _gateway_with_funding({NATIVE_SENTINEL: "2", USDC_ARBITRUM: "100"})

    chain_native, native_amount, erc20_tokens = mg._parse_anvil_funding_for_chain("arbitrum")

    assert chain_native == "ETH"
    assert native_amount == Decimal("2")
    assert erc20_tokens == {USDC_ARBITRUM: Decimal("100")}


def test_regular_erc20_addresses_unaffected() -> None:
    mg = _gateway_with_funding({USDC_ARBITRUM: "250"})

    _chain_native, native_amount, erc20_tokens = mg._parse_anvil_funding_for_chain("arbitrum")

    assert native_amount == Decimal("0")
    assert erc20_tokens == {USDC_ARBITRUM: Decimal("250")}


def test_zero_address_inside_per_chain_section_canonicalizes() -> None:
    """The Arbitrum sessions used nested {chain: {address: amount}} sections —
    the zero-address key folds to native and the ERC-20 sibling is untouched."""
    mg = _gateway_with_funding({"arbitrum": {ZERO_ADDRESS: "1", USDC_ARBITRUM: "100"}})

    _chain_native, native_amount, erc20_tokens = mg._parse_anvil_funding_for_chain("arbitrum")

    assert native_amount == Decimal("1")
    assert erc20_tokens == {USDC_ARBITRUM: Decimal("100")}
