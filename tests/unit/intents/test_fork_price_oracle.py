"""Regression tests for block-consistent Intent proof price inputs."""

from decimal import Decimal
from types import SimpleNamespace

import pytest

from almanak.connectors.aave_v3.addresses import AAVE_V3_TOKENS
from tests.intents.conftest import _aave_v3_fork_price_oracle


class _Call:
    def __init__(self, value: int, observed_blocks: list[int | None]) -> None:
        self._value = value
        self._observed_blocks = observed_blocks

    def call(self, *, block_identifier=None):
        self._observed_blocks.append(block_identifier)
        return self._value


class _OracleFunctions:
    def __init__(self, prices: dict[str, int], observed_blocks: list[int | None]) -> None:
        self._prices = prices
        self._observed_blocks = observed_blocks

    def BASE_CURRENCY_UNIT(self):  # noqa: N802 - mirrors the contract ABI
        return _Call(100_000_000, self._observed_blocks)

    def getAssetPrice(self, address: str):  # noqa: N802 - mirrors the contract ABI
        return _Call(self._prices[address.lower()], self._observed_blocks)


class _Eth:
    def __init__(
        self,
        contract,
        *,
        block_numbers: tuple[int, int] = (123456, 123456),
    ) -> None:
        self._contract = contract
        self._block_numbers = iter(block_numbers)

    @property
    def block_number(self) -> int:
        return next(self._block_numbers)

    def contract(self, **_kwargs):
        return self._contract


def _web3_with_prices(
    prices: dict[str, int],
    observed_blocks: list[int | None],
    *,
    block_numbers: tuple[int, int] = (123456, 123456),
):
    contract = SimpleNamespace(functions=_OracleFunctions(prices, observed_blocks))
    eth = _Eth(contract, block_numbers=block_numbers)
    return SimpleNamespace(eth=eth)


def test_aave_exact_proof_prices_are_all_read_at_one_fork_block() -> None:
    tokens = AAVE_V3_TOKENS["arbitrum"]
    prices = {address.lower(): 200_000_000 for address in tokens.values()}
    observed_blocks: list[int | None] = []

    result = _aave_v3_fork_price_oracle("arbitrum", _web3_with_prices(prices, observed_blocks))

    assert result == {symbol: Decimal("2") for symbol in tokens}
    assert observed_blocks == [None] * (len(tokens) + 1)


def test_aave_exact_proof_rejects_an_unpriced_canonical_reserve() -> None:
    tokens = AAVE_V3_TOKENS["arbitrum"]
    prices = {address.lower(): 200_000_000 for address in tokens.values()}
    prices[next(iter(tokens.values())).lower()] = 0

    with pytest.raises(RuntimeError, match="returned no price for canonical"):
        _aave_v3_fork_price_oracle("arbitrum", _web3_with_prices(prices, []))


def test_aave_exact_proof_rejects_head_drift_during_price_reads() -> None:
    tokens = AAVE_V3_TOKENS["arbitrum"]
    prices = {address.lower(): 200_000_000 for address in tokens.values()}

    with pytest.raises(RuntimeError, match=r"Anvil head moved.*123456 -> 123457"):
        _aave_v3_fork_price_oracle(
            "arbitrum",
            _web3_with_prices(prices, [], block_numbers=(123456, 123457)),
        )
