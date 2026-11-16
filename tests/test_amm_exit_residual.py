"""Aerodrome and Curve zero-output quotes are dust only while both reserves are positive.

Scripted contract results; the economic dust exits run with the captured component
models in `test_aerodrome_economic_model.py` and `test_curve_economic_model.py`.
"""

from types import SimpleNamespace

import pytest

from almanak.caip import AssetType
from almanak.connectors.aerodrome import reader as aerodrome_reader
from almanak.connectors.curve import reader as curve_reader
from almanak.gateway.ethereum import DataError
from almanak.models import AssetAmount

TOKEN0 = AssetType("eip155:8453/erc20:0x4200000000000000000000000000000000000006")
TOKEN1 = AssetType("eip155:8453/erc20:0x833589fcd6edb6e08f4c7c32d4f71b54bda02913")
DUST = AssetAmount(asset=TOKEN0, amount=7)


@pytest.mark.parametrize(("reserves", "empty"), [((10**18, 10**6), False), ((10**18, 0), True)])
def test_aerodrome_router_zero_output_needs_both_reserves(monkeypatch, reserves, empty):
    for name in ("verify_pool", "verify_router"):
        monkeypatch.setattr(aerodrome_reader, name, lambda *args, **kwargs: None)
    monkeypatch.setattr(
        aerodrome_reader,
        "read_pool",
        lambda *args: SimpleNamespace(reserve0=reserves[0], reserve1=reserves[1]),
    )
    source = SimpleNamespace(currency0=TOKEN0, currency1=TOKEN1, stable=False, factory="0x1")
    reader = SimpleNamespace(call=lambda *args, **kwargs: ([DUST.amount, 0],))
    contracts = SimpleNamespace(router="0x2")
    if empty:
        with pytest.raises(DataError, match="empty reserve"):
            aerodrome_reader.quote(reader, source, contracts, None, DUST, TOKEN1)
    else:
        assert aerodrome_reader.quote(reader, source, contracts, None, DUST, TOKEN1) == 0


@pytest.mark.parametrize(("balances", "empty"), [((10**18, 10**6), False), ((0, 10**6), True)])
def test_curve_zero_output_needs_both_coin_balances(monkeypatch, balances, empty):
    monkeypatch.setattr(curve_reader, "verify_pool", lambda *args, **kwargs: None)
    monkeypatch.setattr(curve_reader, "layout", lambda source: SimpleNamespace(index="int128"))
    source = SimpleNamespace(coins=(TOKEN0, TOKEN1), pool="0x3")

    def call(address, method, block, *, inputs=(), args=(), outputs=None):
        return [balances[args[0]]] if method.startswith("balances") else [0]

    reader = SimpleNamespace(call=call)
    if empty:
        with pytest.raises(DataError, match="empty balance"):
            curve_reader.quote(reader, source, None, DUST, TOKEN1)
    else:
        assert curve_reader.quote(reader, source, None, DUST, TOKEN1) == 0
