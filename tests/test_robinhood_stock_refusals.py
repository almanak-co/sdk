"""Stock identity refuses another chain, proxy, beacon slot or beacon runtime."""

from types import SimpleNamespace

import pytest

from almanak.caip import ChainId
from almanak.gateway import robinhood_stock as stock
from almanak.gateway.ethereum import DataError

ADDRESS = "0xd0601ce157db5bdc3162bbac2a2c8af5320d9eec"


def reader(chain="eip155:4663", beacon=stock.BEACON, beacon_code=b"\x60\x80"):
    return SimpleNamespace(
        reader=SimpleNamespace(chain=SimpleNamespace(chain_id=ChainId(chain))),
        storage=lambda address, slot: int(beacon, 16) if slot == stock.BEACON_SLOT else 0,
        code=lambda address: beacon_code,
    )


@pytest.mark.parametrize(
    "chain,code_hash",
    [("eip155:1", stock.PROXY_HASH), ("eip155:4663", "0x" + "00" * 32)],
)
def test_another_chain_or_proxy_runtime_is_refused(chain, code_hash):
    with pytest.raises(DataError, match="verified Robinhood scope"):
        stock.identity(reader(chain), ADDRESS, code_hash)


def test_a_stored_beacon_that_differs_from_the_immutable_is_refused():
    with pytest.raises(DataError, match="immutable dispatch"):
        stock.identity(reader(beacon="0x" + "11" * 20), ADDRESS, stock.PROXY_HASH)


def test_a_beacon_runtime_outside_the_verified_registry_revision_is_refused():
    with pytest.raises(DataError, match="verified registry revision"):
        stock.identity(reader(), ADDRESS, stock.PROXY_HASH)
