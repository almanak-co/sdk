"""Pool native custody is a scoped canonical historical read, including offline replay."""

import pytest

from almanak.backtest.history import Capture, ReadRequest, validate_read
from almanak.caip import ChainId
from almanak.models import Chain

CHAIN = Chain(chain_id=ChainId("eip155:1"), environment="mainnet")
POOL = "0x" + "11" * 20
ANCHOR = {"blockHash": "0x" + "ab" * 32, "requireCanonical": True}


def test_native_pool_balance_is_captured_and_reproduced(tmp_path):
    request = ReadRequest(chain=CHAIN, method="eth_getBalance", params=(POOL, ANCHOR))
    validate_read(request, (POOL,))
    file = tmp_path / "capture.sqlite"
    first = Capture(file)
    try:
        retained = first.put(request, "0x123456")
    finally:
        first.close()
    second = Capture(file)
    try:
        assert second.get(request) == retained
    finally:
        second.close()


@pytest.mark.parametrize(
    "params",
    [
        ("0x" + "22" * 20, ANCHOR),
        (POOL, "latest"),
        (POOL, dict(ANCHOR, requireCanonical=False)),
        (POOL, ANCHOR, {}),
        ({"to": POOL}, ANCHOR),
        (POOL, {"blockNumber": "0x1"}),
    ],
)
def test_native_balance_cannot_escape_market_and_canonical_scope(params):
    request = ReadRequest(chain=CHAIN, method="eth_getBalance", params=params)
    with pytest.raises(ValueError):
        validate_read(request, (POOL,))
