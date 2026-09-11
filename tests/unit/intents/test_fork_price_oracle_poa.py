"""The fork price oracle must read Chainlink through PoA-aware block decoding.

On a PoA chain every `eth_getBlock` raises `ExtraDataLengthError` unless the
middleware remaps `extraData`. `_fetch_prices_from_fork` reads the block header
before each feed, so without the middleware every BSC/Polygon/Avalanche feed read
fails and the chain falls back to LIVE CoinGecko while the quoter stays at the
pinned fork block — the pinned-vs-live skew the function exists to remove. That
failure is silent on a calm day and only surfaces as a price-impact refusal once
the asset has moved, so it needs a control that does not depend on the market.
"""

from __future__ import annotations

from decimal import Decimal

import pytest
from web3 import Web3
from web3.exceptions import ExtraDataLengthError
from web3.providers.base import BaseProvider

from almanak.gateway.utils.rpc_provider import inject_poa_middleware

# A real BSC header's extraData: 280 bytes where a PoW chain allows 32.
_BSC_EXTRA_DATA = "0xda" + "00" * 279


class _PoAHeaderProvider(BaseProvider):
    """Return one PoA-shaped block header for any request."""

    endpoint_uri = "http://127.0.0.1:0"

    def make_request(self, method: str, params: object) -> dict:
        return {
            "jsonrpc": "2.0",
            "id": 1,
            "result": {
                "number": "0x72d7a9d",
                "hash": "0x" + "11" * 32,
                "parentHash": "0x" + "22" * 32,
                "nonce": "0x0000000000000000",
                "sha3Uncles": "0x" + "33" * 32,
                "logsBloom": "0x" + "00" * 256,
                "transactionsRoot": "0x" + "44" * 32,
                "stateRoot": "0x" + "55" * 32,
                "receiptsRoot": "0x" + "66" * 32,
                "miner": "0x" + "77" * 20,
                "difficulty": "0x2",
                "totalDifficulty": "0x2",
                "extraData": _BSC_EXTRA_DATA,
                "size": "0x100",
                "gasLimit": "0x1c9c380",
                "gasUsed": "0x5208",
                "timestamp": "0x68bd3f66",
                "transactions": [],
                "uncles": [],
            },
        }


def _web3_over_poa_header() -> Web3:
    return Web3(_PoAHeaderProvider())


def test_a_poa_header_is_undecodable_without_the_middleware() -> None:
    with pytest.raises(ExtraDataLengthError):
        _web3_over_poa_header().eth.get_block(120421021)


def test_inject_poa_middleware_makes_a_bsc_header_readable() -> None:
    w3 = _web3_over_poa_header()
    inject_poa_middleware(w3, "bsc")
    assert w3.eth.get_block(120421021)["number"] == 120421021


def test_inject_poa_middleware_leaves_a_non_poa_chain_alone() -> None:
    w3 = _web3_over_poa_header()
    inject_poa_middleware(w3, "ethereum")
    with pytest.raises(ExtraDataLengthError):
        w3.eth.get_block(120421021)


def test_the_fork_price_reader_configures_poa_before_reading_a_header(monkeypatch) -> None:
    """Reverting the conftest call fails here even when no chain is reachable."""
    from almanak.gateway.utils import rpc_provider
    from tests.intents import conftest as intent_conftest

    seen: list[tuple[object, str]] = []
    real = rpc_provider.inject_poa_middleware

    def spy(web3: object, chain: str) -> None:
        real(web3, chain)
        seen.append((web3, chain))

    monkeypatch.setattr(rpc_provider, "inject_poa_middleware", spy)
    monkeypatch.setenv("ANVIL_FORK_BLOCK_BSC", "120421021")
    # Port 1 has no listener, so the reader returns after configuring the client.
    monkeypatch.setenv("ANVIL_BSC_PORT", "1")

    assert intent_conftest._fetch_prices_from_fork("bsc") == {}
    assert [chain for _, chain in seen] == ["bsc"]


class _StubRequest:
    """Minimal pytest request stand-in: the fixture only calls getfixturevalue."""

    def __init__(self, chain: str) -> None:
        self.chain = chain

    def getfixturevalue(self, name: str) -> None:
        return None


def _run_oracle_fixture(chain: str, *, pinned: dict, live: dict, monkeypatch):
    """Drive price_oracle_fixture with a chosen pinned/live split."""
    from tests.intents import conftest as intent_conftest

    monkeypatch.setattr(intent_conftest, "_fetch_prices_from_fork", lambda _c: dict(pinned))
    monkeypatch.setattr(intent_conftest, "_fetch_prices_sync", lambda _c: dict(live))
    fixture = intent_conftest._create_price_oracle_fixture(chain)
    return fixture.__wrapped__(_StubRequest(chain))


def test_a_volatile_native_priced_live_against_pinned_stables_is_refused(monkeypatch) -> None:
    """WBNB is outside the ETH-symbol set the old guard used, so this is the
    shape that previously fell through and was reported as bounded."""
    from tests.intents.conftest import CHAIN_CONFIGS

    live = {s: Decimal("1") for s in CHAIN_CONFIGS["bsc"]["tokens"]}
    live["WBNB"] = Decimal("709.5")
    result = _run_oracle_fixture(
        "bsc",
        pinned={"USDC": Decimal("0.9998"), "USDT": Decimal("0.9998")},
        live=live,
        monkeypatch=monkeypatch,
    )
    # Refusal means the whole chain drops to the live dict, which is at least
    # self-consistent; keeping the mix would put the pin age in the WBNB ratio.
    assert result == live
    assert result["USDC"] == Decimal("1")


def test_a_pegged_stable_priced_live_against_pinned_volatile_is_kept(monkeypatch) -> None:
    """The real BSC case: the USDC/USD feed address is dead, so USDC alone goes
    live while WBNB pins. Both legs of a WBNB pair stay time-aligned."""
    pinned = {"USDT": Decimal("0.99983"), "WBNB": Decimal("748.24343561")}
    result = _run_oracle_fixture(
        "bsc",
        pinned=pinned,
        live={"USDC": Decimal("0.999873"), "USDT": Decimal("0.999727"), "WBNB": Decimal("709.59")},
        monkeypatch=monkeypatch,
    )
    assert result["WBNB"] == pinned["WBNB"]
    assert result["USDT"] == pinned["USDT"]
    assert result["USDC"] == Decimal("0.999873")


def test_an_unknown_token_counts_as_volatile_and_is_refused(monkeypatch) -> None:
    """A token the peg registry cannot name must fail loudly, not be assumed stable."""
    from tests.intents.conftest import CHAIN_CONFIGS

    monkeypatch.setitem(CHAIN_CONFIGS["bsc"]["tokens"], "MYSTERY", "0x" + "ab" * 20)
    live = {s: Decimal("1") for s in CHAIN_CONFIGS["bsc"]["tokens"]}
    result = _run_oracle_fixture(
        "bsc",
        pinned={"USDC": Decimal("0.9998"), "USDT": Decimal("0.9998"), "WBNB": Decimal("748")},
        live=live,
        monkeypatch=monkeypatch,
    )
    assert result == live
