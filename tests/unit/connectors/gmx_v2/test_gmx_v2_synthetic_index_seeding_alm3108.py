"""Offline invariant: every listed GMX market can seed its INDEX price (ALM-3108).

The managed-Anvil keeper seeds the GMX Oracle before executing a pending order.
It used to price every token ``Reader.getMarket`` returns by its raw on-chain
address, which silently assumed every index token is a deployed ERC-20. GMX
*synthetic* index tokens are identifier addresses with **no contract**: BTC on
Arbitrum is ``0x47904963fc8b2340414262125aF798B9655E58Cd``, where ``decimals()``
answers nothing and no token registry maps the address to a price. Six of
fifteen Arbitrum markets could not complete a fork lifecycle, and the one that
mattered most — BTC/USD, the second both-sides market — failed while the SDK was
already shipping a BTC/USD Chainlink feed and a static index-decimals table it
simply never consulted.

Why the seeding path and not a data-only assert. A pure ``GMX_V2_MARKETS`` ×
``TOKEN_TO_PAIR`` cross-check would be green on both sides of the fix: it never
touches the executor, so it cannot see whether the executor *uses* the route.
These tests drive ``_seed_oracle_prices`` against a fake chain whose only
interesting property is the real one — the synthetic index addresses have no
code — so the failure they report is the failure a fork reports.

Measured before/after on the 11 markets with an offline-provable index route.
``test_every_routable_market_seeds_its_index_price`` — the outcome — failed for
exactly 1 of the 11 before the fix (``arbitrum:BTC/USD``, the only synthetic
market in that set) and 0 after.
``test_no_market_asks_its_index_address_for_anything`` — the mechanism — failed
for all 11 before and 0 after, because the pre-fix executor read ``decimals()``
off every index address including the ones that answer.

The other 9 listed markets are quarantined in ``QUARANTINED_MARKETS`` below, by
name and with a ticket. They have no Chainlink feed for their base symbol on
their own chain, so no offline route exists to assert; five of them additionally
have a synthetic index token and therefore no address route either. That is a
data gap in the price registry, not a defect in the seeding logic.
"""

from __future__ import annotations

from decimal import Decimal
from types import SimpleNamespace
from typing import Any
from unittest.mock import patch

import pytest
from eth_abi import decode as abi_decode
from eth_abi import encode as abi_encode
from eth_utils import to_checksum_address

from almanak.connectors.gmx_v2 import anvil_order_executor as executor
from almanak.connectors.gmx_v2.addresses import (
    GMX_V2_MARKETS,
    GMX_V2_TOKENS,
    index_token_decimals,
)
from almanak.connectors.gmx_v2.anvil_order_executor import (
    GmxAnvilOrderExecutionError,
    _GmxDependencies,
    _seed_oracle_prices,
)
from almanak.core.chainlink import CHAINLINK_PRICE_FEEDS, TOKEN_TO_PAIR

# The index token every market reports on-chain, and the set whose index token
# is a synthetic placeholder. Imported rather than re-pinned: these were
# captured from ``Reader.getMarket()`` for the market-identity audit, and a
# second hand-maintained copy would be free to drift away from the chain
# without either copy noticing.
from tests.audit.test_gmx_v2_market_identity import (
    EXPECTED_INDEX_TOKENS,
    SYNTHETIC_INDEX_MARKETS,
)

ALL_MARKETS = [(chain, market) for chain, markets in GMX_V2_MARKETS.items() for market in markets]

# Listed markets with no Chainlink feed for their base symbol on their own
# chain. Named individually and paired with a ticket, never derived: a
# quarantine computed from the same predicate the test asserts would accept any
# regression as "expected". ``test_the_quarantine_is_exactly_the_unroutable_set``
# is what keeps it honest in both directions.
#
# Two shapes, both blocked on the same missing data:
#   * synthetic index token AND no feed - no route of any kind (5 markets);
#   * real ERC-20 index token, no feed on THIS chain - the gateway's secondary
#     sources may still answer, exactly as before this fix (4 markets).
_PRICE_GAP_TICKET = "ALM-3117"
QUARANTINED_MARKETS: dict[tuple[str, str], str] = {
    ("arbitrum", "DOGE/USD"): _PRICE_GAP_TICKET,
    ("arbitrum", "LTC/USD"): _PRICE_GAP_TICKET,
    ("arbitrum", "XRP/USD"): _PRICE_GAP_TICKET,
    ("arbitrum", "ATOM/USD"): _PRICE_GAP_TICKET,
    ("arbitrum", "NEAR/USD"): _PRICE_GAP_TICKET,
    ("arbitrum", "AVAX/USD"): _PRICE_GAP_TICKET,
    ("arbitrum", "OP/USD"): _PRICE_GAP_TICKET,
    ("avalanche", "SOL/USD"): _PRICE_GAP_TICKET,
    ("avalanche", "LTC/USD"): _PRICE_GAP_TICKET,
}

ROUTABLE_MARKETS = [pair for pair in ALL_MARKETS if pair not in QUARANTINED_MARKETS]

# Collateral stand-ins for the fake chain. Their identity is irrelevant here —
# what matters is that they are real, deployed ERC-20s, which is the property
# that keeps them on the address route. Real per-market collateral identity is
# audited against the chain in tests/audit/test_gmx_v2_market_identity.py.
_COLLATERALS: dict[str, tuple[tuple[str, int], ...]] = {
    "arbitrum": ((GMX_V2_TOKENS["arbitrum"]["WETH"], 18), (GMX_V2_TOKENS["arbitrum"]["USDC"], 6)),
    "avalanche": ((GMX_V2_TOKENS["avalanche"]["WAVAX"], 18), (GMX_V2_TOKENS["avalanche"]["USDC"], 6)),
}

_ORDER_HANDLER = to_checksum_address("0x" + "66" * 20)
_ORACLE = to_checksum_address("0x" + "77" * 20)
_ROLE_STORE = to_checksum_address("0x" + "88" * 20)
_DATA_STORE = to_checksum_address("0x" + "99" * 20)
_READER = to_checksum_address("0x" + "aa" * 20)
_DEPENDENCIES = _GmxDependencies(
    order_handler=_ORDER_HANDLER,
    oracle=_ORACLE,
    role_store=_ROLE_STORE,
    data_store=_DATA_STORE,
    reader=_READER,
)

_SELECTORS = {
    name: executor._selector(signature).hex()
    for name, signature in (
        ("get_market", executor._GET_MARKET_SIGNATURE),
        ("decimals", executor._DECIMALS_SIGNATURE),
        ("price_count", executor._GET_TOKENS_WITH_PRICES_COUNT_SIGNATURE),
    )
}
_SET_PRIMARY_PRICE_SELECTOR = executor._selector(executor._SET_PRIMARY_PRICE_SIGNATURE).hex()

# One nominal USD price per pair. Values are arbitrary; only the SCALE of the
# seeded bound is asserted, and that comes from the market's declared decimals.
_USD_PRICE = Decimal("100")


class _FakeChain:
    """A fork whose synthetic index addresses genuinely have no code.

    ``eth_call`` against an address with no deployed contract returns empty data
    (``0x``) rather than reverting, which is why the production decimals read
    fails with "could not decode" rather than a revert string. Reproducing that
    exact shape is the point of this fake: a mock that raised instead would
    still catch the defect, but would not prove the executor handles the shape a
    real fork produces.
    """

    def __init__(self, chain: str) -> None:
        self.chain = chain
        self.deployed: dict[str, int] = {address.lower(): decimals for address, decimals in _COLLATERALS[chain]}
        for market in GMX_V2_MARKETS[chain]:
            if (chain, market) in SYNTHETIC_INDEX_MARKETS:
                continue
            index = EXPECTED_INDEX_TOKENS[chain][market]
            declared = index_token_decimals(chain, GMX_V2_MARKETS[chain][market])
            assert declared is not None, f"{chain}:{market} has no declared index decimals"
            self.deployed[index.lower()] = declared
        self.eth_call_targets: list[str] = []

    def market_props(self, market_address: str) -> tuple[str, str, str, str]:
        label = next(
            (name for name, address in GMX_V2_MARKETS[self.chain].items() if address.lower() == market_address.lower()),
            None,
        )
        long_token, short_token = (address for address, _decimals in _COLLATERALS[self.chain])
        # An UNLISTED market is a live market this SDK's catalogue does not
        # know. The chain still answers with a perfectly good index token — a
        # real one, here — which is precisely why the executor must refuse on
        # the catalogue miss rather than on a chain read.
        index = EXPECTED_INDEX_TOKENS[self.chain][label] if label else long_token
        return (market_address, index, long_token, short_token)

    def make_request(self, method: str, params: list[Any]) -> dict[str, Any]:
        if method == "eth_getBlockByNumber":
            return {"result": {"timestamp": "0x64000000"}}
        if method != "eth_call":
            raise AssertionError(f"unexpected RPC on the offline seeding path: {method}")
        call = params[0]
        to = str(call["to"])
        data = str(call["data"]).removeprefix("0x")
        selector, payload = data[:8], bytes.fromhex(data[8:])
        self.eth_call_targets.append(to.lower())
        if selector == _SELECTORS["price_count"]:
            return {"result": "0x" + abi_encode(["uint256"], [0]).hex()}
        if selector == _SELECTORS["get_market"]:
            _data_store, market = abi_decode(["address", "address"], payload)
            props = self.market_props(market)
            return {"result": "0x" + abi_encode(["(address,address,address,address)"], [props]).hex()}
        if selector == _SELECTORS["decimals"]:
            decimals = self.deployed.get(to.lower())
            if decimals is None:
                # No contract at this address: empty return data, no revert.
                return {"result": "0x"}
            return {"result": "0x" + abi_encode(["uint256"], [decimals]).hex()}
        raise AssertionError(f"unexpected eth_call selector {selector} to {to}")


class _FakeGatewayPrices:
    """The gateway price service's two routes, with their real preconditions.

    * an ADDRESS resolves only when a token registry knows it — modelled here as
      "the address has a deployed ERC-20", which is what makes a synthetic index
      address unresolvable in production;
    * a SYMBOL resolves when ``TOKEN_TO_PAIR`` names a pair and this chain
      carries that Chainlink feed. Both tables are the real ones.
    """

    def __init__(self, fake_chain: _FakeChain) -> None:
        self._chain = fake_chain
        self.requested: list[str] = []
        self.market = SimpleNamespace(GetPrice=self._get_price)

    def _get_price(self, request: Any) -> SimpleNamespace:
        token = str(request.token)
        self.requested.append(token)
        if token.lower().startswith("0x"):
            if token.lower() not in self._chain.deployed:
                raise RuntimeError(f"All data sources failed: unknown token {token}")
            return SimpleNamespace(price=str(_USD_PRICE), stale=False)
        pair = TOKEN_TO_PAIR.get(token.upper())
        if not pair or pair not in CHAINLINK_PRICE_FEEDS.get(self._chain.chain, {}):
            raise RuntimeError(f"All data sources failed: no Chainlink feed for {token}")
        return SimpleNamespace(price=str(_USD_PRICE), stale=False)


def _seed(chain: str, market: str) -> tuple[dict[str, tuple[int, int]], _FakeChain, _FakeGatewayPrices]:
    """Run one market through the real seeding path; return the seeded bounds."""
    fake_chain = _FakeChain(chain)
    gateway = _FakeGatewayPrices(fake_chain)
    seeded: dict[str, tuple[int, int]] = {}

    def _capture(provider: Any, sender: str, target: str, data: str, *, kind: str) -> str:
        body = str(data).removeprefix("0x")
        if body[:8] == _SET_PRIMARY_PRICE_SELECTOR:
            token, bounds = abi_decode(["address", "(uint256,uint256)"], bytes.fromhex(body[8:]))
            seeded[to_checksum_address(token)] = (int(bounds[0]), int(bounds[1]))
        return "0x" + "ab" * 32

    with patch.object(executor, "_send_transaction", side_effect=_capture):
        _seed_oracle_prices(
            gateway_client=gateway,
            provider=fake_chain,
            dependencies=_DEPENDENCIES,
            chain=chain,
            markets=(to_checksum_address(GMX_V2_MARKETS[chain][market]),),
        )
    return seeded, fake_chain, gateway


def _has_offline_index_route(chain: str, market: str) -> bool:
    pair = TOKEN_TO_PAIR.get(market.split("/")[0].upper())
    has_feed = bool(pair) and pair in CHAINLINK_PRICE_FEEDS.get(chain, {})
    return has_feed and index_token_decimals(chain, GMX_V2_MARKETS[chain][market]) is not None


@pytest.mark.parametrize(("chain", "market"), ROUTABLE_MARKETS, ids=lambda v: str(v))
def test_every_routable_market_seeds_its_index_price(chain: str, market: str) -> None:
    """The invariant, stated as the outcome: a seeded index price at the right scale.

    Pre-fix this failed for exactly one of the 11 rows — ``arbitrum:BTC/USD``,
    the only synthetic-index market that has a Chainlink feed — and passed for
    the other 10, whose index tokens are deployed ERC-20s. Kept separate from
    the mechanism guard below so that count stays readable: an outcome
    assertion and an implementation assertion fail for different reasons and
    would otherwise be indistinguishable in a red run.
    """
    seeded, _fake_chain, _gateway = _seed(chain, market)

    index = to_checksum_address(EXPECTED_INDEX_TOKENS[chain][market])
    assert index in seeded, f"{chain}:{market} seeded no price for its index token"

    # The bound must be scaled by the market's DECLARED decimals, not by
    # whatever the address happened to answer: a wrong scale is a wrong oracle
    # price, which fills an order at a fabricated level.
    decimals = index_token_decimals(chain, GMX_V2_MARKETS[chain][market])
    expected = int(_USD_PRICE * (Decimal(10) ** (executor._GMX_USD_DECIMALS - decimals)))
    assert seeded[index] == (expected, expected)


@pytest.mark.parametrize(("chain", "market"), ROUTABLE_MARKETS, ids=lambda v: str(v))
def test_no_market_asks_its_index_address_for_anything(chain: str, market: str) -> None:
    """The mechanism, which must hold for EVERY market, not just the synthetic ones.

    A fix that merely special-cased the six known synthetic markets would leave
    the next GMX synthetic listing broken on the day it ships. Asserting the
    index address is never called makes the property structural: the index
    route is the route, and whether a contract happens to exist there is not
    something the executor is allowed to depend on.

    Pre-fix this failed for all 11 rows — that is the ``decimals()`` call the
    ticket identifies as the leg a gateway-side price fix alone cannot clear.
    """
    _seeded, fake_chain, _gateway = _seed(chain, market)

    index = EXPECTED_INDEX_TOKENS[chain][market].lower()
    assert index not in fake_chain.eth_call_targets


@pytest.mark.parametrize(
    ("chain", "market"),
    sorted(SYNTHETIC_INDEX_MARKETS - set(QUARANTINED_MARKETS)),
    ids=lambda v: str(v),
)
def test_a_synthetic_index_market_seeds_from_market_metadata_alone(chain: str, market: str) -> None:
    """The reproduction, stated directly: arbitrum:BTC/USD has no code at its index.

    Kept separate from the parametrized invariant above so the row this ticket
    exists for is named in the test report rather than buried in a sweep.
    """
    assert EXPECTED_INDEX_TOKENS[chain][market].lower() not in _FakeChain(chain).deployed

    seeded, _fake_chain, gateway = _seed(chain, market)

    assert market.split("/")[0] in gateway.requested
    assert to_checksum_address(EXPECTED_INDEX_TOKENS[chain][market]) in seeded


def test_collateral_tokens_still_resolve_by_address_and_measured_decimals() -> None:
    """Regression guard: the real-ERC-20 path must be untouched.

    Long/short tokens are deployed ERC-20s. They keep the address price lookup
    AND the live ``decimals()`` read — the executor must not start guessing
    their scale from market metadata, which knows nothing about them.
    """
    seeded, fake_chain, gateway = _seed("arbitrum", "ETH/USD")

    for address, decimals in _COLLATERALS["arbitrum"]:
        token = to_checksum_address(address)
        if token == to_checksum_address(EXPECTED_INDEX_TOKENS["arbitrum"]["ETH/USD"]):
            # WETH is ETH/USD's own index token: one oracle key, one price,
            # taken from the index route.
            continue
        assert token in seeded, f"{token} was not seeded"
        assert address.lower() in [t.lower() for t in gateway.requested], "collateral was not priced by ADDRESS"
        assert address.lower() in fake_chain.eth_call_targets, "collateral decimals were not measured on-chain"
        expected = int(_USD_PRICE * (Decimal(10) ** (executor._GMX_USD_DECIMALS - decimals)))
        assert seeded[token] == (expected, expected)


def test_the_quarantine_is_exactly_the_unroutable_set() -> None:
    """The quarantine may not rot in either direction.

    Drop a feed and the market must appear here; add one and it must leave.
    Without this the quarantine would quietly absorb a regression as expected
    behaviour, which is the failure mode a named-skip list exists to prevent.
    """
    unroutable = {pair for pair in ALL_MARKETS if not _has_offline_index_route(*pair)}
    assert unroutable == set(QUARANTINED_MARKETS)


def test_every_quarantined_market_cites_a_ticket() -> None:
    assert all(str(ticket).startswith(("ALM-", "VIB-")) for ticket in QUARANTINED_MARKETS.values())


def test_an_unlisted_market_fails_closed_rather_than_seeding_a_guess() -> None:
    """``Empty ≠ Zero``: no symbol means no price, never a fabricated one."""
    fake_chain = _FakeChain("arbitrum")
    with (
        patch.object(executor, "_send_transaction", return_value="0x" + "ab" * 32),
        pytest.raises(GmxAnvilOrderExecutionError, match="not listed"),
    ):
        _seed_oracle_prices(
            gateway_client=_FakeGatewayPrices(fake_chain),
            provider=fake_chain,
            dependencies=_DEPENDENCIES,
            chain="arbitrum",
            markets=(to_checksum_address("0x" + "de" * 20),),
        )
