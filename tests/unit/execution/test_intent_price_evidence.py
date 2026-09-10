"""Intent prices retain provider observations through compiler and ledger shapes."""

from datetime import UTC, datetime
from decimal import Decimal
from unittest.mock import MagicMock

import pytest

from almanak.framework.market.models import PriceData
from almanak.integrations.chainlink.models import FeedSpec
from tests.intents._price_evidence import ObservedPrices, assert_lp_price_provenance
from tests.intents.conftest import _fetch_prices_sync, _read_chainlink


def observed():
    prices = ObservedPrices()
    prices.record(
        "WETH",
        PriceData(
            price=Decimal("2000"),
            source="onchain",
            timestamp=datetime(2026, 9, 10, tzinfo=UTC),
            raw_confidence=0.95,
            stale=False,
        ),
    )
    return prices


@pytest.mark.parametrize("method", ["assign", "update", "union", "delete", "pop", "popitem", "clear"])
def test_scalar_mutations_cannot_reuse_prior_observation(method):
    prices = observed()
    if method == "assign":
        prices["WETH"] = Decimal("2000")
    elif method == "update":
        prices.update(WETH=Decimal("2000"))
    elif method == "union":
        prices |= {"WETH": Decimal("2000")}
    elif method == "delete":
        del prices["WETH"]
    elif method == "pop":
        prices.pop("WETH")
    elif method == "popitem":
        prices.popitem()
    else:
        prices.clear()
    prices.setdefault("WETH", Decimal("2000"))
    assert prices.ledger_inputs()["WETH"] == Decimal("2000")


@pytest.mark.parametrize(
    "merge", [lambda p: p.copy(), lambda p: p | {"USDC": Decimal(1)}, lambda p: {"USDC": Decimal(1)} | p]
)
def test_merges_preserve_matching_records_and_scalar_compatibility(merge):
    prices = merge(observed())
    assert prices["WETH"] * Decimal(2) == 4000
    record = prices.ledger_inputs()["WETH"]
    assert record["confidence"] == "ESTIMATED"
    assert record["oracle_source"] == "onchain"
    assert record["observed_at"] == "2026-09-10T00:00:00+00:00"


def test_update_preserves_observed_replacement_and_self_update():
    prices = ObservedPrices({"WETH": Decimal(1)})
    prices.update(observed())
    prices.update(prices)
    assert prices.ledger_inputs()["WETH"]["confidence"] == "ESTIMATED"


@pytest.mark.parametrize("persisted", ["ESTIMATED", "HIGH", "UNAVAILABLE"])
def test_lp_assertion_requires_actual_provider_confidence(persisted):
    payload = {"token0": "WETH", "amount0": "1", "token1": "USDC", "amount1": "0", "confidence": persisted}
    if persisted == "ESTIMATED":
        assert_lp_price_provenance(payload, observed())
    else:
        with pytest.raises(AssertionError, match="observed ESTIMATED"):
            assert_lp_price_provenance(payload, observed())


def test_lp_assertion_refuses_scalar_provenance():
    payload = {"token0": "WETH", "amount0": "1", "token1": "USDC", "amount1": "0", "confidence": "HIGH"}
    with pytest.raises(AssertionError, match="lacks confidence"):
        assert_lp_price_provenance(payload, ObservedPrices({"WETH": Decimal(2000)}))


@pytest.mark.parametrize("age,expected", [(60, "ESTIMATED"), (4000, "STALE")])
def test_chainlink_uses_provider_timestamp_and_pinned_header_age(age, expected):
    w3 = MagicMock()
    timestamp = 1788998400
    w3.eth.get_block.return_value = {"number": 42, "timestamp": timestamp, "hash": bytes.fromhex("ab" * 32)}
    functions = w3.eth.contract.return_value.functions
    functions.decimals.return_value.call.return_value = 8
    functions.latestRoundData.return_value.call.return_value = (7, 2000 * 10**8, timestamp - age, timestamp - age, 7)
    spec = FeedSpec(chain="polygon", chain_id=137, pair="ETH/USD", address="0x" + "1" * 40)
    result = _read_chainlink(w3, spec, 42, "WETH")
    assert result.to_oracle_entry()["confidence"] == expected
    assert result.timestamp == datetime.fromtimestamp(timestamp - age, tz=UTC)
    assert result.raw_confidence == (0.95 if age == 60 else 0.85)
    functions.latestRoundData.return_value.call.assert_called_once_with(block_identifier=42)


@pytest.mark.parametrize("updated_at", [0, 1788998461])
def test_chainlink_invalid_observation_is_not_promoted(updated_at):
    w3 = MagicMock()
    w3.eth.get_block.return_value = {"number": 42, "timestamp": 1788998400, "hash": bytes.fromhex("ab" * 32)}
    functions = w3.eth.contract.return_value.functions
    functions.decimals.return_value.call.return_value = 8
    functions.latestRoundData.return_value.call.return_value = (7, 10**8, 0, updated_at, 7)
    spec = FeedSpec(chain="polygon", chain_id=137, pair="ETH/USD", address="0x" + "1" * 40)
    assert _read_chainlink(w3, spec, 42, "WETH") is None


@pytest.mark.parametrize(
    "timestamp,expected",
    [(1788998450, "HIGH"), (1788998400, "STALE"), (1788998461, "UNAVAILABLE"), (None, "UNAVAILABLE")],
)
def test_coingecko_preserves_observation_time_or_marks_it_unknown(monkeypatch, timestamp, expected):
    import tests.intents.conftest as fixtures

    monkeypatch.setitem(fixtures.CHAIN_CONFIGS, "test", {"tokens": {"WETH": "unused"}})
    response = MagicMock(status_code=200)
    response.json.return_value = {"weth": {"usd": 2000, "last_updated_at": timestamp}}
    monkeypatch.setitem(fixtures.GLOBAL_TOKEN_IDS, "WETH", "weth")
    get = MagicMock(return_value=response)
    monkeypatch.setattr(fixtures.requests, "get", get)
    monkeypatch.setattr(fixtures.time, "time", lambda: 1788998460)
    prices = _fetch_prices_sync("test")
    assert prices["WETH"] == Decimal(2000)
    assert prices.ledger_inputs()["WETH"]["confidence"] == expected
    assert get.call_args.kwargs["params"]["include_last_updated_at"] == "true"


def test_derived_fork_price_retains_component_freshness_and_one_block(monkeypatch):
    import web3

    import almanak.integrations.chainlink.catalog as catalog_module
    import tests.intents.conftest as fixtures

    monkeypatch.setitem(fixtures.CHAIN_CONFIGS, "test", {"tokens": {"WSTETH": "unused"}})
    monkeypatch.delenv("ANVIL_FORK_BLOCK_TEST", raising=False)
    monkeypatch.delenv("ANVIL_FORK_BLOCK", raising=False)
    monkeypatch.setattr(fixtures, "get_anvil_rpc_url", lambda _: "http://127.0.0.1:8545")
    w3 = MagicMock()
    w3.eth.get_block.return_value = {"number": 42}
    monkeypatch.setattr(web3, "Web3", MagicMock(return_value=w3))
    catalog = MagicMock()
    catalog.feed_for_token.side_effect = lambda _chain, token: "eth-usd" if token == "WETH" else None
    catalog.derived_feed_for_token.return_value = "token-eth"
    monkeypatch.setattr(catalog_module, "CATALOG", catalog)
    recent = datetime(2026, 9, 10, tzinfo=UTC)
    older = datetime(2026, 9, 9, tzinfo=UTC)
    observations = {
        "token-eth": PriceData(price=Decimal(2), source="onchain", timestamp=recent, raw_confidence=0.95, stale=False),
        "eth-usd": PriceData(price=Decimal(2000), source="onchain", timestamp=older, raw_confidence=0.85, stale=True),
    }
    read = MagicMock(side_effect=lambda _w3, spec, _block, _label: observations[spec])
    monkeypatch.setattr(fixtures, "_read_chainlink", read)
    prices = fixtures._fetch_prices_from_fork("test")
    assert prices["WSTETH"] == 4000
    record = prices.ledger_inputs()["WSTETH"]
    assert record["confidence"] == "STALE"
    assert record["observed_at"] == older.isoformat()
    assert record["raw_confidence"] == 0.85 * 0.95
    assert all(call.args[2] == 42 for call in read.call_args_list)
    w3.eth.get_block.assert_called_once_with("latest")
