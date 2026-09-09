"""Raw-token reference composition and coherent contract evidence."""

from __future__ import annotations

import asyncio
import time
from dataclasses import replace
from datetime import UTC, datetime
from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from web3 import AsyncWeb3
from web3.providers import AsyncBaseProvider

from almanak.framework.data.interfaces import PriceResult
from almanak.gateway.data.price.scaled_token_reference import (
    AdjustmentCoherence,
    MultiplierObservation,
    compose_reference,
    read_multiplier,
)
from almanak.gateway.proto import gateway_pb2 as pb
from almanak.gateway.services.market_service import MarketServiceServicer
from almanak.integrations.bstocks.catalog import GOOGLB, reference_profile


def state(**changes):
    now = int(time.time())
    return replace(
        MultiplierObservation(
            Decimal("1.000478058978107511"),
            120603059,
            "0x" + "ab" * 32,
            now - 1,
            now,
            None,
            0,
            GOOGLB.beacon,
            GOOGLB.implementation,
        ),
        **changes,
    )


def underlying(price="200", timestamp=None, stale=False):
    return pb.ReferencePriceResponse(
        instrument="GOOGL",
        quote="USD",
        chain="bsc",
        price=price,
        availability=pb.REFERENCE_PRICE_AVAILABILITY_AVAILABLE,
        confidence=0.95,
        source="chainlink:bsc:GOOGL/USD:0xfeed",
        observed_at=timestamp or int(time.time()),
        stale=stale,
        market_status=pb.REFERENCE_MARKET_STATUS_OPEN,
        market_status_as_of=int(time.time()),
        market_status_source="US_equities_regular",
        basis=pb.REFERENCE_PRICE_BASIS_UNDERLYING_SHARE,
    )


@pytest.mark.parametrize(
    ("multiplier", "share_price", "expected"),
    [
        ("1", "200", "200"),
        ("2", "100", "200"),
        ("0.5", "400", "200"),
        ("1.01", "200", "202"),
        ("1.000478058978107511", "338.67", "338.83190423411567075037"),
    ],
)
def test_reference_units_conserve_splits_and_accumulate_dividends(multiplier, share_price, expected):
    now = int(time.time())
    source = underlying(share_price, timestamp=now)
    result = compose_reference(
        source,
        GOOGLB,
        state(multiplier=Decimal(multiplier), block_timestamp=now - 1, read_at=now),
        AdjustmentCoherence(),
    )
    assert Decimal(result.price) == Decimal(expected)
    assert result.instrument == "GOOGLB"
    assert result.token_address == GOOGLB.address
    assert result.basis == pb.REFERENCE_PRICE_BASIS_RAW_TOKEN
    assert result.observed_at == source.observed_at
    assert result.composition.underlying_price == share_price
    assert source.instrument == "GOOGL" and source.price == share_price


def test_adjustment_change_rejects_pre_change_quote_then_accepts_new_quote():
    now = int(time.time())
    coherence = AdjustmentCoherence()
    old = state(multiplier=Decimal(1), block_timestamp=now - 20)
    assert compose_reference(underlying(timestamp=now - 10), GOOGLB, old, coherence).price == "200"
    changed = state(multiplier=Decimal(2), block_number=120603060, block_timestamp=now - 5)
    rejected = compose_reference(underlying("100", timestamp=now - 10), GOOGLB, changed, coherence)
    assert rejected.price == "" and rejected.reason == "reference_adjustment_alignment_unproven"
    assert compose_reference(underlying("100"), GOOGLB, changed, coherence).price == "200"


def test_delayed_old_block_cannot_pair_previous_multiplier_with_post_split_quote():
    coherence = AdjustmentCoherence()
    newer = state(multiplier=Decimal(2), block_number=101)
    assert compose_reference(underlying("100"), GOOGLB, newer, coherence).price == "200"
    older = replace(newer, multiplier=Decimal(1), block_number=100)
    rejected = compose_reference(underlying("100"), GOOGLB, older, coherence)
    assert rejected.price == "" and rejected.reason == "multiplier_observation_out_of_order"
    assert compose_reference(underlying("100"), GOOGLB, newer, coherence).price == "200"


def test_same_second_quote_does_not_prove_post_adjustment_alignment():
    coherence = AdjustmentCoherence()
    current = state(multiplier=Decimal(2))
    source = underlying("100", timestamp=current.block_timestamp)
    rejected = compose_reference(source, GOOGLB, current, coherence)
    assert rejected.price == "" and rejected.reason == "reference_adjustment_alignment_unproven"
    source.observed_at += 1
    assert compose_reference(source, GOOGLB, current, coherence).price == "200"


@pytest.mark.parametrize("changes", [{"block_hash": "0x" + "cd" * 32}, {"multiplier": Decimal(1)}])
def test_same_height_cannot_replace_the_verified_block_or_state(changes):
    coherence = AdjustmentCoherence()
    current = state(multiplier=Decimal(2))
    assert compose_reference(underlying("100"), GOOGLB, current, coherence).price == "200"
    rejected = compose_reference(underlying("100"), GOOGLB, replace(current, **changes), coherence)
    assert rejected.price == "" and rejected.reason == "multiplier_observation_conflict"


def test_pending_activation_never_uses_next_multiplier_or_hides_old_quote():
    pending = state(scheduled_multiplier=Decimal(2), effective_at=int(time.time()) + 60)
    result = compose_reference(underlying(), GOOGLB, pending, AdjustmentCoherence())
    assert result.price == "" and result.reason == "multiplier_adjustment_pending"
    assert result.composition.scheduled_multiplier == "2"


def test_source_staleness_and_observation_time_are_not_refreshed():
    now = int(time.time())
    coherence = AdjustmentCoherence()
    coherence.observe(GOOGLB, state(block_number=120603058, block_timestamp=now - 200), now - 200)
    source = underlying(timestamp=now - 150, stale=True)
    result = compose_reference(source, GOOGLB, state(), coherence)
    assert result.price
    assert result.observed_at == now - 150 and result.stale is True


@pytest.mark.parametrize(
    "changes",
    [
        {"read_at": 0},
        {"read_at": int(time.time()) - 31},
        {"block_timestamp": int(time.time()) - 31},
        {"read_at": int(time.time()) + 60},
    ],
)
def test_invalid_contract_clock_is_unmeasured(changes):
    result = compose_reference(underlying(), GOOGLB, state(**changes), AdjustmentCoherence())
    assert result.availability == pb.REFERENCE_PRICE_AVAILABILITY_UNMEASURED
    assert result.price == ""


def test_curated_symbol_is_not_an_underlying_alias_or_suffix_inference():
    assert reference_profile("bsc", "googlb") == GOOGLB
    assert reference_profile("bsc", "GOOGLB", GOOGLB.address) == GOOGLB
    assert reference_profile("bsc", "GOOGL") is None
    assert reference_profile("bsc", "UNKNOWNB") is None
    with pytest.raises(ValueError):
        reference_profile("bsc", "GOOGL", GOOGLB.address)
    with pytest.raises(ValueError):
        reference_profile("ethereum", "GOOGLB")


def _word(value):
    return int(value).to_bytes(32, "big")


def _address(value):
    return bytes.fromhex(value[2:]).rjust(32, b"\0")


class _BscBlockProvider(AsyncBaseProvider):
    async def make_request(self, method, params):
        assert method == "eth_getBlockByNumber"
        return {
            "jsonrpc": "2.0",
            "id": 1,
            "result": {"number": "0x1", "timestamp": "0x1", "extraData": "0x" + "11" * 280},
        }


@pytest.mark.asyncio
async def test_gateway_owned_client_can_read_bsc_poa_blocks_and_is_reused():
    service = MarketServiceServicer.__new__(MarketServiceServicer)
    service.settings = SimpleNamespace(network="mainnet")
    service._onchain_lookups = {}
    service._onchain_lookups_lock = asyncio.Lock()
    lookup = SimpleNamespace(_w3=AsyncWeb3(_BscBlockProvider()))
    with (
        patch("almanak.gateway.utils.get_rpc_url", return_value="https://example.invalid"),
        patch("almanak.gateway.services.onchain_lookup.OnChainLookup", return_value=lookup),
    ):
        client = await service._get_onchain_lookup("bsc")
        block = await client._w3.eth.get_block("latest")
        assert len(block["proofOfAuthorityData"]) == 280
        assert await service._get_onchain_lookup("bsc") is client


def _web3(overrides=None):
    words = {
        "0x5c60da1b": _address(GOOGLB.implementation),
        "0xa60bf13d": _word(1000478058978107511),
        "0xdc767007": _word(1000478058978107511),
        "0x97a4064f": _word(0),
        "0x313ce567": _word(18),
    }
    words.update(overrides or {})
    future = asyncio.get_running_loop().create_future()
    future.set_result(56)
    block = {"number": 120603059, "timestamp": int(time.time()), "hash": bytes.fromhex("ab" * 32)}
    eth = SimpleNamespace(
        chain_id=future,
        get_block=AsyncMock(return_value=block),
    )

    def rpc(method, params):
        word = _address(GOOGLB.beacon) if method == "eth_getStorageAt" else words[params[0]["data"]]
        return {"jsonrpc": "2.0", "id": 1, "result": "0x" + word.hex()}

    return SimpleNamespace(
        eth=eth,
        provider=SimpleNamespace(make_request=AsyncMock(side_effect=rpc)),
        to_checksum_address=AsyncWeb3.to_checksum_address,
    )


@pytest.mark.asyncio
async def test_real_abi_words_decode_at_one_block_with_reorg_check():
    w3 = _web3()
    result = await read_multiplier(w3, GOOGLB)
    assert result.multiplier == Decimal("1.000478058978107511")
    assert result.scheduled_multiplier is None
    assert result.block_number == 120603059
    expected_block = {"blockHash": result.block_hash, "requireCanonical": True}
    assert len(w3.provider.make_request.call_args_list) == 6
    for call in w3.provider.make_request.call_args_list:
        assert call.args[1][-1] == expected_block
    assert w3.eth.get_block.call_args_list[-1].args == (120603059,)


@pytest.mark.asyncio
async def test_real_owned_lookup_supports_hash_objects_without_ens_formatter_errors():
    fixture = _web3()
    block = fixture.eth.get_block.return_value

    class Provider(AsyncBaseProvider):
        async def make_request(self, method, params):
            if method == "eth_chainId":
                result = "0x38"
            elif method == "eth_getBlockByNumber":
                result = {
                    "number": hex(block["number"]),
                    "timestamp": hex(block["timestamp"]),
                    "hash": "0x" + block["hash"].hex(),
                    "extraData": "0x" + "11" * 280,
                }
            else:
                assert params[-1] == {"blockHash": "0x" + block["hash"].hex(), "requireCanonical": True}
                return await fixture.provider.make_request(method, params)
            return {"jsonrpc": "2.0", "id": 1, "result": result}

    service = MarketServiceServicer.__new__(MarketServiceServicer)
    service.settings = SimpleNamespace(network="mainnet")
    service._onchain_lookups = {}
    service._onchain_lookups_lock = asyncio.Lock()
    with (
        patch("almanak.gateway.utils.get_rpc_url", return_value="https://example.invalid"),
        patch("almanak.gateway.services.onchain_lookup.AsyncHTTPProvider", return_value=Provider()),
    ):
        result = await service._read_reference_multiplier(GOOGLB)
        assert result.multiplier == Decimal("1.000478058978107511")
        assert result.block_hash == "0x" + block["hash"].hex()
        assert len(fixture.provider.make_request.call_args_list) == 6


@pytest.mark.asyncio
async def test_rpc_without_hash_targeting_fails_closed_without_number_fallback():
    w3 = _web3()
    w3.provider.make_request.side_effect = None
    w3.provider.make_request.return_value = {"error": {"code": -32602, "message": "unsupported block object"}}
    with pytest.raises(ValueError, match="multiplier_rpc_error"):
        await read_multiplier(w3, GOOGLB)
    assert all(isinstance(call.args[1][-1], dict) for call in w3.provider.make_request.call_args_list)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "overrides",
    [
        {"0x5c60da1b": _address("0x" + "11" * 20)},
        {"0x313ce567": _word(6)},
        {"0xa60bf13d": _word(0)},
        {"0xa60bf13d": b""},
        {"0xdc767007": _word(2000000000000000000)},
    ],
)
async def test_bad_contract_evidence_fails_closed(overrides):
    with pytest.raises(ValueError, match="multiplier_"):
        await read_multiplier(_web3(overrides), GOOGLB)


@pytest.mark.asyncio
async def test_reorg_during_read_rejects_mixed_state():
    w3 = _web3()
    old = w3.eth.get_block.return_value
    w3.eth.get_block.side_effect = [old, {**old, "hash": bytes.fromhex("cd" * 32)}]
    with pytest.raises(ValueError, match="reorganized"):
        await read_multiplier(w3, GOOGLB)


@pytest.mark.asyncio
async def test_gateway_routes_curated_token_to_underlying_without_generic_price():
    now = datetime.now(UTC)
    source = SimpleNamespace(
        get_reference_price=AsyncMock(
            return_value=PriceResult(
                price=Decimal(200),
                source="chainlink:bsc:GOOGL/USD:0xfeed",
                timestamp=now,
                confidence=0.95,
            )
        )
    )
    service = MarketServiceServicer.__new__(MarketServiceServicer)
    service.settings = SimpleNamespace(chains=["bsc"])
    service._ensure_initialized = AsyncMock()
    service._price_aggregators = {"bsc": SimpleNamespace(sources=[source])}
    service._read_reference_multiplier = AsyncMock(return_value=state(multiplier=Decimal(2)))
    with patch("almanak.gateway.services.market_service.reference_market_status") as status:
        from almanak.gateway.data.price.market_hours import MarketHoursObservation, ReferenceMarketStatus

        status.return_value = MarketHoursObservation(ReferenceMarketStatus.OPEN, now, "regular_session")
        result = await service.GetReferencePrice(
            pb.ReferencePriceRequest(instrument="GOOGLB", chain="bsc", quote="USD", token_address=GOOGLB.address),
            MagicMock(),
        )
    source.get_reference_price.assert_awaited_once_with("GOOGL", "USD")
    assert result.price == "400" and result.instrument == "GOOGLB"
    assert result.composition.multiplier == "2"


@pytest.mark.asyncio
async def test_legacy_framework_request_cannot_receive_composition_it_cannot_validate():
    service = MarketServiceServicer.__new__(MarketServiceServicer)
    service.settings = SimpleNamespace(chains=["bsc"])
    service._ensure_initialized = AsyncMock()
    service._get_token_reference = AsyncMock()
    result = await service.GetReferencePrice(
        pb.ReferencePriceRequest(instrument="GOOGLB", chain="bsc", quote="USD"), MagicMock()
    )
    assert result.price == "" and result.reason == "reference_token_address_required"
    assert result.availability == pb.REFERENCE_PRICE_AVAILABILITY_UNMEASURED
    service._get_token_reference.assert_not_awaited()


@pytest.mark.asyncio
async def test_uint256_multiplier_precision_is_not_rounded_to_decimal_default_context():
    raw = 2**256 - 1
    digits = str(raw)
    expected = Decimal(digits[:-18] + "." + digits[-18:])
    result = await read_multiplier(_web3({"0xa60bf13d": _word(raw), "0xdc767007": _word(raw)}), GOOGLB)
    assert result.multiplier == expected


@pytest.mark.asyncio
async def test_overlapping_reads_retain_newer_adjustment_when_old_read_finishes_last():
    service = MarketServiceServicer.__new__(MarketServiceServicer)
    service.GetReferencePrice = AsyncMock(side_effect=lambda *args: underlying("100"))
    started = asyncio.Event()
    release = asyncio.Event()
    newer = state(multiplier=Decimal(2), block_number=101)
    calls = 0

    async def read(profile):
        nonlocal calls
        calls += 1
        if calls == 1:
            started.set()
            await release.wait()
            return replace(newer, multiplier=Decimal(1), block_number=100)
        return newer

    service._read_reference_multiplier = read
    delayed = asyncio.create_task(service._get_token_reference(GOOGLB, "USD", MagicMock()))
    await started.wait()
    try:
        result = await service._get_token_reference(GOOGLB, "USD", MagicMock())
        assert result.price == "200"
    finally:
        release.set()
    rejected = await delayed
    assert rejected.price == ""
    assert rejected.reason == "multiplier_observation_out_of_order"


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", [TimeoutError("private endpoint"), ValueError("multiplier_rpc_error")])
async def test_contract_read_failures_are_errored_without_claiming_a_source_observation(failure):
    service = MarketServiceServicer.__new__(MarketServiceServicer)
    service.GetReferencePrice = AsyncMock(return_value=underlying())
    service._read_reference_multiplier = AsyncMock(side_effect=failure)
    result = await service._get_token_reference(GOOGLB, "USD", MagicMock())
    assert result.availability == pb.REFERENCE_PRICE_AVAILABILITY_ERRORED
    assert result.price == result.source == ""
    assert result.observed_at == 0
    assert result.token_address == GOOGLB.address
    assert "private endpoint" not in str(result)


def test_bnb_published_split_example_conserves_the_published_portfolio_value():
    # External oracle: BNB's scaled-ui-amount guide, "2-for-1 stock split" table.
    # The expected portfolio value is published there, not derived by the composer.
    now = int(time.time())
    for multiplier, share_price in [("1", "50"), ("2", "25")]:
        result = compose_reference(
            underlying(share_price, timestamp=now),
            GOOGLB,
            state(multiplier=Decimal(multiplier), block_timestamp=now - 1, read_at=now),
            AdjustmentCoherence(),
        )
        assert Decimal(result.price) * 100 == Decimal("5000")
