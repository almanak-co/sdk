"""A token-targeted request must never accept a bare underlying quote."""

import time
from dataclasses import replace
from datetime import UTC, datetime
from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from almanak.framework.market.models import ReferencePriceBasis
from almanak.framework.market import MarketSnapshotBuilder
from almanak.gateway.proto import gateway_pb2 as pb
from almanak.integrations.bstocks.catalog import GOOGLB


def response():
    now = int(time.time())
    return pb.ReferencePriceResponse(
        instrument="GOOGLB",
        quote="USD",
        chain="bsc",
        price="200",
        availability=pb.REFERENCE_PRICE_AVAILABILITY_AVAILABLE,
        source="composition:bstocks",
        observed_at=now,
        confidence=0.95,
        market_status=pb.REFERENCE_MARKET_STATUS_OPEN,
        market_status_as_of=now,
        market_status_source="regular_session",
        basis=pb.REFERENCE_PRICE_BASIS_RAW_TOKEN,
        token_address=GOOGLB.address,
        composition=pb.ReferencePriceComposition(
            underlying_instrument="GOOGL",
            underlying_price="100",
            underlying_source="verified:GOOGL/USD",
            underlying_observed_at=now,
            multiplier="2",
            multiplier_block_number=120603059,
            multiplier_block_hash="0x" + "ab" * 32,
            multiplier_block_timestamp=now,
            multiplier_read_at=now,
            beacon_address=GOOGLB.beacon,
            implementation_address=GOOGLB.implementation,
            composed_at=now,
        ),
    )


def snapshot(value):
    client = SimpleNamespace(is_connected=True, config=SimpleNamespace(timeout=2), market=MagicMock())
    client.market.GetReferencePrice.return_value = value
    strategy = SimpleNamespace(chain="bsc", wallet_address="0x" + "11" * 20)
    return MarketSnapshotBuilder.for_strategy_runner(
        strategy=strategy, gateway_client=client, runtime_surface="unit_test"
    ), client


@pytest.mark.parametrize("explicit", [False, True])
def test_existing_symbol_call_and_explicit_address_have_the_same_raw_basis(explicit):
    raw = response()
    market, client = snapshot(raw)
    kwargs = {"token_address": GOOGLB.address} if explicit else {}
    result = market.reference_price("GOOGLB", **kwargs)
    assert result.price == Decimal(200)
    assert result.basis is ReferencePriceBasis.RAW_TOKEN
    assert result.composition.multiplier == Decimal(2)
    assert result.composition.underlying_price == Decimal(100)
    assert result.is_tradeable(max_age_seconds=120, now=datetime.fromtimestamp(raw.observed_at, UTC))
    sent = client.market.GetReferencePrice.call_args.args[0]
    assert sent.instrument == "GOOGLB" and sent.token_address == GOOGLB.address


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("basis", pb.REFERENCE_PRICE_BASIS_UNSPECIFIED),
        ("basis", pb.REFERENCE_PRICE_BASIS_UNDERLYING_SHARE),
        ("token_address", ""),
        ("token_address", "0x" + "22" * 20),
        ("instrument", "GOOGL"),
        ("chain", "ethereum"),
        ("quote", "EUR"),
        ("price", "100"),
    ],
)
def test_missing_or_mismatched_identity_never_becomes_a_stock_alias(field, value):
    raw = response()
    setattr(raw, field, value)
    market, _ = snapshot(raw)
    result = market.reference_price("GOOGLB")
    assert result.price is None and result.stale
    assert not result.is_tradeable(max_age_seconds=120)


def test_old_gateway_ignoring_address_field_fails_closed():
    raw = response()
    raw.ClearField("composition")
    raw.ClearField("basis")
    raw.ClearField("token_address")
    market, _ = snapshot(raw)
    assert market.reference_price("GOOGLB", token_address=GOOGLB.address).price is None


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("multiplier", "NaN"),
        ("underlying_price", ""),
        ("underlying_source", ""),
        ("multiplier_block_hash", "0x123"),
        ("multiplier_block_number", 0),
        ("multiplier_read_at", 0),
        ("composed_at", 0),
        ("underlying_observed_at", 1),
        ("beacon_address", "0x" + "11" * 20),
        ("implementation_address", "0x" + "22" * 20),
        ("scheduled_multiplier", "3"),
    ],
)
def test_partial_or_changed_provenance_is_unmeasured(field, value):
    raw = response()
    setattr(raw.composition, field, value)
    market, _ = snapshot(raw)
    result = market.reference_price("GOOGLB")
    assert result.price is None and result.reason


def test_contract_evidence_expires_even_if_underlying_is_within_120_seconds():
    raw = response()
    market, _ = snapshot(raw)
    result = market.reference_price("GOOGLB")
    later = datetime.fromtimestamp(raw.observed_at + 31, UTC)
    assert result.trade_block_reason(max_age_seconds=120, now=later) == "multiplier_observation_stale"


@pytest.mark.parametrize("field", ["multiplier_read_at", "multiplier_block_timestamp"])
def test_public_composition_accepts_naive_utc_clocks_and_still_expires(field):
    raw = response()
    market, _ = snapshot(raw)
    result = market.reference_price("GOOGLB")
    stamp = getattr(result.composition, field).replace(tzinfo=None)
    result = replace(result, composition=replace(result.composition, **{field: stamp}))
    now = datetime.fromtimestamp(raw.observed_at, UTC)
    assert result.trade_block_reason(max_age_seconds=120, now=now) is None
    later = datetime.fromtimestamp(raw.observed_at + 31, UTC)
    assert result.trade_block_reason(max_age_seconds=120, now=later) == "multiplier_observation_stale"


@pytest.mark.parametrize("case", ["future_effective", "pre_effective_quote", "same_second_quote"])
def test_effective_time_guards_reject_incoherent_adjustment_quotes(case):
    raw = response()
    now = raw.observed_at
    if case == "future_effective":
        raw.composition.multiplier_effective_at = now + 1
    else:
        raw.composition.multiplier_effective_at = now - 1
        raw.observed_at = now - (2 if case == "pre_effective_quote" else 1)
        raw.composition.underlying_observed_at = raw.observed_at
    market, _ = snapshot(raw)
    result = market.reference_price("GOOGLB")
    assert result.price is None
    assert result.reason == (
        "reference_composition_adjustment_pending"
        if case == "future_effective"
        else "reference_composition_adjustment_not_aligned"
    )


def test_explicit_underlying_symbol_with_wrapper_address_is_rejected_before_rpc():
    market, client = snapshot(response())
    result = market.reference_price("GOOGL", token_address=GOOGLB.address)
    assert result.price is None
    client.market.GetReferencePrice.assert_not_called()


def test_legacy_underlying_response_remains_compatible():
    raw = response()
    raw.instrument = "GOOGL"
    raw.price = "100"
    for field in ("composition", "basis", "token_address"):
        raw.ClearField(field)
    market, client = snapshot(raw)
    result = market.reference_price("GOOGL")
    assert result.price == Decimal(100) and result.composition is None
    assert client.market.GetReferencePrice.call_args.args[0].token_address == ""


def test_registered_metadata_does_not_create_an_underlying_price_alias():
    from almanak.framework.data.tokens.resolver import TokenResolver
    from almanak.integrations.chainlink.catalog import TOKEN_TO_PAIR

    token = TokenResolver().resolve(GOOGLB.address, "bsc", skip_gateway=True)
    assert token.symbol == "GOOGLB" and token.decimals == 18
    assert "GOOGLB" not in TOKEN_TO_PAIR


def test_reference_metadata_types_are_public_without_changing_balance_type():
    from almanak import ReferencePriceBasis as PublicBasis
    from almanak import ReferencePriceCompositionData
    from almanak.framework.market.models import ReferencePriceCompositionData as CanonicalComposition

    assert PublicBasis is ReferencePriceBasis
    assert ReferencePriceCompositionData is CanonicalComposition
