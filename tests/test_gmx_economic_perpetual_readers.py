"""Perpetual readers capture exact canonical inputs and re-decode without any provider.

All inputs come from the synthetic chain in ``tests/gmx_synthetic_chain.py``. These
checks prove request wiring and replay equivalence only; they are not Reader
comparisons and establish no historical coverage.
"""

from types import SimpleNamespace
from unittest.mock import Mock

import pytest

from almanak.backtest.evm import EvmHistory
from almanak.backtest.types import UnixTime
from almanak.connectors.gmx_v2 import (
    economic_collection,
    economic_data,
    economic_prefetch,
    economic_replay,
)
from almanak.connectors.gmx_v2.economic_book import MarketBook
from almanak.connectors.gmx_v2.economic_collection import (
    contract_bindings,
    dependency_bindings,
    read_frames,
    read_opportunities,
    read_perpetual_opportunity,
    risk_reader,
)
from almanak.connectors.gmx_v2.economic_liquidity_risk import LiquidityRiskState
from almanak.connectors.gmx_v2.economic_perpetual_risk import PerpetualRiskState
from almanak.connectors.gmx_v2.economic_position_fee_reader import position_fee_policies
from almanak.connectors.gmx_v2.economic_position_reader import (
    LIQUIDATION,
    liquidation_handler,
    perpetual_risk,
)
from almanak.connectors.gmx_v2.economic_precision import FLOAT
from almanak.connectors.gmx_v2.economic_replay import ReplayReader, validate_frame
from almanak.connectors.gmx_v2.economic_swap_risk import SwapRiskState
from almanak.gateway.ethereum import DataError
from almanak.models import ZERO_ADDRESS
from tests import gmx_synthetic_chain as chain
from tests import gmx_synthetic_frames as synthetic
from tests.test_gmx_economic_perpetual_data import parameters


@pytest.fixture(params=["arbitrum", "avalanche"])
def synthetic_chain(request):
    return chain.perpetual_chain(request.param)


def settings_for(target, **models):
    return SimpleNamespace(
        position_model=models.get("position_model", "prepared_position_accounting"),
        liquidity_model=models.get("liquidity_model"),
        history=(SimpleNamespace(chain=target.fund.chain),),
        schedule=SimpleNamespace(start=UnixTime(1), deadline=UnixTime(2)),
        data=SimpleNamespace(
            publication=lambda name: synthetic.source(), maximum_risk_blocks=8, max_age_seconds=60
        ),
    )


def replay_for(reader, target, digests=None):
    retained = chain.Retained(reader.evidence())
    return ReplayReader(target.fund.chain, retained, digests or tuple(retained.raw))


def test_perpetual_risk_reader_reproduces_every_control_from_fixed_keys(synthetic_chain):
    reader, target, at = synthetic_chain
    block = reader.pin_block(at.number)
    assert perpetual_risk(reader, target.market, block) == synthetic.perpetual_risk(
        target.market, at
    )
    reader.set(
        "Bool",
        "EXECUTE_ORDER_FEATURE_DISABLED",
        ("address", "uint256"),
        (liquidation_handler(target.market), LIQUIDATION),
        value=True,
    )
    reader.set("Address", "HOLDING_ADDRESS", value=ZERO_ADDRESS)
    reader.cache.clear()
    changed = perpetual_risk(reader, target.market, block)
    assert changed.liquidation_disabled and not changed.holding_address_available
    assert (
        changed.increases
        == changed.decreases
        == synthetic.perpetual_risk(target.market, at).increases
    )


def test_perpetual_opportunity_reads_reproduce_synthetic_state_and_replay_offline(synthetic_chain):
    reader, target, at = synthetic_chain
    block = reader.pin_block(at.number)
    risk = perpetual_risk(reader, target.market, block)
    published = synthetic.publication(target.market, at)
    opportunity = read_perpetual_opportunity(reader, target, published, 0, risk)
    assert opportunity == synthetic.perpetual_opportunity(target.market, at)
    attempts, used = reader.provider_attempts, set(reader.used)
    with replay_for(reader, target) as replay:
        replayed_block = replay.pin_block(at.number)
        replayed_risk = perpetual_risk(replay, target.market, replayed_block)
        assert (
            read_perpetual_opportunity(replay, target, published, 0, replayed_risk) == opportunity
        )
        assert replay.used == used
    assert reader.provider_attempts == attempts
    with pytest.raises(ValueError, match="configured position target"):
        read_perpetual_opportunity(reader, synthetic.exchange_target(), published, 0, risk)


def test_replay_refuses_any_missing_perpetual_read(synthetic_chain):
    reader, target, at = synthetic_chain
    block = reader.pin_block(at.number)
    risk = perpetual_risk(reader, target.market, block)
    published = synthetic.publication(target.market, at)
    read_perpetual_opportunity(reader, target, published, 0, risk)
    retained = chain.Retained(reader.evidence())
    dropped = next(
        digest
        for digest, value in retained.raw.items()
        if '"method":"eth_call"' in value.body and "getUint" not in value.body
    )
    digests = tuple(digest for digest in retained.raw if digest != dropped)
    with ReplayReader(target.fund.chain, retained, digests) as replay:
        with pytest.raises(ValueError, match="missing retained RPC"):
            read_perpetual_opportunity(
                replay,
                target,
                published,
                0,
                perpetual_risk(replay, target.market, replay.pin_block(at.number)),
            )


def test_read_opportunities_dispatches_the_perpetual_family_by_risk_type(
    synthetic_chain, monkeypatch
):
    reader, target, at = synthetic_chain
    block = reader.pin_block(at.number)
    risk = perpetual_risk(reader, target.market, block)
    published = synthetic.publication(target.market, at)
    monkeypatch.setattr(economic_collection, "publication_prices", Mock(return_value=published))
    candidates = {published.transaction: (at.number, at.hash, 0)}
    result = read_opportunities(reader, target.market, synthetic.source(), candidates, risk, target)
    assert result == (synthetic.perpetual_opportunity(target.market, at),)
    with pytest.raises(ValueError, match="configured position target"):
        read_opportunities(reader, target.market, synthetic.source(), candidates, risk)


def test_read_frames_and_validate_frame_use_the_perpetual_readers(synthetic_chain, monkeypatch):
    reader, target, at = synthetic_chain
    published = synthetic.publication(target.market, at)
    verify = Mock()
    monkeypatch.setattr(economic_prefetch, "verify_deployments", verify)
    monkeypatch.setattr(economic_replay, "verify_deployments", verify)
    monkeypatch.setattr(economic_collection, "publication_prices", Mock(return_value=published))
    settings = settings_for(target)
    assert risk_reader(settings) is perpetual_risk
    history = EvmHistory(reader)
    # The collector retains its chain identity and boundary reads with every record.
    reader.used.clear()
    reader._rpc("eth_chainId", [])
    boundary = reader.evidence()
    candidates = {published.transaction: (at.number, at.hash, 0)}
    frames = read_frames(history, target, settings, at, at, candidates)
    assert len(frames) == 1
    frame, raw = frames[0]
    assert frame == synthetic.perpetual_frame(
        "arbitrum" if str(at.chain.chain_id) == "eip155:42161" else "avalanche"
    )
    verify.assert_called_with(reader, target.market, at)
    digests = tuple(dict.fromkeys(value.digest for value in (*boundary, *raw)))
    with ReplayReader(target.fund.chain, chain.Retained((*boundary, *raw)), digests) as replay:
        validate_frame(replay, frame, synthetic.source(), candidates)
        changed = frame.risk[0].model_copy(update={"market_disabled": True})
        with pytest.raises(ValueError, match="retained DataStore"):
            validate_frame(
                replay,
                frame.model_copy(update={"risk": (changed,)}),
                synthetic.source(),
                candidates,
            )
        with pytest.raises(ValueError, match="complete publication scan"):
            validate_frame(
                replay,
                frame.model_copy(update={"opportunities": ()}),
                synthetic.source(),
                candidates,
            )


def test_position_fee_policies_read_referral_and_pro_discounts_for_both_impact_outcomes(
    synthetic_chain,
):
    reader, target, at = synthetic_chain
    block = reader.pin_block(at.number)
    chain.register_referral(
        reader, target, rebate=1000, share=5000, minimum=FLOAT // 100, pro_tier=2, pro=FLOAT // 50
    )
    policies = position_fee_policies(reader, target, block)
    expected_discount = (1000 * 5000 // 10000) * FLOAT // 10000
    assert policies.improved.referral_enabled and policies.worsened.referral_enabled
    assert policies.improved.referral_discount_factor == expected_discount
    assert policies.improved.affiliate_factor == 1000 * FLOAT // 10000 - expected_discount
    assert policies.improved.minimum_affiliate_factor == FLOAT // 100
    assert policies.improved.pro_discount_factor == FLOAT // 50
    assert policies.improved.position_factor == synthetic.fees().improved.position_factor
    assert policies.worsened.position_factor == synthetic.fees().worsened.position_factor
    assert policies.worsened == policies.improved.model_copy(
        update={"position_factor": policies.worsened.position_factor}
    )
    with replay_for(reader, target) as replay:
        assert position_fee_policies(replay, target, replay.pin_block(at.number)) == policies


def test_contract_bindings_for_perpetual_targets_exclude_the_native_asset(synthetic_chain):
    _, target, _ = synthetic_chain
    name = "arbitrum" if str(target.fund.chain.chain_id) == "eip155:42161" else "avalanche"
    config = parameters(name, "position")
    bound = contract_bindings(settings_for(target), config)
    assert len(bound) == 1 and bound[0].chain == target.fund.chain
    contracts = set(bound[0].contracts)
    market = target.market
    assert {
        a.reference for a in (market.long_asset, market.short_asset, market.share_asset)
    } <= contracts
    assert (
        market.wrapped_native.reference in contracts and market.index_asset.reference in contracts
    )
    assert {synthetic.SOURCE_HANDLER, synthetic.SOURCE_ORACLE} <= contracts
    assert liquidation_handler(market) in contracts
    assert all(value.startswith("0x") and len(value) == 42 for value in contracts)
    swap = contract_bindings(settings_for(target), parameters(name, "swap"))
    assert set(swap[0].contracts) >= {a.reference for a in (market.long_asset, market.short_asset)}


def test_dependency_bindings_add_the_discovered_referral_storage_only_for_positions(
    synthetic_chain,
):
    reader, target, at = synthetic_chain
    name = "arbitrum" if str(target.fund.chain.chain_id) == "eip155:42161" else "avalanche"
    config = parameters(name, "position")
    contracts = contract_bindings(settings_for(target), config)
    histories = {target.fund.chain: EvmHistory(reader)}
    boundaries = {target.fund.chain: (at, at)}
    assert dependency_bindings(histories, config, boundaries, contracts) == contracts
    chain.register_referral(reader, target, rebate=1, share=1, minimum=0)
    reader.cache.clear()
    expanded = dependency_bindings(histories, config, boundaries, contracts)
    assert set(expanded[0].contracts) == set(contracts[0].contracts) | {chain.REFERRAL_STORAGE}
    following = histories[target.fund.chain].header(at.number + 1)
    reader.answer(
        target.market.contracts.order_handler,
        "referralStorage()",
        (),
        (),
        ("address",),
        (chain.HOLDING,),
        block=following,
    )
    reader.cache.clear()
    with pytest.raises(ValueError, match="referral storage changed"):
        dependency_bindings(histories, config, {target.fund.chain: (at, following)}, contracts)
    swap_config = parameters(name, "swap")
    assert dependency_bindings(histories, swap_config, boundaries, contracts) == contracts


@pytest.mark.parametrize(
    "position_model,liquidity_model,family",
    [
        (None, None, SwapRiskState),
        (None, "fixed_external_gm_valuation", LiquidityRiskState),
        ("prepared_position_accounting", None, PerpetualRiskState),
    ],
)
def test_book_refuses_frames_from_another_request_family(position_model, liquidity_model, family):
    target = synthetic.perpetual_target()
    frame = synthetic.perpetual_frame()
    settings = settings_for(target, position_model=position_model, liquidity_model=liquidity_model)
    assert economic_data.risk_family(settings) is family
    row = economic_data.Record(
        key=economic_data.key(target),
        unit=economic_data.UNIT,
        payload=frame,
        reference=frame.block,
        source_time=UnixTime(frame.block.timestamp),
        available_at=UnixTime(frame.block.timestamp),
        retrieved_at=UnixTime(frame.block.timestamp),
        evidence=("0" * 64,),
    )
    book = SimpleNamespace(
        reader=SimpleNamespace(timeline=lambda requirement, record: SimpleNamespace(rows=[row])),
        settings=settings,
        timelines={},
        validate_boundaries=Mock(side_effect=DataError("unavailable", "no synthetic boundaries")),
    )
    if family is PerpetualRiskState:
        with pytest.raises(DataError, match="no synthetic boundaries"):
            MarketBook.load_target(book, target)
    else:
        with pytest.raises(ValueError, match="selected request family"):
            MarketBook.load_target(book, target)
