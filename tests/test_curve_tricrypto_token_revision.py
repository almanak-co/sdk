"""January 2024 accounting is constrained by mined storage and fund settlement."""

import copy
import gzip
import json
from dataclasses import asdict
from types import SimpleNamespace

import httpx
import pytest
from eth_abi import decode, encode
from eth_utils import keccak

from almanak.connectors.curve import abi, crypto_history, receipts
from almanak.connectors.curve.domain import CurvePool
from almanak.connectors.curve.economic_pool import crypto_overlay
from almanak.connectors.curve.tricrypto_deployment import revision
from almanak.connectors.curve.tricrypto_observation import TriCryptoObservation
from almanak.connectors.curve.tricrypto_reader import read_pool
from almanak.connectors.curve.tricrypto_receipts import CLAIM, TOKEN_CLAIM
from almanak.gateway.ethereum import DataError
from tests.retained_fixtures import retained
from tests.test_curve_parameter_history import event
from tests.test_curve_reader import CapturedReader
from tests.test_curve_tricrypto_execution import inputs
from tests.test_curve_tricrypto_transitions import evaluated, packed

DIRECTORY = retained("2026-09-07-rollout/curve-optimism-tri-revision")


def retained(name):
    return json.loads(gzip.decompress((DIRECTORY / (name + ".json.gz")).read_bytes()))


CASES = tuple(
    case
    for kind in ("entry", "withdraw", "claims")
    for case in retained("curve-optimism-tri-" + kind + "-fork")["cases"]
)


@pytest.mark.parametrize("case", CASES)
def test_mined_transition_retains_every_accounting_slot(case):
    _, result = evaluated(case)
    predicted = asdict(result)
    predicted["token_accounting"] = result.token_accounting.model_dump(mode="json")
    assert json.loads(json.dumps(predicted)) == case["predicted"]
    layout = revision("tricrypto-2024-01-04.json")["pool_storage_layout"]
    tokens = result.token_accounting
    named = {
        "price_scale_packed": packed(result.state.price_scales),
        "price_oracle_packed": packed(result.prices.cached_oracle),
        "last_prices_packed": packed(result.prices.last_price),
        "last_timestamp": packed((result.prices.last_timestamp, tokens.xcp_timestamp)),
        "D": result.state.invariant,
        "xcp_profit": result.admin.profit,
        "xcp_profit_a": result.admin.claimed_profit,
        "virtual_price": result.admin.virtual_price,
        "totalSupply": result.state.total_supply,
        "admin_lp_virtual_balance": tokens.virtual_admin_shares,
        "last_admin_fee_claim_timestamp": tokens.last_claim_time,
        "cached_xcp_oracle": tokens.cached_xcp_oracle,
        "last_xcp": tokens.last_xcp,
        "xcp_ma_time": tokens.xcp_ma_time,
    }
    expected = {str(layout[name]["slot"]): value for name, value in named.items()}
    expected.update(
        {
            str(layout["balances"]["slot"] + index): value
            for index, value in enumerate(result.state.balances)
        }
    )
    assert case["actual_storage"] == expected
    assert tuple(case.get("actual_amounts", case.get("actual_outputs"))) == result.amounts
    assert case["receipt"]["status"] == "0x1"


@pytest.mark.parametrize("case", CASES)
def test_actual_erc20_calldata_and_admin_settlement(case):
    pool, operation, old_types, name, minimum_index = inputs(case)
    types = tuple(value for value in old_types if value != "bool")
    native = bytes.fromhex(case["transaction"]["input"][2:])
    selector = keccak(text=name + "(" + ",".join(types) + ")")[:4]
    assert native[:4] == selector
    expected = list(decode(types, native[4:]))
    if minimum_index is not None:
        expected[minimum_index] = 1
    assert abi.call(operation).data == "0x" + (selector + encode(types, expected)).hex()
    effects = receipts.movement(case["receipt"], operation, SimpleNamespace(pool=pool))
    assert tuple(value.amount for value in effects.asset_changes) == tuple(
        case["predicted"]["amounts"]
    )
    assert (
        effects.share_change.amount
        == case["predicted"]["state"]["total_supply"] - pool.total_supply
    )


@pytest.fixture
def observation():
    return TriCryptoObservation.model_validate_json(
        json.dumps(retained("curve-optimism-tri-new-observation")["observation"])
    )


def test_full_reader_and_old_wire_compatibility(monkeypatch, observation):
    def forbid(*args, **kwargs):
        raise AssertionError("captured revision cannot access HTTP")

    monkeypatch.setattr(httpx.Client, "send", forbid)
    records = retained("curve-optimism-tri-new-observation")["records"]
    with CapturedReader(observation.source, records) as reader:
        block = reader.pin_block(observation.block.number)
        assert read_pool(reader, observation.source, block) == observation
        old = observation.source.model_copy(update={"implementation_revision": None})
        with pytest.raises(DataError):
            read_pool(reader, old, block)
        assert "implementation_revision" not in old.model_dump(mode="json")
    assert str(observation.source.variant) == "tricrypto_ng_2024"


@pytest.mark.parametrize(
    "changes",
    [
        {"revision": "tricrypto-ng-v2.0.0"},
        {"token_accounting": None},
        {"token_accounting": {"xcp_ma_time": 0}},
    ],
)
def test_incomplete_or_conflicting_revision_refused(observation, changes):
    raw = observation.model_dump(mode="json")
    raw.update(changes)
    with pytest.raises(ValueError):
        TriCryptoObservation.model_validate_json(json.dumps(raw))


def test_xcp_duration_requires_complete_revision_specific_history(observation):
    values = (1000000, 2000000, 10**16, 10**14, 10**14, 600, 900)
    changed = event(observation, crypto_history.TOKEN_PARAMETERS, values)
    fees = crypto_history.parameter_event(changed, observation.source)
    current = observation.model_copy(
        update={
            "packed_fees": fees.packed_fees,
            "packed_rebalancing": fees.packed_rebalancing,
            "token_accounting": observation.token_accounting.model_copy(
                update={"xcp_ma_time": 900}
            ),
        }
    )
    crypto_history.validate_parameters(observation, current, (changed,))
    with pytest.raises(ValueError, match="complete retained event"):
        crypto_history.validate_parameters(observation, current, ())
    with pytest.raises(ValueError, match="ABI differs"):
        crypto_history.parameter_event(
            event(observation, crypto_history.PARAMETERS, values[:6]), observation.source
        )
    invalid = event(observation, crypto_history.TOKEN_PARAMETERS, (*values[:6], 86))
    with pytest.raises(ValueError, match="XCP oracle duration"):
        crypto_history.parameter_event(invalid, observation.source)


def test_virtual_overlay_preserves_intensive_and_extensive_quantities(observation):
    changed = crypto_overlay(observation, observation.total_supply)
    tokens, original = changed.token_accounting, observation.token_accounting
    for name in ("virtual_admin_shares", "cached_xcp_oracle", "last_xcp"):
        assert getattr(tokens, name) == 2 * getattr(original, name)
    assert tokens.xcp_timestamp == original.xcp_timestamp
    assert tokens.xcp_ma_time == original.xcp_ma_time
    with pytest.raises(ValueError, match="future"):
        tokens.oracle(tokens.xcp_timestamp - 1)


def test_old_admin_mint_claim_cannot_authorize_new_token_transfers():
    case = next(case for case in CASES if any(case["predicted"]["admin_amounts"]))
    pool, operation, *_ = inputs(case)
    raw = copy.deepcopy(case["receipt"])
    claim = next(log for log in raw["logs"] if log["topics"][0] == TOKEN_CLAIM)
    claim["topics"][0] = CLAIM
    with pytest.raises(DataError, match="configured revision"):
        receipts.movement(raw, operation, SimpleNamespace(pool=pool))


def test_other_pool_families_cannot_select_tricrypto_revision(observation):
    raw = observation.source.model_dump(mode="json")
    raw["family"] = "stableswap_ng"
    with pytest.raises(ValueError, match="revision"):
        CurvePool.model_validate_json(json.dumps(raw))
