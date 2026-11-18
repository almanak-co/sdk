"""Prepared Aerodrome and Slipstream pools refuse samples outside their frozen scope.

The captured Aerodrome pool stands in for a prepared dataset, so these checks run
without the retained evidence tree.
"""

from types import SimpleNamespace

import pytest

from almanak.backtest.dataset import InputUnavailable
from almanak.connectors import v3_economic_book
from almanak.connectors.aerodrome import deployments
from almanak.connectors.aerodrome.economic_book import PoolBook
from almanak.connectors.aerodrome.economic_collection import contract_bindings
from almanak.connectors.aerodrome.economic_data import key
from almanak.connectors.aerodrome.economic_reporting import position_report
from almanak.connectors.aerodrome_slipstream.economic_book import PoolBook as SlipstreamBook
from almanak.gateway.ethereum import DataError
from almanak.models import ZERO_ADDRESS
from tests.test_aerodrome_economic_model import component


@pytest.fixture
def prepared():
    model, state = component()
    target = state.portfolio.positions[0].payload.target
    return model, state, target, model.book.at(target, state.time)


def book(start, sample_seconds=60, max_age=60, evidence=()):
    result = object.__new__(PoolBook)
    result.settings = SimpleNamespace(
        schedule=SimpleNamespace(start=start),
        data=SimpleNamespace(sample_seconds=sample_seconds, max_age_seconds=max_age),
    )
    result.reader = SimpleNamespace(evidence=lambda digest: evidence[digest])
    return result


def moved(record, blocks):
    return record.reference.model_copy(
        update={"number": record.reference.number + blocks, "hash": "0x" + "ee" * 32}
    )


def test_a_sample_must_name_its_pool_block_grid_time_and_age(prepared):
    _, _, target, record = prepared
    at = record.available_at
    other = SimpleNamespace(**{**vars(record), "reference": moved(record, 1)})
    with pytest.raises(ValueError, match="another pool or block identity"):
        book(at).validate_row(target, other, 0)
    with pytest.raises(ValueError, match="configured time grid"):
        book(at).validate_row(target, record, 1)
    aged = SimpleNamespace(**{**vars(record), "available_at": at + 60})
    with pytest.raises(ValueError, match="source age limit"):
        book(at, max_age=1).validate_row(target, aged, 1)


def test_a_sample_without_retained_headers_cannot_be_verified(prepared):
    _, _, target, record = prepared
    with pytest.raises(ValueError, match="omits required archival RPC evidence"):
        book(record.available_at).validate_row(target, record, 0)


def sample(record, **pool):
    payload = record.payload.model_copy(
        update={"pool": record.payload.pool.model_copy(update=pool)}
    )
    return SimpleNamespace(**{**vars(record), "payload": payload})


def test_consecutive_samples_keep_identities_order_and_fee_indices(prepared):
    record = prepared[3]
    PoolBook.validate_continuity(record, record)
    changed = SimpleNamespace(
        **{
            **vars(record),
            "payload": record.payload.model_copy(
                update={"contracts": record.payload.contracts[1:]}
            ),
        }
    )
    with pytest.raises(ValueError, match="implementation identities changed"):
        PoolBook.validate_continuity(record, changed)
    earlier = SimpleNamespace(**{**vars(record), "reference": moved(record, -1)})
    with pytest.raises(ValueError, match="increasing canonical blocks"):
        PoolBook.validate_continuity(record, earlier)
    forked = SimpleNamespace(**{**vars(record), "reference": moved(record, 0)})
    with pytest.raises(ValueError, match="increasing canonical blocks"):
        PoolBook.validate_continuity(record, forked)
    pool = record.payload.pool
    for index in ("index0", "index1"):
        backwards = sample(record, **{index: getattr(pool, index) - 1})
        with pytest.raises(ValueError, match="fee indices moved backwards"):
            PoolBook.validate_continuity(record, backwards)


def test_history_bindings_cover_exactly_the_pool_chains(prepared):
    model, _, target, _ = prepared
    settings = SimpleNamespace(history=(SimpleNamespace(chain=target.fund.chain),))
    (binding,) = contract_bindings(settings, model.config)
    expected = {
        target.pool.factory,
        target.pool.pool,
        target.contracts.router,
        *(asset.reference for asset in target.assets),
    }
    assert binding.contracts == tuple(sorted(expected))
    with pytest.raises(ValueError, match="exactly the configured pool chains"):
        contract_bindings(SimpleNamespace(history=()), model.config)


def test_an_lp_report_is_modeled_or_names_its_missing_input(prepared):
    model, state, target, _ = prepared
    payload = state.portfolio.positions[0].payload
    modeled = position_report(state, model, payload)
    assert modeled["measurement_status"] == "modeled" and modeled["missing_input"] is None
    assert [claim["amount"] for claim in modeled["uncollected_fees"]] == ["0", "0"]

    def unavailable(target, time):
        raise InputUnavailable(key(target), time, "missing sample")

    model.book = SimpleNamespace(at=unavailable)
    missing = position_report(state, model, payload)
    assert missing["measurement_status"] == "unavailable" and missing["observation"] is None
    assert [claim["amount"] for claim in missing["principal"]] == [None, None]


def identity(address, code="0x00"):
    return SimpleNamespace(address=address, code_hash=code)


def test_shared_contracts_must_keep_one_identity_across_allocations(monkeypatch):
    shared = identity("0x" + "01" * 20)
    pools = {"a": (shared,), "b": (shared,), "c": (identity(shared.address, "0x11"),)}
    monkeypatch.setattr(deployments, "require_direct_fund", lambda *args: None)
    monkeypatch.setattr(deployments, "verify_pool", lambda reader, pool, block: pools[pool])
    monkeypatch.setattr(deployments, "verify_router", lambda *args: ())

    def config(*names):
        return SimpleNamespace(
            allocations=tuple(
                SimpleNamespace(name=name, account=account, pool=name, contracts=None)
                for name, account in names
            ),
            target=lambda name: SimpleNamespace(assets=()),
        )

    reader = SimpleNamespace(reader=None, block=None, identity=lambda address: None)
    assert deployments.identities(config(("a", "fund"), ("b", "fund")), "fund", reader) == (shared,)
    assert deployments.identities(config(("a", "fund"), ("c", "other")), "fund", reader) == (
        shared,
    )
    with pytest.raises(DataError, match="differs across allocations"):
        deployments.identities(config(("a", "fund"), ("c", "fund")), "fund", reader)


def slipstream(swap_module, unstaked_module, contracts):
    module = object.__new__(SlipstreamBook)
    target = SimpleNamespace(
        pool=SimpleNamespace(pool="0xpool", factory="0xfactory"),
        contracts=SimpleNamespace(model_dump=lambda: {"router": "0xrouter"}),
    )
    pool = SimpleNamespace(swap_fee_module=swap_module, unstaked_fee_module=unstaked_module)
    row = SimpleNamespace(payload=SimpleNamespace(pool=pool))
    baseline = SimpleNamespace(contracts=tuple(identity(a) for a in contracts))
    return module.validate_row(target, baseline, row)


def test_slipstream_fee_modules_need_retained_revision_evidence(monkeypatch):
    monkeypatch.setattr(v3_economic_book.PoolBook, "validate_row", lambda *args: None)
    retained = ("0xfactory", "0ximplementation", "0xpool", "0xswap", "0xrouter")
    slipstream("0xswap", ZERO_ADDRESS, retained)
    with pytest.raises(ValueError, match="lack retained revision evidence"):
        slipstream("0xswap", "0xunstaked", retained)
    with pytest.raises(ValueError, match="lack retained revision evidence"):
        slipstream("0xswap", ZERO_ADDRESS, ("0xother", *retained[1:]))
    with pytest.raises(ValueError, match="lack retained revision evidence"):
        slipstream("0xswap", ZERO_ADDRESS, (*retained[:2], "0xrouter", "0xswap", "0xpool"))
