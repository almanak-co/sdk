"""SDK templates and settlement replay independently captured Slipstream execution."""

from copy import deepcopy
from types import SimpleNamespace

import pytest
from eth_abi import encode
from pydantic import TypeAdapter, ValidationError

from almanak.connectors.aerodrome_slipstream import abi, receipts
from almanak.connectors.aerodrome_slipstream.operations import ProtocolOperation
from almanak.connectors.aerodrome_slipstream.reader import nft_position
from almanak.connectors.aerodrome_slipstream.simulation import validate_simulation
from almanak.connectors.pancakeswap_v3.operations import ProtocolOperation as PancakeOperation
from almanak.gateway.ethereum import DataError
from tests.slipstream_support import CAPTURE, nft, operation, target


@pytest.mark.parametrize("index", (0, 1, 2, 3, 4, 5, 6, 7, 12, 16, 17, 18))
def test_independent_native_calls_returns_and_receipts(index):
    step = CAPTURE["steps"][index]
    planned = operation(index)
    assert abi.call(planned).model_dump(mode="json") == step["call"]
    validate_simulation(planned, step["simulation"], None)
    effect = receipts.movement(step["receipt"], planned)
    assert (effect.delta0, effect.delta1) == tuple(
        a - b for a, b in zip(step["after"], step["before"], strict=True)
    )
    assert TypeAdapter(ProtocolOperation).validate_json(planned.model_dump_json()) == planned
    with pytest.raises(ValidationError):
        TypeAdapter(PancakeOperation).validate_json(planned.model_dump_json())


@pytest.mark.parametrize("index", (1, 4, 7, 17))
def test_wrong_fund_receipt_is_refused(index):
    step = CAPTURE["steps"][index]
    receipt = deepcopy(step["receipt"])
    original = target().fund.address[2:]
    for log in receipt["logs"]:
        log["topics"] = [value.replace(original, "ab" * 20) for value in log["topics"]]
        log["data"] = log["data"].replace(original, "ab" * 20)
    with pytest.raises(DataError):
        receipts.movement(receipt, operation(index))


class PositionReader:
    def __init__(self, owner=None, position=None):
        self.owner = owner or target().fund.address
        self.position = position or tuple(CAPTURE["steps"][7]["position"])

    def call(self, address, signature, block, **kwargs):
        assert address == target().contracts.position_manager
        assert kwargs["args"] == (CAPTURE["nft"],)
        if signature == "ownerOf(uint256)":
            return (self.owner,)
        assert signature == "positions(uint256)" and kwargs["outputs"][4] == "int24"
        return self.position


def test_manager_decodes_spacing_and_retains_unstaked_fund_ownership():
    position = nft_position(PositionReader(), target(), nft(), None)
    assert position.liquidity == CAPTURE["steps"][7]["position"][7]
    with pytest.raises(DataError, match="not owned"):
        nft_position(PositionReader(owner="0x" + "ab" * 20), target(), nft(), None)
    altered = list(CAPTURE["steps"][7]["position"])
    altered[4] = 332
    with pytest.raises(DataError, match="pool or range"):
        nft_position(PositionReader(position=tuple(altered)), target(), nft(), None)


def test_router_rejects_wrong_return_envelope_and_minimum():
    planned = operation(1)
    for raw in (
        encode(("uint256",), (planned.minimum.amount - 1,)),
        encode(("bytes[]",), ((encode(("uint256",), (planned.minimum.amount,)),),)),
    ):
        with pytest.raises(DataError):
            validate_simulation(planned, "0x" + raw.hex(), None)


def test_snapshot_checks_actual_operator_fee_context_before_observing(monkeypatch):
    from almanak.connectors.aerodrome_slipstream import observations

    submitter = SimpleNamespace(address="0x" + "bc" * 20)
    binding = SimpleNamespace(profile=SimpleNamespace(submitter=submitter))
    config = SimpleNamespace(
        target=lambda name: target(), accounts=SimpleNamespace(binding=lambda name: binding)
    )
    backend = SimpleNamespace(config=config, reader=object())
    calls = []
    monkeypatch.setattr(observations, "verify_pool", lambda *args: calls.append("verified"))

    def refuse(reader, pool, block, address):
        assert calls == ["verified"] and address == submitter.address
        raise DataError("unsupported_fee_context", "discounted operator")

    monkeypatch.setattr(observations, "require_no_discount", refuse)
    with pytest.raises(DataError, match="discounted operator"):
        observations.snapshot(backend, "lp", object())


def sdk_capture():
    import gzip
    import json

    from tests.slipstream_support import EVIDENCE

    return json.loads(gzip.decompress((EVIDENCE / "sdk-operations.json.gz").read_bytes()))


@pytest.mark.parametrize("index", range(len(sdk_capture()["steps"])))
def test_all_sdk_operations_include_atomic_removal_settlement(index):
    import json

    row = sdk_capture()
    step = row["steps"][index]
    planned = TypeAdapter(ProtocolOperation).validate_json(json.dumps(step["operation"]))
    assert abi.call(planned).model_dump(mode="json") == step["call"]
    validate_simulation(planned, step["simulation"], None)
    actual = receipts.movement(step["receipt"], planned)
    assert actual.model_dump(mode="json") == step["effects"]
    if planned.target.fund.model_dump(mode="json") == row["fund"]:
        assert (actual.delta0, actual.delta1) == tuple(
            a - b for a, b in zip(step["after"], step["before"], strict=True)
        )
    assert step["receipt"]["status"] == "0x1"


def test_mined_mint_reconciliation_uses_the_spacing_position_reader():
    from almanak.connectors.aerodrome_slipstream.reconciliation import reconciled_liquidity

    step = CAPTURE["steps"][4]
    planned = operation(4)
    effect = receipts.movement(step["receipt"], planned)
    backend = SimpleNamespace(reader=PositionReader(position=tuple(step["position"])))
    actual = reconciled_liquidity(backend, None, planned, effect, None)
    assert actual.nft == nft() and actual.liquidity == effect.liquidity_delta
