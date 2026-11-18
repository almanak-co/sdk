"""Spark capability and operation admission retain every execution scope."""

from types import SimpleNamespace

import pytest

from almanak.caip import ChainId
from almanak.connectors.sparklend.authority import admit, declared
from almanak.connectors.sparklend.execution import validate_operation
from almanak.gateway.ethereum import DataError
from tests.sparklend_helpers import candidate_spark_support as candidate_spark_support
from tests.sparklend_helpers import recorded_spark as recorded_spark
from tests.test_sparklend_execution import replay_states


@pytest.mark.parametrize("fault", ("chain", "pool", "fund", "asset", "allocation", "account"))
def test_capability_refuses_another_execution_scope(recorded_spark, fault):
    config = recorded_spark.config
    intent = config.supply("collateral", 1)
    target = intent.target
    other = config.target("loan")
    if fault == "chain":
        chain = target.reserve.chain.model_copy(update={"chain_id": ChainId("eip155:8453")})
        changed = target.model_copy(
            update={"reserve": target.reserve.model_copy(update={"chain": chain})}
        )
    elif fault == "pool":
        changed = target.model_copy(
            update={"reserve": target.reserve.model_copy(update={"pool": "0x" + "12" * 20})}
        )
    elif fault == "fund":
        changed = target.model_copy(
            update={"fund": target.fund.model_copy(update={"address": "0x" + "12" * 20})}
        )
    elif fault == "asset":
        changed = target.model_copy(update={"reserve": other.reserve})
    else:
        changed = target.model_copy(update={fault: "foreign"})
    # Deliberately construct a tampered in-memory intent: trusted admission must
    # reject it even when a caller bypasses the wire decoder's earlier checks.
    with pytest.raises(ValueError, match="exactly one retained reserve capability"):
        admit(declared(config), intent.model_copy(update={"target": changed}))


@pytest.mark.parametrize("kind", ("supply", "borrow"))
def test_admission_carries_no_amount_bound(recorded_spark, kind):
    # Grants are scope-only: an amount far above the recorded capital stays within
    # the allocation's own reserve, bounded by inventory, risk and the Pool.
    config = recorded_spark.config
    allocation = "collateral" if kind == "supply" else "loan"
    admit(declared(config), getattr(config, kind)(allocation, 10**30))


def test_admission_refuses_borrowing_outside_a_borrowing_allocation(recorded_spark):
    config = recorded_spark.config
    assert not config.allocation("collateral").borrow
    for intent in (config.borrow("collateral", 1), config.repay("collateral")):
        with pytest.raises(ValueError, match="require a borrowing allocation"):
            admit(declared(config), intent)


@pytest.mark.parametrize("field", ("supplied_scaled", "debt_scaled"))
def test_operation_admission_refuses_unowned_position_units(recorded_spark, field):
    state, outcome = next(
        (s, o) for s, o in replay_states(recorded_spark) if o.operation.kind == "borrow"
    )
    position = outcome.before.position
    before = outcome.before.model_copy(
        update={"position": position.model_copy(update={field: getattr(position, field) + 1})}
    )
    backend = SimpleNamespace(config=state.config, guard=lambda: None)
    with pytest.raises(DataError, match="owned exposure"):
        validate_operation(backend, state, outcome.operation, before)
