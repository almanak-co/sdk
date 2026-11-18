"""QA claims follow successful Spark operations and actual model reports."""

import pytest

from almanak.connectors.identifiers import ConnectorId
from tests.qa.catalog import AAVE_V3, Case
from tests.qa.worker import assert_measurements, fork_actions
from tests.sparklend_helpers import candidate_spark_support as candidate_spark_support
from tests.sparklend_helpers import recorded_spark as recorded_spark

SPARK = ConnectorId("sparklend")


def test_qa_derives_partial_and_full_exits_from_retained_operations(recorded_spark):
    outcomes = [outcome.model_dump(mode="json") for outcome in recorded_spark.outcomes]
    allocations = {a.name for a in recorded_spark.config.allocations}
    observed = fork_actions(outcomes, allocations, connector=SPARK)
    assert set(AAVE_V3) <= observed
    for item in outcomes:
        if item["operation"]["kind"] == "withdraw" and not item["operation"]["full"]:
            item["success"] = False
    assert "withdraw_amount" not in fork_actions(outcomes, allocations, connector=SPARK)


def test_qa_refuses_missing_spark_economic_measurements():
    case = Case(id="spark-model", lane="backtest", connector=SPARK, model_mode="economic")
    report = {
        "positions": [
            {
                "measurement_status": "modeled",
                "observation": {"retained": True},
                "missing_input": None,
            }
        ],
        "missing_inputs": [],
    }
    assert_measurements(case, report)
    report["positions"][0]["observation"] = None
    with pytest.raises(AssertionError):
        assert_measurements(case, report)
