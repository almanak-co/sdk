"""A swap the prepared ticks cannot fill is refused with the coverage and the cause."""

import gzip
import json
from pathlib import Path
from types import SimpleNamespace

import pytest

from almanak.connectors import dex_api
from almanak.connectors.concentrated_swap import COVERAGE_REFUSAL, coverage_refusal
from almanak.connectors.dex_residual import (
    QuoteUnfillable,
    entry_or_reason,
    refuse_unmeasured_entry,
)
from almanak.connectors.uniswap_v3.economic_data import PoolSample
from almanak.connectors.uniswap_v3.swap_math import exact_input
from almanak.connectors.uniswap_v4 import api as v4_api

CAPTURE = json.loads(
    gzip.decompress(
        (Path(__file__).with_name("fixtures") / "uniswap-v3-pool-samples.json.gz").read_bytes()
    )
)
# 10^15 base units of either asset: more than the captured coverage holds in each direction.
OVERSIZED = 10**24
V3_REFUSAL = "swap requires its exact-size source quote at the decision block"
V4_REFUSAL = "swap requires its explicit exact-size quote at the decision block"


def captured():
    return PoolSample.model_validate_json(json.dumps(CAPTURE[0]["sample"]))


@pytest.mark.parametrize(("zero_for_one", "bound"), [(True, "lower"), (False, "upper")])
def test_refusal_names_the_coverage_bounds_and_the_unfilled_input(zero_for_one, bound):
    sample = captured()
    with pytest.raises(QuoteUnfillable) as refused:
        exact_input(sample, OVERSIZED, zero_for_one)
    message = str(refused.value)
    assert message.startswith(f"{COVERAGE_REFUSAL} [{sample.lower}, {sample.upper}]: ")
    assert f"from tick {sample.pool.tick} to the {bound} bound" in message
    assert f"of {OVERSIZED} input base units unfilled" in message
    assert message.endswith("widen the prepared tick coverage or reduce the swap")
    unfilled = int(message.split(" with ")[1].split(" of ")[0])
    assert 0 < unfilled < OVERSIZED


def test_refusal_is_a_function_of_the_sample_and_the_input_alone():
    sample = captured()
    messages = set()
    for _ in range(2):
        with pytest.raises(QuoteUnfillable) as refused:
            exact_input(sample, OVERSIZED, True)
        messages.add(str(refused.value))
    assert len(messages) == 1
    assert coverage_refusal(sample, 10, 3, False) == (
        f"{COVERAGE_REFUSAL} [{sample.lower}, {sample.upper}]: the price moved from tick "
        f"{sample.pool.tick} to the upper bound with 3 of 10 input base units unfilled; "
        "widen the prepared tick coverage or reduce the swap"
    )


def test_a_swap_inside_the_coverage_is_unchanged():
    sample = captured()
    for case in CAPTURE[0]["comparisons"]:
        quote = case["quote"]
        direction = quote["asset_in"] == str(sample.pool.source.currency0)
        assert exact_input(sample, quote["amount_in"], direction).output == quote["amount_out"]


def unfilled_entry():
    sample = captured()

    def measure():
        exact_input(sample, OVERSIZED, True)

    return sample, entry_or_reason(measure)["entry_quote_error"]


def test_the_entry_observation_records_the_coverage_as_the_reason():
    sample, reason = unfilled_entry()
    assert reason.startswith(
        f"entry quote unavailable: {COVERAGE_REFUSAL} [{sample.lower}, {sample.upper}]: "
    )


class Context:
    def __init__(self, observed):
        self.observed = observed
        self.config = SimpleNamespace(allocation=lambda name: SimpleNamespace(name=name))

    def require_observation(self, name):
        return self.observed


class Actions(dex_api.Actions):
    authoring = SimpleNamespace(swap=lambda allocation, observed, quote: ("swap", quote))


def test_the_author_swap_names_the_coverage_instead_of_only_a_missing_quote():
    sample, reason = unfilled_entry()
    observed = SimpleNamespace(entry_quote=None, exit_quote="exit", entry_quote_error=reason)
    with pytest.raises(ValueError) as refused:
        Actions(Context(observed)).swap("lp")
    message = str(refused.value)
    assert message.startswith(f"{V3_REFUSAL}: entry quote unavailable: {COVERAGE_REFUSAL}")
    assert f"[{sample.lower}, {sample.upper}]" in message
    assert f"of {OVERSIZED} input base units unfilled" in message
    # The exit quote is measured, so an exit swap is not refused for the entry's cause.
    assert Actions(Context(observed)).swap("lp", exit=True) == ("swap", "exit")


def test_the_v4_author_swap_names_the_cause_too():
    observed = SimpleNamespace(
        entry_quote=None, entry_quote_error="entry quote unavailable: the quote call reverted"
    )
    with pytest.raises(ValueError, match="^" + V4_REFUSAL + ": entry quote unavailable: the"):
        v4_api.Actions(Context(observed)).swap("lp")


def test_an_absent_entry_quote_without_a_recorded_cause_is_left_to_the_authoring_refusal():
    refuse_unmeasured_entry(SimpleNamespace(entry_quote=None, entry_quote_error=None), V3_REFUSAL)
    refuse_unmeasured_entry(SimpleNamespace(entry_quote=None), V3_REFUSAL)
    refuse_unmeasured_entry(
        SimpleNamespace(entry_quote="quote", entry_quote_error=None), V3_REFUSAL
    )
    observed = SimpleNamespace(entry_quote=None, exit_quote=None, entry_quote_error=None)
    assert Actions(Context(observed)).swap("lp") == ("swap", None)
