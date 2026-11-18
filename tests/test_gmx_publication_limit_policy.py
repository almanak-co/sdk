"""Nested GMX publication acquisition limits fail closed without declaring support."""

import pytest

from almanak.caip import ChainId
from almanak.connectors.gmx_v2.connector import connector
from almanak.connectors.support import RunMode
from tests.qa.inputs import validated_collection_limits

PUBLICATIONS = {
    "page_blocks": 500,
    "maximum_blocks": 1201,
    "maximum_pages": 3,
    "maximum_logs": 10_000,
    "maximum_transactions": 500,
}


def test_exact_nested_gmx_publication_policy_is_retained():
    limits = {"maximum_risk_blocks": 1201, "publication_limits": PUBLICATIONS}

    assert validated_collection_limits(limits) is limits


@pytest.mark.parametrize(
    "publications,match",
    (
        (None, "every collection limit"),
        ({**PUBLICATIONS, "unexpected": 1}, "every collection limit"),
        ({key: value for key, value in PUBLICATIONS.items() if key != "maximum_logs"}, "every"),
        ({**PUBLICATIONS, "maximum_logs": 0}, "positive"),
        ({**PUBLICATIONS, "maximum_pages": 2}, "page limit"),
    ),
)
def test_nested_gmx_publication_policy_refuses_incomplete_or_unbounded_values(publications, match):
    with pytest.raises(ValueError, match=match):
        validated_collection_limits(
            {"maximum_risk_blocks": 1201, "publication_limits": publications}
        )


def test_nested_gmx_publication_policy_cannot_cover_less_than_its_risk_range():
    with pytest.raises(ValueError, match="block limit"):
        validated_collection_limits(
            {
                "maximum_risk_blocks": 1202,
                "publication_limits": PUBLICATIONS,
            }
        )


def test_existing_flat_collection_limits_remain_valid():
    limits = {
        "max_accounting_samples": 42,
        "max_risk_blocks": 1201,
        "max_risk_samples": 2402,
        "max_samples": 21,
    }

    assert validated_collection_limits(limits) is limits


def test_policy_validation_does_not_promote_gmx_backtests():
    declared = {support.chain: support for support in connector().chain_support}
    assert declared[ChainId("eip155:42161")].modes == (
        RunMode.FORK,
        RunMode.PRODUCTION,
        RunMode.BACKTEST,
    )
    avalanche = declared[ChainId("eip155:43114")]
    assert avalanche.modes == (RunMode.FORK,)
    assert avalanche.historical == ()
