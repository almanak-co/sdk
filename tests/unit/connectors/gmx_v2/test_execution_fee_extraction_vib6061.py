"""GMX keeper execution-fee extraction — VIB-6061.

The fee these tests pin is the one the dashboard's Cost Stack showed nowhere: GMX
escrows our ``msg.value`` at ``createOrder`` and splits it on execution between the
keeper and a refund to us. The keeper's cut is the real native cost of the order,
and on the sealed ``20260726-0035-gmxdca-arb`` run it was ~86% of all native spend
while the Cost Stack displayed transaction gas only.

Every expected number here was read off an UNFILTERED Arbitrum mainnet keeper
receipt (see the fixture's ``_provenance``), not derived from GMX's fee formula.
"""

from __future__ import annotations

import copy
import json
from pathlib import Path

import pytest

from almanak.connectors.gmx_v2.receipt_parser import GMXv2ReceiptParser

_FIXTURE = Path(__file__).parent / "fixtures" / "gmx_execution_fee_receipts_arbitrum.json"

_KEEPER_FEE_TOPIC = "0x57f0018c9e19829fa2f55e53969d49e96f7bc3936cd7453806b7cd0eaf5593ca"
_REFUND_TOPIC = "0xe6db92bfa9c5428bfb1001511cc8b0376a77baf28e4b374949b9770bbbb799a0"


@pytest.fixture(scope="module")
def bundle() -> dict:
    return json.loads(_FIXTURE.read_text())


@pytest.fixture
def parser() -> GMXv2ReceiptParser:
    return GMXv2ReceiptParser(chain="arbitrum")


def _leg(bundle: dict, name: str) -> tuple[dict, str, dict]:
    leg = bundle[name]
    return leg["receipt"], leg["account"], bundle["_provenance"]["expected"][name]


@pytest.mark.parametrize("leg_name", ["open", "close"])
def test_extracts_the_chain_measured_keeper_fee_and_refund(parser, bundle, leg_name):
    """The pair is read from the receipt exactly as the chain emitted it."""
    receipt, account, expected = _leg(bundle, leg_name)

    fee, refund = parser.extract_keeper_execution_fee(receipt, account=account)

    assert fee == expected["keeper_execution_fee_wei"]
    assert refund == expected["execution_fee_refund_wei"]


@pytest.mark.parametrize("leg_name", ["open", "close"])
def test_fee_plus_refund_reconstructs_the_escrow(parser, bundle, leg_name):
    """The escrow splits EXACTLY — which is why the keeper's cut needs no subtraction.

    This is the property that makes booking ``keeper_execution_fee_wei`` correct
    rather than approximate: there is no third recipient of the escrow, so the
    keeper's cut IS what we consumed.
    """
    receipt, account, expected = _leg(bundle, leg_name)

    fee, refund = parser.extract_keeper_execution_fee(receipt, account=account)

    assert fee + refund == expected["escrow_wei"]


# NOT TESTED HERE, deliberately: "the escrow overstates the consumed fee, so book
# the keeper's cut and not the escrow". That ratio is a property of OUR escrow
# sizing -- ``sdk.get_execution_fee`` floors at ``MIN_EXECUTION_FEE_FALLBACK``
# (0.001 ETH), against ~0.000118 ETH actually consumed on the sealed
# 20260726-0035-gmxdca-arb run: ~8.5x. These fixture receipts settle OTHER
# accounts' orders, whose escrow GMX sized tightly (the keeper took 67-75% of it),
# so they cannot show it. Asserting it here would pin a number the artifact does
# not measure.


def test_a_batched_keeper_receipt_measures_nothing(parser, bundle):
    """Two settlements in one receipt are unattributable — never guess between them.

    ``KeeperExecutionFee`` carries no ``orderKey``, so when a keeper transaction
    executes several orders nothing correlates a fee to a settlement. Booking either
    one would charge this strategy for another trader's order.
    """
    receipt, account, _ = _leg(bundle, "open")
    batched = copy.deepcopy(receipt)
    batched["logs"] += [
        log for log in receipt["logs"] if str(log["topics"][1]).lower() in (_KEEPER_FEE_TOPIC, _REFUND_TOPIC)
    ]
    # Defeat ResultEnricher's transactionHash-keyed parse cache.
    batched["transactionHash"] = receipt["transactionHash"][:-1] + "0"

    assert parser.extract_keeper_execution_fee(batched, account=account) == (None, None)


def test_a_refund_to_another_account_measures_nothing(parser, bundle):
    """Ownership is what proves the escrow being split was ours.

    A single-order keeper receipt settling somebody else's order passes the
    count guard by construction, so the receiver check is the only thing standing
    between us and booking their fee.
    """
    receipt, _account, _ = _leg(bundle, "open")

    assert parser.extract_keeper_execution_fee(receipt, account="0x" + "de" * 20) == (None, None)


def test_a_missing_account_measures_nothing(parser, bundle):
    """No owner to compare against means no measurement — not "accept any owner"."""
    receipt, _account, _ = _leg(bundle, "open")

    assert parser.extract_keeper_execution_fee(receipt, account=None) == (None, None)


def test_an_unresolvable_emitter_measures_nothing(bundle):
    """Without a known EventEmitter the fee is a number any contract could write."""
    receipt, account, _ = _leg(bundle, "open")

    parser = GMXv2ReceiptParser(chain="fantom")

    assert parser.extract_keeper_execution_fee(receipt, account=account) == (None, None)


def test_a_forged_emitter_is_rejected(parser, bundle):
    """The fee must come from the canonical emitter, not merely carry the right topic."""
    receipt, account, _ = _leg(bundle, "open")
    forged = copy.deepcopy(receipt)
    for log in forged["logs"]:
        if str(log["topics"][1]).lower() in (_KEEPER_FEE_TOPIC, _REFUND_TOPIC):
            log["address"] = "0x" + "ab" * 20
    forged["transactionHash"] = receipt["transactionHash"][:-1] + "1"

    assert parser.extract_keeper_execution_fee(forged, account=account) == (None, None)


@pytest.mark.parametrize("leg_name", ["open", "close"])
def test_extract_perp_fill_carries_the_fee_when_given_an_account(parser, bundle, leg_name):
    receipt, account, expected = _leg(bundle, leg_name)

    fill = parser.extract_perp_fill(receipt, account=account)

    assert fill is not None
    assert fill.keeper_execution_fee_wei == expected["keeper_execution_fee_wei"]
    assert fill.execution_fee_refund_wei == expected["execution_fee_refund_wei"]


def test_the_fail_closed_variant_measures_the_same_fields(parser, bundle):
    """``extract_perp_fill_result`` must not silently measure less than what it wraps.

    The two differ only in error handling; a kwarg added to one and not the other
    leaves the fee unmeasured BY CONSTRUCTION for every caller of the fail-closed
    variant — invisible, because both still return a valid-looking PerpFillData.
    """
    receipt, account, expected = _leg(bundle, "open")

    result = parser.extract_perp_fill_result(receipt, account=account)

    assert result.value.keeper_execution_fee_wei == expected["keeper_execution_fee_wei"]
    assert result.value.execution_fee_refund_wei == expected["execution_fee_refund_wei"]


def test_extract_perp_fill_without_an_account_leaves_the_fee_unmeasured(parser, bundle):
    """Empty != Zero: no account, no measurement — and the fill economics are untouched."""
    receipt, _account, _ = _leg(bundle, "open")

    fill = parser.extract_perp_fill(receipt)

    assert fill is not None
    assert fill.keeper_execution_fee_wei is None
    assert fill.execution_fee_refund_wei is None
    assert fill.is_open is True  # the pre-existing fill extraction still works


def test_a_fully_consumed_escrow_is_measured_without_a_refund_event(parser, bundle):
    """GMX emits NO ExecutionFeeRefund when the keeper consumes the whole escrow.

    ``GasUtils.payExecutionFee`` emits ``KeeperExecutionFee``, then returns early
    when ``refundFeeAmount == 0`` — before ``emitExecutionFeeRefund``. Requiring the
    refund event would drop precisely the LARGEST possible venue fee and render it
    "not measured", which is this ticket's own defect inverted. Ownership falls back
    to ``OrderExecuted.account``.
    """
    receipt, account, expected = _leg(bundle, "open")
    consumed = copy.deepcopy(receipt)
    consumed["logs"] = [lg for lg in consumed["logs"] if str(lg["topics"][1]).lower() != _REFUND_TOPIC]
    consumed["transactionHash"] = receipt["transactionHash"][:-1] + "2"

    fee, refund = parser.extract_keeper_execution_fee(consumed, account=account)

    assert fee == expected["keeper_execution_fee_wei"]
    assert refund == 0  # a MEASURED zero — the chain proved full consumption


def test_a_fully_consumed_escrow_for_another_account_measures_nothing(parser, bundle):
    """The zero-refund path must not weaken the ownership proof it replaces."""
    receipt, _account, _ = _leg(bundle, "open")
    consumed = copy.deepcopy(receipt)
    consumed["logs"] = [lg for lg in consumed["logs"] if str(lg["topics"][1]).lower() != _REFUND_TOPIC]
    consumed["transactionHash"] = receipt["transactionHash"][:-1] + "3"

    assert parser.extract_keeper_execution_fee(consumed, account="0x" + "de" * 20) == (None, None)
