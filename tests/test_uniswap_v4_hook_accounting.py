"""Reviewed hook terms in the offline swap model and in receipt reconciliation."""

from types import SimpleNamespace

import pytest
from pydantic import ValidationError

from almanak.connectors.concentrated_swap import exact_input as calculate
from almanak.connectors.uniswap_v4 import reconciliation
from almanak.connectors.uniswap_v4.hook_terms import (
    OutputHaircut,
    OverrideFee,
    describe,
    observed_terms,
    read_terms,
)
from almanak.connectors.uniswap_v4.pool import pool_id
from almanak.connectors.uniswap_v4.receipts import movement
from almanak.connectors.uniswap_v4.reconciliation import hook_issues, receipt_movement
from almanak.connectors.uniswap_v4.state import Allowance, Approval
from almanak.connectors.uniswap_v4.swap_math import exact_input, swap_fee, swap_step
from almanak.connectors.uniswap_v4.validation import increases_exposure, validate_entry
from almanak.gateway.ethereum import DataError, Reverted, member_result
from almanak.models import AssetAmount
from tests.uniswap_hook_support import (
    FABLES_RAMP,
    PONS_HOOK,
    PONS_TOKEN,
    PRIORS,
    ROBINHOOD,
    USDG,
    asset,
    mint,
    mint_receipt,
    pool,
    sample,
    swap,
    swap_receipt,
    target,
    transfer,
)

PONS = OutputHaircut(hook_fee_bps=100, creator_tax_bps=200)


def launch(registered=True, hook_fee=100, tax=200):
    zero = "0x" + "00" * 20
    return (registered, False, PRIORS, USDG, zero, zero, zero, tax, 3000, 5000, hook_fee, 300, True)


class Calls:
    """An EthereumReader stand-in that answers each eth_call from a fixed table."""

    def __init__(self, answers):
        self.answers, self.seen = answers, []

    def call(self, to, signature, block, *, inputs=(), args=(), outputs=()):
        self.seen.append((to, signature, args))
        answer = self.answers[signature]
        if isinstance(answer, Exception):
            raise answer
        return answer(args) if callable(answer) else answer


def test_haircut_floors_each_share_like_the_hook():
    assert PONS.take(10_049) == 10_049 * 100 // 10_000 + 10_049 * 200 // 10_000 == 300
    assert PONS.lp_fee(True, 0) == 0
    assert OutputHaircut(hook_fee_bps=1_000, creator_tax_bps=1_000).take(100) == 20


@pytest.mark.parametrize(("hook_fee", "tax"), [(1_000, 1_001), (0, 2_001), (2_001, 0)])
def test_pons_rate_cap_is_the_reviewed_2000_bps_total(hook_fee, tax):
    with pytest.raises(ValidationError):
        OutputHaircut(hook_fee_bps=hook_fee, creator_tax_bps=tax)
    reader = Calls({"launches(bytes32)": launch(hook_fee=hook_fee, tax=tax)})
    with pytest.raises(DataError) as error:
        read_terms(reader, pool("pons"), None)
    assert error.value.kind == "unsupported_pool"


def test_pons_terms_are_the_registered_pool_launch():
    source = pool("pons")
    reader = Calls({"launches(bytes32)": launch()})
    assert read_terms(reader, source, None) == PONS
    assert reader.seen == [(PONS_HOOK, "launches(bytes32)", (bytes.fromhex(pool_id(source)[2:]),))]
    unregistered = Calls({"launches(bytes32)": launch(registered=False)})
    with pytest.raises(DataError, match="not registered"):
        read_terms(unregistered, source, None)


def test_override_fees_read_each_direction_from_the_hook_view():
    fables = Calls({"currentFee(bytes32,bool)": lambda args: (8674 if args[1] else 3499,)})
    assert read_terms(fables, pool("fables"), None) == OverrideFee(fees=(8674, 3499))
    assert [args[1] for _, _, args in fables.seen] == [True, False]
    stable = Calls({"getFee((address,address,uint24,int24,address))": (76, 25)})
    terms = read_terms(stable, pool("stable_pair"), None)
    assert terms == OverrideFee(fees=(76, 25))
    assert (terms.lp_fee(True, 0), terms.lp_fee(False, 0), terms.take(10**6)) == (76, 25, 0)
    assert read_terms(stable, pool("hookless"), None) is None


def test_unmeasured_terms_stay_visible_without_failing_the_observation():
    failing = Calls({"launches(bytes32)": DataError("abi", "launches: invalid ABI response")})
    assert observed_terms(failing, pool("pons"), None) == {
        "hook_error": "hook terms unavailable: abi"
    }
    assert observed_terms(failing, pool("hookless"), None) == {}
    assert describe(failing, pool("hookless"), None) == {}


@pytest.mark.parametrize("kind", ["transport", "http", "malformed", "rate_limit", "unavailable"])
def test_provider_failures_fail_the_observation_instead_of_dropping_terms(kind):
    failing = Calls({"launches(bytes32)": DataError(kind, "launches: provider failed")})
    with pytest.raises(DataError) as error:
        observed_terms(failing, pool("pons"), None)
    assert error.value.kind == kind


def test_a_reverted_term_view_is_hook_evidence():
    with pytest.raises(Reverted) as reverted:
        member_result("eth_call", {"error": {"code": 3, "message": "execution reverted"}})
    failing = Calls({"launches(bytes32)": reverted.value})
    assert observed_terms(failing, pool("pons"), None) == {
        "hook_error": "hook terms unavailable: rpc"
    }


@pytest.mark.parametrize(
    "payload",
    [
        {"code": -32000, "message": "invalid opcode: INVALID"},
        {"code": -32000, "message": "out of gas"},
        {"code": -32603, "message": "internal error"},
        {"message": "no code"},
    ],
)
def test_a_deterministic_rpc_failure_of_the_view_is_hook_evidence(payload):
    # Geth-based nodes, Arbitrum Orbit chains included, report an invalid opcode or
    # out-of-gas in eth_call without "execution reverted"; an exit must still complete.
    with pytest.raises(DataError) as rejected:
        member_result("eth_call", {"error": payload})
    assert rejected.value.kind == "rpc"
    failing = Calls({"launches(bytes32)": rejected.value})
    assert observed_terms(failing, pool("pons"), None) == {
        "hook_error": "hook terms unavailable: rpc"
    }


@pytest.mark.parametrize("code", [-32005, 429])
def test_a_throttled_term_view_fails_the_observation(code):
    with pytest.raises(DataError) as rejected:
        member_result("eth_call", {"error": {"code": code, "message": "limit exceeded"}})
    failing = Calls({"launches(bytes32)": rejected.value})
    with pytest.raises(DataError) as error:
        observed_terms(failing, pool("pons"), None)
    assert error.value is rejected.value


def test_live_snapshots_read_only_the_terms_receipts_reconcile():
    fables = Calls({"currentFee(bytes32,bool)": DataError("transport", "unexpected read")})
    assert observed_terms(fables, pool("fables"), None, live=True) == {}
    assert fables.seen == []
    pons = Calls({"launches(bytes32)": launch()})
    assert observed_terms(pons, pool("pons"), None, live=True) == {"hook_terms": PONS}


@pytest.mark.parametrize("zero_for_one", [True, False])
def test_economic_fill_applies_the_override_fee_per_direction(zero_for_one):
    terms = OverrideFee(fees=(8674, 3499))
    hooked = sample("fables", terms)
    fee = swap_fee(terms.fees[0 if zero_for_one else 1], 0, zero_for_one)
    expected = calculate(hooked, 10**18, zero_for_one, fee, swap_step).output
    stored = calculate(hooked, 10**18, zero_for_one, 3000, swap_step).output
    assert exact_input(hooked, 10**18, zero_for_one).output == expected != stored


@pytest.mark.parametrize("zero_for_one", [True, False])
def test_economic_fill_takes_the_haircut_from_the_output(zero_for_one):
    hooked = sample("pons", PONS)
    gross = calculate(hooked, 10**18, zero_for_one, 3000, swap_step).output
    assert exact_input(hooked, 10**18, zero_for_one).output == gross - PONS.take(gross)


def test_samples_carry_terms_exactly_for_hooked_pools():
    with pytest.raises(ValidationError, match="measured hook terms"):
        sample("pons", None)
    with pytest.raises(ValidationError, match="measured hook terms"):
        sample("hookless", PONS)


def pending(operation, terms):
    return SimpleNamespace(operation=operation, before=SimpleNamespace(hook_terms=terms))


def test_pons_swap_reconciles_its_take_without_a_discrepancy():
    operation = swap("pons", USDG, 10**6, 18 * 10**21, funding=USDG)
    gross = 19_000 * 10**18
    receipt = swap_receipt(operation, gross, take=PONS.take(gross))
    deltas, nft, measured = movement(receipt, operation)
    assert (deltas, nft) == ((-(10**6), gross - PONS.take(gross)), None)
    assert measured == (PONS.take(gross), gross)
    assert hook_issues(pending(operation, PONS), measured) == ()


def test_pons_exit_leg_reconciles_the_take_in_the_funding_currency():
    operation = swap("pons", PRIORS, 10**22, 1, funding=USDG)
    receipt = swap_receipt(operation, 10**7, take=PONS.take(10**7))
    deltas, _, measured = movement(receipt, operation)
    assert deltas == (10**7 - PONS.take(10**7), -(10**22))
    assert hook_issues(pending(operation, PONS), measured) == ()


def test_a_differing_or_unmeasured_take_is_an_accounting_issue_not_a_refusal():
    operation = swap("pons", USDG, 10**6, 1, funding=USDG)
    gross = 19_000 * 10**18
    receipt = swap_receipt(operation, gross, take=PONS.take(gross) + 1)
    deltas, _, measured = movement(receipt, operation)
    assert deltas == (-(10**6), gross - PONS.take(gross) - 1)
    (issue,) = hook_issues(pending(operation, PONS), measured)
    assert "differs from the reviewed haircut" in issue
    (issue,) = hook_issues(pending(operation, None), measured)
    assert "unverified" in issue
    assert hook_issues(pending(operation, PONS), None) == ()


def test_pons_output_below_the_minimum_after_the_take_is_refused():
    gross = 19_000 * 10**18
    operation = swap("pons", USDG, 10**6, gross, funding=USDG)
    with pytest.raises(DataError, match="minimum"):
        movement(swap_receipt(operation, gross, take=PONS.take(gross)), operation)


def test_override_fee_swaps_reconcile_with_the_fee_inside_pool_math():
    operation = swap("fables", USDG, 10**6, 1, funding=USDG)
    receipt = swap_receipt(operation, 2_885 * 10**15, fee=3499)
    deltas, _, measured = movement(receipt, operation)
    assert (deltas, measured) == ((2_885 * 10**15, -(10**6)), None)
    assert hook_issues(pending(operation, OverrideFee(fees=(8674, 3499))), measured) == ()


def test_only_an_output_haircut_profile_may_take_from_the_pool_manager():
    operation = swap("fables", USDG, 10**6, 1, funding=USDG)
    receipt = swap_receipt(operation, 2_885 * 10**15)
    manager = operation.target.pool.pool_manager
    receipt["logs"].insert(-1, transfer(PONS_TOKEN, manager, FABLES_RAMP, 1))
    with pytest.raises(DataError, match="another recipient"):
        movement(receipt, operation)
    pons = swap("pons", USDG, 10**6, 1, funding=USDG)
    receipt = swap_receipt(pons, 10**22, take=PONS.take(10**22))
    # A take of the input currency is not the reviewed output haircut.
    receipt["logs"].insert(1, transfer(USDG, pons.target.pool.pool_manager, PONS_HOOK, 1))
    with pytest.raises(DataError, match="another recipient"):
        movement(receipt, pons)


def changed_pin(monkeypatch, holds):
    monkeypatch.setattr(reconciliation, "AuthorityReader", lambda reader, block: None)
    monkeypatch.setattr(reconciliation, "require_entry_code", lambda *a, entering: holds)
    return SimpleNamespace(reader=None)


def test_an_exit_through_changed_hook_code_is_recorded_from_fund_transfers(monkeypatch):
    operation = swap("fables", PONS_TOKEN, 10**22, 1, funding=USDG)
    receipt = swap_receipt(operation, 10**7)
    manager = operation.target.pool.pool_manager
    # The changed code now takes part of the output, outside the reviewed model.
    receipt["logs"].insert(-1, transfer(USDG, manager, FABLES_RAMP, 10**5))
    with pytest.raises(DataError, match="another recipient"):
        movement(receipt, operation)
    backend = changed_pin(monkeypatch, holds=False)
    deltas, nft, measured, (issue,) = receipt_movement(
        backend, pending(operation, None), receipt, None
    )
    assert (deltas, nft, measured) == ((-(10**22), 10**7), None, None)
    assert "differs from its pin" in issue and "reviewed model" in issue


def test_a_changed_pin_stays_visible_when_the_receipt_still_fits(monkeypatch):
    operation = swap("fables", USDG, 10**6, 1, funding=USDG)
    receipt = swap_receipt(operation, 2_885 * 10**15, fee=3499)
    backend = changed_pin(monkeypatch, holds=False)
    *movements, (issue,) = receipt_movement(backend, pending(operation, None), receipt, None)
    assert movements == [(2_885 * 10**15, -(10**6)), None, None]
    assert "differs from its pin" in issue and "reviewed model" not in issue
    backend = changed_pin(monkeypatch, holds=True)
    assert receipt_movement(backend, pending(operation, None), receipt, None)[3] == ()


@pytest.mark.parametrize("kind", ["approve_permit2", "revoke_permit2"])
def test_approvals_never_carry_a_changed_hook_note(monkeypatch, kind):
    selected = target("stable_pair")
    approval = Approval(
        target=selected,
        kind=kind,
        asset=selected.pool.currency0,
        spender=selected.contracts.router,
        amount=0 if kind.startswith("revoke") else 10**6,
        expiration=0 if kind.startswith("revoke") else 1,
    )
    backend = changed_pin(monkeypatch, holds=False)
    receipt = {"status": "0x1", "logs": []}
    assert receipt_movement(backend, pending(approval, None), receipt, None) == (
        (0, 0),
        None,
        None,
        (),
    )


@pytest.mark.parametrize("name", ["fables", "stable_pair"])
def test_hooked_mints_reconcile_like_hookless_ones(name):
    operation = mint(name)
    receipt = mint_receipt(operation, 7, (10**6, 2 * 10**6))
    deltas, nft, measured = movement(receipt, operation)
    assert deltas == (-(10**6), -2 * 10**6) and nft.token_id == "7" and measured is None


def test_a_mint_mined_after_the_hook_changed_still_records_its_nft(monkeypatch):
    operation = mint("stable_pair")
    receipt = mint_receipt(operation, 7, (10**6, 2 * 10**6))
    source = operation.target.pool
    # Upgraded code skims the deposit, outside the reviewed model.
    receipt["logs"].append(
        transfer(source.currency0.reference, operation.target.fund.address, source.hooks, 1)
    )
    with pytest.raises(DataError, match="unauthorized counterparty"):
        movement(receipt, operation)
    backend = changed_pin(monkeypatch, holds=False)
    deltas, nft, measured, (issue,) = receipt_movement(
        backend, pending(operation, None), receipt, None
    )
    assert deltas == (-(10**6) - 1, -2 * 10**6) and nft.token_id == "7" and measured is None
    assert "differs from its pin" in issue and "reviewed model" in issue


RUNNING = SimpleNamespace(mode="running", outcomes=())
ALLOCATION = SimpleNamespace(minimum_native_balance=0)


def observed(name, terms, error=None):
    return SimpleNamespace(
        target=target(name, USDG),
        hook_terms=terms,
        hook_error=error,
        native_balance=SimpleNamespace(amount=1),
    )


def test_an_entry_through_an_output_haircut_without_terms_is_refused():
    entry = swap("pons", USDG, 10**6, 1, funding=USDG)
    funding = Approval(
        target=entry.target,
        kind="approve_erc20",
        asset=entry.amount.asset,
        spender=entry.target.contracts.permit2,
        amount=10**6,
    )
    assert increases_exposure(None, entry) and increases_exposure(None, funding)
    with pytest.raises(DataError, match="requires measured hook terms") as error:
        validate_entry(RUNNING, observed("pons", None, "hook terms unavailable: abi"), ALLOCATION)
    assert error.value.kind == "hook_terms"
    assert "hook terms unavailable: abi" in str(error.value)
    validate_entry(RUNNING, observed("pons", PONS), ALLOCATION)
    # The Quoter already executes an override fee; its terms only explain the quote.
    validate_entry(RUNNING, observed("fables", None), ALLOCATION)
    validate_entry(RUNNING, observed("hookless", None), ALLOCATION)


def test_an_exit_without_terms_proceeds_and_records_its_take_as_unverified():
    exit_leg = swap("pons", PRIORS, 10**22, 1, funding=USDG)
    assert not increases_exposure(None, exit_leg)
    deltas, _, measured = movement(swap_receipt(exit_leg, 10**7, take=PONS.take(10**7)), exit_leg)
    assert deltas == (10**7 - PONS.take(10**7), -(10**22))
    (issue,) = hook_issues(pending(exit_leg, None), measured)
    assert "unverified" in issue


class Owned(SimpleNamespace):
    def model_copy(self, update):
        return Owned(**{**vars(self), **update})


def refunded_exit(monkeypatch, pulled, refund):
    """An exit through changed hook code whose hook returns part of the fund's input."""
    operation = swap("fables", PONS_TOKEN, pulled, 1, funding=USDG)
    receipt = swap_receipt(operation, 10**7)
    source, fund = operation.target, operation.target.fund.address
    receipt["logs"].append(transfer(PONS_TOKEN, source.pool.pool_manager, fund, refund))
    pons, usdg = source.pool.currency0, source.pool.currency1
    permit2, router = source.contracts.permit2, source.contracts.router

    def allowances(amount):
        return (
            Allowance(asset=pons, spender=permit2, amount=amount),
            Allowance(asset=pons, spender=router, amount=amount, expiration=10),
        )

    def wallet(held):
        return dict(wallet0=AssetAmount(asset=pons, amount=held[0]), wallet1=held[1])

    gas = AssetAmount(asset=asset(ROBINHOOD, "0x" + "ee" * 20), amount=1)
    common = dict(native_balance=gas, payer=None, position_liquidity=0)
    start = (5 * 10**22, 0)
    before = SimpleNamespace(
        owned=Owned(
            inventory0=AssetAmount(asset=pons, amount=start[0]),
            inventory1=AssetAmount(asset=usdg, amount=0),
            position=None,
            entry_complete=True,
        ),
        allowances=allowances(10**23),
        hook_terms=None,
        wallet0=AssetAmount(asset=pons, amount=start[0]),
        wallet1=AssetAmount(asset=usdg, amount=start[1]),
        **common,
    )
    # On chain the router pulled its exact input; the refund returned to the wallet.
    after = SimpleNamespace(
        allowances=allowances(10**23 - pulled),
        wallet0=AssetAmount(asset=pons, amount=start[0] - pulled + refund),
        wallet1=AssetAmount(asset=usdg, amount=10**7),
        block=SimpleNamespace(timestamp=1),
        **common,
    )
    monkeypatch.setattr(reconciliation, "snapshot", lambda *args, **kwargs: after)
    monkeypatch.setattr(
        reconciliation, "charged_fees", lambda *args: (gas.model_copy(update={"amount": 0}), None)
    )
    monkeypatch.setattr(reconciliation, "Outcome", lambda **fields: SimpleNamespace(**fields))
    pending = SimpleNamespace(
        operation=operation,
        before=before,
        decision=SimpleNamespace(intent=SimpleNamespace(kind="exit_exchange")),
        tx_hash=None,
        nonce=0,
        call=None,
        execution=None,
    )
    return pending, receipt


def test_a_refund_of_part_of_the_input_through_changed_hook_code_records_the_exit(monkeypatch):
    pulled, refund = 10**22, 10**21
    pending, receipt = refunded_exit(monkeypatch, pulled, refund)
    with pytest.raises(DataError):
        movement(receipt, pending.operation)
    backend = changed_pin(monkeypatch, holds=False)
    outcome = reconciliation.reconcile(backend, pending, receipt, None)
    assert (outcome.delta0, outcome.delta1) == (refund - pulled, 10**7)
    assert outcome.after.allowances[0].amount == 10**23 - pulled
    (issue,) = outcome.accounting_issues
    assert "differs from its pin" in issue and "reviewed model" in issue
