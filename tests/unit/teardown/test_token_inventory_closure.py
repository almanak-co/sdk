"""ERC-20 closure is proven by the exit's own transactions, never by a whole-wallet balance of zero.

The scenario wallet holds 1 WETH it never bought (managed-Anvil funding, or a
user's pre-existing holding) plus the tracked lot the strategy acquired. A
correct exit sells exactly the tracked lot and leaves the idle 1 WETH behind.
"""

import json
from dataclasses import replace
from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from almanak.framework.teardown.models import PositionInfo, PositionType
from almanak.framework.teardown.token_inventory_closure import (
    TokenClosureRejected,
    capture_token_exit,
    complete_token_exit,
)

WALLET = "0x" + "1" * 40
WETH = "0x" + "c" * 40
USDG = "0x" + "d" * 40
TX_APPROVE, TX_SWAP = "0x" + "2" * 64, "0x" + "4" * 64
H0, H1 = "0x" + "a" * 64, "0x" + "b" * 64
AMOUNT = 1_113_821_510_891_031
IDLE = 10**18


@pytest.fixture(autouse=True)
def resolver(monkeypatch):
    tokens = {"WETH": WETH, WETH: WETH, "USDG": USDG, USDG: USDG}
    fake = MagicMock()

    def resolve(token, chain):
        address = tokens.get(token) or tokens.get(token.lower())
        if address is None:
            raise ValueError(token)
        return SimpleNamespace(address=address, decimals=18 if address == WETH else 6)

    fake.resolve.side_effect = resolve
    monkeypatch.setattr("almanak.framework.data.tokens.get_token_resolver", lambda: fake)
    return fake


@pytest.fixture
def scenario():
    position = PositionInfo(
        PositionType.TOKEN, "uniswap_v3_weth_position", "robinhood", "uniswap_v3", Decimal(3), details={"token": "WETH"}
    )
    strategy = SimpleNamespace(deployment_id="deployment:test", chain="robinhood", wallet_address=WALLET)
    intent = SimpleNamespace(from_token="WETH", amount=Decimal(AMOUNT) / 10**18, chain="robinhood")
    transactions = {
        TX_APPROVE: dict(
            hash=TX_APPROVE, **{"from": WALLET}, blockNumber="0x65", blockHash=H1, nonce="0x8", value="0x0"
        ),
        TX_SWAP: dict(hash=TX_SWAP, **{"from": WALLET}, blockNumber="0x65", blockHash=H1, nonce="0x9", value="0x0"),
    }
    receipts = {
        h: dict(transactionHash=h, **{"from": WALLET}, blockNumber="0x65", blockHash=H1, status="0x1")
        for h in transactions
    }
    state = {"end_nonce": 10, "end_balance": IDLE, "hash1": H1}
    gateway = MagicMock()

    def call(request, **kwargs):
        args = json.loads(request.params)
        if request.method == "eth_getBlockByNumber":
            result = (
                {"number": "0x64", "hash": H0}
                if args[0] == "0x64" or (args[0] == "latest" and not state.get("landed"))
                else {"number": "0x65", "hash": state["hash1"]}
            )
        elif request.method == "eth_getCode":
            result = state.get("code", "0x")
        elif request.method == "eth_getTransactionCount":
            result = "0x8" if args[1] == "0x64" else hex(state["end_nonce"])
        elif request.method == "eth_getTransactionByHash":
            result = transactions[args[0]]
        elif request.method == "eth_getTransactionReceipt":
            result = receipts[args[0]]
            state["landed"] = True
        else:
            raise AssertionError(request.method)
        return SimpleNamespace(success=True, result=json.dumps(result))

    gateway.rpc.Call.side_effect = call
    gateway.query_erc20_balance.side_effect = lambda **kw: (
        (IDLE + AMOUNT if kw["block"] == 100 else state["end_balance"]) if kw["token_address"] == WETH else 0
    )
    result = SimpleNamespace(
        success=True,
        swap_amounts=SimpleNamespace(amount_in_decimal_resolved=True, token_in="WETH", amount_in=AMOUNT),
        transaction_results=[SimpleNamespace(tx_hash=TX_APPROVE), SimpleNamespace(tx_hash=TX_SWAP)],
    )
    anchor = capture_token_exit(
        strategy=strategy,
        intent=intent,
        positions=[position],
        tracked={"WETH": Decimal(AMOUNT) / 10**18},
        gateway=gateway,
    )
    return SimpleNamespace(
        position=position,
        strategy=strategy,
        intent=intent,
        transactions=transactions,
        receipts=receipts,
        state=state,
        gateway=gateway,
        result=result,
        anchor=anchor,
    )


def test_exact_tracked_sale_proves_closure_while_idle_balance_remains(scenario):
    s = scenario
    proof = complete_token_exit(s.anchor, s.result, {}, s.gateway)
    assert proof.terminal_balance == IDLE
    check = proof.verify(s.position, WALLET, s.gateway, deployment_id=s.strategy.deployment_id, tracked={})
    assert check.closed and not check.unmeasured


def test_idle_balance_without_a_proof_is_still_a_measured_residual(scenario):
    from almanak.framework.teardown.token_post_condition import token_balance_teardown_post_condition

    s = scenario
    check = token_balance_teardown_post_condition(s.position, WALLET, gateway_client=s.gateway, block=101)
    assert not check.closed and not check.unmeasured


_PROOF_FAULTS = {
    "wrong_sender": lambda s: s.transactions[TX_SWAP].update({"from": "0x" + "3" * 40}),
    "wrong_receipt_hash": lambda s: s.receipts[TX_SWAP].update(transactionHash="0x" + "3" * 64),
    "failed_receipt": lambda s: s.receipts[TX_SWAP].update(status="0x0"),
    "extra_nonce": lambda s: s.state.update(end_nonce=11),
    "missing_transaction": lambda s: setattr(s.result, "transaction_results", []),
    "duplicate_transaction": lambda s: setattr(s.result, "transaction_results", s.result.transaction_results * 2),
    "partial_fill": lambda s: setattr(s.result.swap_amounts, "amount_in", AMOUNT - 1),
    "wrong_token": lambda s: setattr(s.result.swap_amounts, "token_in", "USDG"),
    "unresolved_input": lambda s: setattr(s.result.swap_amounts, "amount_in_decimal_resolved", False),
    "inflow": lambda s: s.state.update(end_balance=IDLE + 1),
    "outflow": lambda s: s.state.update(end_balance=IDLE - 1),
    "reorg": lambda s: s.state.update(hash1=H0),
    "execution_failed": lambda s: setattr(s.result, "success", False),
}
_INVENTORY_FAULTS = {"partial_disposal": {"WETH": Decimal("0.000001")}, "missing_accounting": None}


@pytest.mark.parametrize("fault", sorted(_PROOF_FAULTS))
def test_incomplete_or_conflicting_proof_refuses(scenario, fault):
    _PROOF_FAULTS[fault](scenario)
    with pytest.raises(ValueError):
        complete_token_exit(scenario.anchor, scenario.result, {}, scenario.gateway)


@pytest.mark.parametrize("fault", sorted(_INVENTORY_FAULTS))
def test_unclosed_or_unmeasured_inventory_refuses(scenario, fault):
    with pytest.raises(ValueError):
        complete_token_exit(scenario.anchor, scenario.result, _INVENTORY_FAULTS[fault], scenario.gateway)


@pytest.mark.parametrize("fault", ["reorg", "nonce_trails_exit", "later_balance_change", "reacquired"])
def test_stale_proof_requires_fresh_chain_and_inventory_revalidation(scenario, fault):
    s = scenario
    proof = complete_token_exit(s.anchor, s.result, {}, s.gateway)
    tracked = {}
    if fault == "reorg":
        s.state["hash1"] = H0
    elif fault == "nonce_trails_exit":
        s.state["end_nonce"] = 9
    elif fault == "later_balance_change":
        s.state["end_balance"] += 5
    elif fault == "reacquired":
        tracked = {"WETH": Decimal("0.001")}
    with pytest.raises(ValueError) as raised:
        proof.verify(s.position, WALLET, s.gateway, deployment_id=s.strategy.deployment_id, tracked=tracked)
    # Only a contradiction re-measures the balance; reorgs and trailing reads stay unmeasured.
    contradiction = fault in {"later_balance_change", "reacquired"}
    assert isinstance(raised.value, TokenClosureRejected) is contradiction


def test_later_teardown_transactions_keep_an_unchanged_balance_proof_valid(scenario):
    # A second exit or a consolidation swap advances the wallet nonce without
    # touching this token; the unchanged balance still proves the closure.
    s = scenario
    proof = complete_token_exit(s.anchor, s.result, {}, s.gateway)
    s.state["end_nonce"] = 14

    check = proof.verify(s.position, WALLET, s.gateway, deployment_id=s.strategy.deployment_id, tracked={})

    assert check.closed


@pytest.mark.parametrize("fault", ["wrong_position", "wrong_wallet", "wrong_deployment", "wrong_token"])
def test_proof_cannot_be_transferred_to_another_identity(scenario, fault):
    s = scenario
    proof = complete_token_exit(s.anchor, s.result, {}, s.gateway)
    deployment_id = s.strategy.deployment_id
    if fault == "wrong_position":
        s.position.position_id = "another"
    elif fault == "wrong_deployment":
        deployment_id = "deployment:other"
    elif fault == "wrong_token":
        s.position.details = {"token": "USDG"}
    wallet = "0x" + "3" * 40 if fault == "wrong_wallet" else WALLET
    with pytest.raises(ValueError, match="identity"):
        proof.verify(s.position, wallet, s.gateway, deployment_id=deployment_id, tracked={})


@pytest.mark.parametrize(
    "fault", ["no_inventory", "excess_amount", "short_amount", "multiple_positions", "balance_short", "contract_sender"]
)
def test_capture_refuses_unattributed_or_ambiguous_inventory(scenario, fault):
    s = scenario
    tracked = {"WETH": Decimal(AMOUNT) / 10**18}
    positions = [s.position]
    if fault == "no_inventory":
        tracked = None
    elif fault == "excess_amount":
        s.intent.amount *= 2
    elif fault == "short_amount":
        s.intent.amount /= 2
    elif fault == "multiple_positions":
        positions *= 2
    elif fault == "balance_short":
        s.gateway.query_erc20_balance.side_effect = None
        s.gateway.query_erc20_balance.return_value = AMOUNT - 1
    elif fault == "contract_sender":
        s.state["code"] = "0x6000"
    with pytest.raises(ValueError):
        capture_token_exit(
            strategy=s.strategy, intent=s.intent, positions=positions, tracked=tracked, gateway=s.gateway
        )


@pytest.mark.parametrize("from_token", ["USDG", "ETH", "UNKNOWN"])
def test_intent_that_sells_no_token_position_has_no_anchor(scenario, from_token):
    s = scenario
    s.intent.from_token = from_token
    assert (
        capture_token_exit(
            strategy=s.strategy,
            intent=s.intent,
            positions=[s.position],
            tracked={"WETH": Decimal(AMOUNT) / 10**18},
            gateway=s.gateway,
        )
        is None
    )


@pytest.mark.asyncio
async def test_manager_and_post_reconciliation_share_chain_bound_proof(scenario):
    from almanak.framework.teardown.plan_a_reconciliation import reconcile_known_positions_against_chain
    from almanak.framework.teardown.teardown_manager import TeardownManager

    s = scenario
    proof = complete_token_exit(s.anchor, s.result, {}, s.gateway)
    manager = TeardownManager()
    manager._inventory_closure_proofs = {s.anchor.key: proof}
    manager.runner_helpers = replace(manager.runner_helpers, get_native_closure_inventory=lambda *args: {})
    manager._teardown_gateway_client = lambda: s.gateway
    manager._teardown_rpc_url = lambda: None
    manager._teardown_wallet_address = lambda strategy: WALLET
    summary = SimpleNamespace(positions=[s.position])
    verification = await manager._verify_closure_detailed(
        s.strategy, pre_execution_positions=summary, close_receipt_block=101
    )
    assert verification.all_closed
    assert verification.positions_closed == 1
    report = await reconcile_known_positions_against_chain(
        summary=summary,
        gateway_client=s.gateway,
        market=None,
        wallet_address=WALLET,
        phase="post",
        token_closure_authority=lambda position, wallet, gateway: manager._inventory_closure_check(
            position, wallet, gateway, deployment_id=s.strategy.deployment_id, strategy=s.strategy
        ),
    )
    assert len(report.diverged) == 1
    assert not report.confirmed


def test_manager_records_the_proof_after_a_committed_exit(scenario):
    from almanak.framework.teardown.teardown_manager import TeardownManager

    s = scenario
    manager = TeardownManager()
    manager._inventory_closure_positions = [s.position]
    manager._inventory_closure_proofs = {}
    manager.runner_helpers = replace(
        manager.runner_helpers,
        get_native_closure_inventory=lambda *args: {"WETH": Decimal(AMOUNT) / 10**18},
    )
    manager._teardown_gateway_client = lambda: s.gateway
    context = SimpleNamespace(chain="robinhood", wallet_address=WALLET)
    anchor = manager._capture_inventory_exit(s.strategy, s.intent, context)
    assert anchor == s.anchor
    manager.runner_helpers = replace(manager.runner_helpers, get_native_closure_inventory=lambda *args: {})
    manager._complete_inventory_exit(s.strategy, anchor, s.result)
    assert set(manager._inventory_closure_proofs) == {s.anchor.key}


def _manager_with_proof(s):
    from almanak.framework.teardown.teardown_manager import TeardownManager

    proof = complete_token_exit(s.anchor, s.result, {}, s.gateway)
    manager = TeardownManager()
    manager._inventory_closure_proofs = {s.anchor.key: proof}
    manager.runner_helpers = replace(manager.runner_helpers, get_native_closure_inventory=lambda *args: {})
    manager._teardown_gateway_client = lambda: s.gateway
    manager._teardown_rpc_url = lambda: None
    manager._teardown_wallet_address = lambda strategy: WALLET
    return manager


@pytest.mark.asyncio
async def test_a_later_teardown_exit_does_not_unverify_an_already_closed_token(scenario):
    s = scenario
    manager = _manager_with_proof(s)
    s.state["end_nonce"] = 14

    verification = await manager._verify_closure_detailed(
        s.strategy, pre_execution_positions=SimpleNamespace(positions=[s.position]), close_receipt_block=101
    )

    assert verification.all_closed
    assert verification.positions_closed == 1


@pytest.mark.asyncio
@pytest.mark.parametrize("inflow", [AMOUNT, 11], ids=["lot_returned", "third_party_dust"])
async def test_a_rejected_proof_on_a_wallet_with_a_proven_idle_holding_stays_unverified(scenario, inflow):
    # The whole-account balance still contains the wallet's own 1 WETH, so a
    # re-measurement would report it as this position's residual: anyone able
    # to send 11 wei would fail a correctly closed teardown.
    from almanak.framework.teardown.models import VerificationStatus

    s = scenario
    manager = _manager_with_proof(s)
    s.state["end_balance"] = IDLE + inflow

    verification = await manager._verify_closure_detailed(
        s.strategy, pre_execution_positions=SimpleNamespace(positions=[s.position]), close_receipt_block=101
    )

    assert verification.verification_status is VerificationStatus.UNVERIFIED


@pytest.fixture
def no_idle_balance(monkeypatch):
    import sys

    monkeypatch.setattr(sys.modules[__name__], "IDLE", 0)


@pytest.mark.asyncio
async def test_tokens_returned_after_the_close_block_are_read_at_the_latest_block(no_idle_balance, scenario):
    # Zero at the close receipt, back in the wallet one block later: only a
    # latest-block read of the rejected proof's position sees the residual.
    from almanak.framework.teardown.models import VerificationStatus

    s = scenario
    manager = _manager_with_proof(s)
    H2 = "0x" + "e" * 64
    original_call = s.gateway.rpc.Call.side_effect

    def call(request, **kwargs):
        args = json.loads(request.params)
        if request.method == "eth_getBlockByNumber" and args[0] in ("latest", "0x66"):
            return SimpleNamespace(success=True, result=json.dumps({"number": "0x66", "hash": H2}))
        return original_call(request, **kwargs)

    s.gateway.rpc.Call.side_effect = call
    s.gateway.query_erc20_balance.side_effect = lambda **kw: (
        {100: AMOUNT, 101: 0}.get(kw["block"], AMOUNT) if kw["token_address"] == WETH else 0
    )

    verification = await manager._verify_closure_detailed(
        s.strategy, pre_execution_positions=SimpleNamespace(positions=[s.position]), close_receipt_block=101
    )

    assert not verification.all_closed
    assert verification.verification_status is VerificationStatus.FAILED
    # The re-measurement is pinned to the block the rejection observed, never an
    # unpinned "latest" that a trailing read replica could answer pre-exit.
    reads = [call.kwargs["block"] for call in s.gateway.query_erc20_balance.call_args_list]
    assert "latest" not in reads and reads[-1] == 0x66


@pytest.mark.asyncio
async def test_an_unmeasured_proof_read_stays_unverified_rather_than_a_residual(scenario):
    from almanak.framework.teardown.models import VerificationStatus

    # The proof's own nonce read fails while the balance stays readable: with
    # the idle 1 WETH still held, re-measuring would report a false residual.
    s = scenario
    manager = _manager_with_proof(s)
    original_call = s.gateway.rpc.Call.side_effect

    def call(request, **kwargs):
        if request.method == "eth_getTransactionCount":
            return SimpleNamespace(success=False, result="", error="upstream timeout")
        return original_call(request, **kwargs)

    s.gateway.rpc.Call.side_effect = call

    verification = await manager._verify_closure_detailed(
        s.strategy, pre_execution_positions=SimpleNamespace(positions=[s.position]), close_receipt_block=101
    )

    assert verification.verification_status is VerificationStatus.UNVERIFIED
