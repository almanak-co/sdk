from copy import deepcopy
from types import SimpleNamespace

import pytest

from almanak.framework.anvil.accounts import anvil_default_address
from qa_lab.chains import TOKENS
from qa_lab.e2e_actor_terminal import actor_terminal_predicate, capture_actor_terminal, validate_actor_terminal_contract
from qa_lab.e2e_card import canonical


@pytest.fixture
def fork():
    reads = []
    values = {TOKENS["arbitrum"]["WETH"][0].lower(): 0, TOKENS["arbitrum"]["USDC"][0].lower(): 123456}
    pool = {"pending": {}, "queued": {}}

    def call(tx, block_identifier):
        reads.append(block_identifier)
        return values[tx["to"].lower()].to_bytes(32, "big")

    client = SimpleNamespace(
        eth=SimpleNamespace(
            get_block=lambda tag: {"number": 101, "hash": b"a" * 32},
            call=call,
            get_balance=lambda wallet, block_identifier: 10**18,
        ),
        provider=SimpleNamespace(make_request=lambda method, params: {"result": pool}),
    )
    context = SimpleNamespace(
        chain="arbitrum",
        network="anvil",
        assert_rpc_identity=lambda supplied=None: client,
        public_identity=lambda: {"chain": "arbitrum", "chain_id": 42161, "network": "anvil", "fork_block": 100},
    )
    return SimpleNamespace(context=context, client=client, pool=pool, values=values, reads=reads)


def test_actor_zero_is_measured_while_retained_capital_is_not_burned(fork):
    raw = capture_actor_terminal(fork.context)
    result = actor_terminal_predicate(raw)
    assert fork.reads == [101, 101]
    assert result["status"] == "PASS"
    assert result["balances_raw"] == {"WETH": "0", "USDC": "123456"}
    assert result["native_wei"] == str(10**18)
    assert result["other_assets"] == result["e2e_admission"] == "UNMEASURED"


@pytest.mark.parametrize("kind", ["pending", "queued"])
def test_zero_weth_cannot_hide_a_future_actor_transaction(fork, kind):
    fork.pool[kind][anvil_default_address(1)] = {"0x1": {"hash": "0x" + "11" * 32}}
    result = actor_terminal_predicate(capture_actor_terminal(fork.context))
    assert result["status"] == "FAIL"
    assert result["pending_transactions"] == 1


def test_residual_weth_is_not_closed(fork):
    fork.values[TOKENS["arbitrum"]["WETH"][0].lower()] = 1
    assert actor_terminal_predicate(capture_actor_terminal(fork.context))["status"] == "FAIL"


@pytest.mark.parametrize(
    "mutation", ["missing_balance", "wrong_call", "units", "missing_queue", "ambiguous_account", "wrong_wallet"]
)
def test_missing_or_contradictory_terminal_evidence_cannot_pass(fork, mutation):
    raw = deepcopy(capture_actor_terminal(fork.context))
    if mutation == "missing_balance":
        raw["tokens"]["WETH"]["response"] = "0x"
    elif mutation == "wrong_call":
        raw["tokens"]["WETH"]["call"]["data"] = "0x70a08231" + "0" * 64
    elif mutation == "units":
        raw["tokens"]["WETH"]["decimals"] = 6
    elif mutation == "missing_queue":
        del raw["txpool"]["queued"]
    elif mutation == "wrong_wallet":
        raw["wallet"] = "0x" + "33" * 20
    else:
        wallet = anvil_default_address(1)
        raw["txpool"]["pending"] = {wallet: {}, wallet.lower(): {}}
    assert actor_terminal_predicate(raw)["status"] == (
        "UNMEASURED" if mutation in {"missing_balance", "missing_queue"} else "FAIL"
    )


def test_incomplete_rpc_token_response_is_never_zero(fork):
    fork.client.eth.call = lambda *args, **kwargs: b""
    with pytest.raises(ValueError, match="unmeasured"):
        capture_actor_terminal(fork.context)


def test_a_transaction_mined_between_balance_and_txpool_reads_invalidates_capture(fork):
    calls = []

    def advancing(tag):
        calls.append(tag)
        return {"number": 101 if len(calls) == 1 else 102, "hash": b"a" * 32}

    fork.client.eth.get_block = advancing
    with pytest.raises(ValueError, match="block changed"):
        capture_actor_terminal(fork.context)


@pytest.fixture
def terminal_bundle(fork, tmp_path):
    raw = capture_actor_terminal(fork.context)
    (tmp_path / "stimulus-terminal.json").write_bytes(canonical(raw))
    subject = {
        "fork_identity": raw["fork_identity"],
        "end_block": raw["block_number"],
        "end_block_hash": raw["block_hash"],
    }
    (tmp_path / "positions-terminal.json").write_bytes(canonical(subject))
    cell = {
        "strategy_path": "strategies/accounting/lp_dual",
        "chain": "arbitrum",
        "protocol": "uniswap_v3",
        "primitive": "lp",
        "network": "anvil",
        "exec_path": "eoa",
    }
    contract = {"actor_terminal": {"schema_version": 1, "model": "stimulus-weth-and-pending-v1"}}
    return tmp_path, raw, subject, contract, cell


def test_terminal_admission_recomputes_raw_inventory_and_names_authorities(terminal_bundle):
    bundle, _, _, contract, cell = terminal_bundle
    (bundle / "stimulus-cleanup.json").write_bytes(canonical({"status": "FAIL"}))
    result = validate_actor_terminal_contract(bundle, contract, catalog_cell=cell)
    assert result["status"] == "PASS"
    assert result["scope"] == "stimulus_weth_and_pending_transactions"
    assert result["other_assets"] == result["e2e_admission"] == "UNMEASURED"
    assert result["source_artifacts"] == ["stimulus-terminal.json", "positions-terminal.json"]


@pytest.mark.parametrize("mutation", ["residual", "pending", "missing", "fork", "block", "hash", "surface"])
def test_producer_pass_cannot_hide_invalid_actor_terminal_inventory(terminal_bundle, mutation):
    bundle, raw, subject, contract, cell = terminal_bundle
    if mutation == "residual":
        raw["tokens"]["WETH"]["response"] = "0x" + "0" * 63 + "1"
    elif mutation == "pending":
        raw["txpool"]["pending"][anvil_default_address(1)] = {"0x1": {}}
    elif mutation == "missing":
        del raw["tokens"]["WETH"]
    elif mutation == "fork":
        subject["fork_identity"] = {**subject["fork_identity"], "run_id": "different-run"}
    elif mutation == "block":
        subject["end_block"] += 1
    elif mutation == "hash":
        subject["end_block_hash"] = "0x" + "bb" * 32
    else:
        cell["network"] = "mainnet"
    (bundle / "stimulus-terminal.json").write_bytes(canonical(raw))
    (bundle / "positions-terminal.json").write_bytes(canonical(subject))
    (bundle / "stimulus-cleanup.json").write_bytes(canonical({"status": "PASS"}))
    with pytest.raises(ValueError):
        validate_actor_terminal_contract(bundle, contract, catalog_cell=cell)
