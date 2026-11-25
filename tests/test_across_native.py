"""A direct bridge of native ETH through Across: one deposit that carries its amount as
value, a recipient paid native ETH, and a refund paid in the wrapped token.

`tests/fixtures/across-native-captures.json` holds read-only captures of 25 November
2026: a deposit a stranger sent to the Ethereum SpokePool with native value, the Base
fill that paid its recipient, fills of the wrapped native token on other chains and to
other kinds of recipient, a refund on Base and two venue answers. Nothing in it was
sent by this repository, and none of it is a lifecycle proof: no run of this SDK has
sent a native bridge, on a fork or on mainnet. Each test runs one reader, encoder or
check over real bytes, or over values stated here.
"""

import json
from datetime import UTC, datetime
from pathlib import Path
from types import SimpleNamespace

import httpx
import pytest
from eth_abi import decode
from pydantic import ValidationError

from almanak import ax_actions, ax_bridge
from almanak.caip import ChainId
from almanak.chains import evm_network
from almanak.connectors.across import direct as across_direct
from almanak.connectors.across import execution, planning, resolution
from almanak.connectors.across import quote as venue
from almanak.connectors.across import settlement_reader as chain
from almanak.connectors.across.authority import declared
from almanak.connectors.across.domain import NATIVE_DIRECT_ONLY, SPOKE_POOLS, Route
from almanak.connectors.across.lifecycle import AcrossLifecycle
from almanak.connectors.across.models import Allocation
from almanak.connectors.across.observations import PoolState, Snapshot, snapshot
from almanak.connectors.across.operations import (
    DEPOSIT_TYPES,
    ApproveOperation,
    DepositOperation,
    plan_call,
)
from almanak.connectors.across.receipts import movement, opened_transfer
from almanak.connectors.across.relay import parse_reference
from almanak.connectors.across.resolution import DirectSelection, MarketSelection, ResolvedRoute
from almanak.connectors.across.state import Checkpoint, DecisionRecord, Pending
from almanak.connectors.direct import ALLOCATION, ActionRequest, Wallet
from almanak.gateway.accounts import prepare_call
from almanak.gateway.ethereum import DataError
from almanak.gateway.production_fees import validate_budget
from almanak.models import Asset, AssetAmount, Block, Chain
from almanak.operator_deployment import ConsumedNonce
from almanak.request_runtime import drop_superseded, release_pending
from almanak.strategy_api import Memory
from almanak.triggers import OperatorEvent
from tests.test_ax_bridge import Ledger, late_gateway, parsed
from tests.test_expired_attempt_release import gateway_service

CAPTURES = json.loads(
    (Path(__file__).parent / "fixtures" / "across-native-captures.json").read_text()
)
SENT, FILLS, REFUND = CAPTURES["deposit"], CAPTURES["fills"], CAPTURES["refund"]
ETHEREUM = Chain(chain_id=ChainId("eip155:1"), environment="mainnet")
BASE = Chain(chain_id=ChainId("eip155:8453"), environment="mainnet")
ETH = evm_network(ETHEREUM.chain_id).native_asset
BASE_ETH = evm_network(BASE.chain_id).native_asset
WETH = evm_network(ETHEREUM.chain_id).wrapped_native
BASE_WETH = evm_network(BASE.chain_id).wrapped_native
PIN, BASE_PIN = SPOKE_POOLS[ETHEREUM.chain_id], SPOKE_POOLS[BASE.chain_id]
SENDER, RECIPIENT = "0x" + "11" * 20, "0x" + "22" * 20
AMOUNT = 10**16
FEE_CEILING = 10**15
NOW = 1_795_622_300
BLOCK = Block(chain=ETHEREUM, number=26_493_300, hash="0x" + "ab" * 32, timestamp=NOW)
BASE_BLOCK = Block(chain=BASE, number=49_990_000, hash="0x" + "bc" * 32, timestamp=NOW)
LIMITS = ax_actions.FeeLimits(max_gas_price_wei=10**10, max_transaction_fee=FEE_CEILING)
# Front ends append a short tag after the ABI encoding, which the SpokePool does not read.
MAX_TAG_BYTES = 8


def pool_state(pin) -> PoolState:
    return PoolState(
        address=pin.address,
        implementation=pin.implementation,
        implementation_code_hash=pin.implementation_code_hash,
        quote_time_buffer=3600,
        fill_deadline_buffer=21600,
        deposits_paused=False,
    )


POOL, BASE_POOL = pool_state(PIN), pool_state(BASE_PIN)
SELECTION = DirectSelection(
    connector="across",
    chain=ETHEREUM.chain_id,
    asset=ETH,
    destination_chain=BASE.chain_id,
    destination_asset=BASE_ETH,
    max_fee_bps=10,
    quote="across_api",
)
RESOLVED = ResolvedRoute(
    selection=SELECTION,
    block=BLOCK,
    asset=Asset(chain=ETHEREUM, identifier=ETH, decimals=18),
    pool=POOL,
    wrapped_native=WETH.reference,
    destination=Asset(chain=BASE, identifier=BASE_ETH, decimals=18),
    destination_block=BASE_BLOCK,
    destination_pool=BASE_POOL,
    destination_wrapped_native=BASE_WETH.reference,
)
ROUTE = RESOLVED.route


def operator_config(sender=SENDER, recipient=RECIPIENT, amount=AMOUNT, route=RESOLVED):
    """The operator deployment `ax bridge` builds for a native route."""
    wallet = Wallet(account="fund", address=sender)
    request = ActionRequest("bridge", amount, recipient=recipient)
    positions = across_direct.positions(None, BLOCK, route, wallet, request)
    provider = ax_actions.alchemy(ETHEREUM.chain_id)
    return ax_actions.configuration("across", positions, wallet, provider, ETHEREUM, LIMITS)


CONFIG = operator_config()
TARGET = CONFIG.target(ALLOCATION)
INTENT = CONFIG.bridge(ALLOCATION, AMOUNT)


def seen(config=CONFIG, balance=AMOUNT + FEE_CEILING, **changes) -> Snapshot:
    target = config.target(ALLOCATION)
    values = {
        "target": target,
        "block": BLOCK,
        "wallet_balance": balance,
        "allowance": 0,
        "pool": POOL,
        "native_balance": AssetAmount(asset=ETH, amount=balance),
        "payer": target.fund,
        **changes,
    }
    return Snapshot(**values)


def quoted(target=TARGET, amount=AMOUNT, **changes) -> venue.Quote:
    values = {
        "source": "across_api",
        "input_amount": amount,
        "output_amount": amount - amount // 10_000,
        "timestamp": NOW - 60,
        "fill_deadline": NOW - 60 + 7200,
        "exclusive_relayer": "0x" + "00" * 20,
        "exclusivity_parameter": 0,
        "spoke_pool": target.route.spoke_pool,
        "destination_spoke_pool": target.route.destination_pool.address,
        "input_token": target.route.input_token,
        "input_decimals": 18,
        "output_token": target.route.output_token,
        "output_decimals": 18,
        "amount_too_low": False,
        "response": "{}",
        **changes,
    }
    return venue.Quote(**values)


def deposit(config=CONFIG, amount=AMOUNT, value=AMOUNT) -> DepositOperation:
    target = config.target(ALLOCATION)
    values = {
        "target": target,
        "amount": AssetAmount(asset=target.asset, amount=amount),
        "quote": quoted(target, amount),
        "quote_time_buffer": 3600,
        "value": value,
    }
    return DepositOperation(**values)


def state(config=CONFIG, **changes):
    """The checkpoint fields planning and validation read."""
    decision = SimpleNamespace(intent=config.bridge(ALLOCATION, AMOUNT), identity="0" * 32)
    values = {
        "config": config,
        "capabilities": declared(config),
        "decision": decision,
        "outcomes": (),
        "baseline": (),
        "mode": "running",
        **changes,
    }
    return SimpleNamespace(**values)


def loaded(model, values: dict):
    """A model read from its wire form, as a configuration or a checkpoint is."""
    return model.model_validate_json(json.dumps(values))


def number(chain_id: str) -> int:
    return int(chain_id.split(":")[1])


def address(word: bytes) -> str:
    return "0x" + word[12:].hex()


# Identity: the native asset by its CAIP-19 reference, wrapped only inside the connector.


def test_a_native_route_keeps_the_native_identity_and_wraps_only_for_the_pool():
    assert (ROUTE.asset, ROUTE.destination_asset) == (ETH, BASE_ETH)
    assert str(ETH) == "eip155:1/slip44:60" and ROUTE.native
    assert (ROUTE.input_token, ROUTE.output_token) == (WETH.reference, BASE_WETH.reference)
    assert (TARGET.asset, INTENT.amount.asset) == (ETH, ETH)
    # The venue has no native parameter: it is asked with the wrapped tokens.
    asked = venue.request(TARGET, AMOUNT)
    assert (asked["inputToken"], asked["outputToken"]) == (WETH.reference, BASE_WETH.reference)
    # A route of two tokens states no wrapped token and serializes as it always did.
    assert "wrapped" not in loaded(
        Route, {**ROUTE.model_dump(mode="json"), "wrapped": None, **USDC_ROUTE}
    ).model_dump(mode="json")
    assert Route.model_validate_json(ROUTE.model_dump_json()) == ROUTE


USDC_ROUTE = {
    "asset": "eip155:1/erc20:0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48",
    "destination_asset": "eip155:8453/erc20:0x833589fcd6edb6e08f4c7c32d4f71b54bda02913",
    "decimals": 6,
}


def stated(**changes) -> dict:
    values = {
        "connector": "across",
        "chain": "eip155:1",
        "asset": str(ETH),
        "destination_chain": "eip155:8453",
        "destination_asset": str(BASE_ETH),
        "max_fee_bps": 10,
        "quote": "across_api",
        **changes,
    }
    return values


@pytest.mark.parametrize(
    ("changes", "cause"),
    [
        # Native on one side only, either way round.
        ({"destination_asset": str(BASE_WETH)}, "on both chains or on neither"),
        ({"asset": str(WETH)}, "on both chains or on neither"),
        # BNB Smart Chain, Polygon and Avalanche: another coin is native there.
        (
            {"chain": "eip155:56", "asset": "eip155:56/slip44:714"},
            "needs ETH as the native coin of both chains",
        ),
        (
            {"destination_chain": "eip155:137", "destination_asset": "eip155:137/slip44:966"},
            "needs ETH as the native coin of both chains",
        ),
        (
            {"destination_chain": "eip155:43114", "destination_asset": "eip155:43114/slip44:9005"},
            "needs ETH as the native coin of both chains",
        ),
        # ETH named as the native coin of a chain whose native coin is another.
        ({"chain": "eip155:56", "asset": "eip155:56/slip44:60"}, "is not the native asset"),
    ],
)
def test_a_native_asset_is_admitted_as_eth_on_both_chains_only(changes, cause):
    with pytest.raises(ValidationError, match=cause):
        loaded(DirectSelection, stated(**changes))


def test_every_chain_whose_native_coin_is_eth_takes_a_native_route_to_another():
    eth_chains = [c for c in SPOKE_POOLS if evm_network(c).native_asset.reference == "60"]
    assert sorted(eth_chains, key=lambda c: number(str(c))) == [
        ChainId(f"eip155:{n}") for n in (1, 10, 4663, 8453, 42161)
    ]
    for source in eth_chains:
        there = next(c for c in eth_chains if c != source)
        selection = loaded(
            DirectSelection,
            stated(
                chain=str(source),
                asset=str(evm_network(source).native_asset),
                destination_chain=str(there),
                destination_asset=str(evm_network(there).native_asset),
            ),
        )
        assert selection.native


def test_a_strategy_names_no_native_route():
    """An author file is read as a `MarketSelection`, and an allocation without the
    operator's `adopt` statement refuses the route itself."""
    with pytest.raises(ValidationError, match="direct command `almanak ax bridge` only"):
        loaded(MarketSelection, stated())
    held = CONFIG.allocation(ALLOCATION).model_dump(mode="json")
    held.pop("adopt")
    with pytest.raises(ValidationError) as error:
        loaded(Allocation, {**held, "destination_account": "base"})
    assert NATIVE_DIRECT_ONLY in str(error.value)
    # The wrapped token as an ERC-20 sent asset stays refused, for a command too.
    with pytest.raises(ValidationError, match="wrapped native asset are not supported"):
        loaded(
            Route,
            {
                **ROUTE.model_dump(mode="json"),
                "asset": str(WETH),
                "destination_asset": str(BASE_WETH),
                "wrapped": None,
            },
        )


def test_a_native_route_retains_the_wrapped_tokens_its_pools_named():
    raw = ROUTE.model_dump(mode="json")
    for wrapped in (None, [WETH.reference, WETH.reference], [BASE_WETH.reference, WETH.reference]):
        with pytest.raises(ValidationError, match="retains the wrapped native token"):
            loaded(Route, {**raw, "wrapped": wrapped})
    with pytest.raises(ValidationError, match="18 decimals"):
        loaded(Route, {**raw, "decimals": 6})
    with pytest.raises(ValidationError, match="only a native Across route"):
        loaded(Route, {**raw, **USDC_ROUTE})


class PoolReads:
    """A reader at one pinned block that answers the pool's own wrapped native token."""

    def __init__(self, block, wrapped, asked=None):
        self.chain, self.block, self.wrapped = block.chain, block, wrapped
        self.asked = [] if asked is None else asked

    def pin_block(self, selector):
        return self.block

    def asset(self, token, block):
        return Asset(chain=self.chain, identifier=token, decimals=18)

    def call(self, to, signature, block, *, inputs=(), args=(), outputs=()):
        self.asked.append((to, signature, block))
        assert signature == "wrappedNativeToken()" and outputs == ("address",)
        return (self.wrapped,)


def test_resolution_reads_each_pools_wrapped_native_token_at_its_anchor(monkeypatch):
    pools = {PIN.address: POOL, BASE_PIN.address: BASE_POOL}
    monkeypatch.setattr(resolution, "read_pool", lambda reader, pool, block: pools[pool])
    # The pool's answer is checksummed, as a node returns an address.
    near = PoolReads(BLOCK, "0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2")
    far = PoolReads(BASE_BLOCK, BASE_WETH.reference)
    source = resolution.resolve_market(near, SELECTION, BLOCK)
    assert (source.wrapped_native, source.destination_wrapped_native) == (WETH.reference, None)
    completed = resolution.complete_market(far, source, BASE_BLOCK)
    assert completed == RESOLVED and completed.route == ROUTE
    assert near.asked == [(PIN.address, "wrappedNativeToken()", BLOCK)]
    assert far.asked == [(BASE_PIN.address, "wrappedNativeToken()", BASE_BLOCK)]
    resolution.admit_route(completed)


def test_a_pool_that_names_another_wrapped_token_resolves_no_native_route(monkeypatch):
    monkeypatch.setattr(resolution, "read_pool", lambda reader, pool, block: POOL)
    with pytest.raises(DataError) as error:
        resolution.resolve_market(PoolReads(BLOCK, "0x" + "99" * 20), SELECTION, BLOCK)
    assert error.value.kind == "unsupported_route"
    assert "names 0x" + "99" * 20 in str(error.value)


def test_a_route_of_two_tokens_reads_no_wrapped_native_token(monkeypatch):
    """Retained resolution reads of running deployments hold no such read."""
    monkeypatch.setattr(resolution, "read_pool", lambda reader, pool, block: POOL)
    reader = PoolReads(BLOCK, None)
    selection = loaded(
        MarketSelection,
        stated(asset=USDC_ROUTE["asset"], destination_asset=USDC_ROUTE["destination_asset"]),
    )
    resolved = resolution.resolve_market(reader, selection, BLOCK)
    assert reader.asked == [] and "wrapped_native" not in resolved.model_dump()


# One step: the deposit carries its amount as value, with no approval.


def test_a_native_bridge_is_one_deposit_that_carries_its_amount():
    before = seen()
    assert planning.step(CONFIG, state(), INTENT, before) == "deposit"
    operation = planning.operation(CONFIG, state(), INTENT, before, quote=quoted())
    assert operation == deposit() and operation.value == AMOUNT
    call = plan_call(operation)
    assert (call.to, call.value) == (PIN.address, AMOUNT)
    arguments = decode(DEPOSIT_TYPES, bytes.fromhex(call.data[10:]))
    assert (address(arguments[2]), address(arguments[3])) == (WETH.reference, BASE_WETH.reference)
    assert across_direct.planned_steps("bridge", 0, AMOUNT, native=True) == ["deposit"]
    # The transaction the gateway signs carries the value from the wallet itself.
    execution_call = prepare_call(CONFIG.accounts, "fund", call)
    assert (execution_call.to, execution_call.value) == (PIN.address, AMOUNT)


def test_a_native_route_plans_no_approval_and_no_revocation():
    with pytest.raises(ValueError, match="no approval"):
        plan_call(ApproveOperation(target=TARGET, amount=INTENT.amount))
    with pytest.raises(ValueError, match="no approval to revoke"):
        planning.step(CONFIG, state(), CONFIG.revoke(ALLOCATION), seen())
    request = ActionRequest("revoke-bridge")
    with pytest.raises(ValueError, match="there is none to revoke"):
        ax_bridge.deployment(None, SELECTION, request, {}, None, "endpoint", LIMITS)


def test_a_deposit_whose_value_is_not_its_amount_cannot_be_built_or_signed():
    """The SpokePool takes its wrapped token with value equal to the amount, or with no
    value by allowance. Only the first is a native bridge."""
    for value in (0, AMOUNT - 1, AMOUNT + 1):
        with pytest.raises(ValidationError, match="carries exactly its amount as value"):
            deposit(value=value)
    # The gateway reads a recorded operation through the same model.
    raw = deposit().model_dump(mode="json")
    assert raw["value"] == AMOUNT
    for tampered in ({**raw, "value": 1}, {k: v for k, v in raw.items() if k != "value"}):
        with pytest.raises(ValidationError, match="carries exactly its amount as value"):
            loaded(DepositOperation, tampered)
    # The planned step is the one the gateway admits, and it carries the amount.
    execution.require_planned(CONFIG, state(), deposit(), seen())
    with pytest.raises(ValueError):
        execution.require_planned(
            CONFIG, state(), deposit(amount=AMOUNT + 1, value=AMOUNT + 1), seen()
        )


def test_a_deposit_of_a_token_carries_no_value():
    from tests.across_support import AMOUNT as TOKENS
    from tests.across_support import TARGET as TOKEN_TARGET
    from tests.across_support import quote

    values = {
        "target": TOKEN_TARGET,
        "amount": AssetAmount(asset=TOKEN_TARGET.asset, amount=TOKENS),
        "quote": quote(),
        "quote_time_buffer": 3600,
    }
    plain = DepositOperation(**values)
    assert plain.value == 0 and "value" not in plain.model_dump()
    assert plan_call(plain).value == 0
    with pytest.raises(ValidationError, match="a deposit of a token carries none"):
        DepositOperation(**values, value=TOKENS)


class Balances:
    """A reader that serves the wallet's native balance and refuses every token read."""

    def __init__(self, balance):
        self.balance, self.asked = balance, []

    def _rpc(self, method, params, *, allow_none=False):
        self.asked.append((method, params[0]))
        assert method == "eth_getBalance"
        return hex(self.balance)

    def call(self, *arguments, **keywords):
        raise AssertionError("a native route reads no token balance and no allowance")


def test_the_observation_reads_the_native_balance_and_no_allowance(monkeypatch):
    from almanak.connectors.across import observations

    monkeypatch.setattr(observations, "read_pool", lambda reader, pool, block: POOL)
    reader = Balances(7 * AMOUNT)
    read = snapshot(SimpleNamespace(config=CONFIG, reader=reader), ALLOCATION, BLOCK)
    assert read == seen(balance=7 * AMOUNT) and read.allowance == 0
    assert reader.asked == [("eth_getBalance", SENDER)]
    for changes in ({"allowance": 1}, {"wallet_balance": 1}):
        with pytest.raises(ValidationError, match="native balance and no allowance"):
            seen(**changes)


# Funding and accounting.


def test_the_wallet_must_hold_the_amount_plus_the_fee_ceiling():
    operation = deposit()
    short = seen(balance=AMOUNT + FEE_CEILING - 3)
    with pytest.raises(DataError) as error:
        execution.validate_entry(state(), operation, short)
    assert error.value.kind == "funding_unavailable"
    message = str(error.value)
    assert f"holds {AMOUNT + FEE_CEILING - 3} of {ETH}, 3 less" in message
    assert f"the {AMOUNT} to bridge plus the transaction fee ceiling {FEE_CEILING}" in message
    # The amount alone is not enough: its gas leaves the same balance.
    with pytest.raises(DataError, match="less than"):
        execution.validate_entry(state(), operation, seen(balance=AMOUNT))
    execution.validate_entry(state(), operation, seen(balance=AMOUNT + FEE_CEILING))
    assert execution.fee_ceiling(CONFIG, "fund") == FEE_CEILING


def test_a_signed_native_deposit_reserves_its_value_in_the_shared_budget():
    """The shared budget counts a signed transaction's value. Recovery and resend of a
    native deposit therefore need the amount and the fee, not the fee alone."""
    attempt = signed()
    held = state(pending=attempt)
    fee = attempt.gas * attempt.gas_price
    balance = AssetAmount(asset=ETH, amount=AMOUNT + fee)
    validate_budget(held, attempt.operation, balance, attempt.gas, attempt.gas_price)
    short = AssetAmount(asset=ETH, amount=AMOUNT + fee - 1)
    with pytest.raises(DataError, match=f"{AMOUNT} value") as error:
        validate_budget(held, attempt.operation, short, attempt.gas, attempt.gas_price)
    assert error.value.kind == "gas_budget"


def test_the_native_balance_falls_by_the_gas_paid_plus_the_amount_sent():
    before, gas = seen(), 12_345
    mined = seen(balance=FEE_CEILING - gas)
    assert execution.accounting_issues(before, mined, AMOUNT, gas) == ()
    # Gas alone, what a token deposit expects, leaves the amount unexplained.
    gas_only = seen(balance=AMOUNT + FEE_CEILING - gas)
    assert execution.accounting_issues(before, gas_only, AMOUNT, gas) == (
        "gas payer balance includes unexplained external movements",
    )
    # A reverted deposit moves no value: only its gas left.
    assert execution.accounting_issues(before, gas_only, 0, gas) == ()
    assert execution.accounting_issues(before, mined, 0, gas) != ()
    # A native deposit spends no allowance, and its snapshots state none.
    assert execution.allowance_after(deposit(), 0, AMOUNT) == 0


def test_a_token_deposit_still_expects_gas_only_from_the_payer():
    from tests.across_support import AMOUNT as TOKENS
    from tests.across_support import snapshot as token_snapshot

    before = token_snapshot()
    paid = before.native_balance.model_copy(update={"amount": before.native_balance.amount - 5})
    after = token_snapshot(wallet_balance=0, native_balance=paid)
    assert execution.accounting_issues(before, after, TOKENS, 5) == ()
    assert execution.accounting_issues(before, after, TOKENS, 4) != ()


# Known answers: a native deposit a stranger sent, and its fill.


def real_deposit() -> DepositOperation:
    """The stranger's deposit as this connector would have signed it, from its calldata."""
    arguments = decode(DEPOSIT_TYPES, bytes.fromhex(SENT["input"][10:]))
    depositor, recipient, input_token, output_token, sent, offered = arguments[:6]
    destination, relayer, quoted_at, fill_deadline, exclusivity, message = arguments[6:]
    assert (destination, message) == (number(SENT["destination"]), b"")
    assert (address(input_token), address(output_token)) == (WETH.reference, BASE_WETH.reference)
    config = operator_config(address(depositor), address(recipient), sent)
    target = config.target(ALLOCATION)
    quote = quoted(
        target,
        sent,
        output_amount=offered,
        timestamp=quoted_at,
        fill_deadline=fill_deadline,
        exclusive_relayer=address(relayer),
        exclusivity_parameter=exclusivity,
    )
    return DepositOperation(
        target=target,
        amount=AssetAmount(asset=ETH, amount=sent),
        quote=quote,
        quote_time_buffer=3600,
        value=sent,
    )


def test_the_planner_reproduces_a_real_native_deposit_and_its_value():
    operation = real_deposit()
    call = plan_call(operation)
    assert (call.to, call.value) == (PIN.address, int(SENT["value"], 16))
    assert SENT["to"].lower() == call.to and call.value == operation.amount.amount
    # Everything the contract decodes is byte for byte what this connector encodes.
    assert SENT["input"].startswith(call.data)
    tag = SENT["input"][len(call.data) :]
    assert 0 < len(tag) <= 2 * MAX_TAG_BYTES
    assert SENT["from"].lower() == operation.target.fund.address


def test_the_receipt_of_a_real_native_deposit_opens_the_transfer_its_fill_matched():
    operation, fill = real_deposit(), SENT["fill"]
    moved, mined = movement(SENT["receipt"], operation, SENT["block_timestamp"])
    assert moved == operation.value == int(SENT["value"], 16)
    assert mined["deposit_id"] == SENT["deposit_id"]
    transfer = opened_transfer(operation, mined)
    assert (transfer.sent.asset, transfer.offered.asset) == (ETH, BASE_ETH)
    reference = parse_reference(transfer.reference)
    assert reference.relay_hash == SENT["relay_hash"]
    assert (fill["status_before"], fill["status_after"]) == (0, chain.FILLED)
    from almanak.connectors.across import settlement

    row, decoded = settlement.matching_fill((fill["log"],), reference.relay_hash, 8453)
    assert decoded["updated_output"] == operation.quote.output_amount
    assert settlement.matching_fill((fill["log"],), reference.relay_hash, 1) is None
    assert row["address"].lower() == BASE_PIN.address


def test_a_native_deposit_that_pulled_the_wrapped_token_is_refused():
    """With no value the SpokePool pulls the wrapped token by allowance instead."""
    operation = real_deposit()
    fund, pool = operation.target.fund.address, PIN.address
    pull = {
        "address": WETH.reference,
        "topics": [
            "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef",
            "0x" + fund[2:].rjust(64, "0"),
            "0x" + pool[2:].rjust(64, "0"),
        ],
        "data": "0x" + operation.value.to_bytes(32).hex(),
        "logIndex": "0x0",
    }
    receipt = {**SENT["receipt"], "logs": [pull, *SENT["receipt"]["logs"]]}
    with pytest.raises(DataError, match="moved the wrapped token from the fund"):
        movement(receipt, operation, SENT["block_timestamp"])


# Arrival: the fill event's amount, held to what the pool did with its wrapped token.


class Destination:
    """A destination chain that serves one retained receipt and the pool's own wrapped
    native token."""

    def __init__(self, capture, wrapped=None):
        self.chain = Chain(chain_id=ChainId(capture["chain"]), environment="mainnet")
        self.receipt = capture["receipt"]
        self.wrapped = wrapped or capture["wrapped_native"]

    def pin_block(self, selector):
        return self.head(selector)

    def head(self, number=0) -> Block:
        return Block(chain=self.chain, number=number, hash="0x" + "cd" * 32, timestamp=NOW)

    def _rpc(self, method, params, *, allow_none=False):
        assert (method, params) == ("eth_getTransactionReceipt", [self.receipt["transactionHash"]])
        return self.receipt

    def call(self, to, signature, block, *, inputs=(), args=(), outputs=()):
        assert signature == "wrappedNativeToken()"
        return (self.wrapped,)


def paid_contract(capture) -> tuple[str, int]:
    """The recipient and amount of the captured fill to a contract, from its own event.

    Its relay carried a message, so it is no relay this connector deposits and the fill
    decoder names none. It is the one capture of the SpokePool paying its wrapped native
    token as a token: no fill of a relay without a message to a contract was found.
    """
    assert chain.decoded_fill(capture["log"]) == {"relay": None}
    data = decode(chain.FILL_TYPES, bytes.fromhex(capture["log"]["data"][2:]))
    updated_recipient, _, updated_output, _ = data[-1]
    assert address(updated_recipient) == capture["recipient"]
    return capture["recipient"], updated_output


def filled(capture) -> tuple[ax_bridge.Deposit, dict]:
    """The deposit a captured fill names, and the fill, decoded from its own event."""
    fill = chain.decoded_fill(capture["log"])
    relay = fill["relay"]
    stated = ax_bridge.Deposit(
        transaction="0x" + "00" * 32,
        block=0,
        depositor=address(relay[0]),
        recipient=address(relay[1]),
        input_token=address(relay[3]),
        output_token=address(relay[4]),
        sent=relay[5],
        offered=relay[6],
        quoted_at=0,
        fill_deadline=relay[9],
    )
    return stated, fill


def measured(capture, **reader) -> dict:
    far = Destination(capture, **reader)
    stated, fill = filled(capture)
    return ax_bridge.payout(far, capture["spoke_pool"], stated, capture["log"], fill, far.head())


def native_delta(capture) -> int:
    return int(capture["recipient_native_after"]) - int(capture["recipient_native_before"])


@pytest.mark.parametrize(
    ("name", "evidence"),
    [
        ("withdrawal_optimism", chain.WITHDRAWAL),
        ("delegating_base", chain.WITHDRAWAL),
        ("burn_arbitrum", chain.BURN),
        ("burn_robinhood", chain.BURN),
    ],
)
def test_a_fill_to_a_plain_recipient_is_measured_from_the_pools_unwrap(name, evidence):
    capture = FILLS[name]
    stated, fill = filled(capture)
    assert stated.recipient == capture["recipient"]
    assert stated.output_token == capture["wrapped_native"]
    result = measured(capture)
    native = evm_network(ChainId(capture["chain"])).native_asset
    assert result == {
        "received": str(fill["updated_output"]),
        "asset": str(native),
        "paid_as": "native",
        "measured_by": evidence,
        "note": ax_bridge.PAID_NATIVE,
    }
    # What the event states is what the recipient's native balance rose by in that
    # block, read from the chain when the fill was captured.
    assert native_delta(capture) == fill["updated_output"]
    code = capture["recipient_code"]
    assert code == "0x" or code.startswith(ax_bridge.DELEGATION_PREFIX)


def test_the_fill_of_the_captured_native_deposit_paid_its_recipient_native_eth():
    fill = SENT["fill"]
    assert measured(fill) == {
        "received": str(real_deposit().quote.output_amount),
        "asset": str(BASE_ETH),
        "paid_as": "native",
        "measured_by": chain.WITHDRAWAL,
        "note": ax_bridge.PAID_NATIVE,
    }
    assert native_delta(fill) == real_deposit().quote.output_amount
    assert fill["recipient_code"] == "0x"


def test_a_fill_to_a_contract_is_reported_as_the_wrapped_token_it_was_paid():
    capture = FILLS["contract_base"]
    recipient, amount = paid_contract(capture)
    stated = ax_bridge.Deposit(
        transaction="0x" + "00" * 32,
        block=0,
        depositor=SENDER,
        recipient=recipient,
        input_token=WETH.reference,
        output_token=BASE_WETH.reference,
        sent=amount,
        offered=amount,
        quoted_at=0,
        fill_deadline=0,
    )
    fill = {"updated_recipient": bytes(12) + bytes.fromhex(recipient[2:]), "updated_output": amount}
    far = Destination(capture)
    result = ax_bridge.payout(far, BASE_PIN.address, stated, capture["log"], fill, far.head())
    assert result == {
        "received": str(amount),
        "asset": str(BASE_WETH),
        "paid_as": "wrapped_token",
        "measured_by": chain.TOKEN_TRANSFER,
        "note": ax_bridge.PAID_WRAPPED,
    }
    assert "native" not in result["asset"] and "slip44" not in result["asset"]
    # The recipient held code, and its native balance did not move.
    assert capture["recipient_code_head"].startswith("0x6080") and native_delta(capture) == 0


def without(receipt, topic) -> dict:
    return {**receipt, "logs": [r for r in receipt["logs"] if r["topics"][0] != topic]}


@pytest.mark.parametrize(
    ("name", "topic"),
    [("withdrawal_optimism", chain.WITHDRAWAL_TOPIC), ("burn_arbitrum", chain.TRANSFER_TOPIC)],
)
def test_a_fill_without_its_unwrap_reports_nothing_as_received(name, topic):
    capture = FILLS[name]
    stripped = {**capture, "receipt": without(capture["receipt"], topic)}
    with pytest.raises(DataError) as error:
        chain.native_payout(
            stripped["receipt"],
            capture["log"],
            capture["wrapped_native"],
            capture["recipient"],
            filled(capture)[1]["updated_output"],
        )
    assert error.value.kind == "integrity" and "nothing is reported as received" in str(error.value)


def test_an_unwrap_of_another_amount_or_after_the_next_fill_is_no_evidence():
    capture = FILLS["withdrawal_optimism"]
    amount = filled(capture)[1]["updated_output"]
    arguments = (capture["log"], capture["wrapped_native"], capture["recipient"])
    receipt = capture["receipt"]
    assert chain.native_payout(receipt, *arguments, amount) == chain.WITHDRAWAL
    with pytest.raises(DataError, match="no unwrap and no transfer"):
        chain.native_payout(receipt, *arguments, amount + 1)
    # The same unwrap after the pool's next fill belongs to that fill.
    last = max(int(row["logIndex"], 16) for row in receipt["logs"])
    following = {**capture["log"], "logIndex": hex(int(capture["log"]["logIndex"], 16) + 1)}
    moved = [
        {**row, "logIndex": hex(last + 1 + index)}
        for index, row in enumerate(receipt["logs"])
        if row["topics"][0] == chain.WITHDRAWAL_TOPIC
    ]
    kept = [row for row in receipt["logs"] if row["topics"][0] != chain.WITHDRAWAL_TOPIC]
    reordered = {**receipt, "logs": [*kept, following, *moved]}
    assert chain.fill_window(reordered, capture["log"]) == [
        row for row in kept if int(row["logIndex"], 16) > int(capture["log"]["logIndex"], 16)
    ]
    with pytest.raises(DataError, match="no unwrap and no transfer"):
        chain.native_payout(reordered, *arguments, amount)


def test_an_unwrap_beside_a_token_payment_is_refused_as_ambiguous():
    """A plain recipient is paid by an unwrap and a contract by a transfer, never both."""
    plain, contract = FILLS["delegating_base"], FILLS["contract_base"]
    recipient, amount = paid_contract(contract)
    index = max(int(row["logIndex"], 16) for row in contract["receipt"]["logs"]) + 1
    unwrap = next(
        row for row in plain["receipt"]["logs"] if row["topics"][0] == chain.WITHDRAWAL_TOPIC
    )
    extra = {**unwrap, "data": "0x" + amount.to_bytes(32).hex(), "logIndex": hex(index)}
    receipt = {**contract["receipt"], "logs": [*contract["receipt"]["logs"], extra]}
    with pytest.raises(DataError, match="no unwrap and no transfer"):
        chain.native_payout(receipt, contract["log"], BASE_WETH.reference, recipient, amount)


def test_a_fill_that_names_another_recipient_is_refused():
    capture = FILLS["withdrawal_optimism"]
    stated, fill = filled(capture)
    other = ax_bridge.Deposit(**{**stated.__dict__, "recipient": RECIPIENT})
    far = Destination(capture)
    with pytest.raises(DataError, match="another recipient"):
        ax_bridge.native_payout(far, other, capture["log"], fill)


def test_a_pool_that_does_not_unwrap_the_token_is_read_as_any_token_route():
    """The network definition only says when the pool is worth asking: the pool's own
    `wrappedNativeToken()` decides, and a token route is measured as it always was."""
    capture = FILLS["delegating_base"]
    stated, _ = filled(capture)
    pool = capture["spoke_pool"]
    named = Destination(capture)
    assert ax_bridge.unwraps(named, pool, stated.output_token, named.head())
    other = Destination(capture, wrapped="0x" + "99" * 20)
    assert not ax_bridge.unwraps(other, pool, stated.output_token, other.head())
    # A token that is not the network's wrapped native token asks the pool nothing.
    usdc = USDC_ROUTE["destination_asset"].split(":")[-1]
    assert not ax_bridge.is_wrapped_native(BASE.chain_id, usdc)
    assert not ax_bridge.unwraps(SimpleNamespace(chain=BASE), pool, usdc, None)


# Refund: paid in the wrapped token, also when native value was sent.


class Source:
    """The source chain of the refund capture: its receipts and the deposit's sender."""

    chain = BASE

    def __init__(self, sent_to, value):
        self.sent_to, self.value = sent_to, value
        self.receipts = {
            r["transactionHash"]: r for r in (REFUND["receipt"], REFUND["deposit"]["receipt"])
        }

    def _rpc(self, method, params, *, allow_none=False):
        if method == "eth_getTransactionByHash":
            return {"to": self.sent_to, "value": self.value}
        assert method == "eth_getTransactionReceipt"
        return self.receipts[params[0]]


def refunded_deposit() -> ax_bridge.Deposit:
    (row,) = [
        log
        for log in REFUND["deposit"]["receipt"]["logs"]
        if log["address"].lower() == BASE_PIN.address
    ]
    deposit_id = int(row["topics"][2], 16)
    relay, found = ax_bridge.decoded_deposit(row, 8453, deposit_id)
    from almanak.connectors.across.relay import relay_hash

    assert relay_hash(relay, number(REFUND["destination"])) == REFUND["relay_hash"]
    return found


def test_a_real_refund_of_a_native_depositor_is_a_payment_of_the_wrapped_token():
    found = refunded_deposit()
    assert found.depositor == REFUND["depositor"] and found.input_token == BASE_WETH.reference
    # The depositor paid native value to another contract, which deposited for it.
    assert 0 < found.sent < int(REFUND["deposit"]["value"], 16)
    near = Source(REFUND["deposit"]["to"], REFUND["deposit"]["value"])
    (row,) = [
        log
        for log in REFUND["receipt"]["logs"]
        if log["address"].lower() == BASE_WETH.reference
        and log["topics"][2].endswith(found.depositor[2:])
    ]
    payment = ax_bridge.leaf_payment(near, BASE_PIN.address, found, row)
    assert payment == {
        "transaction": REFUND["receipt"]["transactionHash"],
        "block": int(REFUND["receipt"]["blockNumber"], 16),
        "amounts": [str(found.sent)],
        "paid": [str(found.sent)],
        "equals_sent": True,
    }
    # Over that block the depositor's wrapped balance rose by the amount and its native
    # balance did not move: the refund is never unwrapped.
    assert int(REFUND["depositor_wrapped_delta"]) == found.sent
    assert int(REFUND["depositor_native_delta"]) == 0
    assert (REFUND["destination_fill_status"], REFUND["depositor_code"]) == (0, "0x")
    listed = {"payments": [payment], "note": ax_bridge.UNATTRIBUTED}
    told = ax_bridge.wrapped_refund(BASE.chain_id, found, listed)
    assert told["paid_in"] == str(BASE_WETH) and told["payments"] == [payment]
    assert "never in the native asset" in told["note"] and str(BASE_WETH) in told["note"]
    assert told["note"].startswith(ax_bridge.UNATTRIBUTED)


def test_a_refund_of_any_other_token_is_listed_as_it_always_was():
    found = refunded_deposit()
    usdc = USDC_ROUTE["destination_asset"].split(":")[-1]
    other = ax_bridge.Deposit(**{**found.__dict__, "input_token": usdc})
    listed = {"payments": [], "note": ax_bridge.UNATTRIBUTED}
    assert ax_bridge.wrapped_refund(BASE.chain_id, other, listed) is listed


def test_the_status_says_whether_the_deposit_paid_the_pool_native_value():
    found = refunded_deposit()
    # This depositor paid another contract, which deposited for it: not this command's
    # to call native.
    through = Source(REFUND["deposit"]["to"], REFUND["deposit"]["value"])
    assert REFUND["deposit"]["to"] != BASE_PIN.address
    assert ax_bridge.sent_as_value(through, BASE_PIN.address, found) is False
    direct = Source(BASE_PIN.address, hex(found.sent))
    assert ax_bridge.sent_as_value(direct, BASE_PIN.address, found) is True
    assert (
        ax_bridge.sent_as_value(Source(BASE_PIN.address, "0x0"), BASE_PIN.address, found) is False
    )
    token = f"eip155:8453/erc20:{BASE_WETH.reference}"
    told = ax_bridge.described(BASE.chain_id, found, True)
    assert told["sent"] == {
        "asset": str(BASE_ETH),
        "amount": str(found.sent),
        "recorded_as": token,
    }
    assert told["sent_as_native_value"] is True
    plain = ax_bridge.described(BASE.chain_id, found, False)
    assert plain["sent"] == {"asset": token, "amount": str(found.sent)}
    assert plain["sent_as_native_value"] is False
    assert "sent_as_native_value" not in ax_bridge.described(BASE.chain_id, found)
    usdc = USDC_ROUTE["destination_asset"].split(":")[-1]
    other = ax_bridge.Deposit(**{**found.__dict__, "input_token": usdc})
    assert ax_bridge.sent_as_value(SimpleNamespace(chain=BASE), BASE_PIN.address, other) is None


def test_a_deposit_transaction_the_endpoint_does_not_serve_is_reported_as_such():
    class Absent(Source):
        def _rpc(self, method, params, *, allow_none=False):
            return None

    with pytest.raises(DataError) as error:
        ax_bridge.sent_as_value(Absent("", "0x0"), BASE_PIN.address, refunded_deposit())
    assert error.value.kind == "unavailable"


# The venue.


def test_the_venues_answer_for_the_wrapped_tokens_is_the_quote_of_a_native_route():
    asked = CAPTURES["quote"]
    quote = venue.parse(asked["answer"], asked["amount"])
    assert (quote.input_token, quote.output_token) == (WETH.reference, BASE_WETH.reference)
    assert (quote.input_decimals, quote.output_decimals) == (18, 18)
    assert list(venue.route_refusals(quote, ROUTE)) == []
    assert asked["amount"] - quote.output_amount <= venue.fee_ceiling(asked["amount"], 10)
    # The venue states its own minimum, about 0.0003 ETH when this was read.
    assert int(asked["answer"]["limits"]["minDeposit"]) == 303076223670253


def test_the_venues_too_low_refusal_is_its_own_cause():
    refused = CAPTURES["too_low"]
    with pytest.raises(DataError) as error:
        venue.answer(httpx.Response(refused["status"], json=refused["body"]))
    assert error.value.kind == "amount_too_low"
    assert "too low relative to its fees" in str(error.value)
    # The venue's own words are not repeated, and any other refusal keeps its kind.
    assert refused["body"]["message"] not in str(error.value)
    for response in (
        httpx.Response(400, json={"code": "INVALID_PARAM"}),
        httpx.Response(400, content=b"not json"),
        httpx.Response(400, json=["AMOUNT_TOO_LOW"]),
        httpx.Response(500, json=refused["body"]),
        httpx.Response(400, content=b" " * (venue.MAX_ANSWER_BYTES + 1)),
    ):
        with pytest.raises(DataError) as other:
            venue.answer(response)
        assert other.value.kind == "quote_refused"


# Recipient.


class Far:
    """The destination reader of a route's completion: one head and one code answer."""

    chain = BASE

    def __init__(self, code):
        self.code = code

    def __enter__(self):
        return self

    def __exit__(self, *arguments):
        return None

    def pin_block(self, selector):
        return BASE_BLOCK

    def _rpc(self, method, params, *, allow_none=False):
        assert (method, params[0]) == ("eth_getCode", RECIPIENT)
        return self.code


def completed(monkeypatch, code, route=RESOLVED):
    resolver = ax_actions.resolution("across")
    monkeypatch.setattr(ax_bridge, "destination_reader", lambda chain_id, far: Far(code))
    monkeypatch.setattr(resolver, "complete_market", lambda reader, resolved, block: route)
    return ax_bridge.completed("across", ax_bridge.Far(), route, RECIPIENT, False)


def test_a_native_bridge_refuses_a_recipient_that_holds_contract_code(monkeypatch):
    with pytest.raises(DataError) as error:
        completed(monkeypatch, "0x6080604052")
    assert error.value.kind == "recipient_is_contract"
    message = str(error.value)
    assert RECIPIENT in message and "eip155:8453" in message
    assert (
        "would pay it the wrapped token" in message and "nothing was planned or signed" in message
    )
    # No code, and a plain account that delegates its code (EIP-7702), are admitted.
    for code in ("0x", "0xef0100" + "ab" * 20):
        assert completed(monkeypatch, code) == RESOLVED


def test_a_token_route_still_admits_a_contract_recipient_on_the_destination(monkeypatch):
    from tests.test_ax_bridge import ROUTE as TOKEN_ROUTE

    assert completed(monkeypatch, "0x6080604052", TOKEN_ROUTE) == TOKEN_ROUTE


# Preview and result.


class Venue:
    source = venue.SOURCE

    def __init__(self, answer):
        self.answer = answer

    def get(self, target, amount, block_timestamp):
        return httpx.Response(200, json=self.answer)


def preview(config=CONFIG) -> dict:
    asked = CAPTURES["quote"]
    answer = {**asked["answer"], "timestamp": str(NOW - 60), "fillDeadline": str(NOW + 7140)}
    answer["exclusivityDeadline"] = 0
    from almanak.gateway.execution import VenueAnswers

    gateway = SimpleNamespace(
        venue_transports={venue.QUOTE_VENUE: Venue(answer)}, venue_answers=VenueAnswers()
    )
    decision = SimpleNamespace(intent=config.bridge(ALLOCATION, AMOUNT), identity="0" * 32)
    planned = SimpleNamespace(decision=decision, plan=SimpleNamespace(before=seen(config)))
    return across_direct.details(gateway, planned)


def test_the_preview_states_what_is_sent_paid_and_refunded_before_anything_is_signed():
    result = preview()
    assert result["steps"] == ["deposit"]
    assert (result["recipient"], result["recipient_is_sender"]) == (RECIPIENT, False)
    quote = result["quote"]
    assert (quote["source"], quote["input_amount"], quote["max_fee_bps"]) == (
        "across_api",
        str(AMOUNT),
        10,
    )
    assert quote["output_amount"] == "9997123121438735"
    assert quote["fee"] == str(AMOUNT - 9997123121438735)
    assert quote["fee_ceiling"] == str(AMOUNT * 10 // 10_000)
    assert result["native"] == {
        "sent_as": "native_value",
        "sent_asset": "eip155:1/slip44:60",
        "approval": None,
        "paid_as": "native",
        "paid_asset": "eip155:8453/slip44:60",
        "refund_asset": f"eip155:1/erc20:{WETH.reference}",
    }


def test_the_note_states_the_payout_and_the_refund_of_a_native_bridge():
    request = ActionRequest("bridge", AMOUNT, recipient=RECIPIENT)
    note = across_direct.note(CONFIG, (), request)
    for statement in (
        "sends the chain's native ETH as the deposit's own value",
        "one transaction with no approval",
        "The recipient is paid native ETH",
        "decides the form of the payout on chain when the fill lands",
        "A recipient that rejects ETH leaves the transfer unfilled until it is refunded",
        "A refund of an unfilled transfer is paid in the wrapped token (WETH) on the source"
        " chain, to the sending wallet, not in ETH",
        "Only the gateway holds the quote to --max-fee-bps",
        f"The recipient {RECIPIENT} is not the sending wallet",
        "without a checksum",
    ):
        assert statement in note
    # A token bridge says none of it.
    from tests.test_ax_bridge import BRIDGE
    from tests.test_ax_bridge import operator_config as token_config

    assert "native ETH" not in across_direct.note(token_config(), (), BRIDGE)
    assert "native" not in across_direct.details(
        SimpleNamespace(), SimpleNamespace(**revocation_plan(token_config))
    )


def revocation_plan(token_config) -> dict:
    from tests.test_ax_bridge import REVOKE
    from tests.test_ax_bridge import seen as token_seen

    config = token_config(REVOKE)
    decision = SimpleNamespace(intent=config.revoke(ALLOCATION), identity="0" * 32)
    return {"decision": decision, "plan": SimpleNamespace(before=token_seen(config))}


def test_the_position_of_a_native_route_states_no_allowance():
    created = AcrossLifecycle.initial(CONFIG, None, declared(CONFIG), None)
    observed = created.model_copy(update={"initialized": True, "baseline": (seen(),)})
    assert across_direct.position(observed) == {"allowance": None, "transfers": []}
    assert Checkpoint.model_validate_json(observed.model_dump_json()) == observed


def test_the_command_line_names_the_native_asset_by_its_caip_19_reference():
    args = parsed(
        "--chain",
        "eip155:1",
        "--asset",
        "eip155:1/slip44:60",
        "--destination-asset",
        "eip155:8453/slip44:60",
        "--amount",
        str(AMOUNT),
    )
    assert ax_actions.market_request(args) == SELECTION
    assert ax_actions.action_request(args).amount == AMOUNT


# Recovery of a value-carrying step.


def decision(config=CONFIG) -> DecisionRecord:
    return DecisionRecord(
        trigger=OperatorEvent(started_at=datetime.fromtimestamp(NOW, UTC), request="1" * 32),
        identity="0" * 32,
        intent=config.bridge(ALLOCATION, AMOUNT),
        observations=(seen(config),),
        memory=Memory(),
    )


def signed(config=CONFIG) -> Pending:
    operation = deposit(config)
    call = plan_call(operation)
    return Pending(
        attempt_id="attempt-1",
        decision=decision(config),
        operation=operation,
        before=seen(config),
        call=call,
        execution=prepare_call(config.accounts, "fund", call),
        nonce=3,
        gas_estimate=70_000,
        gas=87_500,
        gas_price=10**9,
        raw_transaction="0x02",
        tx_hash="0x" + "cd" * 32,
    )


def waiting(attempt) -> Checkpoint:
    created = AcrossLifecycle.initial(CONFIG, None, declared(CONFIG), None)
    return created.model_copy(
        update={
            "initialized": True,
            "baseline": (seen(),),
            "decision": attempt.decision,
            "pending": attempt,
        }
    )


def test_recovery_releases_an_expired_native_deposit_and_signs_nothing():
    attempt = signed()
    assert (attempt.call.value, attempt.execution.value) == (AMOUNT, AMOUNT)
    held = waiting(attempt)
    assert Checkpoint.model_validate_json(held.model_dump_json()) == held
    deadline = attempt.operation.deadline
    live = late_gateway(CONFIG, attempt, deadline)
    live.reader.block = BLOCK.model_copy(update={"timestamp": deadline})
    with pytest.raises(ValueError, match="never sent or past its protocol deadline"):
        live.require_releasable(attempt)
    late = late_gateway(CONFIG, attempt, deadline + 1)
    late.reader.block = BLOCK.model_copy(update={"timestamp": deadline + 1})
    late.require_releasable(attempt)
    released = release_pending(held)
    assert (released.pending, released.decision, released.plan) == (None, None, None)
    assert released.released == (attempt,) and released.transfers == ()
    service = gateway_service(held, late)
    service.validate_pending_resolution(held, released)
    service.validate_released_record(held, released)
    assert Checkpoint.model_validate_json(released.model_dump_json()) == released
    assert ax_actions.unresolved(held) == {
        "unresolved": {"operation": "deposit", "transaction": attempt.tx_hash}
    }
    result = ax_actions.executed_result(released, released.outcomes, None)
    assert result["released_expired"] == [{"operation": "deposit", "transaction": attempt.tx_hash}]
    assert (result["executed"], result["completed"]) == (True, False)


def test_a_native_deposit_whose_nonce_was_consumed_is_dropped_with_its_proof():
    attempt = signed()
    held = waiting(attempt)
    proof = ConsumedNonce(
        operation="deposit",
        transaction=attempt.tx_hash,
        submitter=SENDER,
        nonce=attempt.nonce,
        consumed_by="0x" + "bb" * 32,
        block=BLOCK,
    )
    dropped = drop_superseded(held, proof)
    assert (dropped.pending, dropped.decision, dropped.released) == (None, None, ())
    assert dropped.superseded == (proof,)
    service = gateway_service(held, SimpleNamespace(superseded=lambda pending: proof))
    service.validate_pending_resolution(held, dropped)
    assert Checkpoint.model_validate_json(dropped.model_dump_json()) == dropped
    # Another proof than the gateway's own admits nothing.
    other = proof.model_copy(update={"consumed_by": "0x" + "cc" * 32})
    forged = gateway_service(held, SimpleNamespace(superseded=lambda pending: other))
    with pytest.raises(ValueError, match="differs from the gateway's own"):
        forged.validate_pending_resolution(held, dropped)
    result = ax_actions.executed_result(dropped, dropped.outcomes, None)
    assert result["released_consumed_nonce"][0]["transaction"] == attempt.tx_hash
    assert (result["executed"], result["completed"]) == (True, False)


# `ax bridge-status` over the captured deposit and its fill.


class Mainnet(Ledger):
    """A ledger that also serves one transaction, as the deposit's own chain does."""

    sent: dict | None = None

    def _rpc(self, method, params, *, allow_none=False):
        if method == "eth_getTransactionByHash":
            return self.sent
        return super()._rpc(method, params, allow_none=allow_none)


def source_chain(head_after=20_000) -> Mainnet:
    mined = int(SENT["receipt"]["blockNumber"], 16)
    near = Mainnet(
        ETHEREUM,
        (mined, SENT["block_timestamp"]),
        mined + head_after,
        [SENT["receipt"]],
        {"getRelayerRefund(address,address)": (0,)},
    )
    near.sent = {"to": SENT["to"], "value": SENT["value"]}
    return near


def destination_chain(filled_status=2, receipts=None, head_after=10) -> Mainnet:
    fill = SENT["fill"]
    mined = int(fill["receipt"]["blockNumber"], 16)
    calls = {
        "fillStatuses(bytes32)": (filled_status,),
        "wrappedNativeToken()": ("0x4200000000000000000000000000000000000006",),
    }
    return Mainnet(
        BASE,
        (mined, fill["block_timestamp"]),
        mined + head_after,
        [fill["receipt"]] if receipts is None else receipts,
        calls,
    )


def status(monkeypatch, near, far, **options) -> dict:
    readers = {ETHEREUM.chain_id: near, BASE.chain_id: far}
    monkeypatch.setattr("almanak.ax.observation_reader", lambda args, chain_id: readers[chain_id])
    monkeypatch.setattr(
        "almanak.connectors.across.observations.read_pool", lambda reader, pool, block: POOL
    )
    values = {
        "connector": "across",
        "chain": "eip155:1",
        "destination_chain": "eip155:8453",
        "reference": None,
        "deposit_tx": SENT["receipt"]["transactionHash"],
        "block": "finalized",
        "refund_from_block": None,
        "env_file": Path(".env"),
        "public_rpc": None,
        "destination_public_rpc": None,
        **options,
    }
    return ax_bridge.status(SimpleNamespace(**values))


def test_the_status_of_the_captured_native_bridge_is_a_native_arrival(monkeypatch):
    operation, fill = real_deposit(), SENT["fill"]
    result = status(monkeypatch, source_chain(), destination_chain())
    quoted_at = operation.quote.timestamp
    assert result["reference"] == f"{SENT['deposit_id']}:{SENT['relay_hash']}:{quoted_at}"
    assert result["deposit"]["sent"] == {
        "asset": "eip155:1/slip44:60",
        "amount": str(operation.value),
        "recorded_as": f"eip155:1/erc20:{WETH.reference}",
    }
    assert result["deposit"]["sent_as_native_value"] is True
    assert (result["status"], result["arrival"]) == (
        "arrived",
        {
            "transaction": fill["receipt"]["transactionHash"],
            "block": int(fill["receipt"]["blockNumber"], 16),
            "received": str(operation.quote.output_amount),
            "asset": "eip155:8453/slip44:60",
            "paid_as": "native",
            "measured_by": "wrapped_token_withdrawal",
            "note": ax_bridge.PAID_NATIVE,
            "fill_type": "fast",
        },
    )
    # By reference the same deposit is found again, and the same arrival measured.
    again = status(
        monkeypatch,
        source_chain(),
        destination_chain(),
        reference=result["reference"],
        deposit_tx=None,
    )
    assert (again["deposit"], again["arrival"]) == (result["deposit"], result["arrival"])


def test_a_native_fill_without_its_unwrap_is_an_integrity_refusal_of_the_status(monkeypatch):
    fill = SENT["fill"]
    stripped = without(fill["receipt"], chain.WITHDRAWAL_TOPIC)
    with pytest.raises(DataError) as error:
        status(monkeypatch, source_chain(), destination_chain(receipts=[stripped]))
    assert error.value.kind == "integrity"


def test_an_unfilled_native_bridge_lists_refunds_as_payments_of_the_wrapped_token(monkeypatch):
    """Past the fill deadline with no fill, the refund scan reads the wrapped token on
    the source chain and says so. This deposit was in fact filled: the unfilled state is
    stated here, to read the listing."""
    near = source_chain()
    far = destination_chain(filled_status=0, receipts=[], head_after=30_000)
    result = status(monkeypatch, near, far)
    assert result["status"] == "unfilled" and "arrival" not in result
    refund = result["refund"]
    assert refund["paid_in"] == f"eip155:1/erc20:{WETH.reference}"
    assert refund["payments"] == [] and "never in the native asset" in refund["note"]
    # The scan asked for transfers of the wrapped token from the pool to the depositor.
    asked = [q for q in near.queries if q["address"] == WETH.reference]
    assert asked and all(q["topics"][0] == chain.TRANSFER_TOPIC for q in asked)


# A Safe sends no native route.


def test_a_native_route_is_sent_from_a_plain_wallet_only():
    from tests.test_across_permissions import safe

    raw = json.loads(CONFIG.model_dump_json())
    profile = safe("fund", ETHEREUM, SENDER).profile
    raw["accounts"]["bindings"][0]["profile"] = json.loads(profile.model_dump_json())
    with pytest.raises(ValidationError, match="from a plain wallet, not a Safe"):
        type(CONFIG).model_validate_json(json.dumps(raw))
