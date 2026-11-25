"""An author names one route and two accounts; the source chain's reads resolve it."""

from types import SimpleNamespace

import pytest
from eth_utils import keccak

from almanak.configuration.unresolved import parse_inputs
from almanak.configuration.yaml import ConfigurationError, Document
from almanak.connectors.across.author_inputs import AllocationInput
from almanak.connectors.across.domain import SPOKE_POOLS
from almanak.connectors.across.observations import IMPLEMENTATION_SLOT, snapshot
from almanak.connectors.across.resolution import (
    MarketSelection,
    complete_market,
    resolve_market,
)
from almanak.connectors.composition import connector_for, connector_from_wire
from almanak.connectors.support import EconomicAction, RunMode
from almanak.gateway.ethereum import DataError
from almanak.models import Asset
from tests.across_support import (
    AMOUNT,
    ARBITRUM,
    BASE,
    BASE_USDC,
    BLOCK,
    CONFIG,
    PIN,
    POOL,
    SENDER,
    TARGET,
    USDC,
)

CODE = "0x6080"
VALUES = {
    "depositQuoteTimeBuffer()": 3600,
    "fillDeadlineBuffer()": 21600,
    "pausedDeposits()": False,
    "balanceOf(address)": AMOUNT,
    "allowance(address,address)": 7,
}


class Reader:
    """Canonical reads at one pinned block, as the chain would answer them."""

    chain = ARBITRUM

    def __init__(self, implementation=PIN.implementation, code=CODE):
        self.implementation, self.code, self.calls = implementation, code, []

    def _rpc(self, method, params):
        selector = {"blockHash": BLOCK.hash, "requireCanonical": True}
        assert params[-1] == selector
        if method == "eth_getStorageAt":
            assert params[:2] == [PIN.address, IMPLEMENTATION_SLOT]
            return "0x" + "00" * 12 + self.implementation[2:]
        if method == "eth_getCode":
            return self.code
        assert (method, params[0]) == ("eth_getBalance", SENDER)
        return hex(10**18)

    def call(self, address, signature, block, inputs=(), args=(), outputs=("uint256",)):
        assert block == BLOCK
        self.calls.append((address, signature, tuple(args)))
        return (VALUES[signature],)

    def asset(self, identifier, block):
        return Asset(chain=block.chain, identifier=identifier, decimals=6)

    def pin_block(self, number):
        return BLOCK


class DestinationReader:
    """The destination chain's own reads at its own anchor."""

    def __init__(self, block, decimals=6):
        self.block, self.chain, self.decimals = block, block.chain, decimals

    def pin_block(self, number):
        return self.block

    def asset(self, identifier, block):
        return Asset(chain=block.chain, identifier=identifier, decimals=self.decimals)


def completed(resolved, chain=BASE, decimals=6):
    block = BLOCK.model_copy(update={"chain": chain, "number": 30_000_000})
    return complete_market(DestinationReader(block, decimals), resolved, block)


def selection(**changes):
    values = {
        "connector": "across",
        "chain": ARBITRUM.chain_id,
        "asset": USDC,
        "destination_chain": BASE.chain_id,
        "destination_asset": BASE_USDC,
        "max_fee_bps": 10,
        "quote": "across_api",
        **changes,
    }
    return MarketSelection(**values)


def test_the_snapshot_reads_the_fund_and_the_spoke_pool_at_the_pinned_block():
    reader = Reader()
    backend = SimpleNamespace(config=CONFIG, reader=reader)
    observed = snapshot(backend, "move", BLOCK)
    assert (observed.wallet_balance, observed.allowance) == (AMOUNT, 7)
    assert observed.pool == POOL.model_copy(
        update={"implementation_code_hash": "0x" + keccak(bytes.fromhex(CODE[2:])).hex()}
    )
    assert (observed.payer, observed.native_balance.amount) == (TARGET.fund, 10**18)
    assert (USDC.reference, "allowance(address,address)", (SENDER, PIN.address)) in reader.calls


def test_resolution_refuses_an_implementation_that_was_not_reviewed():
    with pytest.raises(DataError) as error:
        resolve_market(Reader(), selection(), BLOCK)
    assert error.value.kind == "deployment_changed"


def test_a_resolved_route_becomes_an_allocation_between_two_accounts(monkeypatch):
    reviewed = PIN.model_copy(
        update={"implementation_code_hash": "0x" + keccak(bytes.fromhex(CODE[2:])).hex()}
    )
    monkeypatch.setitem(SPOKE_POOLS, ARBITRUM.chain_id, reviewed)
    unverified = resolve_market(Reader(), selection(), BLOCK)
    with pytest.raises(ValueError, match="not verified on its chain"):
        _ = unverified.route
    resolved = completed(unverified)
    assert resolved.destination == Asset(chain=BASE, identifier=BASE_USDC, decimals=6)
    with pytest.raises(ValueError, match="different decimals: 6 sent, 18 received"):
        completed(unverified, decimals=18)
    with pytest.raises(DataError) as error:
        completed(unverified, chain=ARBITRUM)
    assert error.value.kind == "chain_mismatch"
    assert resolved.assets == (Asset(chain=ARBITRUM, identifier=USDC, decimals=6),)
    assert resolved.summary()["spoke_pool"] == PIN.address and len(resolved.digest) == 64
    choice = AllocationInput(
        market="usdc",
        account="arb",
        destination_account="base",
        capital="1000",
        minimum_native_balance="0.001",
    )
    allocation = choice.resolve("move", resolved)
    assert allocation == CONFIG.allocation("move")
    assert (
        AllocationInput(
            market="usdc", account="arb", destination_account="base", minimum_native_balance="0"
        )
        .resolve("move", resolved)
        .capital
        == 0
    )
    with pytest.raises(DataError) as error:
        resolve_market(Reader(), selection(), BLOCK.model_copy(update={"chain": BASE}))
    assert error.value.kind == "chain_mismatch"


@pytest.mark.parametrize(
    ("changes", "cause"),
    [
        ({"destination_chain": ARBITRUM.chain_id, "destination_asset": USDC}, "two different"),
        (
            {
                "destination_chain": "eip155:1",
                "destination_asset": "eip155:1/erc20:0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48",
            },
            "no pinned SpokePool on eip155:1",
        ),
        ({"quote": "other"}, "across_api"),
    ],
)
def test_a_market_selection_refuses_an_unsupported_route(changes, cause):
    with pytest.raises(ValueError, match=cause):
        MarketSelection.model_validate_json(
            selection().model_copy(update=changes).model_dump_json()
        )


def document(**allocation):
    return Document(
        {
            "strategy": {
                "decision_interval": "60s",
                "allocations": {
                    "move": {
                        "market": "usdc",
                        "account": "arb",
                        "destination_account": "base",
                        "capital": "1000",
                        "minimum_native_balance": "0.001",
                        **allocation,
                    }
                },
            },
            "markets": {
                "usdc": {
                    "connector": "across",
                    "chain": "eip155:42161",
                    "asset": str(USDC),
                    "destination_chain": "eip155:8453",
                    "destination_asset": str(BASE_USDC),
                    "max_fee_bps": 10,
                    "quote": "across_api",
                }
            },
            "accounts": {
                "arb": {"chain": "eip155:42161"},
                "base": {"chain": "eip155:8453"},
                "other": {"chain": "eip155:42161"},
            },
        },
        {},
        "config.yaml",
    )


def test_the_author_file_names_a_destination_account_on_the_destination_chain():
    inputs = parse_inputs(document()).inputs
    assert inputs.strategy.allocations["move"].destination_account == "base"
    assert inputs.markets["usdc"].max_fee_bps == 10
    with pytest.raises(ConfigurationError, match="destination chain differ"):
        parse_inputs(document(destination_account="other"))
    with pytest.raises(ConfigurationError, match="Unknown named account"):
        parse_inputs(document(destination_account="nowhere"))


def test_the_connector_declares_fork_runs_and_backtests_for_the_proven_source_chain_only():
    binding = connector_from_wire({"connector": "across"})
    assert connector_for(CONFIG).codec.checkpoint is binding.codec.checkpoint
    source, destination = binding.support_for(ARBITRUM.chain_id), binding.support_for(BASE.chain_id)
    assert (source.modes, destination.modes) == ((RunMode.FORK, RunMode.BACKTEST), ())
    (historical,) = source.historical
    # The model keeps no approval, so the historical declaration lists no revoke.
    assert historical.actions == (EconomicAction.BRIDGE,) and not destination.historical
    assert historical.backtest_start_at.isoformat() == "2026-08-15T00:00:00+00:00"
    assert source.account_profiles == destination.account_profiles == ("eoa",)
    assert any("constructs the quote" in text for text in source.restrictions)
    assert any("no run mode" in text for text in destination.restrictions)
    binding.require_target(ARBITRUM.chain_id, TARGET, RunMode.FORK, account_profile="eoa")
    with pytest.raises(ValueError, match="fork is unsupported"):
        binding.require_target(BASE.chain_id, TARGET, RunMode.FORK)
    binding.require_target(ARBITRUM.chain_id, TARGET, RunMode.BACKTEST, model=historical.model)
    for chain, mode in (
        (ARBITRUM.chain_id, RunMode.PRODUCTION),
        (BASE.chain_id, RunMode.PRODUCTION),
        (BASE.chain_id, RunMode.BACKTEST),
    ):
        with pytest.raises(ValueError, match="is unsupported"):
            binding.require_target(chain, TARGET, mode)
    assert binding.codec.actions(SimpleNamespace(config=CONFIG)) is CONFIG


class ForkReader(Reader):
    """The same reads through one fork node, which names its own environment."""

    def __init__(self, block):
        super().__init__()
        self.chain, self.block = block.chain, block

    def call(self, address, signature, block, inputs=(), args=(), outputs=("uint256",)):
        return super().call(address, signature, BLOCK, inputs, args, outputs)

    def pin_block(self, number):
        return self.block


def test_resolution_through_two_fork_nodes_takes_each_chain_from_its_account(monkeypatch):
    from almanak.models import Chain, StrategyAccounts
    from tests.across_support import RECIPIENT, eoa, parameters

    reviewed = PIN.model_copy(
        update={"implementation_code_hash": "0x" + keccak(bytes.fromhex(CODE[2:])).hex()}
    )
    monkeypatch.setitem(SPOKE_POOLS, ARBITRUM.chain_id, reviewed)
    here = Chain(chain_id=ARBITRUM.chain_id, environment="anvil:0x" + "a1" * 32)
    there = Chain(chain_id=BASE.chain_id, environment="anvil:0x" + "b2" * 32)
    resolved = completed(
        resolve_market(
            ForkReader(BLOCK.model_copy(update={"chain": here})),
            selection(),
            BLOCK.model_copy(update={"chain": here}),
        ),
        chain=there,
    )
    choice = AllocationInput(
        market="usdc", account="arb", destination_account="base", minimum_native_balance="0"
    )
    allocation = choice.resolve("move", resolved)
    assert (allocation.route.chain, allocation.route.destination_chain) == (here, BASE.chain_id)
    assert allocation.route.decimals == 6
    forks = StrategyAccounts(bindings=(eoa("arb", here, SENDER), eoa("base", there, RECIPIENT)))
    target = parameters(allocation, accounts=forks).target("move")
    assert (target.fund.chain, target.destination_fund.chain) == (here, there)
    mixed = StrategyAccounts(bindings=(eoa("arb", here, SENDER), eoa("base", BASE, RECIPIENT)))
    with pytest.raises(ValueError, match="one environment"):
        parameters(allocation, accounts=mixed)


class SymbolReader(DestinationReader):
    """The destination's reads, each recorded as evidence as the bootstrap reader does."""

    def __init__(self, block):
        super().__init__(block)
        self.evidence = []

    def call(self, address, signature, block, inputs=(), args=(), outputs=()):
        from almanak.gateway.resolution_evm import ResolutionRead

        read = ResolutionRead(
            method="eth_call", params=(address, signature), result="USDC", retrieved_at=1.0
        )
        self.evidence.append(read)
        return ("USDC",)

    def __enter__(self):
        return self

    def __exit__(self, *error):
        return False


def test_a_two_chain_market_is_completed_and_replayed_on_its_destination(monkeypatch):
    from almanak.configuration import parameters
    from almanak.gateway.bootstrap import MarketFacts, complete_named_market

    reviewed = PIN.model_copy(
        update={"implementation_code_hash": "0x" + keccak(bytes.fromhex(CODE[2:])).hex()}
    )
    monkeypatch.setitem(SPOKE_POOLS, ARBITRUM.chain_id, reviewed)
    monkeypatch.setitem(VALUES, "symbol()", "USDC")
    source = resolve_market(Reader(), selection(), BLOCK)
    anchor = BLOCK.model_copy(update={"chain": BASE, "number": 30_000_000})
    partial = MarketFacts(name="usdc", resolved=source, evidence=(), symbols={USDC: "USDC"})
    reader = SymbolReader(anchor)
    facts = complete_named_market(reader, partial, anchor)
    assert facts.resolved.destination == Asset(chain=BASE, identifier=BASE_USDC, decimals=6)
    assert facts.resolved.destination_block == anchor
    assert facts.symbols == {USDC: "USDC", BASE_USDC: "USDC"}
    (read,) = facts.destination_evidence
    assert read.params == (BASE_USDC.reference, "symbol()")
    assert "destination_evidence" not in partial.model_dump()
    # The replay reads the destination facts from their own retained reads again.
    opened = []

    def retained(chain, evidence):
        opened.append((chain, evidence))
        return SymbolReader(anchor)

    monkeypatch.setattr(parameters, "RetainedResolutionReader", retained)
    observed, symbols = parameters.replayed_market(Reader(), facts)
    assert (observed, symbols) == (facts.resolved, facts.symbols)
    assert opened == [(BASE, facts.destination_evidence)]
