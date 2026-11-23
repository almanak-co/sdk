"""An author names one route and two accounts; the source chain's reads resolve it."""

from types import SimpleNamespace

import pytest
from eth_utils import keccak

from almanak.configuration.unresolved import parse_inputs
from almanak.configuration.yaml import ConfigurationError, Document
from almanak.connectors.across.author_inputs import AllocationInput
from almanak.connectors.across.domain import SPOKE_POOLS
from almanak.connectors.across.observations import IMPLEMENTATION_SLOT, snapshot
from almanak.connectors.across.resolution import MarketSelection, resolve_market
from almanak.connectors.composition import connector_for, connector_from_wire
from almanak.connectors.support import RunMode
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
    resolved = resolve_market(Reader(), selection(), BLOCK)
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


def test_the_connector_is_bound_and_declares_no_run_mode_yet():
    binding = connector_from_wire({"connector": "across"})
    assert connector_for(CONFIG).codec.checkpoint is binding.codec.checkpoint
    for chain in (ARBITRUM.chain_id, BASE.chain_id):
        assert binding.support_for(chain).modes == ()
        with pytest.raises(ValueError, match="fork is unsupported"):
            binding.require_target(chain, TARGET, RunMode.FORK)
    assert binding.codec.actions(SimpleNamespace(config=CONFIG)) is CONFIG
