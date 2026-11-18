"""The fork funding ladder: order, terminal routes, rollback and the transfer check."""

import copy
import json
from types import SimpleNamespace

import httpx
import pytest
from eth_utils import keccak
from pydantic import ValidationError

from almanak.caip import AssetType, ChainId
from almanak.chains import evm_network
from almanak.gateway import fork_funding
from almanak.gateway.fork_funding import (
    DEAL_TIMEOUT_SECONDS,
    PROBED_SLOTS,
    ForkDegraded,
    HolderRecipe,
    Recipes,
    SeedRecipe,
    SlotRecipe,
    SnapshotLost,
    fund_erc20,
    recipes,
    seeded_slot,
)
from almanak.gateway.roles_storage import mapping_slot
from almanak.gateway.safe_funding import fund_asset, reviewed_funder
from almanak.models import AssetAmount, Chain

TOKEN = "0x" + "ab" * 20
FUND = "0x" + "12" * 20
HOLDER = "0x" + "34" * 20
ASSET = AssetType(f"eip155:1/erc20:{TOKEN}")
BALANCE_OF = "70a08231"
TOTAL_SUPPLY = "18160ddd"
TRANSFER = "a9059cbb"


def balance_slot(account, slot):
    return mapping_slot("address", account, slot)


class FakeChain:
    """A single ERC-20 with a configurable balance layout and transfer behavior."""

    def __init__(self, slot=None, *, seed=None, deal=False, fee=0, frozen_slot=None, supply=10**30):
        self.slot, self.seed, self.deal, self.fee = slot, seed, deal, fee
        self.frozen_slot, self.supply, self.code = frozen_slot, supply, "0x6080"
        self.state = {"storage": {}, "native": {}}
        self.snapshots, self.calls, self.taken = {}, [], 0
        self.impersonated, self.timeouts, self.failed_reverts = set(), {}, 0

    def location(self, account):
        if self.seed is not None:
            return seeded_slot(account, self.seed)
        return balance_slot(account, self.slot) if self.slot is not None else None

    def held(self, account):
        return self.state["storage"].get(self.location(account), 0)

    def __call__(self, method, params=(), *, timeout=None):
        self.calls.append((method, list(params)))
        self.timeouts[method] = timeout
        return getattr(self, method)(*params)

    def evm_snapshot(self):
        self.taken += 1
        key = hex(self.taken)
        self.snapshots[key] = copy.deepcopy(self.state)
        return key

    def evm_revert(self, key):
        if self.failed_reverts:
            self.failed_reverts -= 1
            return False
        self.state = self.snapshots[key]
        # Anvil drops the reverted snapshot and every later one.
        self.snapshots = {k: v for k, v in self.snapshots.items() if int(k, 16) < int(key, 16)}
        return True

    def eth_getCode(self, account, block):
        return self.code if account == TOKEN else "0x"

    def eth_call(self, call, block):
        assert call["to"] == TOKEN and block == "latest"
        if call["data"][2:10] == TOTAL_SUPPLY:
            return word(self.supply)
        assert call["data"][2:10] == BALANCE_OF
        return word(self.held("0x" + call["data"][-40:]))

    def anvil_setStorageAt(self, token, slot, value):
        self.state["storage"][int(slot, 16)] = int(value, 16)
        return True

    def anvil_dealERC20(self, account, token, amount):
        if not self.deal:
            raise RuntimeError("fork RPC failed: anvil_dealERC20: no slot")
        self.state["storage"][self.location(account)] = int(amount, 16)

    def eth_getBalance(self, account, block):
        return self.state["native"].get(account, "0x5")

    def anvil_setBalance(self, account, value):
        self.state["native"][account] = value
        return True

    def anvil_impersonateAccount(self, account):
        self.impersonated.add(account)

    def anvil_stopImpersonatingAccount(self, account):
        self.impersonated.discard(account)

    def eth_gasPrice(self):
        return "0x7"

    def eth_sendTransaction(self, tx):
        if tx["from"] not in self.impersonated:
            raise RuntimeError("fork RPC failed: eth_sendTransaction: No Signer available")
        assert tx["gasPrice"] == "0x7"
        data = tx["data"][2:]
        assert tx["to"] == TOKEN and data[:8] == TRANSFER
        sender, recipient = tx["from"], "0x" + data[32:72]
        amount = int(data[72:], 16)
        frozen = self.frozen_slot is not None and self.state["storage"].get(
            balance_slot(sender, self.frozen_slot)
        )
        if frozen or self.held(sender) < amount:
            raise RuntimeError("fork RPC failed: eth_sendTransaction: execution reverted")
        storage = self.state["storage"]
        storage[self.location(sender)] = self.held(sender) - amount
        storage[self.location(recipient)] = self.held(recipient) + amount - self.fee
        return "0xhash"

    def wait_receipt(self, tx_hash):
        return {"status": "0x1"}

    def methods(self):
        return [method for method, _ in self.calls]


def word(value):
    return "0x" + value.to_bytes(32, "big").hex()


def node_for(chain):
    origin = Chain(chain_id=ChainId("eip155:1"), environment="mainnet")
    return SimpleNamespace(rpc=chain, origin=SimpleNamespace(chain=origin))


@pytest.fixture
def tables(monkeypatch):
    data = recipes().model_copy(update={"holders": {}, "known_slots": {}, "seed_recipes": {}})
    monkeypatch.setattr(fork_funding, "recipes", lambda: data)
    return data


def test_every_recipe_row_is_reachable_on_a_supported_chain():
    loaded = recipes()
    for table in (loaded.holders, loaded.known_slots, loaded.seed_recipes):
        for asset in table:
            network = evm_network(asset.chain)
            assert reviewed_funder(asset) is None, f"{asset} is shadowed by its reviewed funder"
            assert asset != network.wrapped_native, f"{asset} is always funded by wrapping"


def test_ported_tables_keep_the_previously_refused_tokens():
    loaded = recipes()
    slots = {str(asset): row.slot for asset, row in loaded.known_slots.items()}
    assert slots["eip155:43114/erc20:0x152b9d0fdc40c096757f570a51e494bd4b943e50"] == 0
    assert slots["eip155:42161/erc20:0xfd086bc7cd5c481dcc9c85ebe478a1c0b69fcbb9"] == 51
    assert slots["eip155:1/erc20:0x7f39c581f595b53c5cb19bd0b3f8da6c935e2ca0"] == 0
    cbbtc = AssetType("eip155:8453/erc20:0xcbb7c0000ab88b473b1f5afd9ef808440eed33bf")
    assert loaded.holders[cbbtc].holder == "0xbdb9300b7cde636d9cd4aff00f6f009ffbbc8ee6"


@pytest.mark.parametrize(
    ("table", "row"),
    [
        ("seed_recipes", {"symbol": "X", "seed": 2**32}),
        ("known_slots", {"symbol": "X", "slot": -1}),
        ("known_slots", {"symbol": "X", "slot": True}),
        ("known_slots", {"symbol": "X", "slot": 1, "note": "unreviewed"}),
        ("holders", {"symbol": "X", "holder": "0x" + "AB" * 20}),
        ("holders", {"symbol": "X", "holder": "0x" + "ab" * 19}),
    ],
)
def test_malformed_recipe_rows_refuse_to_load(table, row):
    raw = json.loads(recipes().model_dump_json())
    raw[table][str(ASSET)] = row
    with pytest.raises(ValidationError):
        Recipes.model_validate_json(json.dumps(raw))


def test_known_slot_precedes_deal_and_probing(tables):
    tables.known_slots[ASSET] = SlotRecipe(symbol="T", slot=51)
    chain = FakeChain(51, deal=True)
    assert fund_erc20(node_for(chain), FUND, ASSET, 1000) == {
        "method": "known_slot",
        "slot": "0x33",
    }
    assert chain.held(FUND) == 1000
    assert "anvil_dealERC20" not in chain.methods()


def test_deal_runs_before_probing_when_no_recipe_matches(tables):
    chain = FakeChain(7, deal=True)
    assert fund_erc20(node_for(chain), FUND, ASSET, 1000) == {"method": "deal_erc20"}
    assert "anvil_setStorageAt" not in chain.methods()
    assert chain.timeouts["anvil_dealERC20"] == DEAL_TIMEOUT_SECONDS


def test_a_deal_timeout_stops_funding_without_reverting_the_degraded_fork(tables):
    chain = FakeChain(0)

    def still_tracing(*_):
        raise httpx.ReadTimeout("timed out")

    chain.anvil_dealERC20 = still_tracing
    with pytest.raises(ForkDegraded, match="after 20 s; the fork is degraded; recreate it$"):
        fund_erc20(node_for(chain), FUND, ASSET, 1000)
    assert chain.methods()[-1] == "anvil_dealERC20"
    assert not {"evm_revert", "anvil_setStorageAt"} & set(chain.methods())


def test_probe_misses_roll_back_before_the_matching_slot(tables):
    chain = FakeChain(51)
    route = fund_erc20(node_for(chain), FUND, ASSET, 1000)
    assert route == {"method": "probed_slot", "slot": "0x33"}
    writes = [int(p[1], 16) for m, p in chain.calls if m == "anvil_setStorageAt"]
    assert writes == [balance_slot(FUND, s) for s in PROBED_SLOTS[:8]]
    assert chain.state["storage"] == {balance_slot(FUND, 51): 1000}


def test_a_blacklist_write_is_reverted_before_the_real_slot_is_found(tables):
    chain = FakeChain(9, frozen_slot=3)
    assert fund_erc20(node_for(chain), FUND, ASSET, 1000)["slot"] == "0x9"
    assert not chain.state["storage"].get(balance_slot(FUND, 3))


def test_a_known_slot_that_fails_the_transfer_check_falls_through(tables):
    tables.known_slots[ASSET] = SlotRecipe(symbol="T", slot=51)
    chain = FakeChain(51, frozen_slot=51, deal=True)
    with pytest.raises(ValueError) as refused:
        fund_erc20(node_for(chain), FUND, ASSET, 1000)
    message = str(refused.value)
    for route in ("known_slot 0x33", "deal_erc20", "probed_slot 0x0", "probed_slot 0x34"):
        assert route + ":" in message
    assert "execution reverted" in message
    assert chain.state["storage"] == {}


def test_storage_routes_refuse_a_balance_above_total_supply(tables):
    tables.known_slots[ASSET] = SlotRecipe(symbol="T", slot=0)
    chain = FakeChain(0, deal=True, supply=999)
    with pytest.raises(ValueError) as refused:
        fund_erc20(node_for(chain), FUND, ASSET, 1000)
    message = str(refused.value)
    for route in ("known_slot 0x0", "deal_erc20", "probed_slot 0x0"):
        assert route + ": totalSupply 999 is below the written balance" in message
    assert chain.state["storage"] == {}


def test_fee_on_transfer_tokens_refuse_every_route(tables):
    chain = FakeChain(0, fee=1, deal=True)
    with pytest.raises(ValueError, match="transfer check sent 100 but recipient received 99"):
        fund_erc20(node_for(chain), FUND, ASSET, 1000)
    assert chain.state["storage"] == {}


def test_a_prefunded_transfer_check_recipient_is_a_miss(tables):
    chain = FakeChain(0, deal=True)
    recipient = "0x" + keccak(bytes.fromhex(FUND[2:] + TOKEN[2:]))[-20:].hex()
    chain.state["storage"][balance_slot(recipient, 0)] = 1
    with pytest.raises(
        ValueError, match=f"deal_erc20: transfer-check recipient {recipient} already"
    ):
        fund_erc20(node_for(chain), FUND, ASSET, 1000)
    assert chain.state["storage"] == {balance_slot(recipient, 0): 1}


def test_seed_recipe_skips_numeric_probing(tables):
    tables.seed_recipes[ASSET] = SeedRecipe(symbol="T", seed=0x87A211A2)
    chain = FakeChain(seed=0x87A211A2)
    assert fund_erc20(node_for(chain), FUND, ASSET, 5) == {
        "method": "seed_recipe",
        "seed": "0x87a211a2",
    }
    chain = FakeChain(seed=1)
    with pytest.raises(ValueError, match="seed_recipe 0x87a211a2: .*; deal_erc20: [^;]*$"):
        fund_erc20(node_for(chain), FUND, ASSET, 5)


def test_holder_transfer_restores_the_holder_native_balance(tables):
    tables.holders[ASSET] = HolderRecipe(symbol="T", holder=HOLDER)
    chain = FakeChain(0, supply=1)
    chain.state["storage"][balance_slot(HOLDER, 0)] = 5000
    assert fund_erc20(node_for(chain), FUND, ASSET, 1000) == {
        "method": "holder_transfer",
        "holder": HOLDER,
    }
    assert chain.held(FUND) == 1000 and chain.held(HOLDER) == 4000
    assert chain.state["native"][HOLDER] == "0x5" and not chain.impersonated


def test_a_failed_send_stops_impersonating_and_rolls_back(tables):
    tables.holders[ASSET] = HolderRecipe(symbol="T", holder=HOLDER)
    chain = FakeChain(0)
    chain.state["storage"][balance_slot(HOLDER, 0)] = 5000

    def receipt_unknown(tx_hash):
        raise TimeoutError("fork fixture receipt remains unknown")

    chain.wait_receipt = receipt_unknown
    with pytest.raises(TimeoutError, match="receipt remains unknown"):
        fund_erc20(node_for(chain), FUND, ASSET, 1000)
    assert not chain.impersonated and chain.held(FUND) == 0
    assert chain.state["native"] == {} and chain.snapshots == {}


def test_a_short_holder_refuses_without_storage_fallthrough(tables):
    tables.holders[ASSET] = HolderRecipe(symbol="T", holder=HOLDER)
    tables.known_slots[ASSET] = SlotRecipe(symbol="T", slot=0)
    chain = FakeChain(0, deal=True)
    chain.state["storage"][balance_slot(HOLDER, 0)] = 999
    with pytest.raises(ValueError, match="holder_transfer 0x3434.*: holder had 999 .* below 1000$"):
        fund_erc20(node_for(chain), FUND, ASSET, 1000)
    assert not {"anvil_setStorageAt", "anvil_dealERC20"} & set(chain.methods())


def test_reviewed_funders_are_terminal_and_transfer_checked(tables):
    tables.known_slots[ASSET] = SlotRecipe(symbol="T", slot=0)

    def reviewed(node, fund, token, amount):
        node.rpc("anvil_setStorageAt", [token, hex(balance_slot(fund, 0)), hex(amount)])

    chain = FakeChain(0)
    assert fund_erc20(node_for(chain), FUND, ASSET, 10, reviewed) == {"method": "reviewed"}
    chain = FakeChain(0, fee=1)
    with pytest.raises(ValueError, match=r"exactly; reviewed: transfer check sent 1 .*0$"):
        fund_erc20(node_for(chain), FUND, ASSET, 10, reviewed)


def test_a_reviewed_funder_refusal_is_a_terminal_miss(tables):
    tables.known_slots[ASSET] = SlotRecipe(symbol="T", slot=0)

    def reviewed(node, fund, token, amount):
        node.rpc("anvil_setStorageAt", [token, hex(balance_slot(fund, 0)), hex(amount)])
        raise ValueError("runtime differs from its verified layout")

    chain = FakeChain(0, deal=True)
    with pytest.raises(ValueError, match="exactly; reviewed: runtime differs from its verified"):
        fund_erc20(node_for(chain), FUND, ASSET, 10, reviewed)
    assert chain.state["storage"] == {} and "anvil_dealERC20" not in chain.methods()


class GarbledFirstRead(FakeChain):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.garbled = 1

    def eth_call(self, call, block):
        if self.garbled:
            self.garbled -= 1
            return "0x"
        return super().eth_call(call, block)


def test_an_empty_balance_read_is_a_rolled_back_miss(tables):
    tables.known_slots[ASSET] = SlotRecipe(symbol="T", slot=3)
    chain = GarbledFirstRead(0, deal=True)
    assert fund_erc20(node_for(chain), FUND, ASSET, 1000) == {"method": "deal_erc20"}
    assert chain.state["storage"].get(balance_slot(FUND, 3)) is None
    assert "0x1" not in chain.snapshots  # the failed route's snapshot was reverted


def test_a_token_without_code_refuses_before_any_route(tables):
    chain = FakeChain(0, deal=True)
    chain.code = "0x"
    funding = SimpleNamespace(balance=AssetAmount(asset=ASSET, amount=1000), method="local_storage")
    with pytest.raises(ValueError, match=f"no contract code at {TOKEN}$"):
        fund_asset(node_for(chain), None, FUND, funding)
    assert chain.methods() == ["eth_getCode"]


def test_an_unexpected_error_is_rolled_back_then_raised(tables):
    tables.known_slots[ASSET] = SlotRecipe(symbol="T", slot=0)
    chain = FakeChain(0)

    def broken(*_):
        raise TypeError("unexpected")

    chain.anvil_setStorageAt = broken
    with pytest.raises(TypeError, match="unexpected"):
        fund_erc20(node_for(chain), FUND, ASSET, 1000)
    assert "evm_revert" in chain.methods() and chain.snapshots == {}


def test_a_lost_snapshot_is_never_a_miss(tables):
    chain = FakeChain(51)
    chain.failed_reverts = 1
    with pytest.raises(SnapshotLost, match="could not restore its fork snapshot"):
        fund_erc20(node_for(chain), FUND, ASSET, 1000)
    assert chain.methods().count("evm_revert") == 1


def test_a_lost_transfer_check_snapshot_rolls_back_the_route_and_raises(tables):
    tables.known_slots[ASSET] = SlotRecipe(symbol="T", slot=0)
    chain = FakeChain(0, deal=True)
    chain.failed_reverts = 1
    with pytest.raises(SnapshotLost):
        fund_erc20(node_for(chain), FUND, ASSET, 1000)
    reverts = [params for method, params in chain.calls if method == "evm_revert"]
    assert reverts == [["0x2"], ["0x1"]]
    assert chain.state["storage"] == {} and chain.snapshots == {}
    assert "anvil_dealERC20" not in chain.methods()


def test_amounts_above_uint256_refuse_before_any_write(tables):
    chain = FakeChain(0)
    with pytest.raises(ValueError, match="unsigned 256-bit"):
        fund_erc20(node_for(chain), FUND, ASSET, 2**256)
    assert chain.calls == []


def test_zero_amounts_still_prove_a_transfer(tables):
    chain = FakeChain(0, deal=True)
    assert fund_erc20(node_for(chain), FUND, ASSET, 0) == {"method": "deal_erc20"}
    assert "eth_sendTransaction" in chain.methods()


def test_fixture_evidence_records_the_route(tables, monkeypatch):
    tables.known_slots[ASSET] = SlotRecipe(symbol="T", slot=0)
    chain = FakeChain(0)
    chain.eth_blockNumber = lambda: "0x65"
    node = node_for(chain)
    funding = SimpleNamespace(balance=AssetAmount(asset=ASSET, amount=7), method="local_storage")
    evidence = fund_asset(node, None, FUND, funding)
    assert evidence == {
        "kind": "simulated_initial_capital",
        "fund": FUND,
        "origin_asset": str(ASSET),
        "amount": 7,
        "method": "known_slot",
        "slot": "0x0",
        "block": 101,
    }

    def wrap(node, owner, fund, item):
        chain.state["storage"][balance_slot(fund, 0)] = item.balance.amount

    monkeypatch.setattr("almanak.gateway.safe_funding.fund_wrapped", wrap)
    wrapped = SimpleNamespace(balance=AssetAmount(asset=ASSET, amount=7), method="wrap_native")
    before = len(chain.calls)
    evidence = fund_asset(node, None, HOLDER, wrapped)
    assert evidence["method"] == "wrap_native" and set(evidence) == {
        "kind",
        "fund",
        "origin_asset",
        "amount",
        "method",
        "block",
    }
    assert "evm_snapshot" not in chain.methods()[before:]
