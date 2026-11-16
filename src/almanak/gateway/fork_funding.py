"""Ordered ERC-20 funding for fork fixtures with exact, transferable results."""

import re
from functools import cache, partial
from pathlib import Path
from typing import Annotated

import httpx
from eth_utils import keccak
from pydantic import Field

from almanak.caip import AssetType
from almanak.gateway.evm import erc20_address
from almanak.gateway.roles_storage import mapping_slot
from almanak.gateway.safe_fixture import calldata
from almanak.models import Model

OPENZEPPELIN_ERC20_NAMESPACE = 0x52C63247E1F47DB19D5CE0460030C497F067CA4CEBF71BA98EEADABE20BACE00
PROBED_SLOTS = (0, 1, 2, 3, 4, 5, 9, 51, 52, OPENZEPPELIN_ERC20_NAMESPACE)
STORAGE_ROUTES = frozenset({"known_slot", "seed_recipe", "deal_erc20", "probed_slot"})
DEAL_TIMEOUT_SECONDS = 20
GAS_ALLOWANCE = 10**20


class FundingMiss(Exception):
    """One funding route did not produce exact, transferable capital."""


class SnapshotLost(Exception):
    """Never a miss: after a failed revert the fork state is unknown."""


class ForkDegraded(Exception):
    pass


LowerAddress = Annotated[str, Field(pattern=r"^0x[0-9a-f]{40}$")]


class Provenance(Model):
    repository: str
    commit: Annotated[str, Field(pattern=r"^[0-9a-f]{40}$")]
    sources: tuple[str, ...]
    filtered: tuple[str, ...]


class HolderRecipe(Model):
    symbol: str
    holder: LowerAddress


class SlotRecipe(Model):
    symbol: str
    slot: Annotated[int, Field(ge=0, lt=2**256)]


class SeedRecipe(Model):
    symbol: str
    seed: Annotated[int, Field(ge=0, lt=2**32)]


class Recipes(Model):
    provenance: Provenance
    holders: dict[AssetType, HolderRecipe]
    known_slots: dict[AssetType, SlotRecipe]
    seed_recipes: dict[AssetType, SeedRecipe]


@cache
def recipes() -> Recipes:
    path = Path(__file__).with_name("artifacts") / "erc20-funding-recipes.json"
    return Recipes.model_validate_json(path.read_text())


def balance(rpc, token, account):
    return read_uint(rpc, token, "balanceOf(address)", (account,))


def total_supply(rpc, token):
    return read_uint(rpc, token, "totalSupply()", ())


def read_uint(rpc, token, signature, args):
    data = calldata(signature, ("address",) * len(args), args)
    raw = rpc("eth_call", [{"to": token, "data": data}, "latest"])
    if not isinstance(raw, str) or re.fullmatch(r"0x[0-9a-fA-F]{64}", raw) is None:
        raise FundingMiss(f"{signature.split('(')[0]} returned no uint256")
    return int(raw, 16)


def require_code(node, token):
    code = node.rpc("eth_getCode", [token, "latest"])
    if not isinstance(code, str) or not code.removeprefix("0x").strip("0"):
        raise ValueError(f"fixture cannot fund an ERC-20 with no contract code at {token}")


def fund_erc20(node, fund, asset, amount, reviewed=None):
    """Fund exactly `amount` through the first passing route; refuse naming every outcome."""
    if amount >= 2**256:
        raise ValueError("fixture funding requires an unsigned 256-bit base-unit amount")
    token = erc20_address(asset, node.origin.chain.chain_id)
    outcomes = []
    for method, evidence, apply in routes(node, fund, token, asset, amount, reviewed):
        try:
            attempt(node, fund, token, amount, apply, minted=method in STORAGE_ROUTES)
        except FundingMiss as miss:
            outcomes.append(" ".join((method, *evidence.values())) + f": {miss}")
            continue
        return {"method": method, **evidence}
    raise ValueError(f"fixture cannot fund {asset} exactly; " + "; ".join(outcomes))


def routes(node, fund, token, asset, amount, reviewed):
    if reviewed is not None:
        return [("reviewed", {}, partial(run_reviewed, reviewed, node, fund, token, amount))]
    holder = recipes().holders.get(asset)
    if holder is not None:
        move = partial(transfer_from_holder, node, fund, token, amount, holder.holder)
        return [("holder_transfer", {"holder": holder.holder}, move)]
    return storage_routes(node, fund, token, asset, amount)


def storage_routes(node, fund, token, asset, amount):
    write = partial(set_storage, node, token)
    found: list[tuple[str, dict[str, str], partial]] = []
    known = recipes().known_slots.get(asset)
    if known is not None:
        slot = mapping_slot("address", fund, known.slot)
        found.append(("known_slot", {"slot": hex(known.slot)}, partial(write, slot, amount)))
    seed = recipes().seed_recipes.get(asset)
    if seed is not None:
        slot = seeded_slot(fund, seed.seed)
        found.append(("seed_recipe", {"seed": hex(seed.seed)}, partial(write, slot, amount)))
    found.append(("deal_erc20", {}, partial(deal, node, fund, token, amount)))
    if seed is None:
        found.extend(
            (
                "probed_slot",
                {"slot": hex(slot)},
                partial(write, mapping_slot("address", fund, slot), amount),
            )
            for slot in PROBED_SLOTS
        )
    return found


def run_reviewed(reviewed, node, fund, token, amount):
    try:
        reviewed(node, fund, token, amount)
    except ValueError as exc:
        raise FundingMiss(str(exc)) from exc


def attempt(node, fund, token, amount, apply, *, minted):
    snapshot = node.rpc("evm_snapshot")
    try:
        apply()
        if balance(node.rpc, token, fund) != amount:
            raise FundingMiss("balance differs from the declared amount")
        if minted and (supply := total_supply(node.rpc, token)) < amount:
            raise FundingMiss(f"totalSupply {supply} is below the written balance")
        check_transfer(node, fund, token, amount)
    except ForkDegraded:
        raise
    except (FundingMiss, RuntimeError) as exc:
        revert(node, snapshot)
        raise FundingMiss(str(exc)) from exc
    except BaseException:
        revert(node, snapshot)
        raise


def revert(node, snapshot):
    if node.rpc("evm_revert", [snapshot]) is not True:
        raise SnapshotLost("fixture funding could not restore its fork snapshot")


def check_transfer(node, fund, token, amount):
    """Prove a fresh recipient receives exactly what the funded account sends, then undo it."""
    sent = max(1, amount // 10) if amount else 0
    recipient = "0x" + keccak(bytes.fromhex(fund[2:] + token[2:]))[-20:].hex()
    if balance(node.rpc, token, recipient) != 0:
        raise FundingMiss(f"transfer-check recipient {recipient} already holds a balance")
    snapshot = node.rpc("evm_snapshot")
    try:
        send_as(node, fund, token, transfer_data(recipient, sent))
        moved = balance(node.rpc, token, recipient), balance(node.rpc, token, fund)
    finally:
        revert(node, snapshot)
    if moved != (sent, amount - sent):
        raise FundingMiss(f"transfer check sent {sent} but recipient received {moved[0]}")


def transfer_from_holder(node, fund, token, amount, holder):
    # At the head: anvil refuses calls pinned to an Arbitrum-family fork block, whose
    # header has no blob gas, and nothing on the fork has moved the holder's balance.
    held = balance(node.rpc, token, holder)
    if held < amount:
        raise FundingMiss(f"holder had {held} on the fork, below {amount}")
    send_as(node, holder, token, transfer_data(fund, amount))


def send_as(node, sender, to, data):
    native = node.rpc("eth_getBalance", [sender, "latest"])
    node.rpc("anvil_setBalance", [sender, hex(int(native, 16) + GAS_ALLOWANCE)])
    node.rpc("anvil_impersonateAccount", [sender])
    try:
        price = node.rpc("eth_gasPrice")
        tx = {"from": sender, "to": to, "data": data, "gasPrice": price}
        tx_hash = node.rpc("eth_sendTransaction", [tx])
        node.rpc.wait_receipt(tx_hash)
    finally:
        node.rpc("anvil_stopImpersonatingAccount", [sender])
        node.rpc("anvil_setBalance", [sender, native])


def transfer_data(recipient, amount):
    return calldata("transfer(address,uint256)", ("address", "uint256"), (recipient, amount))


def deal(node, fund, token, amount):
    # The node keeps tracing balanceOf after the client gives up, starving later requests,
    # so a timeout ends funding on this fork instead of reverting and trying the next route.
    try:
        node.rpc("anvil_dealERC20", [fund, token, hex(amount)], timeout=DEAL_TIMEOUT_SECONDS)
    except httpx.TimeoutException as exc:
        raise ForkDegraded(
            f"anvil_dealERC20 still running after {DEAL_TIMEOUT_SECONDS} s; "
            "the fork is degraded; recreate it"
        ) from exc


def set_storage(node, token, slot, value):
    node.rpc("anvil_setStorageAt", [token, word(slot), word(value)])


def word(value):
    return "0x" + value.to_bytes(32, "big").hex()


def seeded_slot(fund, seed):
    return int.from_bytes(
        keccak(bytes.fromhex(fund[2:]) + bytes(8) + seed.to_bytes(4, "big")), "big"
    )
