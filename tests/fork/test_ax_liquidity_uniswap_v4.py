"""`almanak ax` liquidity commands on deployed Uniswap V4 pools, one process per command.

Every cell that declares the liquidity actions is rehearsed, and the Ethereum pool of
the native coin beside them: an isolated Anvil fork of the chain stands in for mainnet
behind the production adapter's own planning, signing and reconciliation, and the
command's own resolution builds each deployment. It is rehearsal evidence, not a
mainnet run.
"""

import json
from dataclasses import replace

import pytest

from almanak import ax_actions
from almanak.connectors.composition import connector_from_wire
from almanak.connectors.direct import ALLOCATION, ActionRequest
from almanak.connectors.uniswap_v4 import abi
from almanak.connectors.uniswap_v4 import reader as reads
from almanak.connectors.uniswap_v4.math import liquidity_for
from almanak.connectors.uniswap_v4.models import Range, Target
from almanak.connectors.uniswap_v4.pool import read_pool
from almanak.connectors.uniswap_v4.position_request import PositionRequest, read_liquidity_position
from almanak.gateway.ethereum import EthereumReader
from almanak.gateway.evm import erc20_address
from almanak.gateway.fork_funding import balance, read_uint, send_as
from almanak.gateway.safe_fixture import calldata
from almanak.models import Account
from tests.fork import test_ax_liquidity as family_liquidity
from tests.fork import test_ax_swap as family
from tests.fork import test_ax_swap_uniswap_v4 as swaps

pytestmark = pytest.mark.fork
CONNECTOR = "uniswap_v4"
SLIPPAGE_BPS = 100
PRICED = ("open-liquidity", "close-liquidity")
OPENING = ["approve_erc20", "approve_permit2", "approve_erc20", "approve_permit2", "mint"]
# The liquidity reference of each chain; the Ethereum one carries no chain in its name.
REFERENCES = {
    "eip155:1": "uniswap_v4_lp",
    "eip155:43114": "uniswap_v4_avalanche_lp",
    "eip155:8453": "uniswap_v4_base_lp",
    "eip155:42161": "uniswap_v4_arbitrum_lp",
    "eip155:10": "uniswap_v4_optimism_lp",
    "eip155:56": "uniswap_v4_bnb_lp",
    "eip155:137": "uniswap_v4_polygon_lp",
    "eip155:4663": "uniswap_v4_robinhood_lp",
}
# The wallet holds this many times the reference's funding and the trader as much again.
# In the deepest pools a smaller position earns less than one base unit of fees from a
# trade its quote can still cover.
MULTIPLE = 40
MAX_TOKEN, MAX_PERMIT, FAR = swaps.MAX_TOKEN, swaps.MAX_PERMIT, swaps.FAR
LIMITS = family_liquidity.LIMITS
# A trade across many ticks of a thin pool takes more gas than the default limit.
TRADE_LIMITS = family_liquidity.TRADE_LIMITS


def declared_chains():
    """Every chain that declares opening liquidity as a direct action."""
    cells = connector_from_wire({"connector": CONNECTOR}).chain_support
    return [str(c.chain) for c in cells if "open_liquidity" in map(str, c.direct_actions)]


def funded(name):
    """A reference with the rehearsal's larger funding."""
    cell = swaps.reference(name)
    return replace(cell, funded=cell.funded * MULTIPLE)


class Pool:
    """One funded wallet and one trader on one fork of one pool; each call is one command."""

    def __init__(self, wallet, trader):
        self.wallet, self.trader = wallet, trader
        self.fork, self.cell, self.owner = wallet.fork, wallet.cell, wallet.owner
        held = wallet.target()
        self.pool, self.contracts, self.assets = held.pool, held.contracts, held.assets
        self.tokens = tuple(erc20_address(asset, self.cell.chain) for asset in self.assets)
        self.manager = self.contracts.position_manager
        # The reference's funded asset as an index into the pool's token order.
        self.funded = self.assets.index(self.cell.asset)

    def other(self):
        """The same pool as the trader sees it."""
        return Pool(self.trader, self.wallet)

    def reader(self):
        return EthereumReader(self.wallet.chain, self.fork.rpc.endpoint)

    def request(self, action, **stated):
        priced = action in PRICED
        return ActionRequest(
            action,
            slippage_bps=SLIPPAGE_BPS if priced else None,
            deadline_seconds=300 if priced else None,
            **stated,
        )

    def __call__(self, action, *, execute=True, limits=LIMITS, **stated):
        request = self.request(action, **stated)
        config = ax_actions.deployment(
            CONNECTOR,
            self.cell.market,
            request,
            self.wallet.wallet,
            ax_actions.alchemy(self.cell.chain),
            self.fork.rpc.endpoint,
            limits,
        )
        gateway = family.backend(self.fork, config, self.wallet.wallet, self.wallet.identity)
        try:
            return ax_actions.run(config, gateway, request, execute=execute)
        finally:
            gateway.close()

    def recover(self, record):
        return self.wallet.recover(record)

    def halve(self):
        """Sell half of the funded token for the other one, with `ax swap`."""
        sold = self.balances()[self.funded] // 2
        result = self.wallet(sold, execute=True, limits=TRADE_LIMITS)
        family_liquidity.assert_clean(result, swaps.STEPS)

    def trade(self):
        """The trader sells its funded token through the pool, which earns the pool fees."""
        sold = self.other().balances()[self.funded] // 2
        result = self.trader(sold, execute=True, limits=TRADE_LIMITS)
        family_liquidity.assert_clean(result)

    def balances(self):
        """The wallet's balances of the pool's two held tokens, in base units."""
        return tuple(balance(self.fork.rpc, token, self.owner) for token in self.tokens)

    def coin(self):
        return int(self.fork.rpc("eth_getBalance", [self.owner, "latest"]), 16)

    def target(self, ticks):
        """The pool with a range for this wallet, for the connector's own call builders."""
        fund = Account(chain=self.wallet.chain, address=self.owner)
        return Target(
            allocation=ALLOCATION,
            account="treasury",
            fund=fund,
            pool=self.pool,
            contracts=self.contracts,
            range=Range(lower=ticks["tick_lower"], upper=ticks["tick_upper"]),
        )

    def around(self):
        """Three tick spacings around the pool's tick: a range both tokens go into."""
        spacing = self.pool.tick_spacing
        with self.reader() as reader:
            tick = read_pool(reader, self.pool, reader.pin_block("latest")).tick
        floor = tick // spacing * spacing
        return {"tick_lower": floor - spacing, "tick_upper": floor + 2 * spacing}

    def stated(self):
        """A quarter of what the wallet holds of each token, as the mint's most."""
        held = self.balances()
        return {"amount0": held[0] // 4, "amount1": held[1] // 4}

    def allowances(self):
        """Every nonzero allowance as a result lists it: each token's ERC-20 approval to
        Permit2, then its Permit2 allowances to the router and the position manager."""
        with self.reader() as reader:
            seen = reader.pin_block("latest")
            target = self.target(self.around())
            found = [self.token_allowance(reader, target, asset, seen) for asset in self.assets]
            return [row for rows in found for row in rows if row["amount"] != "0"]

    def token_allowance(self, reader, target, asset, block):
        token = reads.allowance(reader, target, asset, block)
        rows = [{"asset": str(asset), "spender": self.contracts.permit2, "amount": str(token)}]
        for spender in (self.contracts.router, self.manager):
            amount, expiration, _ = reads.permit_allowance(reader, target, asset, spender, block)
            granted = {"asset": str(asset), "spender": spender, "amount": str(amount)}
            rows.append({**granted, "expiration": expiration})
        return rows

    def nft_owner(self, token_id):
        """The NFT's owner, or None once it is burned."""
        data = calldata("ownerOf(uint256)", ("uint256",), (token_id,))
        try:
            raw = self.fork.rpc("eth_call", [{"to": self.manager, "data": data}, "latest"])
        except RuntimeError:
            return None
        return "0x" + raw[-40:]

    def next_token_id(self):
        return read_uint(self.fork.rpc, self.manager, "nextTokenId()", ())

    def read(self, token_id):
        """`ax liquidity-position` at the fork's head, through the read's own function."""
        named = {**dict(self.cell.key), "token_id": token_id}
        request = PositionRequest.model_validate_json(json.dumps(named))
        with self.reader() as reader:
            return read_liquidity_position(reader, request, reader.pin_block("latest"))

    def approve_unlimited(self):
        """The largest ERC-20 approval to Permit2 and the largest Permit2 allowance to the
        position manager, for both tokens, given outside `ax`."""
        for token in self.tokens:
            approval = (self.contracts.permit2, MAX_TOKEN)
            send_as(
                self.fork,
                self.owner,
                token,
                calldata("approve(address,uint256)", ("address", "uint256"), approval),
            )
            types = ("address", "address", "uint160", "uint48")
            granted = calldata(
                "approve(address,address,uint160,uint48)",
                types,
                (token, self.manager, MAX_PERMIT, FAR),
            )
            send_as(self.fork, self.owner, self.contracts.permit2, granted)

    def mint_outside(self):
        """Mint straight against the position manager from the wallet, over unlimited
        approvals; returns the new NFT's token ID and its range."""
        ticks, amounts = self.around(), self.stated()
        target = self.target(ticks)
        self.approve_unlimited()
        with self.reader() as reader:
            price = read_pool(reader, self.pool, reader.pin_block("latest")).sqrt_price_x96
        most = amounts["amount0"], amounts["amount1"]
        liquidity = liquidity_for(*most, price, *reads.sqrt_bounds(target)) * 99 // 100
        token_id = self.next_token_id()
        call = abi.mint(target, liquidity, *most, self.wallet.head_time() + 3000)
        send_as(self.fork, self.owner, call.to, call.data)
        return token_id, ticks


def prepared(fork, cell, tmp_path, monkeypatch):
    wallet = swaps.prepared(fork, cell, tmp_path, monkeypatch)
    funded_trader = family.funded_wallet(fork, cell, tmp_path / "trader")
    trader = swaps.Setting(fork, cell, funded_trader, wallet.identity)
    return Pool(wallet, trader)


@pytest.fixture(params=declared_chains(), ids=lambda chain: family.NETWORKS[chain])
def setting(request, tmp_path, monkeypatch):
    cell = funded(REFERENCES[request.param])
    with swaps.started(request, cell) as fork:
        yield prepared(fork, cell, tmp_path, monkeypatch)


@pytest.fixture
def native(request, tmp_path, monkeypatch):
    cell = funded(swaps.NATIVE_REFERENCE)
    with swaps.started(request, cell) as fork:
        yield prepared(fork, cell, tmp_path, monkeypatch)


def moved(result):
    return [int(item["amount"]) for item in result["position_after"]["moved"]]


def assert_moved(setting, result, start):
    """The wallet moved by exactly what the result reports, and the result's allowances
    are the chain's."""
    after = setting.balances()
    assert [now - before for now, before in zip(after, start, strict=True)] == moved(result)
    assert result["position_after"]["allowances"] == setting.allowances()


def assert_previewed(setting, ticks, stated):
    preview = setting("open-liquidity", execute=False, **ticks, **stated)
    step = preview["first_step"]["operation"]
    assert (preview["executed"], preview["position_before"]["position"]) == (False, None)
    assert (step["kind"], step["spender"]) == ("approve_erc20", setting.contracts.permit2)
    assert setting.allowances() == []


def open_position(setting, ticks, stated, steps=OPENING):
    """Mint with `ax open-liquidity`; returns the token ID and what the mint used."""
    start = setting.balances()
    opened = setting("open-liquidity", **ticks, **stated)
    family_liquidity.assert_clean(opened, steps)
    held = opened["position_after"]["position"]
    token_id = int(held["token_id"])
    assert (setting.nft_owner(token_id), held["range"]) == (
        setting.owner,
        {"lower": ticks["tick_lower"], "upper": ticks["tick_upper"]},
    )
    assert_moved(setting, opened, start)
    return token_id, [-value for value in moved(opened)]


def unspent(setting, stated, used):
    """What stays approved of each stated amount: what the mint did not take. Nothing
    stays of a native leg, whose whole stated amount the position manager pulls as the
    wrapped token and sweeps back to the wallet unused."""
    rest = [stated["amount0"] - used[0], stated["amount1"] - used[1]]
    native = setting.wallet.target().native_index
    if native is not None:
        rest[native] = 0
    return rest


def assert_unused_stays_approved(setting, stated, used):
    """What the mint did not take of each stated amount stays approved to Permit2, and
    to the position manager through Permit2 until the mint's deadline."""
    rest = unspent(setting, stated, used)
    listed = setting.allowances()
    amounts = [int(row["amount"]) for row in listed]
    assert sorted(amounts) == sorted(value for value in rest for _ in (0, 1) if value)
    assert all(0 <= value <= most for value, most in zip(used, stated.values(), strict=True))
    assert {row["spender"] for row in listed} <= {setting.contracts.permit2, setting.manager}


def assert_read(setting, token_id, ticks):
    """`ax liquidity-position` reports the position the mint made."""
    seen = setting.read(token_id)
    assert (seen["owner"], seen["position"]["token_id"]) == (setting.owner, str(token_id))
    assert seen["position"]["range"] == {"lower": ticks["tick_lower"], "upper": ticks["tick_upper"]}
    assert int(seen["position"]["liquidity"]) > 0 and seen["pool"]["in_range"]
    return [int(item["amount"]) for item in seen["principal"]]


def collect_fees(setting, token_id):
    """Collect with `ax collect-liquidity-fees` after a trade; returns what it paid."""
    setting.trade()
    owed = [int(item["amount"]) for item in setting.read(token_id)["owed"]]
    start = setting.balances()
    collected = setting("collect-liquidity-fees", token_id=token_id)
    family_liquidity.assert_clean(collected, ["collect"])
    assert_moved(setting, collected, start)
    assert moved(collected) == owed and sum(owed) > 0
    assert collected["position_after"]["position"]["token_id"] == str(token_id)
    return moved(collected)


def close_position(setting, token_id):
    """Close with `ax close-liquidity`: the principal returns and the NFT is burned."""
    start = setting.balances()
    closed = setting("close-liquidity", token_id=token_id)
    family_liquidity.assert_clean(closed, ["burn"])
    assert_moved(setting, closed, start)
    assert (closed["position_after"]["position"], setting.nft_owner(token_id)) == (None, None)
    assert min(moved(closed)) >= 0 and sum(moved(closed)) > 0
    return moved(closed)


def test_a_position_is_opened_read_collected_and_closed(setting):
    setting.halve()
    ticks, stated = setting.around(), setting.stated()
    assert_previewed(setting, ticks, stated)
    token_id, used = open_position(setting, ticks, stated)
    assert_unused_stays_approved(setting, stated, used)
    principal = assert_read(setting, token_id, ticks)
    collect_fees(setting, token_id)
    released = close_position(setting, token_id)
    # A trade moved the price, so only the total is compared: principal came back.
    assert sum(principal) > 0 and sum(released) > 0


def foreign_position(setting):
    """A position the trader minted in the same pool."""
    trader = setting.other()
    trader.halve()
    return trader.mint_outside()[0]


def position_of_another_key(setting, attempts=400):
    """An NFT of the same position manager whose pool key is another one."""
    key = abi.pool_key(setting.pool)
    with setting.reader() as reader:
        block = reader.pin_block("latest")
        for token_id in range(1, attempts):
            found = reader.call(
                setting.manager,
                "getPoolAndPositionInfo(uint256)",
                block,
                inputs=("uint256",),
                args=(token_id,),
                outputs=(abi.POOL_KEY, "uint256"),
            )
            if found[0] != key and setting.nft_owner(token_id) is not None:
                return token_id
    raise AssertionError("no position of another pool key among the first token IDs")


def assert_refused(setting, token_id, reason):
    for action in ("collect-liquidity-fees", "close-liquidity"):
        with pytest.raises(ValueError, match=reason):
            setting(action, execute=False, token_id=token_id)


def assert_adopted(setting, token_id, ticks):
    """A preview adopts the position as the chain states it and plans its collection."""
    preview = setting("collect-liquidity-fees", execute=False, token_id=token_id)
    held = preview["position_before"]["position"]
    assert (held["token_id"], preview["first_step"]["operation"]["kind"]) == (
        str(token_id),
        "collect",
    )
    assert held["range"] == {"lower": ticks["tick_lower"], "upper": ticks["tick_upper"]}
    assert int(held["liquidity"]) == int(setting.read(token_id)["position"]["liquidity"])


def granted(setting):
    """The wallet's nonzero allowances at the head, by token and spender."""
    return {(row["asset"], row["spender"]): int(row["amount"]) for row in setting.allowances()}


def assert_unlimited_stand(setting, earlier, used):
    """The unlimited approvals a mint was served over stand: Permit2's whole, the
    token's whole or lowered by what the mint took, as that token is written."""
    now = granted(setting)
    for asset, spent in zip(map(str, setting.assets), used, strict=True):
        before = earlier[(asset, setting.contracts.permit2)]
        assert now[(asset, setting.manager)] == MAX_PERMIT
        assert now[(asset, setting.contracts.permit2)] in swaps.after_spend(before, spent)


def test_a_position_minted_outside_is_adopted_and_foreign_ones_are_refused(setting):
    setting.halve()
    token_id, ticks = setting.mint_outside()
    assert_adopted(setting, token_id, ticks)
    assert_refused(setting, foreign_position(setting), "belongs to")
    assert_refused(setting, position_of_another_key(setting), "position of another pool key")
    collect_fees(setting, token_id)
    # A mint over the standing unlimited approvals needs no approval of its own.
    earlier = granted(setting)
    second, used = open_position(setting, setting.around(), setting.stated(), ["mint"])
    assert_unlimited_stand(setting, earlier, used)
    close_position(setting, token_id)
    close_position(setting, second)


def test_an_interrupted_open_is_recovered_from_its_receipt(setting, monkeypatch, capsys):
    setting.halve()
    ticks, stated = setting.around(), setting.stated()
    start = setting.balances()

    def opening(*, execute):
        return setting("open-liquidity", execute=execute, **ticks, **stated)

    record = family.stopped_while_waiting_for("mint", opening, monkeypatch, capsys)
    family.assert_blocked(opening, record, "mint")
    recovered = setting.recover(record)
    assert (recovered["resolved"], recovered["discarded_request"]) == (True, False)
    assert (family.operations(recovered), family_liquidity.troubled(recovered)) == (OPENING, [])
    assert (ax_actions.kept_records(), record.exists()) == ([], False)
    assert_moved(setting, recovered, start)
    close_position(setting, int(recovered["position_after"]["position"]["token_id"]))


def test_a_native_pool_position_holds_the_wrapped_token_and_moves_no_native_value(native):
    native.halve()
    coin = native.coin()
    ticks, stated = native.around(), native.stated()
    token_id, used = open_position(native, ticks, stated)
    assert_unused_stays_approved(native, stated, used)
    collect_fees(native, token_id)
    close_position(native, token_id)
    # Only gas left the wallet's native balance: the pool took and paid the wrapped token.
    assert str(native.pool.currency0) == swaps.NATIVE_COIN and 0 < coin - native.coin() < 5 * 10**16


def test_every_declared_cell_has_a_liquidity_reference_to_rehearse():
    chains = declared_chains()
    found = [swaps.reference(REFERENCES[chain]) for chain in chains]
    assert len(chains) == 8 and [str(item.chain) for item in found] == chains
    assert all(item.funded > 0 and item.asset.chain == item.chain for item in found)
