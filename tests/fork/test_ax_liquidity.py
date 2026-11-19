"""The direct liquidity commands of `almanak ax` on deployed V3-family pools.

Every cell that declares the liquidity actions as direct actions is rehearsed, each
step as its own command with its own deployment: an isolated Anvil fork of the chain
stands in for mainnet behind the production adapter's own planning, signing and
reconciliation, and the command's own resolution builds each deployment. It is
rehearsal evidence, not a mainnet run.
"""

import json
from dataclasses import dataclass
from importlib import import_module
from pathlib import Path

import pytest
import yaml
from eth_account import Account as Signer
from eth_utils import to_checksum_address

from almanak import ax_actions
from almanak.caip import AssetType, ChainId
from almanak.connectors.composition import connector_from_wire
from almanak.connectors.concentrated_math import (
    addition_maximums,
    liquidity_for,
    sqrt_at_tick,
)
from almanak.connectors.concentrated_math import amounts as taken_amounts
from almanak.connectors.direct import ALLOCATION, ActionRequest
from almanak.connectors.v3_pool_position_request import (
    PositionRequest,
    accrued_fees,
    read_liquidity_position,
)
from almanak.connectors.v3_pool_reader import nft_record
from almanak.connectors.v3_pool_request import family
from almanak.gateway.credentials import write_private
from almanak.gateway.ethereum import DataError, EthereumReader
from almanak.gateway.evm import chain_number, erc20_address
from almanak.gateway.fork_config import ForkTarget
from almanak.gateway.fork_fixture import Fork
from almanak.gateway.fork_funding import balance, fund_erc20, read_uint
from almanak.gateway.read_endpoints import read_endpoint
from almanak.gateway.safe_fixture import calldata
from almanak.models import AssetAmount, Chain
from tests.fork.test_ax_swap import NETWORKS, PASSWORD, as_mainnet, backend

pytestmark = pytest.mark.fork
ROOT = Path(__file__).parents[2]
SLIPPAGE_BPS = 100
CONNECTORS = ("uniswap_v3", "pancakeswap_v3", "aerodrome_slipstream")
PRICED = ("swap", "open-liquidity", "increase-liquidity", "decrease-liquidity", "close-liquidity")
# The wallet holds this many times the reference's funding, and the trader that makes
# fees accrue sells as much again. In the deepest pools a smaller position would earn
# less than one base unit of fees from a trade its quote can still cover.
WALLET_MULTIPLE = 40
TRADER_MULTIPLE = 40
# Wide enough for every chain's pinned block: a mint on Polygon costs above half a POL
# at the 870 gwei its pinned block quotes.
LIMITS = ax_actions.FeeLimits(max_gas_price_wei=2000 * 10**9, max_transaction_fee=2 * 10**18)
# The trader's swap crosses many ticks of a thin pool, beyond the default gas limit.
TRADE_LIMITS = ax_actions.FeeLimits(
    LIMITS.max_gas_price_wei, 5 * LIMITS.max_transaction_fee, max_gas_limit=5_000_000
)
TRANSFER = "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"
# One cell per connector for the scenarios in which another account acts between the
# signing and the mining of a step.
PROBES = [
    ("uniswap_v3", "eip155:8453"),
    ("pancakeswap_v3", "eip155:56"),
    ("aerodrome_slipstream", "eip155:8453"),
]
# The trader holds this many times the reference's funding: selling all of it moves
# the price of the deepest probed pool by several basis points.
TRADER_FUNDING = 400
FAR_DEADLINE = 2**40


def declared_cells():
    """Every connector and chain that declares opening liquidity as a direct action."""
    return [
        (name, str(support.chain))
        for name in CONNECTORS
        for support in connector_from_wire({"connector": name}).chain_support
        if "open_liquidity" in {str(action) for action in support.direct_actions}
    ]


@dataclass(frozen=True)
class Reference:
    """The pinned block, pool and funded asset of a cell's liquidity reference.

    The reference's own range is not used: the wallet's swap moves the price first, so
    each test takes a range around the pool's tick at that moment.
    """

    connector: str
    chain: ChainId
    pool: str
    height: int
    block_hash: str
    asset: AssetType
    # Base units of `asset` the reference funds; the wallet holds a multiple of it.
    funded: int
    native: int


def reference(connector: str, chain: str) -> Reference:
    name = f"{connector}_{NETWORKS[chain]}_lp"
    fork = yaml.safe_load((ROOT / "examples/operators" / f"{name}.fork.yaml").read_text())["fork"]
    config = yaml.safe_load((ROOT / "strategies" / name / "config.yaml").read_text())
    (anchor,) = fork["anchors"]
    (fund,) = fork["funds"]
    (funded,) = fund["assets"]
    (operator,) = fork["operators"]
    return Reference(
        connector=connector,
        chain=ChainId(chain),
        pool=config["markets"]["pool"]["pool"],
        height=anchor["height"],
        block_hash=anchor["reference"],
        asset=AssetType(funded["asset"]),
        funded=funded["amount"],
        native=operator["native_balance"]["amount"],
    )


@pytest.fixture(params=declared_cells(), ids=lambda cell: f"{cell[0]}-{NETWORKS[cell[1]]}")
def cell(request):
    return reference(*request.param)


@pytest.fixture
def fork(request, cell):
    env = request.config.getoption("--fork-env").resolve()
    chain = Chain(chain_id=cell.chain, environment="mainnet")
    with EthereumReader(chain, read_endpoint(cell.chain, env)) as reader:
        origin = reader.pin_block(cell.height)
    assert origin.hash == cell.block_hash
    target = ForkTarget(chain=chain, provider=ax_actions.alchemy(cell.chain), block=origin)
    with Fork(env, origin, target=target) as running:
        yield running


def funded_wallet(fork, cell, directory, amount):
    """A plain account funded with the cell's asset, as `almanak wallet create` records it."""
    signer = fork.funded_signer(None, 0, cell.native)
    fund_erc20(fork, signer.address.lower(), cell.asset, amount)
    directory.mkdir(mode=0o700)
    stores = {"treasury": Signer.encrypt(signer.key, PASSWORD, kdf="pbkdf2", iterations=1000)}
    write_private(directory / "keystores.json", json.dumps(stores))
    write_private(directory / "keystore-password", PASSWORD)
    wallet = {
        "account": "treasury",
        "address": signer.address,
        "keystores": str(directory / "keystores.json"),
        "password_file": str(directory / "keystore-password"),
    }
    write_private(directory / "wallet.json", json.dumps(wallet))
    return ax_actions.read_wallet(directory), signer


class Setting:
    """One funded wallet on one fork; each call is one command with its own deployment."""

    def __init__(self, fork, cell, wallet, signer, identity):
        self.fork, self.cell, self.wallet, self.identity = fork, cell, wallet, identity
        self.signer = signer
        self.owner = wallet["address"].lower()
        resolution = ax_actions.resolution(cell.connector)
        self.market = resolution.MarketSelection(
            connector=cell.connector, chain=cell.chain, pool=cell.pool
        )
        with self.reader() as reader:
            resolved = resolution.resolve_market(reader, self.market, reader.pin_block("latest"))
        self.pool, self.contracts = resolved.pool, resolved.contracts
        self.assets = (self.pool.currency0, self.pool.currency1)
        self.tokens = tuple(erc20_address(asset, cell.chain) for asset in self.assets)
        self.manager = self.contracts.position_manager
        # The cell's funded asset as an index into the pool's currency order.
        self.funded = self.assets.index(cell.asset)

    def reader(self):
        chain = Chain(chain_id=self.cell.chain, environment="mainnet")
        return EthereumReader(chain, self.fork.rpc.endpoint)

    def other(self, wallet, signer):
        """The same pool for another wallet on the same fork."""
        return Setting(self.fork, self.cell, wallet, signer, self.identity)

    def request(self, action, **stated):
        bounds = {"slippage_bps": SLIPPAGE_BPS, "deadline_seconds": 300}
        return ActionRequest(action, **{**(bounds if action in PRICED else {}), **stated})

    def config(self, action, limits=LIMITS, **stated):
        """The deployment as the command builds it, resolution included."""
        return ax_actions.deployment(
            self.cell.connector,
            self.market,
            self.request(action, **stated),
            self.wallet,
            ax_actions.alchemy(self.cell.chain),
            self.fork.rpc.endpoint,
            limits,
        )

    def __call__(self, action, *, execute=True, limits=LIMITS, before_send=None, **stated):
        config = self.config(action, limits, **stated)
        request = self.request(action, **stated)
        return self.run(config, request, execute=execute, before_send=before_send)

    def run(self, config, request, *, execute=True, before_send=None):
        gateway = backend(self.fork, config, self.wallet, self.identity)
        interpose(gateway, before_send)
        try:
            return ax_actions.run(config, gateway, request, execute=execute)
        finally:
            gateway.close()

    def recover(self, record):
        return ax_actions.recover(
            record, lambda stored: backend(self.fork, stored, self.wallet, self.identity)
        )

    def swap(self, amount, limits=LIMITS):
        """Sell `amount` of the funded asset for the pool's other currency, with `ax swap`."""
        result = self("swap", limits=limits, amount=amount, sell=str(self.cell.asset))
        assert_clean(result, ["approve", "swap"])
        return result

    def balances(self):
        """The wallet's balances of currency0 and currency1, in base units."""
        return tuple(balance(self.fork.rpc, token, self.owner) for token in self.tokens)

    def allowances(self):
        """Every nonzero allowance the wallet gave the router or the position manager."""
        found = {}
        for asset, token in zip(self.assets, self.tokens, strict=True):
            for spender in (self.contracts.router, self.manager):
                amount = read_uint(
                    self.fork.rpc, token, "allowance(address,address)", (self.owner, spender)
                )
                if amount:
                    found[(str(asset), spender)] = amount
        return found

    def nft_owner(self, token_id):
        """The NFT's owner, or None once it is burned."""
        data = calldata("ownerOf(uint256)", ("uint256",), (token_id,))
        try:
            raw = self.fork.rpc("eth_call", [{"to": self.manager, "data": data}, "latest"])
        except RuntimeError:
            return None
        return "0x" + raw[-40:]

    def read(self, token_id):
        """`ax liquidity-position` at the fork's head, through the read's own function."""
        request = PositionRequest(chain=self.cell.chain, pool=self.cell.pool, token_id=token_id)
        with self.reader() as reader:
            return read_liquidity_position(
                reader, self.cell.connector, request, reader.pin_block("latest")
            )

    def owed(self, token_id):
        """What a collection would pay for the position, as the read reports it."""
        return [int(item["amount"]) for item in self.read(token_id)["owed"]]

    def pool_state(self):
        reads = import_module(f"almanak.connectors.{self.cell.connector}.reader")
        with self.reader() as reader:
            return reads.read_pool(reader, self.pool, reader.pin_block("latest"))

    def tick(self):
        return self.pool_state().tick

    def offered(self, stated):
        """What an addition of the stated amounts offers at the pool's price: no more of
        either currency than its range takes within the price tolerance."""
        lower, upper = sqrt_at_tick(stated["tick_lower"]), sqrt_at_tick(stated["tick_upper"])
        bps = stated.get("slippage_bps", SLIPPAGE_BPS)
        price = self.pool_state().sqrt_price_x96
        return addition_maximums(stated["amount0"], stated["amount1"], price, lower, upper, bps)

    def sell_outside(self, amount):
        """Approve the pool's router; returns the act that sells the funded asset through
        it, outside `ax`."""
        package = f"almanak.connectors.{self.cell.connector}"
        operation = import_module(f"{package}.operations").Swap(
            target=self.config("revoke-exchange").target(ALLOCATION),
            amount=AssetAmount(asset=self.cell.asset, amount=amount),
            minimum=AssetAmount(asset=self.assets[1 - self.funded], amount=1),
            deadline=FAR_DEADLINE,
        )
        call = import_module(f"{package}.abi").call(operation)
        approval = calldata("approve(address,uint256)", ("address", "uint256"), (call.to, amount))
        self.send(self.tokens[self.funded], approval)
        return lambda: self.send(call.to, call.data)

    def add_outside(self, token_id, amounts):
        """Approve the position manager; returns the act that adds to an NFT, outside
        `ax`. The position manager lets any account add to any position."""
        from almanak.connectors.v3_pool_abi import INCREASE_PARAMS

        self.approve_outside(amounts)
        params = (token_id, *amounts, 0, 0, FAR_DEADLINE)
        data = calldata(f"increaseLiquidity({INCREASE_PARAMS})", (INCREASE_PARAMS,), (params,))
        return lambda: self.send(self.manager, data)

    def send(self, to, data):
        """One transaction signed by the wallet and sent outside `ax`."""
        rpc, sender = self.fork.rpc, self.signer.address
        call = {"from": sender, "to": to_checksum_address(to), "data": data}
        signed = self.signer.sign_transaction(
            {
                "chainId": chain_number(self.cell.chain),
                "nonce": int(rpc("eth_getTransactionCount", [sender, "pending"]), 16),
                "to": call["to"],
                "data": data,
                "value": 0,
                # A mint's use can exceed its estimate once the estimate's refunds are gone.
                "gas": int(rpc("eth_estimateGas", [call]), 16) * 3 // 2,
                "gasPrice": int(rpc("eth_gasPrice"), 16),
            }
        )
        return rpc.wait_receipt(
            rpc("eth_sendRawTransaction", ["0x" + signed.raw_transaction.hex()])
        )

    def approve_outside(self, amounts):
        """Approve the position manager for each nonzero amount, outside `ax`."""
        for token, amount in zip(self.tokens, amounts, strict=True):
            if amount:
                approval = calldata(
                    "approve(address,uint256)", ("address", "uint256"), (self.manager, amount)
                )
                self.send(token, approval)

    def mint_call(self, lower, upper, amounts):
        """The position manager's `mint` for this pool, paying the wallet's own address."""
        if self.cell.connector == "aerodrome_slipstream":
            from almanak.connectors.aerodrome_slipstream.abi import MINT_PARAMS

            key, tail = self.pool.tick_spacing, (0,)
        else:
            from almanak.connectors.v3_pool_abi import MINT_PARAMS

            key, tail = self.pool.fee, ()
        params = (*self.tokens, key, lower, upper, *amounts, 0, 0, self.owner, FAR_DEADLINE, *tail)
        return calldata(f"mint({MINT_PARAMS})", (MINT_PARAMS,), (params,))

    def mint_outside(self, lower, upper, amount0, amount1):
        """Mint straight against the position manager; returns the new NFT's token ID."""
        self.approve_outside((amount0, amount1))
        receipt = self.send(self.manager, self.mint_call(lower, upper, (amount0, amount1)))
        (minted,) = [log for log in receipt["logs"] if self.mints(log)]
        return int(minted["topics"][3], 16)

    def mints(self, log):
        """Whether a log is the position manager's transfer of a new NFT."""
        if log["address"].lower() != self.manager:
            return False
        return log["topics"][0] == TRANSFER and int(log["topics"][1], 16) == 0

    def empty_outside(self, token_id):
        """Remove all of a position's liquidity without collecting what that releases."""
        from almanak.connectors.v3_pool_abi import DECREASE_PARAMS

        liquidity = int(self.read(token_id)["position"]["liquidity"])
        params = (token_id, liquidity, 0, 0, FAR_DEADLINE)
        self.send(
            self.manager,
            calldata(f"decreaseLiquidity({DECREASE_PARAMS})", (DECREASE_PARAMS,), (params,)),
        )

    def stake_outside(self, token_id):
        """Deposit an NFT into the pool's gauge, outside `ax`; returns the gauge."""
        gauge = self.pool_state().gauge
        approval = calldata("approve(address,uint256)", ("address", "uint256"), (gauge, token_id))
        self.send(self.manager, approval)
        self.send(gauge, calldata("deposit(uint256)", ("uint256",), (token_id,)))
        return gauge

    def collects(self, holder, token_id):
        """What the position manager would pay a collection by the NFT's holder now."""
        from almanak.connectors.v3_pool_abi import COLLECT_PARAMS, MAX128

        params = (token_id, holder, MAX128, MAX128)
        call = {
            "from": holder,
            "to": self.manager,
            "data": calldata(f"collect({COLLECT_PARAMS})", (COLLECT_PARAMS,), (params,)),
        }
        raw = self.fork.rpc("eth_call", [call, "latest"])
        return [int(raw[2:66], 16), int(raw[66:130], 16)]

    def unstaked_accrual(self, token_id):
        """The fees the range's fee growth would have added to an unstaked position."""
        selected = family(self.cell.connector)
        with self.reader() as reader:
            block = reader.pin_block("latest")
            observation = selected.read_pool(reader, self.pool, block)
            record = nft_record(reader, self.manager, token_id, block, "int24")
            return accrued_fees(reader, selected, observation, block, record)

    def around(self, width=1):
        """A range both currencies go into: the tick spacing the pool's tick is in, and
        `width` spacings either side of it."""
        spacing = self.pool.tick_spacing
        floor = self.tick() // spacing * spacing
        return {"tick_lower": floor - width * spacing, "tick_upper": floor + (width + 1) * spacing}

    def one_sided_range(self):
        """A range beside the price that takes only the funded asset."""
        spacing = self.pool.tick_spacing
        floor = self.tick() // spacing * spacing
        if self.funded == 0:
            return floor + 10 * spacing, floor + 20 * spacing
        return floor - 20 * spacing, floor - 10 * spacing

    def stated(self, funded, other=0):
        """Stated amounts in the pool's currency order."""
        amounts = [other, other]
        amounts[self.funded] = funded
        return {"amount0": amounts[0], "amount1": amounts[1]}


def interpose(gateway, before_send):
    """Run an act once, between the gateway's checks of a signed step and its sending.

    `before_send` is the kind of step and the act, or None. The act stands for what
    another account does between the signing of a step and its mining. The gateway
    checks a signed step more than once before it sends it, so the act is tied to the
    send of the signed bytes themselves.
    """
    if before_send is None:
        return
    kind, act = before_send
    checked, send, waiting = gateway.check_submission, gateway.reader._rpc, {}

    def check(pending, permissions):
        checked(pending, permissions)
        if pending.operation.kind == kind:
            waiting.setdefault(pending.raw_transaction, act)

    def rpc(method, params, **options):
        if method == "eth_sendRawTransaction":
            waiting.pop(params[0], lambda: None)()
        return send(method, params, **options)

    gateway.check_submission, gateway.reader._rpc = check, rpc


@pytest.fixture
def setting(fork, cell, tmp_path, monkeypatch):
    # Records and claims stay out of the operator's own state: the first action sweeps
    # every record it finds there.
    monkeypatch.setattr("almanak.state.claims_root", lambda: tmp_path / "state/claims")
    wallet, signer = funded_wallet(fork, cell, tmp_path / "wallet", cell.funded * WALLET_MULTIPLE)
    trader = funded_wallet(fork, cell, tmp_path / "trader", cell.funded * TRADER_FUNDING)
    identity = as_mainnet(fork, cell.chain)
    found = Setting(fork, cell, wallet, signer, identity)
    found.trader = found.other(*trader)
    return found


def operations(result):
    return [item["operation"] for item in result["outcomes"]]


def troubled(result):
    """The outcomes that reverted or carry an accounting issue."""
    return [
        item for item in result["outcomes"] if not item["success"] or "accounting_issues" in item
    ]


def assert_clean(result, expected=None):
    """The command completed, every step is reconciled, and no outcome has an issue."""
    ended = [key for key in ("refused", "unresolved", "record_kept") if key in result]
    assert (result["completed"], ended, troubled(result)) == (True, [], [])
    assert expected in (None, operations(result))


def amounts(result, moment, field):
    return [int(item["amount"]) for item in result[moment][field]]


def reported_allowances(result):
    return {
        (item["asset"], item["spender"]): int(item["amount"])
        for item in result["position_after"]["allowances"]
    }


def listed(allowances):
    """Allowances as a result lists them."""
    return [
        {"asset": asset, "spender": spender, "amount": str(amount)}
        for (asset, spender), amount in allowances.items()
    ]


def gained(setting, start):
    """What the wallet's two balances changed by since `start`."""
    return [new - old for new, old in zip(setting.balances(), start, strict=True)]


def assert_moved(setting, result, start):
    """The wallet moved by exactly what the result reports, and allowances are as reported."""
    moved = amounts(result, "position_after", "moved")
    assert gained(setting, start) == moved
    assert reported_allowances(result) == setting.allowances()
    return moved


def assert_released(setting, result, start):
    """A removal pays the wallet in at least one currency and takes nothing from it."""
    released = assert_moved(setting, result, start)
    assert min(released) >= 0 < sum(released)
    return released


def approvals(offer):
    """One approval for each currency the offer holds."""
    return ["approve"] * sum(1 for amount in offer if amount)


def approved(setting, amounts):
    """The nonzero amounts as allowances of the position manager, in currency order."""
    return {
        (str(asset), setting.manager): amount
        for asset, amount in zip(setting.assets, amounts, strict=True)
        if amount
    }


def halves(setting):
    """The wallet after it swapped half its funding: stated amounts to open and to add."""
    setting.swap(setting.balances()[setting.funded] // 2)
    held = setting.balances()
    return (
        {"amount0": held[0] // 2, "amount1": held[1] // 2},
        {"amount0": held[0] // 4, "amount1": held[1] // 4},
    )


def assert_previewed(setting, ticks, stated):
    """A preview plans the first approval, attributes the stated amounts and signs nothing."""
    start = setting.balances()
    preview = setting("open-liquidity", execute=False, **ticks, **stated)
    step = preview["first_step"]["operation"]["kind"]
    assert (preview["action"], preview["executed"], step) == ("open-liquidity", False, "approve")
    assert preview["position_before"]["position"] is None
    assert amounts(preview, "position_before", "inventory") == list(stated.values())
    assert (setting.balances(), setting.allowances()) == (start, {})


def open_position(setting, ticks, stated):
    """Open a position; returns its report and what the mint used of each currency."""
    start, offer = setting.balances(), setting.offered({**ticks, **stated})
    opened = setting("open-liquidity", **ticks, **stated)
    assert_clean(opened, [*approvals(offer), "mint"])
    used = [-moved for moved in assert_moved(setting, opened, start)]
    minted = opened["position_after"]["position"]
    assert minted["range"] == {"lower": ticks["tick_lower"], "upper": ticks["tick_upper"]}
    assert setting.nft_owner(int(minted["token_id"])) == setting.owner
    assert_unused(setting, opened, stated, offer, used)
    return minted, used


def assert_unused(setting, opened, stated, offer, used):
    """The mint used at most what it offered, which is at most the stated amounts. What
    it left of the statement is reported unused; what it left of the offer stays
    approved to the position manager, and nothing else is."""
    unused = amounts(opened, "position_after", "inventory")
    assert [most - spent for most, spent in zip(stated.values(), used, strict=True)] == unused
    left = [most - spent for most, spent in zip(offer, used, strict=True)]
    assert min(*used, *left) >= 0
    assert setting.allowances() == approved(setting, left)


def shortfalls(used, principal):
    """What the mint took of each currency beyond the principal the read reports."""
    return [spent - int(item["amount"]) for spent, item in zip(used, principal, strict=True)]


def assert_read(setting, minted, used):
    """The read reports the minted position, in range, with the principal the mint put in."""
    seen = setting.read(int(minted["token_id"]))
    assert (seen["owner"], seen["pool"]["in_range"]) == (setting.owner, True)
    assert seen["position"] == {key: minted[key] for key in seen["position"]}
    # A mint rounds what it takes up; the principal a removal would pay rounds down.
    assert set(shortfalls(used, seen["principal"])) <= {0, 1, 2}


def increase_position(setting, minted, stated):
    """Add the stated amounts to the position; returns its grown liquidity."""
    start = setting.balances()
    increased = setting("increase-liquidity", token_id=int(minted["token_id"]), **stated)
    assert_clean(increased)
    assert increased["position_before"]["position"] == minted
    kinds = operations(increased)
    assert (kinds[-1], set(kinds) <= {"approve", "increase"}) == ("increase", True)
    assert_moved(setting, increased, start)
    grown = int(increased["position_after"]["position"]["liquidity"])
    assert grown > int(minted["liquidity"])
    return grown


def earn_fees(setting, token_id):
    """Another account trades through the pool; returns what the position is then owed."""
    setting.trader.swap(setting.cell.funded * TRADER_MULTIPLE, TRADE_LIMITS)
    owed = setting.owed(token_id)
    assert owed[setting.funded] > 0
    return owed


def collect_fees(setting, token_id, owed, liquidity):
    """The collection pays exactly what the read reported as owed, and keeps the liquidity."""
    start = setting.balances()
    collected = setting("collect-liquidity-fees", token_id=token_id)
    assert_clean(collected, ["collect"])
    assert assert_moved(setting, collected, start) == owed
    kept = int(collected["position_after"]["position"]["liquidity"])
    assert (kept, amounts(collected, "position_after", "inventory")) == (liquidity, owed)
    return collected


def decrease_position(setting, token_id, liquidity):
    """Remove a third of the liquidity; all of what is left cannot be decreased away."""
    start, part = setting.balances(), liquidity // 3
    decreased = setting("decrease-liquidity", token_id=token_id, liquidity=part)
    assert_clean(decreased, ["decrease"])
    assert_released(setting, decreased, start)
    assert int(decreased["position_after"]["position"]["liquidity"]) == liquidity - part
    with pytest.raises(ValueError, match="--liquidity must be below the position's"):
        setting("decrease-liquidity", token_id=token_id, liquidity=liquidity - part)


def close_position(setting, token_id):
    """Close the position: its NFT is burned, and no later command finds it."""
    start = setting.balances()
    closed = setting("close-liquidity", token_id=token_id)
    assert_clean(closed, ["close"])
    released = assert_released(setting, closed, start)
    assert (closed["position_after"]["position"], setting.nft_owner(token_id)) == (None, None)
    with pytest.raises(ValueError, match=f"holds no NFT {token_id}"):
        setting("collect-liquidity-fees", token_id=token_id)
    return released


def revoke_everything(setting):
    """What a mint and an increase did not use is still approved. The revocation clears
    it and moves nothing, and a second one finds nothing to revoke."""
    left, start = setting.allowances(), setting.balances()
    revoked = setting("revoke-exchange")
    assert_clean(revoked, ["approve"] * len(left))
    assert (bool(left), revoked["position_before"]["allowances"]) == (True, listed(left))
    assert (reported_allowances(revoked), setting.allowances(), setting.balances()) == (
        {},
        {},
        start,
    )
    with pytest.raises(ValueError, match="no allowance in either pool currency"):
        setting("revoke-exchange")


def test_a_position_is_opened_read_increased_collected_decreased_closed_and_revoked(setting):
    first, more = halves(setting)
    ticks = setting.around()
    assert_previewed(setting, ticks, first)
    minted, used = open_position(setting, ticks, first)
    assert_read(setting, minted, used)
    token_id = int(minted["token_id"])
    grown = increase_position(setting, minted, more)
    collect_fees(setting, token_id, earn_fees(setting, token_id), grown)
    decrease_position(setting, token_id, grown)
    close_position(setting, token_id)
    revoke_everything(setting)


def trader_position(setting):
    """A position of another account in the same pool, which takes only the funded asset."""
    trader = setting.trader
    beside = trader.one_sided_range()
    return trader.mint_outside(*beside, **trader.stated(setting.cell.funded)), beside


def assert_adopted(setting, collected, held):
    """The deployment started with the NFT as the chain held it, and nothing else."""
    adopted = collected["position_before"]["position"]
    assert held["position"] == {key: adopted[key] for key in held["position"]}
    assert (held["owner"], amounts(collected, "position_before", "inventory")) == (
        setting.owner,
        [0, 0],
    )


def assert_principal_returned(released, principal):
    """Each currency a close paid is the principal the read reported, less the slippage."""
    floors = [int(item["amount"]) * (10000 - SLIPPAGE_BPS) // 10000 for item in principal]
    assert min(paid - floor for paid, floor in zip(released, floors, strict=True)) >= 0


def adopt_collect_and_close(setting, token_id):
    """Collect from and close a position the commands did not mint."""
    owed = earn_fees(setting, token_id)
    held = setting.read(token_id)
    collected = collect_fees(setting, token_id, owed, int(held["position"]["liquidity"]))
    assert_adopted(setting, collected, held)
    assert_principal_returned(close_position(setting, token_id), held["principal"])


def assert_emptied_refused(setting, emptied):
    """An emptied position is refused by the commands that collect from it or remove."""
    for action, stated in (
        ("collect-liquidity-fees", {}),
        ("close-liquidity", {}),
        ("decrease-liquidity", {"liquidity": 1}),
    ):
        with pytest.raises(ValueError, match=f"NFT {emptied} holds no liquidity"):
            setting(action, token_id=emptied, **stated)


def refill_and_close(setting, emptied):
    """An emptied position takes an increase, and closing it then also pays what the
    position manager still owed it."""
    owed, start = setting.owed(emptied), setting.balances()
    assert max(owed) > 0
    increased = setting(
        "increase-liquidity", token_id=emptied, amount0=start[0] // 8, amount1=start[1] // 8
    )
    assert_clean(increased)
    closed = setting("close-liquidity", token_id=emptied)
    assert_clean(closed, ["close"])
    # The increase and the close cancel but for the rounding of each; the rest is the
    # owed amount, which the close collected.
    net = [got - due for got, due in zip(gained(setting, start), owed, strict=True)]
    assert (min(net) >= -2, setting.nft_owner(emptied)) == (True, None)


def assert_foreign_refused(setting, foreign, beside):
    """An NFT another account owns is refused by name, and a deployment built past that
    check is refused at its first observation, before anything is planned."""
    trader = setting.trader
    assert setting.nft_owner(foreign) == trader.owner
    with pytest.raises(ValueError, match=f"NFT {foreign} belongs to {trader.owner}"):
        setting("collect-liquidity-fees", token_id=foreign)
    stated = setting.request("collect-liquidity-fees", token_id=foreign)
    with pytest.raises(DataError, match="not owned by the named fund") as refusal:
        setting.run(adopting(setting, foreign, beside), stated)
    assert refusal.value.kind == "ownership"


def adopting(setting, token_id, ticks):
    """The wallet's deployment, stated to adopt an NFT the command itself would refuse."""
    config = setting.config("revoke-exchange")
    document = config.model_dump(mode="json")
    (allocation,) = document["allocations"]
    allocation["range"] = {"lower": ticks[0], "upper": ticks[1]}
    allocation["adopt"]["nft"] = f"{setting.cell.chain}/erc721:{setting.manager}/{token_id}"
    return type(config).model_validate_json(json.dumps(document))


def assert_other_pool_refused(setting):
    """An NFT of another pool is refused although the wallet owns it."""
    elsewhere = take_position_of_another_pool(setting)
    assert setting.nft_owner(elsewhere) == setting.owner
    for action in ("collect-liquidity-fees", "close-liquidity"):
        with pytest.raises(ValueError, match=f"NFT {elsewhere} is a position of another pool"):
            setting(action, token_id=elsewhere)
    with pytest.raises(ValueError, match="is a position of another pool"):
        setting.read(elsewhere)


def holder_elsewhere(setting, token_id):
    """The owner of an NFT of another token pair, or None for this pair or no NFT."""
    data = calldata("positions(uint256)", ("uint256",), (token_id,))
    try:
        raw = setting.fork.rpc("eth_call", [{"to": setting.manager, "data": data}, "latest"])[2:]
    except RuntimeError:
        return None
    pair = ("0x" + raw[2 * 64 + 24 : 3 * 64], "0x" + raw[3 * 64 + 24 : 4 * 64])
    return None if pair == setting.tokens else setting.nft_owner(token_id)


def take_position_of_another_pool(setting, attempts=400):
    """Give the wallet an existing NFT of another pool of the same position manager."""
    rpc, manager = setting.fork.rpc, setting.manager
    found = ((token_id, holder_elsewhere(setting, token_id)) for token_id in range(1, attempts))
    token_id, holder = next(item for item in found if item[1] is not None)
    rpc("anvil_impersonateAccount", [holder])
    rpc("anvil_setBalance", [holder, hex(10**18)])
    move = calldata(
        "transferFrom(address,address,uint256)",
        ("address", "address", "uint256"),
        (holder, setting.owner, token_id),
    )
    sent = {"from": holder, "to": manager, "data": move, "gasPrice": rpc("eth_gasPrice")}
    rpc.wait_receipt(rpc("eth_sendTransaction", [sent]))
    rpc("anvil_stopImpersonatingAccount", [holder])
    return token_id


def test_a_position_minted_outside_is_adopted_and_foreign_ones_are_refused(setting):
    first, more = halves(setting)
    lower, upper = setting.around().values()
    token_id = setting.mint_outside(lower, upper, *first.values())
    emptied = setting.mint_outside(lower, upper, *more.values())
    setting.empty_outside(emptied)
    foreign, beside = trader_position(setting)
    outside = setting.allowances()

    adopt_collect_and_close(setting, token_id)
    # The commands gave no allowance and took none of those given outside them.
    assert setting.allowances() == outside

    assert_emptied_refused(setting, emptied)
    refill_and_close(setting, emptied)
    start = setting.balances()
    assert_foreign_refused(setting, foreign, beside)
    assert_other_pool_refused(setting)
    assert (setting.balances(), ax_actions.kept_records()) == (start, [])


def stopped_while_waiting_for(kind, setting, monkeypatch, capsys, stated):
    """Run an open that is interrupted while its step of this kind waits for a receipt."""
    from almanak.operator_runtime import OperatorRuntime

    def stop(self, state):
        if state.pending is not None and state.pending.operation.kind == kind:
            raise KeyboardInterrupt

    with monkeypatch.context() as patched:
        patched.setattr(OperatorRuntime, "wait", stop)
        with pytest.raises(KeyboardInterrupt):
            setting("open-liquidity", **stated)
    events = [json.loads(line) for line in capsys.readouterr().err.splitlines() if "event" in line]
    (kept,) = [event for event in events if event.get("event") == "record_kept"]
    return Path(kept["path"])


def assert_blocked(setting, record, kind):
    (listed_record,) = ax_actions.kept_records()
    assert (listed_record["path"], listed_record["unresolved"]["operation"]) == (str(record), kind)
    # The kept record blocks the wallet until its signed step is resolved.
    with pytest.raises(RuntimeError, match="already claimed"):
        setting("revoke-exchange", execute=False)


def assert_left(setting, result, stated, start):
    """Some of the open's approvals stand, exactly as the result reports them, and no
    balance moved."""
    left = setting.allowances()
    most = approved(setting, setting.offered(stated))
    assert (bool(left), left.items() <= most.items()) == (True, True)
    assert (reported_allowances(result), setting.balances()) == (left, start)
    return left


def refused_after_approvals(setting, stated, start):
    """A gas limit that admits exactly the first approval refuses a later step."""
    gas = setting("open-liquidity", execute=False, **stated)["first_step"]["gas"]
    tight = ax_actions.FeeLimits(
        LIMITS.max_gas_price_wei, LIMITS.max_transaction_fee, max_gas_limit=gas
    )
    refused = setting("open-liquidity", limits=tight, **stated)
    ending = (refused["executed"], refused["completed"], refused["refused"]["kind"])
    assert ending == (True, False, "gas_limit")
    assert (set(operations(refused)), "record_kept" in refused) == ({"approve"}, False)
    assert refused["position_after"]["position"] is None
    return assert_left(setting, refused, stated, start)


def recover_interrupted_approval(setting, stated, start, monkeypatch, capsys):
    """Interrupted while its first approval waits: recovery resolves that approval from
    its receipt and drops the rest of the open, which was never signed."""
    record = stopped_while_waiting_for("approve", setting, monkeypatch, capsys, stated)
    assert_blocked(setting, record, "approve")
    recovered = setting.recover(record)
    ending = (recovered["resolved"], recovered["discarded_request"], "unresolved" in recovered)
    assert (ending, operations(recovered)) == ((True, True, False), ["approve"])
    assert (ax_actions.kept_records(), record.exists()) == ([], False)
    assert recovered["position_after"]["position"] is None
    assert len(assert_left(setting, recovered, stated, start)) == 1


def recover_interrupted_mint(setting, stated, start, monkeypatch, capsys):
    """Restated, the open reuses the approval that stands. Interrupted while its mint
    waits, recovery reconciles the mint from its receipt: the position exists."""
    record = stopped_while_waiting_for("mint", setting, monkeypatch, capsys, stated)
    assert_blocked(setting, record, "mint")
    recovered = setting.recover(record)
    ending = (recovered["resolved"], recovered["discarded_request"], troubled(recovered))
    assert (ending, operations(recovered)) == ((True, False, []), ["approve", "mint"])
    assert (ax_actions.kept_records(), record.exists()) == ([], False)
    minted = recovered["position_after"]["position"]
    assert setting.nft_owner(int(minted["token_id"])) == setting.owner
    assert_moved(setting, recovered, start)


def test_an_open_that_is_refused_or_interrupted_reports_the_allowances_it_left(
    setting, monkeypatch, capsys
):
    first, _ = halves(setting)
    stated = {**setting.around(), **first}
    start = setting.balances()

    left = refused_after_approvals(setting, stated, start)
    revoked = setting("revoke-exchange")
    assert_clean(revoked, ["approve"] * len(left))
    assert (setting.allowances(), setting.balances()) == ({}, start)

    recover_interrupted_approval(setting, stated, start, monkeypatch, capsys)
    recover_interrupted_mint(setting, stated, start, monkeypatch, capsys)


probes = pytest.mark.parametrize(
    "cell", PROBES, indirect=True, ids=lambda cell: f"{cell[0]}-{NETWORKS[cell[1]]}"
)


def unbalanced(setting, bps):
    """A statement in a wide range around the price holding all the wallet's funded asset
    and a twentieth of the other currency: about twenty times the funded asset the range
    takes."""
    held = setting.balances()
    stated = setting.stated(held[setting.funded], held[1 - setting.funded] // 20)
    return {**setting.around(50), **stated, "slippage_bps": bps}


def price_move(setting, amount):
    """How far the trader's sale of `amount` moves the pool's price, in basis points.
    The sale is measured on the fork and undone."""
    rpc = setting.fork.rpc
    before = setting.pool_state().sqrt_price_x96
    snapshot = rpc("evm_snapshot")
    setting.trader.sell_outside(amount)()
    after = setting.pool_state().sqrt_price_x96
    assert rpc("evm_revert", [snapshot]) is True
    return abs(after**2 - before**2) * 10000 // before**2


def sized_sale(setting):
    """An amount of the funded asset whose sale by the trader moves the pool's price by
    a few percent at most, and that move in basis points. A thin pool takes less."""
    amount = setting.trader.balances()[setting.funded]
    move = price_move(setting, amount)
    while move > 300:
        amount //= 4
        move = price_move(setting, amount)
    return amount, move


def moved_beyond(before, after, bps):
    """Whether the pool's price moved by more than the tolerance, as the gateway measures it."""
    old, new = before**2, after**2
    return abs(new - old) * 10000 > old * bps


def assert_mint_reverted(setting, result, offer, start):
    """The approvals stand as reported, the mint reverted on chain, and no token left
    the wallet."""
    ending = (result["completed"], operations(result)[-1], "record_kept" in result)
    assert ending == (False, "mint", False)
    assert (result["position_after"]["position"], setting.balances()) == (None, start)
    assert setting.allowances() == approved(setting, offer) == reported_allowances(result)
    assert (troubled(result), ax_actions.kept_records()) == ([result["outcomes"][-1]], [])


def assert_surplus_was_exposed(setting, stated, offer, price):
    """Offered whole, the same statement would have been mined at the moved price: the
    other currency is still used in full, and more of the surplus is taken than the
    bounded offer holds."""
    lower, upper = sqrt_at_tick(stated["tick_lower"]), sqrt_at_tick(stated["tick_upper"])
    whole = (stated["amount0"], stated["amount1"])
    liquidity = liquidity_for(*whole, price, lower, upper)
    taken = taken_amounts(liquidity, price, lower, upper, up=True)
    funded, other = setting.funded, 1 - setting.funded
    # The other currency is used but for the rounding of the liquidity it buys.
    unused = (whole[other] - taken[other]) * 10**6
    assert (taken[funded] > offer[funded], unused <= whole[other]) == (True, True)


@probes
def test_a_mint_mined_after_the_price_left_its_tolerance_reverts_and_takes_no_surplus(setting):
    halves(setting)
    trader = setting.trader
    selling, move = sized_sale(setting)
    # The tolerance is a third of what the sale moves the price by.
    tolerance = move // 3
    stated = unbalanced(setting, tolerance)
    offer, start = setting.offered(stated), setting.balances()
    price = setting.pool_state().sqrt_price_x96
    # Another account sells the surplus currency once the mint is signed: the direction
    # in which the range takes ever more of it.
    push = trader.sell_outside(selling)
    result = setting("open-liquidity", before_send=("mint", push), **stated)
    moved = setting.pool_state().sqrt_price_x96
    assert (tolerance >= 1, moved_beyond(price, moved, tolerance)) == (True, True)
    assert_mint_reverted(setting, result, offer, start)
    assert_surplus_was_exposed(setting, stated, offer, moved)


def dust(setting):
    """A hundredth of the trader's two balances, to add to a position it does not own."""
    held = setting.trader.balances()
    return held[0] // 100, held[1] // 100


def assert_changed_outside(result, kind, success):
    """The step is recorded as mined, with the position's outside change as an
    accounting issue, and no record is kept."""
    (outcome,) = result["outcomes"]
    assert (outcome["operation"], outcome["success"], result["completed"]) == (
        kind,
        success,
        success,
    )
    (issue,) = outcome["accounting_issues"]
    assert issue.endswith("liquidity position includes unexplained external changes")
    kept = ("record_kept" in result, "unresolved" in result, ax_actions.kept_records())
    assert kept == (False, False, [])


def observed_liquidity(setting, result, token_id):
    """The liquidity the result reports for the NFT, which is what the chain holds."""
    reported = int(result["position_after"]["position"]["liquidity"])
    assert reported == int(setting.read(token_id)["position"]["liquidity"])
    return reported


@probes
def test_a_position_another_account_adds_to_while_a_step_is_in_flight_is_recorded_as_observed(
    setting,
):
    first, _ = halves(setting)
    minted, _ = open_position(setting, setting.around(), first)
    token_id, held = int(minted["token_id"]), int(minted["liquidity"])
    trader = setting.trader
    # The trader needs both currencies to add inside the range.
    trader.swap(setting.cell.funded * 4)

    # A decrease is mined after someone added to the NFT: its own effects are proven by
    # its receipt, so it is recorded, with the position as the chain holds it.
    add = trader.add_outside(token_id, dust(setting))
    decreased = setting(
        "decrease-liquidity", token_id=token_id, liquidity=held // 3, before_send=("decrease", add)
    )
    assert_changed_outside(decreased, "decrease", True)
    left = observed_liquidity(setting, decreased, token_id)
    assert left > held - held // 3

    # A close signed for the liquidity it observed reverts when more was added: it is
    # recorded as a failed step, and the NFT stays the wallet's.
    add = trader.add_outside(token_id, dust(setting))
    closed = setting("close-liquidity", token_id=token_id, before_send=("close", add))
    assert_changed_outside(closed, "close", False)
    assert observed_liquidity(setting, closed, token_id) > left
    assert setting.nft_owner(token_id) == setting.owner

    # Nothing is left locked: restated, the close adopts the position as it is.
    close_position(setting, token_id)


@pytest.mark.parametrize(
    "cell",
    [("aerodrome_slipstream", "eip155:8453")],
    indirect=True,
    ids=["aerodrome_slipstream-base"],
)
def test_a_slipstream_position_staked_in_its_gauge_is_reported_staked_and_owed_no_fees(setting):
    first, _ = halves(setting)
    lower, upper = setting.around().values()
    token_id = setting.mint_outside(lower, upper, *first.values())
    gauge = setting.stake_outside(token_id)
    setting.trader.swap(setting.cell.funded * TRADER_MULTIPLE, TRADE_LIMITS)

    seen = setting.read(token_id)
    assert (seen["owner"], seen["staked"], seen["owed"]) == (gauge, True, seen["owed_recorded"])
    # The position manager pays the gauge's own collection exactly that and no fees,
    # although the range's fee growth since the stake is positive.
    assert setting.collects(gauge, token_id) == [int(item["amount"]) for item in seen["owed"]]
    assert max(setting.unstaked_accrual(token_id)) > 0


def test_every_declared_cell_has_a_liquidity_reference_to_rehearse():
    cells = declared_cells()
    found = [reference(name, chain) for name, chain in cells]
    assert {name for name, _ in cells} == set(CONNECTORS)
    assert all(item.funded > 0 and item.asset.chain == item.chain for item in found)
