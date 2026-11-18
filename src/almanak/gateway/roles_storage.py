"""Storage and bytecode verification for the pinned Safe / Roles implementation pair."""

import json
from pathlib import Path

from eth_abi import encode
from eth_utils import keccak

from almanak.gateway import robinhood_stock
from almanak.gateway.ethereum import UNREAD, DataError
from almanak.gateway.permission_models import ContractIdentity
from almanak.gateway.roles_conditions import packed
from almanak.gateway.safe_fixture import ROLES_SINGLETON, SAFE_L2_SINGLETON, SAFE_SINGLETON
from almanak.models import ZERO_ADDRESS

ARTIFACTS = Path(__file__).with_name("artifacts")
# Canonical Safe 1.4.1 singletons by runtime code hash. SafeL2 is the same contract with
# execution events; Safe deploys it on L2s.
SAFE_SINGLETONS = {
    SAFE_SINGLETON: "0x1fe2df852ba3299d6534ef416eefa406e56ced995bca886ab7a553e6d0c5e1c4",
    SAFE_L2_SINGLETON: "0xb1f926978a0f44a2c0ec8fe822418ae969bd8c3f18d61e5103100339894f81ff",
}
# Safe's canonical CompatibilityFallbackHandler 1.4.1, which standard Safe setup installs. It
# answers token receiver callbacks and EIP-1271 checks and holds no authority over the Safe.
COMPATIBILITY_FALLBACK_HANDLER = "0xfd0732dc9e303f09fcef3a7388ad10a83459ec99"
COMPATIBILITY_FALLBACK_HANDLER_HASH = (
    "0x7c6007a5d711cea8dfd5d91f5940ec29c7f200fe511eb1fc1397b367af3c42f9"
)
GUARD_SLOT = int.from_bytes(keccak(text="guard_manager.guard.address"), "big")
FALLBACK_HANDLER_SLOT = int.from_bytes(keccak(text="fallback_manager.handler.address"), "big")
SAFE_PROXY_HASH = "0xd7d408ebcd99b2b70be43e20253d6d92a8ea8fab29bd3be7f55b10032331fb4c"
IMPLEMENTATION_SLOT = int.from_bytes(keccak(text="eip1967.proxy.implementation"), "big") - 1
# These verified proxy bytecodes use the pre-EIP-1967 Zeppelin storage layout.
ZEPPELIN_PROXY_HASHES = frozenset(
    (
        "0x7dbf0ee7d6d69b563891ceb7056cf0ff0fa09d21a48e5f812d89981c2ef95944",
        "0xd80d4b7c890cb9d6a4893e6b52bc34b56b25335cb13716e0d1d31383e6b41505",
        "0x7140a935aa3bb55d334d6d325fea277e47674770b10823feefa6f8b2c58af5fc",
        "0xa6705a10bb756b5dea144591118be77d7af0c3eee3bf2dfe2583dcb0364fefab",
        "0xad30d819dbc47814b7e6cb837fd7cc57fcb591479a38596ee93de4fc52e8c435",
        "0xaad43333d28e146557f1c682e8a4226743fb231fdd00a577512233bd9e920008",
        "0xa119f674d44072e257f162aee49c7aba9b7a04fe2e8c84fcefddb366ab6d40cd",
    )
)
MATIC_PROXY_HASH = "0xe0039e200dc94a84760089ea18de72126e37d0c2aabbd11ca76d4b258f2249ff"
MATIC_IMPLEMENTATION_SLOT = int.from_bytes(keccak(text="matic.network.proxy.implementation"), "big")
USDC_IMPLEMENTATION_SLOT = int.from_bytes(keccak(text="org.zeppelinos.proxy.implementation"), "big")
BEACON_SLOT = int.from_bytes(keccak(text="eip1967.proxy.beacon"), "big") - 1
# Compiled ClonableBeaconProxy and UpgradeableBeacon revisions, not token allowlists.
BEACON_PROXY_HASH = "0x9bb54fba14f3f66acb4acfdcb38af3737d65543274ad5e9bc794080b876ddf18"
BEACON_CODE_HASH = "0x335bc199b68d92971edf04b2d907a18617249dc49f86d7699c4eec68cb882d6c"


def mapping_slot(type_: str, key, slot: int) -> int:
    return int.from_bytes(keccak(encode((type_, "uint256"), (key, slot))), "big")


def slot_address(word: int) -> str:
    if word >= 2**160:
        raise DataError("authority", "contract address storage has unexpected high bits")
    return "0x" + word.to_bytes(20, "big").hex()


# Independent reads go in JSON-RPC batches. Providers such as Alchemy meter each call
# inside a batch, and a gateway RPC cap below the batch size shrinks batches so each one
# can still be admitted.
BATCH_SIZE = 100


def batch_size(reader) -> int:
    maximum = reader.reader.provider_limit.maximum
    return BATCH_SIZE if maximum is None else max(1, min(BATCH_SIZE, maximum))


# Reads at the observation block that repeat from one verification to the next.
REPLAYED = frozenset(("eth_getCode", "eth_getStorageAt", "eth_call"))


class ReadPlan:
    """The block-pinned reads of one account's last authority verification.

    The next verification sends them in batches at its own observation block before it
    starts, then answers each read it makes from those results. A read it no longer makes
    is unused, and one it newly needs goes to the provider, so the outcome is that of
    sequential reads at the same block.
    """

    def __init__(self):
        self.reads: tuple[tuple[str, tuple], ...] = ()


def read_key(method, params) -> str:
    return json.dumps([method, params], sort_keys=True)


class AuthorityReader:
    def __init__(self, reader, block, plan: ReadPlan | None = None):
        if reader._pinned != block:
            raise ValueError("authority reads require this reader's pinned observation")
        self.reader, self.block = reader, block
        self.tag = {"blockHash": block.hash, "requireCanonical": True}
        self.rpc_count = 0
        self.plan = plan
        self.prefetched: dict[str, object] = {}
        self.replayable: dict[str, tuple[str, tuple]] = {}
        if plan is not None and plan.reads:
            self.prefetch(plan.reads)

    def prefetch(self, reads):
        calls = [(method, [*params, self.tag]) for method, params in reads]
        size = batch_size(self)
        chunks = (calls[offset : offset + size] for offset in range(0, len(calls), size))
        results = [r for chunk in self.batches(chunks, 1, tolerant=True) for r in chunk]
        for (method, params), result in zip(calls, results, strict=True):
            if result is not UNREAD:
                self.prefetched[read_key(method, params)] = result

    def retain_plan(self):
        """Keep this verification's reads for the next one; called after it succeeds."""
        if self.plan is not None:
            self.plan.reads = tuple(self.replayable.values())

    def rpc(self, method, params):
        key = read_key(method, params)
        if self.plan is not None and method in REPLAYED and params[-1] == self.tag:
            self.replayable.setdefault(key, (method, tuple(params[:-1])))
        if key in self.prefetched:
            return self.prefetched[key]
        self.rpc_count += 1
        return self.reader._rpc(method, params)

    def batches(self, batches, in_flight, *, tolerant=False):
        def counted():
            for calls in batches:
                self.rpc_count += len(calls)
                yield calls

        return self.reader._rpc_batches(counted(), in_flight, tolerant=tolerant)

    def code(self, address, tag=None) -> bytes:
        return bytes.fromhex(self.rpc("eth_getCode", [address, tag or self.tag])[2:])

    def storage(self, address, slot) -> int:
        return int(self.rpc("eth_getStorageAt", [address, hex(slot), self.tag]), 16)

    def call(self, address, signature, *, inputs=(), args=(), outputs=("address",)):
        if self.plan is not None:
            return self.reader.call(
                address,
                signature,
                self.block,
                inputs=inputs,
                args=args,
                outputs=outputs,
                rpc=self.rpc,
            )
        self.rpc_count += 1
        return self.reader.call(
            address, signature, self.block, inputs=inputs, args=args, outputs=outputs
        )

    def identity(self, address) -> ContractIdentity:
        code = self.code(address)
        if not code:
            raise DataError("authority", "permission target has no deployed code")
        code_hash = "0x" + keccak(code).hex()
        if code_hash == robinhood_stock.PROXY_HASH:
            return robinhood_stock.identity(self, address, code_hash)
        if code_hash == BEACON_PROXY_HASH:
            return self.beacon_identity(address, code_hash)
        slot = (
            USDC_IMPLEMENTATION_SLOT if code_hash in ZEPPELIN_PROXY_HASHES else IMPLEMENTATION_SLOT
        )
        if code_hash == MATIC_PROXY_HASH:
            slot = MATIC_IMPLEMENTATION_SLOT
        implementation = slot_address(self.storage(address, slot))
        implementation_hash = None
        if implementation != ZERO_ADDRESS:
            implementation_hash = "0x" + keccak(self.code(implementation)).hex()
        return ContractIdentity(
            address=address,
            code_hash=code_hash,
            implementation=implementation if implementation_hash else None,
            implementation_hash=implementation_hash,
            implementation_slot=slot if implementation_hash else None,
        )

    def beacon_identity(self, address, code_hash) -> ContractIdentity:
        beacon = slot_address(self.storage(address, BEACON_SLOT))
        beacon_hash = "0x" + keccak(self.code(beacon)).hex()
        if beacon_hash != BEACON_CODE_HASH:
            raise DataError("authority", "token beacon differs from its verified revision")
        (implementation,) = self.call(beacon, "implementation()")
        implementation_code = self.code(implementation)
        if implementation == ZERO_ADDRESS or not implementation_code:
            raise DataError("authority", "token beacon implementation has no deployed code")
        return ContractIdentity(
            address=address,
            code_hash=code_hash,
            implementation=implementation,
            implementation_hash="0x" + keccak(implementation_code).hex(),
            beacon=beacon,
            beacon_hash=beacon_hash,
        )

    def modules(self, address):
        modules, next_ = self.call(
            address,
            "getModulesPaginated(address,uint256)",
            inputs=("address", "uint256"),
            args=("0x" + "0" * 39 + "1", 100),
            outputs=("address[]", "address"),
        )
        if int(next_, 16) != 1:
            raise DataError("authority", "dedicated profile has too many modules")
        return tuple(modules)


def verify_implementations(reader: AuthorityReader, profile):
    safe = profile.fund.address
    if "0x" + keccak(reader.code(safe)).hex() != SAFE_PROXY_HASH:
        raise DataError("authority", "Safe proxy differs from the supported artifact")
    (singleton,) = reader.call(safe, "masterCopy()")
    (version,) = reader.call(safe, "VERSION()", outputs=("string",))
    if singleton not in SAFE_SINGLETONS or version != "1.4.1":
        raise DataError("authority", "Safe implementation is outside the supported profile")
    if "0x" + keccak(reader.code(singleton)).hex() != SAFE_SINGLETONS[singleton]:
        raise DataError("authority", "Safe singleton code differs from its verified artifact")
    roles_code = reader.code(ROLES_SINGLETON)
    artifact = json.loads((ARTIFACTS / "roles-2.1.0-bytecode.json").read_text())
    if roles_code.hex() != artifact["bytecode"].removeprefix("0x"):
        raise DataError("authority", "Roles singleton code differs from the 2.1.0 artifact")
    expected = "363d3d373d3d3d363d73" + ROLES_SINGLETON[2:] + "5af43d82803e903d91602b57fd5bf3"
    if reader.code(profile.roles_module).hex() != expected:
        raise DataError("authority", "Roles proxy does not use the pinned singleton")


def verify_wiring(reader: AuthorityReader, profile, *, operator_enabled: bool):
    verify_implementations(reader, profile)
    safe, roles, operator = profile.fund.address, profile.roles_module, profile.operator.address
    if reader.modules(safe) != (roles,):
        raise DataError("authority", "Safe must enable only its dedicated Roles module")
    expected_modules = (operator,) if operator_enabled else ()
    if reader.modules(roles) != expected_modules:
        raise DataError("authority", "Roles has missing or alternative operator authority")
    for field in ("owner", "avatar", "target"):
        if reader.call(roles, field + "()")[0] != safe:
            raise DataError("authority", "Roles owner/avatar/target must all be the fund Safe")
    (owners,) = reader.call(safe, "getOwners()", outputs=("address[]",))
    if operator in owners:
        raise DataError("authority", "operator must not have Safe owner authority")
    if reader.storage(safe, GUARD_SLOT):
        raise DataError("authority", "dedicated Safe profile requires no guard")
    verify_fallback_handler(reader, safe)


def verify_fallback_handler(reader: AuthorityReader, safe):
    """No handler, or Safe's canonical compatibility handler with its verified code."""
    handler = reader.storage(safe, FALLBACK_HANDLER_SLOT)
    if not handler:
        return
    code_hash = "0x" + keccak(reader.code(COMPATIBILITY_FALLBACK_HANDLER)).hex()
    if (
        slot_address(handler) != COMPATIBILITY_FALLBACK_HANDLER
        or code_hash != COMPATIBILITY_FALLBACK_HANDLER_HASH
    ):
        raise DataError("authority", "Safe fallback handler is not the canonical 1.4.1 handler")


def verify_grant_storage(reader: AuthorityReader, profile, grant):
    role_slot = mapping_slot("bytes32", bytes.fromhex(profile.role_key[2:]), 4)
    target_slot = mapping_slot("address", grant.target, role_slot + 1)
    if reader.storage(profile.roles_module, target_slot) != 2:
        raise DataError("authority", "target is not restricted to scoped functions")
    scope_key = bytes.fromhex(grant.target[2:]) + grant.selector + bytes(8)
    header = reader.storage(profile.roles_module, mapping_slot("bytes32", scope_key, role_slot + 2))
    if grant.condition is None:
        # Roles BufferPacker's selector-only header, with no Send/DelegateCall
        # option, pointer or condition count. Only zero-argument grants use it.
        if header != 1 << 216:
            raise DataError("authority", "zero-argument function has altered execution authority")
        return
    pointer = "0x" + (header & (2**160 - 1)).to_bytes(20, "big").hex()
    if header >> 160 != len(grant.conditions) << 80:
        raise DataError("authority", "function scope has extra options or wildcard authority")
    if reader.code(pointer) != b"\x00" + packed(grant.conditions):
        raise DataError("authority", "stored predicate differs from its generated condition tree")
