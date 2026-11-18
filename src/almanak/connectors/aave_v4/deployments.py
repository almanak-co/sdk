"""Verified Aave implementation semantics, independent of configured market addresses."""

from eth_utils import keccak

from almanak.gateway.ethereum import DataError
from almanak.models import Address, Block, Hash32, Model

IMPLEMENTATION_SLOT = hex(int.from_bytes(keccak(text="eip1967.proxy.implementation")) - 1)
ADMIN_SLOT = int.from_bytes(keccak(text="eip1967.proxy.admin")) - 1
PROXY_TEMPLATE = "0x4da959606431815039381d2e362ef1bf2b5014c1f5ba28516e6c432237900420"


class ContractRevision(Model):
    address: Address
    code_hash: Hash32
    implementation_slot: Hash32


# Verified against the pinned reference strategy contract-action probes.
SUPPORTED_REVISIONS = (
    ContractRevision(
        address="0x94e7a5dcbe816e498b89ab752661904e2f56c485",
        code_hash="0xd5a0f33aeed2fcb35727b0f4978a7c54c970a99cca1057f98f3d673ad117b542",
        implementation_slot="0x000000000000000000000000abd0e26fe17bde4f1f1187ed8aa80c274e03d8b5",
    ),
    ContractRevision(
        address="0xcca852bc40e560adc3b1cc58ca5b55638ce826c9",
        code_hash="0x6fe3bd0c490eec4935b35b737f57bd344dfcaf04cc8fe2a1685339cd595d1d63",
        implementation_slot="0x000000000000000000000000fe89fd96f270ac3c0f11921af0390dbb1340f704",
    ),
    ContractRevision(
        address="0x973a023a77420ba610f06b3858ad991df6d85a08",
        code_hash="0x7ec944d28e7b1386e87b9f9928e2cb861edc9c6a86f3b2aea6f01ecb94b21969",
        implementation_slot="0x0000000000000000000000007a603b734ea48cd02ef38671b6cd3e06573f7450",
    ),
    ContractRevision(
        address="0x943827dca022d0f354a8a8c332da1e5eb9f9f931",
        code_hash="0xeb3eadb4956e4fd465eb746b3f91cc853a894a2590ea85acac3655cc13e8232b",
        implementation_slot="0x000000000000000000000000e1e61cc36ebdbe30d51d04f38d7930663dcfe940",
    ),
)


def revision(reader, target: str, block: Block) -> ContractRevision:
    selector = {"blockHash": block.hash, "requireCanonical": True}
    raw = reader._rpc("eth_getCode", [target, selector])
    if not isinstance(raw, str) or raw == "0x":
        raise DataError("undeployed", "requested contract has no historical bytecode")
    code_hash = "0x" + keccak(bytes.fromhex(raw.removeprefix("0x"))).hex()
    slot = reader._rpc("eth_getStorageAt", [target, IMPLEMENTATION_SLOT, selector])
    return ContractRevision(address=target, code_hash=code_hash, implementation_slot=slot)


# Spoke revisions include their compiled network-specific oracle/wrapper immutables.
# The Hub implementation is identical on both supported networks.
IMPLEMENTATIONS = {
    "spoke": frozenset(
        (
            "0x41b906d98443c68139467f7bfd8194e361c8d488c55a3cded7ecbf30c9b42eb6",
            "0x69f5f22ff1114b0b6e16fb1a3991171ff9c7154dfd95e156f7a29cefc33202c9",
            "0xdc24a4527e1257b19f12e6fb870dee142970384deb1cca4341b1dea2b1bc1caf",
        )
    ),
    "hub": frozenset(("0xfbcf2a40498e9d4b98580d87cfb9cdb8fbd57db6c381fd53ab140e508979a7a4",)),
}


def verify_deployments(reader, source, block):
    from almanak.gateway.roles_storage import AuthorityReader

    authority = AuthorityReader(reader, block)
    for role, target in (("spoke", source.spoke), ("hub", source.hub)):
        verify_contract(authority, role, target)
    reader.pin_block(block.number)


def verify_contract(authority, role, target):
    verify_proxy(authority, target)
    identity = authority.identity(target)
    if identity.implementation_hash not in IMPLEMENTATIONS[role]:
        raise DataError(
            "deployment_changed", "Aave implementation is outside the verified revisions"
        )
    return identity


def verify_proxy(authority, target):
    code = authority.code(target)
    admin = authority.storage(target, ADMIN_SLOT)
    if not 0 < admin < 2**160:
        raise DataError("deployment_changed", "Aave proxy has invalid administrator storage")
    word = admin.to_bytes(32, "big")
    # The verified OZ proxy has one PUSH32 immutable admin operand. Normalizing
    # that operand admits other deployments without accepting different proxy code.
    if (
        code.count(word) != 1
        or "0x" + keccak(code.replace(word, bytes(32))).hex() != PROXY_TEMPLATE
    ):
        raise DataError("deployment_changed", "Aave proxy differs from the verified implementation")


def validate_markets(config):
    # Relationships and reserve metadata are verified against pinned chain reads.
    for allocation in config.positions:
        source = allocation.reserve
        if source.spoke == source.hub:
            raise ValueError("Aave requires distinct Spoke and Hub contracts")


def historical_revision_supported(observed):
    return any(
        (observed.code_hash, observed.implementation_slot)
        == (verified.code_hash, verified.implementation_slot)
        for verified in SUPPORTED_REVISIONS
    )
