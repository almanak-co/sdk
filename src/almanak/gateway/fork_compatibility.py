"""Refuse a fork lifecycle over contracts a local Anvil fork cannot execute.

Some chains implement contracts in the node itself rather than in EVM bytecode.
Base's B20 stock tokens answer `eth_getCode` with the single byte 0xef, a prefix
EIP-3541 reserves so that no deployed EVM contract can begin with it. Anvil copies
that code into the fork and fails every call to the contract with "EVM error
OpcodeNotFound", while mainnet reads, and so resolution, succeed. The refusal names
every such contract before funding or any decision instead of a decision read failing
later.
"""

from collections.abc import Callable, Iterable, Sequence
from contextlib import ExitStack
from pathlib import Path
from typing import Any, NamedTuple

from almanak.caip import AssetType, ChainId
from almanak.gateway.ethereum import DataError

UNSUPPORTED_ON_FORK = "unsupported_on_fork"
# An EIP-7702 delegation designator, 0xef0100 and the 20-byte delegate, is executable:
# Anvil runs the delegate's code. Any other code starting 0xef is not EVM bytecode.
DELEGATION_PREFIX = "ef0100"
DELEGATION_HEX_LENGTH = 2 * 23
SHOWN_CODE_BYTES = 4

# Reads every contract of one chain and answers their codes in the same order.
CodeReader = Callable[[ChainId, Sequence[str]], Sequence[Any]]


class Offender(NamedTuple):
    chain: ChainId
    contract: str
    role: str
    code: str

    def describe(self) -> str:
        shown = self.code[: 2 + 2 * SHOWN_CODE_BYTES]
        if len(self.code) > len(shown):
            shown += "..."
        return f"{self.role} {self.contract} on {self.chain} (code {shown})"


class UnsupportedOnFork(DataError):
    """Contracts the strategy uses have code a local fork cannot execute."""

    check_code = UNSUPPORTED_ON_FORK
    remedy = (
        "Local fork simulation cannot execute these contracts. Test the strategy with "
        "markets whose contracts are EVM bytecode, or export and deploy it without a "
        "fork lifecycle."
    )

    def __init__(self, offenders: Sequence[Offender]):
        self.offenders = tuple(offenders)
        super().__init__(
            UNSUPPORTED_ON_FORK,
            "local fork simulation cannot execute "
            + "; ".join(o.describe() for o in self.offenders)
            + ": no EVM bytecode, only the EIP-3541 reserved prefix 0xef of a contract "
            "the chain's node implements natively. This is an infrastructure limitation "
            "of local fork simulation, not a strategy fault; the strategy can still be "
            "exported and deployed.",
        )


def executable_on_fork(code: Any) -> bool:
    """Whether a fork can run this account code; empty code (an EOA) is not refused here."""
    if not isinstance(code, str) or not code.startswith("0x"):
        raise DataError("malformed", "eth_getCode: invalid code response")
    body = code[2:].lower()
    if len(body) % 2 or body.strip("0123456789abcdef"):
        raise DataError("malformed", "eth_getCode: invalid code response")
    if not body.startswith("ef"):
        return True
    return len(body) == DELEGATION_HEX_LENGTH and body.startswith(DELEGATION_PREFIX)


def fork_contracts(markets: Iterable[Any], funds: Iterable[Any] = ()) -> dict[ChainId, dict]:
    """Every contract the lifecycle calls, by chain, with the role a refusal names.

    The resolved markets supply their ERC-20 assets and verified contract identities,
    which hold the router, quoter and position manager where a connector verifies them;
    the fixture funds supply the tokens funding writes. Native assets have no code.
    """
    found: dict[ChainId, dict[str, str]] = {}

    def add(chain: ChainId, contract: str, role: str) -> None:
        found.setdefault(chain, {}).setdefault(contract.lower(), role)

    def token(asset: AssetType) -> None:
        if asset.namespace == "erc20":
            add(asset.chain, asset.reference, "token")

    for market in markets:
        resolved = market.resolved
        for asset in resolved.assets:
            token(asset.identifier)
        for identity in getattr(resolved, "identities", ()):
            add(resolved.block.chain.chain_id, identity.address, f"{market.name} contract")
    for fund in funds:
        for funding in fund.assets:
            token(funding.balance.asset)
    return found


def require_fork_executable(read_codes: CodeReader, contracts: dict) -> None:
    """Read every contract, then refuse naming each one the fork cannot execute."""
    offenders = []
    for chain, entries in contracts.items():
        codes = read_codes(chain, tuple(entries))
        if len(codes) != len(entries):
            raise DataError("malformed", "eth_getCode: invalid response count")
        for (contract, role), code in zip(entries.items(), codes, strict=True):
            if not executable_on_fork(code):
                offenders.append(Offender(chain, contract, role, code))
    if offenders:
        raise UnsupportedOnFork(offenders)


def check_provider_code(plan: Any, markets: Iterable[Any], env_file: Path) -> None:
    """The online check: one batch per chain through the operator's provider at the fork block."""
    from almanak.gateway.fork_config import provider_endpoint
    from almanak.gateway.read_endpoints import mainnet_reader

    targets = {target.chain.chain_id: target for target in plan.forks}
    with ExitStack() as stack:

        def read_codes(chain: ChainId, contracts: Sequence[str]) -> list[Any]:
            target = targets[chain]
            endpoint = provider_endpoint(target.provider, env_file)
            reader = stack.enter_context(mainnet_reader(chain, endpoint))
            reader.verify_chain()
            selector: Any = (
                target.block
                if target.block == "latest"
                else {"blockHash": target.block.hash, "requireCanonical": True}
            )
            calls = [("eth_getCode", [contract, selector]) for contract in contracts]
            return reader.batch(calls)

        require_fork_executable(read_codes, fork_contracts(markets, plan.funds))


def check_node_code(plan: Any, markets: Iterable[Any], nodes: Any) -> None:
    """Fork setup: read code from the started local fork nodes themselves."""
    by_chain = {chain.chain_id: node for chain, node in nodes.nodes.items()}

    def read_codes(chain: ChainId, contracts: Sequence[str]) -> list[Any]:
        node = by_chain[chain]
        return [node.rpc("eth_getCode", [contract, "latest"]) for contract in contracts]

    require_fork_executable(read_codes, fork_contracts(markets, plan.funds))
