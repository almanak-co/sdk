"""Qualify public RPC endpoints against what `almanak ax` asks of one.

A (name, chain) pair belongs in `almanak.gateway.providers.PUBLIC_RPC` only after it
passed every check here in repeated full runs. The checks drive the SDK's own readers
through a sample of the reads a public observation and a direct action make, so a pass
means those calls were served, at the block they were pinned to, at the time of the
run. It is a point-in-time result and no promise for later: a transient refusal fails
a run as it would fail a command, and an endpoint can pass one run and fail the next.

This tool is live and explicit: it opens connections to the endpoints it is given, so
no offline test runs it. It reads no env file and no provider key, and it signs and
sends nothing. Whether an endpoint accepts `eth_sendRawTransaction` is therefore not
checked.

    uv run --locked python -m scripts.qualify_public_rpc
    uv run --locked python -m scripts.qualify_public_rpc --name drpc --chain eip155:8453
    uv run --locked python -m scripts.qualify_public_rpc \
        --candidate bnbchain eip155:56 https://bsc-dataseed.bnbchain.org

Without arguments every listed pair is checked, one after another with a pause. A
`--candidate` checks an endpoint that is not listed. One JSON report goes to standard
output, naming hosts only, and the exit status is 1 when any checked endpoint fails.
An endpoint whose only failures are rate limits is checked again after a pause. Each
failed check also carries the reader's own `transient` mark; it is reported, not retried.
A wrong command line exits 2 before any endpoint is opened.
"""

import argparse
import json
import time
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime
from types import SimpleNamespace
from typing import Any, cast
from urllib.parse import urlsplit

from eth_utils import keccak

from almanak.ax_actions import MAX_OBSERVATION_AGE_SECONDS, FeeLimits, gas_policy
from almanak.caip import ChainId
from almanak.chains import EvmNetwork, evm_network
from almanak.gateway.ethereum import (
    CONTEXT_PROBE,
    EXECUTION_RETRY_BUDGET,
    DataError,
    EthereumReader,
    throttled,
)
from almanak.gateway.evm import erc20_address
from almanak.gateway.op_fees import read_parameters
from almanak.gateway.production import ProductionGateway, validate_endpoint
from almanak.gateway.production_config import GasPolicy
from almanak.gateway.production_fees import quoted_fees
from almanak.gateway.providers import PUBLIC_RPC, PublicProvider
from almanak.gateway.read_endpoints import mainnet_reader, verify_public
from almanak.models import Block, Chain

SCHEMA = "almanak-public-rpc-qualification-v1"
# The burn address: funded on every chain, so a call from it estimates, and it never
# sends, so its two nonce tags agree.
ACCOUNT = "0x000000000000000000000000000000000000dead"
DECIMALS = "0x" + keccak(text="decimals()")[:4].hex()
# No transaction has this hash; an endpoint must answer it with null, not an error.
UNKNOWN_TRANSACTION = "0x" + keccak(text="almanak public endpoint qualification").hex()
# Blocks behind the head for the read pinned by number. The head's timestamp differs
# there on every listed chain, so a read the provider runs at its head is told apart.
NUMBER_DEPTH = 64
# How far back to look for a mined transaction whose receipt is read.
RECEIPT_BLOCKS = 16
RECEIPT_FIELDS = frozenset(
    {
        "transactionHash",
        "blockHash",
        "blockNumber",
        "from",
        "to",
        "gasUsed",
        "effectiveGasPrice",
        "status",
        "logs",
    }
)


@dataclass(frozen=True)
class Probe:
    network: EvmNetwork
    endpoint: str
    # Built as `public_reader` builds an observation reader.
    reads: EthereumReader
    # Built as the production adapter builds its reader for a direct action.
    execution: EthereumReader

    @property
    def wrapped(self) -> str:
        return erc20_address(self.network.wrapped_native, self.network.chain)


def require(condition: bool, message: str) -> None:
    if not condition:
        raise DataError("inconsistent", message)


def quantity(value: Any, method: str) -> int:
    """A JSON-RPC hex quantity. Any other answer is the endpoint's malformed answer."""
    try:
        if not isinstance(value, str) or not value.startswith("0x"):
            raise ValueError("not a hex quantity")
        return int(value, 16)
    except ValueError:
        raise DataError("malformed", f"{method}: invalid quantity") from None


def data(value: Any, method: str) -> str:
    """JSON-RPC hex data, as code and storage reads answer."""
    if not isinstance(value, str) or not value.startswith("0x"):
        raise DataError("malformed", f"{method}: invalid data")
    return value


# What the SDK's own decoding raises on an answer of another shape than the method's.
UNDECODABLE = (TypeError, KeyError, IndexError, ValueError)


def decoded(read: Callable[..., Any], *arguments: Any) -> Any:
    """Run an SDK read that decodes raw answers itself.

    A command would stop on the same answer with an error of its own, so the check
    fails as malformed. The error's text can repeat the answer and is not reported.
    """
    try:
        return read(*arguments)
    except DataError:
        raise
    except UNDECODABLE as error:
        raise DataError(
            "malformed", f"{read.__name__}: undecodable answer ({type(error).__name__})"
        ) from None


def identity(probe: Probe) -> dict:
    """`public_reader`: the chain number, the pinned genesis and a head within 120 seconds."""
    probe.reads.verify_chain()
    verify_public(probe.reads)
    return {}


def production_guard(probe: Probe) -> dict:
    """`ProductionGateway`: an HTTPS endpoint, the chain, a client that is no test node
    and the genesis again, as every privileged request of a direct action verifies."""
    validate_endpoint(probe.endpoint)
    adapter = cast(ProductionGateway, SimpleNamespace(reader=probe.execution))
    decoded(ProductionGateway.verify_provider, adapter)
    return {}


def context_timestamp(reader: EthereumReader, pin: dict) -> int:
    """The block timestamp the provider's EVM saw while running a read pinned by hash."""
    raw = reader._rpc("eth_call", [{"data": CONTEXT_PROBE}, pin])
    words = bytes.fromhex(raw[2:]) if isinstance(raw, str) and raw.startswith("0x") else b""
    if len(words) != 96:
        raise DataError("malformed", "eth_call: invalid block context probe result")
    return int.from_bytes(words[:32])


def pinned_reads(probe: Probe, block: Block) -> dict:
    """Call, code, storage and balance reads naming the block's hash with `requireCanonical`.

    Every observation reads this way. The context probe proves the provider ran the
    read at the pinned block instead of answering from another one.
    """
    reader = probe.reads
    pin = {"blockHash": block.hash, "requireCanonical": True}
    asset = reader.asset(probe.network.wrapped_native, block)
    code = data(reader._rpc("eth_getCode", [probe.wrapped, pin]), "eth_getCode")
    slot = data(reader._rpc("eth_getStorageAt", [probe.wrapped, "0x0", pin]), "eth_getStorageAt")
    balance = quantity(reader._rpc("eth_getBalance", [ACCOUNT, pin]), "eth_getBalance")
    require(asset.decimals == 18, "wrapped native asset does not report 18 decimals")
    require(len(code) > 2 and len(slot) == 66, "contract code or storage is not served")
    executed = context_timestamp(reader, pin)
    require(
        executed == block.timestamp,
        f"a read pinned to block {block.number} (timestamp {block.timestamp}) ran at "
        f"timestamp {executed}",
    )
    return {"block": block.number, "burn_address_balance": balance}


def reads_at_finalized(probe: Probe) -> dict:
    """The default `--block finalized` of every `ax` read."""
    return pinned_reads(probe, probe.reads.pin_block("finalized"))


def reads_at_latest(probe: Probe) -> dict:
    """`--block latest`, and the head a direct action resolves its market at."""
    return pinned_reads(probe, probe.reads.pin_block("latest"))


def reads_at_number(probe: Probe) -> dict:
    """An explicit `--block <number>`, behind the head."""
    head = probe.reads.pin_block("latest")
    return pinned_reads(probe, probe.reads.pin_block(max(0, head.number - NUMBER_DEPTH)))


def submitter_state(probe: Probe, clock: Callable[[], float] = time.time) -> dict:
    """`EvmGateway.observe`, `require_fresh` and `estimate_execution`: the head by its
    number, no older than an action accepts, and a nonce both tags agree on."""
    reader = probe.execution
    block = reader.pin_block(quantity(reader._rpc("eth_blockNumber", []), "eth_blockNumber"))
    age = clock() - block.timestamp
    require(
        -15 <= age <= MAX_OBSERVATION_AGE_SECONDS,
        f"head by number is {int(age)} seconds old",
    )
    count = "eth_getTransactionCount"
    latest, pending = (
        quantity(reader._rpc(count, [ACCOUNT, tag]), count) for tag in ("latest", "pending")
    )
    require(latest == pending, "the latest and pending nonces of an idle account differ")
    return {"block": block.number, "nonce": latest}


def action_policy(network: EvmNetwork) -> GasPolicy:
    """The fee policy of a direct action on the network; the ceilings play no part in a quote."""
    limits = FeeLimits(max_gas_price_wei=1, max_transaction_fee=1)
    return GasPolicy.model_validate_json(json.dumps(gas_policy("probe", network.chain, limits)))


def fee_quote(probe: Probe) -> dict:
    """`quoted_fees`: gas estimated at the pinned block's hash, a one-block fee history
    with one reward percentile and the next base fee, and the node's gas price where the
    chain's fee history reports no tips."""
    reader = probe.execution
    block = reader.pin_block("latest")
    call = {"from": ACCOUNT, "to": probe.wrapped, "value": "0x0", "data": DECIMALS}
    fees = decoded(quoted_fees, reader, action_policy(probe.network), block, call)
    return {
        "block": block.number,
        "gas_estimate": fees["gas_estimate"],
        "gas_price_wei": fees["gas_price"],
        "node_gas_price_wei": fees["fee_quote"].node_gas_price,
    }


def op_fees(probe: Probe) -> dict:
    """OP Stack chains: the pinned GasPriceOracle and L1Block revisions and their fee
    parameters, which bound and reconcile the L1 data and operator fees of an action."""
    if probe.network.family != "optimism":
        return {"applies": False}
    block = probe.execution.pin_block("latest")
    parameters = decoded(read_parameters, probe.execution, block)
    return {"applies": True, "block": block.number, "revision": parameters.revision}


def recent_transaction(reader: EthereumReader, head: int) -> str:
    """A transaction from the middle of the newest block that holds one.

    The middle avoids the system transactions some chains put first or last in a block.
    """
    for number in range(head, max(-1, head - RECEIPT_BLOCKS), -1):
        block = reader._rpc("eth_getBlockByNumber", [hex(number), False])
        hashes = block.get("transactions") if isinstance(block, dict) else None
        if not isinstance(hashes, list) or not all(isinstance(item, str) for item in hashes):
            raise DataError("malformed", "eth_getBlockByNumber: invalid transaction list")
        if hashes:
            return hashes[len(hashes) // 2]
    raise DataError("unavailable", f"no transaction in the last {RECEIPT_BLOCKS} blocks")


def receipts(probe: Probe) -> dict:
    """`EvmGateway.receipt`, `submitted` and `execution_cost`: a mined transaction and its
    receipt with the fields reconciliation reads, on a block that is canonical by number,
    and null for a transaction the provider does not know."""
    reader = probe.execution
    known = recent_transaction(reader, reader.pin_block("latest").number)
    receipt = reader._rpc("eth_getTransactionReceipt", [known], allow_none=True)
    require(isinstance(receipt, dict), "a mined transaction has no receipt")
    missing = sorted(RECEIPT_FIELDS - set(receipt))
    require(not missing, f"receipt lacks {', '.join(missing)}")
    number = quantity(receipt["blockNumber"], "eth_getTransactionReceipt")
    block = reader.pin_block(number)
    require(block.hash == receipt["blockHash"], "receipt block is not canonical by number")
    held = reader._rpc("eth_getTransactionByHash", [known], allow_none=True)
    require(held is not None, "a mined transaction is not found by its hash")
    # What a transaction that is not mined yet answers while an action waits for it.
    for method in ("eth_getTransactionByHash", "eth_getTransactionReceipt"):
        unknown = reader._rpc(method, [UNKNOWN_TRANSACTION], allow_none=True)
        require(unknown is None, f"{method}: an unknown transaction hash was answered")
    return {"block": block.number}


Check = Callable[[Probe], dict]
CHECKS: tuple[tuple[str, Check], ...] = (
    ("identity", identity),
    ("production_guard", production_guard),
    ("reads_at_finalized", reads_at_finalized),
    ("reads_at_latest", reads_at_latest),
    ("reads_at_number", reads_at_number),
    ("submitter_state", submitter_state),
    ("fee_quote", fee_quote),
    ("op_fees", op_fees),
    ("receipts", receipts),
)


def outcome(label: str, check: Check, probe: Probe) -> dict:
    """One check's result. Refusal text is the SDK's own, never the provider's.

    `transient` is the reader's own mark on a failure it re-sent until its bounds ran
    out: a later attempt may pass. Anything else is the endpoint's definite answer to
    this run. Neither is retried here, and both fail the check as they fail a command.
    """
    try:
        return {"check": label, "passed": True, **check(probe)}
    except DataError as error:
        return {
            "check": label,
            "passed": False,
            "error": error.kind,
            "message": str(error),
            "rate_limited": throttled(error),
            "transient": bool(error.transient),
        }
    except ValueError as error:
        return {"check": label, "passed": False, "error": "invalid", "message": str(error)}


def run_checks(probe: Probe, checks: tuple[tuple[str, Check], ...] = CHECKS) -> list[dict]:
    results = []
    for label, check in checks:
        results.append(outcome(label, check, probe))
        if label == "identity" and not results[-1]["passed"]:
            # Nothing an endpoint of unproven identity reports counts for anything.
            break
    return results


Target = tuple[str, ChainId, str]


def qualify(target: Target) -> dict:
    name, chain, endpoint = target
    network = evm_network(chain)
    mainnet = Chain(chain_id=chain, environment="mainnet")
    with (
        mainnet_reader(chain, endpoint, observation=True) as reads,
        EthereumReader(mainnet, endpoint, retry_budget=EXECUTION_RETRY_BUDGET) as execution,
    ):
        checks = run_checks(Probe(network, endpoint, reads, execution))
    return {
        "name": name,
        "chain": str(chain),
        "host": urlsplit(endpoint).hostname,
        "listed": PUBLIC_RPC.get(name, {}).get(chain) == endpoint,
        "passed": all(check["passed"] for check in checks),
        "checks": checks,
    }


def rate_limited(report: dict) -> bool:
    """Whether every failed check of the report failed on a rate limit."""
    failed = [check for check in report["checks"] if not check["passed"]]
    return bool(failed) and all(check.get("rate_limited") for check in failed)


def settled(
    target: Target,
    attempts: int,
    pause: float,
    *,
    qualify: Callable[[Target], dict] = qualify,
    wait: Callable[[float], None] = time.sleep,
) -> dict:
    """Qualify one endpoint, again after a pause while rate limits are its only failures."""
    for attempt in range(1, attempts + 1):
        report = qualify(target)
        if attempt == attempts or not rate_limited(report):
            break
        wait(pause)
    return {**report, "attempts": attempt}


def candidate(values: list[str]) -> Target:
    name, chain, endpoint = values
    try:
        PublicProvider(kind="public_rpc", name=name)
    except ValueError:
        raise ValueError(f"{name!r} is not a lowercase endpoint name") from None
    try:
        network = evm_network(ChainId(chain))
    except ValueError:
        raise ValueError(f"{chain!r} is not a CAIP-2 chain with a pinned genesis") from None
    try:
        validate_endpoint(endpoint)
    except ValueError:
        raise ValueError("the endpoint must be an HTTPS URL without credentials") from None
    return name, network.chain, endpoint


def listed(name: str | None, chain: str | None) -> list[Target]:
    return [
        (listed_name, listed_chain, endpoint)
        for listed_name, chains in PUBLIC_RPC.items()
        for listed_chain, endpoint in chains.items()
        if name in (None, listed_name) and chain in (None, str(listed_chain))
    ]


def targets(args: argparse.Namespace) -> list[Target]:
    """The endpoints to check; a selection that names nothing checkable is a ValueError."""
    filtered = args.name is not None or args.chain is not None
    if args.candidate and filtered:
        raise ValueError("--name and --chain select listed endpoints, not a --candidate")
    if args.candidate:
        return [candidate(values) for values in args.candidate]
    selected = listed(args.name, args.chain)
    if not selected:
        raise ValueError("no listed public endpoint matches --name and --chain")
    return selected


def whole(minimum: int) -> Callable[[str], int]:
    def parse(value: str) -> int:
        if not (value.isascii() and value.isdigit()) or int(value) < minimum:
            raise argparse.ArgumentTypeError(f"expected a whole number of at least {minimum}")
        return int(value)

    return parse


def seconds(value: str) -> float:
    try:
        parsed = float(value)
    except ValueError:
        parsed = -1.0
    if not 0 <= parsed <= 3600:
        raise argparse.ArgumentTypeError("expected seconds from 0 to 3600")
    return parsed


def arguments(argv: list[str] | None) -> argparse.Namespace:
    """The parsed command line with its `targets`; a wrong one ends as a usage error."""
    parser = argparse.ArgumentParser(description=(__doc__ or "").splitlines()[0])
    parser.add_argument("--name", help="check only the listed endpoints of this name")
    parser.add_argument("--chain", help="check only the listed endpoints of this CAIP-2 chain")
    parser.add_argument(
        "--candidate",
        nargs=3,
        action="append",
        metavar=("NAME", "CHAIN", "URL"),
        help="check an HTTPS endpoint that is not listed, instead of the listed ones",
    )
    parser.add_argument(
        "--attempts", type=whole(1), default=3, help="attempts while rate limited (at least 1)"
    )
    parser.add_argument("--retry-pause", type=seconds, default=30.0, help="seconds between them")
    parser.add_argument("--pause", type=seconds, default=2.0, help="seconds between endpoints")
    args = parser.parse_args(argv)
    try:
        args.targets = targets(args)
    except ValueError as error:
        parser.error(str(error))
    return args


def main(argv: list[str] | None = None, *, wait: Callable[[float], None] = time.sleep) -> int:
    args = arguments(argv)
    reports: list[dict[str, Any]] = []
    for target in args.targets:
        if reports:
            wait(args.pause)
        reports.append(settled(target, args.attempts, args.retry_pause, wait=wait))
    passed = all(report["passed"] for report in reports)
    document = {
        "schema": SCHEMA,
        "checked_at": datetime.now(UTC).isoformat(timespec="seconds"),
        "passed": passed,
        "endpoints": reports,
    }
    print(json.dumps(document, indent=2))
    return 0 if passed else 1


if __name__ == "__main__":
    raise SystemExit(main())
