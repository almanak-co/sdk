"""The direct bridge commands of `almanak ax`, and the read-only status of one transfer.

`ax bridge` and `ax revoke-bridge` run through `ax_actions` as every direct action does.
What differs is here: a route names two chains, so the deployment is resolved on the
chain it sends from and completed on the destination chain with a second reader. After
that the deployment has one chain. Nothing is read or signed on the destination chain
while the action runs.

The second reader comes from the same env file, or, without a provider key, from a
second named public endpoint: `--public-rpc` names the endpoint of the chain the asset
leaves and `--destination-public-rpc` the endpoint of the chain it arrives on. Neither
is ever chosen for the caller, and each gives the public proof before anything is
resolved. The venue's quote is asked from the venue's own API either way.

`ax bridge-status` reads what became of one transfer, named by the reference `ax bridge`
reported. It keeps no state: the deposit is read again from the source SpokePool and
held to the reference's relay hash, and everything else follows from that deposit. It
plans and signs nothing.
"""

from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path

from almanak.gateway.ethereum import DataError

# What a provider answers when it will not or cannot serve a read: a refusal of the
# request, a throttle, no answer, or data it does not hold.
PROVIDER_REFUSALS = frozenset({"rpc", "http", "transport", "unavailable"})

# Pages of the source chain a refund scan reads at most in one command. A refund is paid
# in a later bundle, hours after the fill deadline; the scan says where it stopped, and
# `--refund-from-block` continues it.
MAXIMUM_REFUND_PAGES = 200
DESTINATION_REQUIRED = (
    "--public-rpc names the endpoint of the chain the asset leaves, and this command also"
    " reads the chain it arrives on: name that chain's public endpoint with"
    " --destination-public-rpc"
)
DESTINATION_ALONE = (
    "--destination-public-rpc names the second endpoint of --public-rpc; with another"
    " transport that transport serves both chains"
)
UNATTRIBUTED = (
    "A refund payment names the wallet and the token, never the deposit it pays back, and"
    " one payment can cover several deposits. A payment listed here was named by an"
    " executed refund leaf; none is attributed to this transfer. One equal to the sent"
    " amount is consistent with its refund and may be another deposit's."
)


def arguments(name: str, action) -> None:
    """The route, and for a bridge the amount, the fee ceiling and the recipient."""
    from almanak.ax_actions import asset, base_units

    action.add_argument("--chain", required=True, help="strict CAIP-2 chain the asset leaves")
    action.add_argument("--asset", type=asset, required=True, help="CAIP-19 asset sent")
    action.add_argument(
        "--destination-chain", required=True, help="strict CAIP-2 chain the asset arrives on"
    )
    action.add_argument(
        "--destination-asset", type=asset, required=True, help="CAIP-19 asset received"
    )
    if name != "bridge":
        # A revocation reads the chain the asset leaves only.
        action.set_defaults(amount=None, max_fee_bps=0, recipient=None, destination_public_rpc=None)
        return
    destination_public_rpc_option(action)
    action.add_argument(
        "--amount", type=base_units, required=True, help="base units of the asset sent"
    )
    action.add_argument(
        "--max-fee-bps",
        type=fee_bps,
        required=True,
        help="refuse a venue quote whose whole fee exceeds this many basis points of the"
        " amount sent, 0 to 10000; a gateway check, the SpokePool checks no price",
    )
    action.add_argument(
        "--recipient",
        type=evm_address,
        required=True,
        help="address on the destination chain that receives; state the wallet's own"
        " address to keep the funds",
    )


def fee_bps(value: str) -> int:
    import argparse

    if not (value.isascii() and value.isdigit()) or int(value) > 10_000:
        raise argparse.ArgumentTypeError("expected 0 to 10000 basis points")
    return int(value)


def evm_address(value: str) -> str:
    """A nonzero EVM address, returned as stated: its case says whether it carried a
    checksum, which the command reports. Every later use validates and lowers it."""
    import argparse

    from almanak.models import ZERO_ADDRESS, address

    try:
        parsed = address(value)
    except ValueError as error:
        raise argparse.ArgumentTypeError(f"expected an EVM address: {error}") from None
    if parsed == ZERO_ADDRESS:
        raise argparse.ArgumentTypeError("expected a nonzero EVM address")
    return value


def transaction_hash(value: str) -> str:
    import argparse

    body = value[2:].lower()
    if not value.startswith("0x") or len(body) != 64 or set(body) - set("0123456789abcdef"):
        raise argparse.ArgumentTypeError("expected a transaction hash: 0x and 64 hex digits")
    return "0x" + body


def destination_public_rpc_option(command) -> None:
    from almanak.gateway.providers import PUBLIC_RPC

    command.add_argument(
        "--destination-public-rpc",
        choices=sorted(PUBLIC_RPC),
        help="with --public-rpc: the public endpoint of the chain the asset arrives on,"
        " from the same list; required then, and never chosen for the caller",
    )


def public_refusal(args, reads_destination: bool) -> str | None:
    """Why the public endpoints a command names cannot serve its route, or None.

    Each chain's endpoint is named by the caller and must be a listed pair; nothing is
    chosen or substituted for a missing or unlisted one.
    """
    from almanak.caip import ChainId
    from almanak.gateway.providers import public_endpoint

    source, destination = args.public_rpc, args.destination_public_rpc
    if source is None:
        return None if destination is None else DESTINATION_ALONE
    if reads_destination and destination is None:
        return DESTINATION_REQUIRED
    named = [(source, args.chain)]
    if reads_destination:
        named.append((destination, args.destination_chain))
    try:
        for name, chain in named:
            public_endpoint(name, ChainId(chain))
    except ValueError as error:
        return str(error)
    return None


def validate(parser, args) -> None:
    """Refuse a public transport that does not name a listed endpoint for each chain
    the command reads, before any endpoint is opened. Only a revocation reads the
    source chain alone."""
    refusal = public_refusal(args, args.ax_command != "revoke-bridge")
    if refusal is not None:
        parser.error(refusal)


def market_request(args):
    """The route a command names, as the connector's own market selection.

    A revocation sends nothing, so it states no fee ceiling: its route carries the
    strictest one, under which no deposit with a fee could be admitted.
    """
    import json

    from almanak.ax_actions import resolution

    values = {
        "connector": args.connector,
        "chain": args.chain,
        "asset": args.asset,
        "destination_chain": args.destination_chain,
        "destination_asset": args.destination_asset,
        "max_fee_bps": args.max_fee_bps,
        "quote": "across_api",
    }
    # An operator's selection: it may name the native asset, which an author's may not.
    return resolution(args.connector).DirectSelection.model_validate_json(json.dumps(values))


def prove_genesis(reader) -> None:
    """Hold a destination reader to its chain's pinned genesis block, as the production
    gateway holds the source chain's."""
    from almanak.chains import evm_network

    if reader.pin_block(0).hash != evm_network(reader.chain.chain_id).genesis:
        raise DataError("chain_mismatch", "the destination provider has a different genesis")


# EIP-7702: a plain account that delegates answers this prefix and its delegate's address.
DELEGATION_PREFIX = "0xef0100"


def contract_code(reader, address: str, block) -> bool:
    """Whether the address holds contract code at the block. A delegating plain account
    is no contract: its key signs on every chain."""
    code = reader._rpc("eth_getCode", [address, {"blockHash": block.hash}])
    if not isinstance(code, str) or not code.startswith("0x"):
        raise DataError("malformed", "eth_getCode: invalid code response")
    delegating = code.lower().startswith(DELEGATION_PREFIX) and len(code) == 48
    return len(code) > 2 and not delegating


def require_receivable(recipient: str, on_source: bool, on_destination: bool, chains) -> None:
    """Refuse a recipient that is a contract where the asset leaves and nothing where
    it arrives.

    A contract account of one chain, such as a Safe, is not the same account at the
    same address on another: until it is deployed there nobody may control it. This is
    a check of the command before anything is planned, not on-chain enforcement.
    """
    if on_source and not on_destination:
        raise DataError(
            "recipient_not_deployed",
            f"the recipient {recipient} is a contract on {chains[0]} and holds no code on"
            f" {chains[1]}: a contract account does not exist at the same address on"
            " another chain until it is deployed there, and what it receives before may be"
            " unrecoverable; nothing was planned or signed",
        )


def require_plain(recipient: str, holds_code: bool, chain) -> None:
    """Refuse a recipient of a native route that holds contract code where it is paid.

    The destination SpokePool pays the native asset to an address with no code, or with
    a delegation (EIP-7702), and its wrapped token to any other. A native route records
    a native arrival only, so a contract recipient is refused, with no override. The
    code is read once, before anything is planned; the pool decides again when the fill
    lands. A check of the command, not on-chain enforcement.
    """
    if holds_code:
        raise DataError(
            "recipient_is_contract",
            f"the recipient {recipient} holds contract code on {chain}: the SpokePool would"
            " pay it the wrapped token, not the native asset, which a native bridge does"
            " not record; nothing was planned or signed",
        )


def require_approval_route(request, action) -> None:
    """A native route gives the SpokePool no approval, so it has none to revoke."""
    if action.action != "bridge" and request.native:
        raise ValueError(
            "a native bridge gives the SpokePool no approval, so there is none to revoke;"
            " an approval of the wrapped token is not this route's"
        )


@dataclass(frozen=True)
class Far:
    """The transport of the chain the asset arrives on: the env file of the action, or
    the public endpoint the caller named for that chain."""

    env_file: Path | None = None
    public_rpc: str | None = None


def destination_reader(chain, far: Far):
    """The destination chain's reader, proven before the route is completed on it.

    A provider key's endpoint shows its chain number and the pinned genesis block. A
    named public endpoint shows both and a recent head, the proof every public reader
    gives: nobody answers for it, so it is held to no less than the source's.
    """
    from almanak.gateway import read_endpoints

    if far.public_rpc is not None:
        with proof_cause(far.public_rpc, chain):
            return read_endpoints.public_reader(chain, far.public_rpc)
    if far.env_file is None:
        raise ValueError(DESTINATION_REQUIRED)
    reader = read_endpoints.verified_reader(chain, far.env_file)
    try:
        prove_genesis(reader)
    except Exception:
        reader.__exit__()
        raise
    return reader


def completed(connector: str, far: Far, resolved, recipient: str, on_source: bool):
    """The route completed on its destination chain, at that chain's own anchor, and
    held to the reviewed SpokePool pins on both chains. The recipient is checked there
    too, under `require_receivable`."""
    from almanak.ax_actions import resolution

    resolver = resolution(connector)
    selection = resolved.selection
    with destination_reader(selection.destination_chain, far) as reader:
        block = reader.pin_block("latest")
        route = resolver.complete_market(reader, resolved, block)
        on_destination = contract_code(reader, recipient, block)
    resolver.admit_route(route)
    chains = (selection.chain, selection.destination_chain)
    require_receivable(recipient, on_source, on_destination, chains)
    if selection.native:
        require_plain(recipient, on_destination, selection.destination_chain)
    return route


@dataclass(frozen=True)
class SourceRoute:
    """A revocation's route, stated from the source chain alone."""

    route: object


def revocation_route(resolved) -> SourceRoute:
    """The route of a revocation: the sent token and the SpokePool at the reviewed proxy
    address, as read on the source chain, on any implementation that still serves the
    pool views every Across observation reads.

    A revocation sends nothing, so it reads nothing on the destination chain and holds
    neither pool to its pin: a pin difference or a destination outage must never keep an
    approval standing. The destination pool is the pin as written in code and the
    received asset is as stated, both unread; the revocation uses neither. The spender
    cleared is the proxy the pins name today. An allowance to a proxy the pins no longer
    name is not this command's to clear.
    """
    from almanak.connectors.across.domain import Route, spoke_pool

    selection = resolved.selection
    return SourceRoute(
        Route(
            chain=resolved.block.chain,
            asset=selection.asset,
            destination_chain=selection.destination_chain,
            destination_asset=selection.destination_asset,
            decimals=resolved.asset.decimals,
            spoke_pool=resolved.pool.address,
            destination_pool=spoke_pool(selection.destination_chain),
            max_fee_bps=selection.max_fee_bps,
            quote=selection.quote,
        )
    )


def deployment(far: Far, request, action, wallet, provider, endpoint, limits):
    """The operator deployment of one wallet along one route, on the chain it leaves.

    A named public endpoint gives the public proof before the route is resolved through
    it, on the source chain here and on the destination chain in `destination_reader`.
    """
    from almanak.ax_actions import configuration, prove_public, resolution
    from almanak.connectors.direct import Wallet, binding
    from almanak.gateway.ethereum import EthereumReader
    from almanak.gateway.providers import PublicProvider
    from almanak.models import Chain

    connector = request.connector
    require_approval_route(request, action)
    chain = Chain(chain_id=request.chain, environment="mainnet")
    account = Wallet(account=wallet["account"], address=wallet["address"].lower())
    stated = None if action.recipient is None else action.recipient.lower()
    with EthereumReader(chain, endpoint) as reader:
        if isinstance(provider, PublicProvider):
            with proof_cause(provider.name, chain.chain_id):
                prove_public(reader)
        block = reader.pin_block("latest")
        resolved = resolution(connector).resolve_market(reader, request, block)
        on_source = stated is not None and contract_code(reader, stated, block)
    if action.action == "bridge" and stated is not None:
        route = completed(connector, far, resolved, stated, on_source)
    else:
        route = revocation_route(resolved)
    positions = binding(connector).positions(None, block, route, account, action)
    return configuration(connector, positions, account, provider, chain, limits)


@dataclass(frozen=True)
class Deposit:
    """One mined deposit, decoded from its own event on the source SpokePool."""

    transaction: str
    block: int
    depositor: str
    recipient: str
    input_token: str
    output_token: str
    sent: int
    offered: int
    quoted_at: int
    fill_deadline: int


def account(word: bytes) -> str:
    return "0x" + word[12:].hex()


def log_span(public_rpc: str | None) -> int:
    """Blocks one page of a status scan spans: a settlement page, or the fewer a named
    public endpoint serves in one log query.

    A status read commits no page, so the span changes no fact a completed scan
    reports. It changes what a command costs, and how far a refund scan reaches in one
    command: that scan is capped in pages (`MAXIMUM_REFUND_PAGES`), one request each,
    so through a narrower endpoint it stops earlier with `complete` false and is
    continued from `scanned.to` + 1."""
    from almanak.connectors.across.settlement_reader import MAXIMUM_EVENT_BLOCK_SPAN
    from almanak.gateway.providers import PUBLIC_LOG_SPAN

    served = PUBLIC_LOG_SPAN.get(public_rpc or "", MAXIMUM_EVENT_BLOCK_SPAN)
    return min(MAXIMUM_EVENT_BLOCK_SPAN, served)


def pages(reader, first: int, last: int, limit: int | None = None, span: int | None = None):
    """Bounded block ranges from `first` to `last`, each pinned at both ends."""
    span = span or log_span(None)
    count = 0
    while first <= last and (limit is None or count < limit):
        end = min(first + span - 1, last)
        yield reader.pin_block(first), reader.pin_block(end)
        first, count = end + 1, count + 1


def decoded_deposit(row, origin: int, deposit_id: int) -> tuple[tuple, Deposit]:
    """A `FundsDeposited` log as the relay it asks for, and its fields for the report."""
    from eth_abi import decode
    from eth_abi.exceptions import DecodingError

    from almanak.connectors.across.receipts import DATA_TYPES, topic_word

    try:
        data = decode(DATA_TYPES, bytes.fromhex(row["data"][2:]))
    except (DecodingError, ValueError):
        raise DataError("integrity", "Across deposit event data is malformed") from None
    input_token, output_token, sent, offered, quoted_at, fill_deadline = data[:6]
    exclusivity, recipient, relayer, message = data[6:]
    depositor = topic_word(row, 3)
    relay = (
        depositor,
        recipient,
        relayer,
        input_token,
        output_token,
        sent,
        offered,
        origin,
        deposit_id,
        fill_deadline,
        exclusivity,
        message,
    )
    return relay, Deposit(
        transaction=row["transactionHash"],
        block=int(row["blockNumber"], 16),
        depositor=account(depositor),
        recipient=account(recipient),
        input_token=account(input_token),
        output_token=account(output_token),
        sent=sent,
        offered=offered,
        quoted_at=quoted_at,
        fill_deadline=fill_deadline,
    )


def read_deposit(reader, pool, destination: int, reference, head, span=None) -> Deposit:
    """The deposit the reference names, found by its identifier and held to its relay hash.

    A deposit is mined no earlier than its quote and no later than the SpokePool's quote
    buffer after it, so only those blocks are read.
    """
    from almanak.connectors.across import settlement_reader as chain
    from almanak.connectors.across.observations import read_pool
    from almanak.connectors.across.receipts import FUNDS_DEPOSITED_TOPIC
    from almanak.connectors.across.relay import relay_hash
    from almanak.gateway.evm import chain_number

    buffer = read_pool(reader, pool, chain.canonical(reader, head)).quote_time_buffer
    first = chain.block_at_or_before(reader, reference.quoted_at, head)
    last = chain.block_at_or_before(reader, reference.quoted_at + buffer, head)
    origin = chain_number(reader.chain.chain_id)
    topics = [
        FUNDS_DEPOSITED_TOPIC,
        chain.uint_topic(destination),
        chain.uint_topic(reference.deposit_id),
    ]
    for start, end in pages(reader, first.number, last.number, span=span):
        for row in chain.logs(reader, pool, topics, start, end):
            if len(row["topics"]) != 4:
                continue
            relay, deposit = decoded_deposit(row, origin, reference.deposit_id)
            if relay_hash(relay, destination) == reference.relay_hash:
                return held_to_quote(deposit, reference)
    raise DataError(
        "not_found",
        "no deposit with this reference left the source chain for the destination chain"
        " in the blocks its quote allows, as of the source chain's latest block; check the"
        " chains and the reference, or name the deposit's transaction with --deposit-tx",
    )


def held_to_quote(deposit: Deposit, reference) -> Deposit:
    """The relay hash does not cover the quote's timestamp, and the arrival scan starts
    from it, so a reference that states another one is refused."""
    if deposit.quoted_at != reference.quoted_at:
        raise DataError(
            "integrity",
            f"the reference states the quote timestamp {reference.quoted_at}, and its"
            f" deposit was mined with {deposit.quoted_at}; nothing is read for it",
        )
    return deposit


def mined_receipt(reader, tx_hash: str) -> dict:
    """The successful receipt of a transaction in a block that is still canonical."""
    receipt = reader._rpc("eth_getTransactionReceipt", [tx_hash], allow_none=True)
    if receipt is None or receipt.get("status") != "0x1":
        raise DataError(
            "not_found",
            "no successful transaction with this hash is mined on the source chain, as of"
            " its latest block; check the hash and the chain",
        )
    if reader.pin_block(int(receipt["blockNumber"], 16)).hash != receipt["blockHash"]:
        raise DataError("reorganization", "the deposit transaction's block is not canonical")
    return receipt


def deposit_of(reader, pool: str, destination: int, tx_hash: str):
    """The reference and the deposit of the one deposit a transaction made through the
    reviewed SpokePool towards the destination chain.

    The deposit's identifier is assigned when it is mined, so a transfer has no
    reference before; its transaction hash is known from signing and leads here.
    """
    from almanak.connectors.across import settlement_reader as chain
    from almanak.connectors.across.receipts import FUNDS_DEPOSITED_TOPIC
    from almanak.connectors.across.relay import DepositReference, relay_hash
    from almanak.gateway.evm import chain_number

    wanted = [FUNDS_DEPOSITED_TOPIC, chain.uint_topic(destination)]
    rows = [
        row
        for row in mined_receipt(reader, tx_hash)["logs"]
        if row["address"].lower() == pool and len(row["topics"]) == 4
        if [topic.lower() for topic in row["topics"][:2]] == wanted
    ]
    if len(rows) != 1:
        raise DataError(
            "not_found",
            f"the transaction holds {len(rows)} deposits through the reviewed SpokePool"
            " towards the destination chain, and one is needed; check the hash and the"
            " chains",
        )
    deposit_id = int(rows[0]["topics"][2], 16)
    relay, deposit = decoded_deposit(rows[0], chain_number(reader.chain.chain_id), deposit_id)
    reference = DepositReference(
        deposit_id=deposit_id,
        relay_hash=relay_hash(relay, destination),
        quoted_at=deposit.quoted_at,
    )
    return reference, deposit


def read_arrival(
    reader, pool, origin: int, deposit: Deposit, reference, head, span=None
) -> dict | None:
    """The fill of the deposit's relay and the amount it paid the recipient, or None
    while the destination SpokePool does not hold the relay as filled."""
    from almanak.connectors.across import settlement
    from almanak.connectors.across import settlement_reader as chain
    from almanak.connectors.across.settlement_types import FILL_TYPES
    from almanak.gateway.evm import chain_number

    if chain.fill_status(reader, pool, reference.relay_hash, head) != chain.FILLED:
        return None
    destination = chain_number(reader.chain.chain_id)
    first = chain.block_at_or_before(reader, reference.quoted_at, head)
    last = chain.block_at_or_before(reader, deposit.fill_deadline, head)
    for start, end in pages(reader, first.number, last.number, span=span):
        rows = chain.fill_logs(reader, pool, origin, reference.deposit_id, start, end)
        found = settlement.matching_fill(rows, reference.relay_hash, destination)
        if found is None:
            continue
        row, fill = found
        return {
            "transaction": row["transactionHash"],
            "block": int(row["blockNumber"], 16),
            **payout(reader, pool, deposit, row, fill, head),
            "fill_type": FILL_TYPES[fill["fill_type"]],
        }
    raise DataError(
        "integrity",
        "the destination SpokePool holds the relay as filled, but no fill of it is in the"
        " blocks between its quote and its fill deadline; nothing is reported as received",
    )


def is_wrapped_native(chain_id, token: str) -> bool:
    """Whether the token is the network definition's wrapped native token of the chain."""
    from almanak.chains import evm_network

    return token == evm_network(chain_id).wrapped_native.reference


def unwraps(reader, pool: str, token: str, head) -> bool:
    """Whether the SpokePool treats `token` as its wrapped native token, which it
    unwraps for a recipient without code. Read from the pool, never from a table alone:
    the network definition only says when the read is worth making."""
    from almanak.connectors.across import settlement_reader as chain

    if not is_wrapped_native(reader.chain.chain_id, token):
        return False
    block = chain.canonical(reader, head)
    (named,) = reader.call(pool, "wrappedNativeToken()", block, outputs=("address",))
    return named.lower() == token


PAID_NATIVE = (
    "Paid in the chain's native asset. The payment itself leaves no log: the amount is"
    " the fill event's, matched by the SpokePool's unwrap of the same amount of its"
    " wrapped native token after that event."
)
PAID_WRAPPED = (
    "Paid in the wrapped native token, not in the native asset: the SpokePool pays the"
    " token to a recipient that holds contract code when the fill lands."
)


def payout(reader, pool: str, deposit: Deposit, row, fill, head) -> dict:
    """The amount the fill paid the recipient, and for the pool's wrapped native token
    the asset it was paid in and the evidence that measured it."""
    from almanak.connectors.across import settlement

    if not unwraps(reader, pool, deposit.output_token, head):
        _, paid = settlement.paid_by_fill(
            reader, deposit.recipient, deposit.output_token, row, fill
        )
        return {"received": str(paid)}
    return native_payout(reader, deposit, row, fill)


def native_payout(reader, deposit: Deposit, row, fill) -> dict:
    """A fill of the pool's wrapped native token: the event's amount, held to what the
    pool did with the token after that event. The asset reported is the one paid."""
    from almanak.chains import evm_network
    from almanak.connectors.across import settlement_reader as chain
    from almanak.connectors.across.operations import word

    if fill["updated_recipient"] != word(deposit.recipient):
        raise chain.integrity("an Across fill paid another recipient than the deposit's")
    amount, chain_id = fill["updated_output"], reader.chain.chain_id
    form = chain.native_payout(
        chain.receipt_of(reader, row), row, deposit.output_token, deposit.recipient, amount
    )
    native = form in chain.UNWRAPS
    token = f"{chain_id}/erc20:{deposit.output_token}"
    return {
        "received": str(amount),
        "asset": str(evm_network(chain_id).native_asset) if native else token,
        "paid_as": "native" if native else "wrapped_token",
        "measured_by": form,
        "note": PAID_NATIVE if native else PAID_WRAPPED,
    }


def sent_as_value(reader, pool: str, deposit: Deposit) -> bool | None:
    """Whether the deposit's own transaction paid the SpokePool its amount as native
    value. None for an input token that is not the chain's wrapped native token, which
    reads nothing. A deposit made through another contract reads as False: what that
    contract was paid is not this command's to say."""
    if not is_wrapped_native(reader.chain.chain_id, deposit.input_token):
        return None
    sent = reader._rpc("eth_getTransactionByHash", [deposit.transaction], allow_none=True)
    if sent is None:
        raise DataError("unavailable", "the deposit's transaction is not served by the endpoint")
    return (sent.get("to") or "").lower() == pool and int(sent["value"], 16) == deposit.sent


WRAPPED_REFUND = (
    " This deposit's input is the chain's wrapped native token ({asset}), and a refund is"
    " paid in that token on the source chain, never in the native asset, also when the"
    " deposit was sent as native value."
)


def wrapped_refund(chain_id, deposit: Deposit, refund: dict) -> dict:
    """The refund listing, saying which asset a payment is in when that is the wrapped
    native token; unchanged for any other input token."""
    if not is_wrapped_native(chain_id, deposit.input_token):
        return refund
    asset = f"{chain_id}/erc20:{deposit.input_token}"
    note = refund["note"] + WRAPPED_REFUND.format(asset=asset)
    return {**refund, "paid_in": asset, "note": note}


def leaf_payment(reader, pool: str, deposit: Deposit, row) -> dict | None:
    """A payment to the depositor that an executed refund leaf names, or None."""
    from almanak.connectors.across import settlement_reader as chain

    receipt = chain.receipt_of(reader, row)
    named = chain.refund_entries(receipt, pool, deposit.input_token, deposit.depositor)
    if not named:
        return None
    # A leaf can name an amount the pool deferred and never transferred, so the amounts
    # the pool paid are read from its own token transfers in the same transaction.
    paid = chain.refund_payments(receipt, deposit.input_token, pool, deposit.depositor)
    return {
        "transaction": row["transactionHash"],
        "block": int(row["blockNumber"], 16),
        "amounts": [str(amount) for amount in named],
        "paid": [str(amount) for amount in paid],
        "equals_sent": deposit.sent in named and deposit.sent in paid,
    }


def read_refunds(reader, pool: str, deposit: Deposit, head, start: int | None, span=None) -> dict:
    """Refund-leaf payments of the sent asset to the depositor since the fill deadline.

    Nothing here says which deposit a payment returns; see `UNATTRIBUTED`.
    """
    from almanak.connectors.across import settlement_reader as chain

    # A deposit can still fill in a block stamped with its deadline, so no refund of it is
    # in that block or an earlier one. A stated cursor continues a scan and never precedes it.
    expired = chain.block_at_or_before(reader, deposit.fill_deadline, head).number + 1
    start = expired if start is None else max(start, expired)
    # Read first, while the block read is young: a public endpoint can leave state of an
    # older block unanswered, and the scan below can take minutes.
    held = chain.deferred_refund(reader, pool, deposit.input_token, deposit.depositor, head)
    # An empty range, a start past the head, has scanned up to the block before it.
    payments: list[dict] = []
    scanned_to = start - 1
    seen: set[str] = set()
    for first, end in pages(reader, start, head.number, MAXIMUM_REFUND_PAGES, span):
        rows = chain.refund_logs(reader, deposit.input_token, pool, deposit.depositor, first, end)
        # A payment record covers its whole transaction, so each is decoded once.
        fresh = {row["transactionHash"]: row for row in rows if row["transactionHash"] not in seen}
        seen.update(fresh)
        payments += [p for row in fresh.values() if (p := leaf_payment(reader, pool, deposit, row))]
        scanned_to = end.number
    return {
        "payments": payments,
        "scanned": {"from": start, "to": scanned_to},
        # Whether the scan reached the block read; otherwise continue from `to` + 1.
        "complete": scanned_to == head.number,
        # What the SpokePool holds for the wallet because a refund transfer to it failed.
        "deferred_in_spoke_pool": str(held),
        "note": UNATTRIBUTED,
    }


def described(chain_id, deposit: Deposit, native: bool | None = None) -> dict:
    """The deposit as the SpokePool recorded it. `native` is `sent_as_value`: stated
    only for a deposit of the wrapped native token, and when true the asset sent is the
    native one, with the token the pool recorded beside it."""
    from almanak.chains import evm_network

    token = f"{chain_id}/erc20:{deposit.input_token}"
    sent = {"asset": token, "amount": str(deposit.sent)}
    stated: dict = {}
    if native:
        sent = {**sent, "asset": str(evm_network(chain_id).native_asset), "recorded_as": token}
    if native is not None:
        stated = {"sent_as_native_value": native}
    return {
        "transaction": deposit.transaction,
        "block": deposit.block,
        "depositor": deposit.depositor,
        "recipient": deposit.recipient,
        "sent": sent,
        **stated,
        "offered": str(deposit.offered),
        "fill_deadline": deposit.fill_deadline,
    }


IN_FLIGHT = (
    "No fill is in the destination chain's {tag} block or before it. A fill mined in a"
    " later block is not read yet: a finalized block trails the head by minutes."
)


def outcome(
    near, far, pools, deposit: Deposit, reference, tag: str, refund_from, public=(None, None)
) -> dict:
    """What became of the deposit as of each chain's `tag` block. A received amount
    appears only when a fill was measured; an open transfer states none, never zero.

    Unfilled: the SpokePool reverts a fill of a relay whose fill deadline is before its
    block's timestamp, so a block past the deadline that holds the relay as not filled
    shows that no fill can follow.
    """
    from almanak.gateway.evm import chain_number

    with public_cause(public[1], far.chain.chain_id):
        far_head = far.pin_block(tag)
        origin = chain_number(near.chain.chain_id)
        span = log_span(public[1])
        arrival = read_arrival(far, pools[1], origin, deposit, reference, far_head, span)
    read_at = {"block": tag, "destination_block": far_head.number}
    if arrival is not None:
        return {"read_at": read_at, "status": "arrived", "arrival": arrival}
    if far_head.timestamp <= deposit.fill_deadline:
        return {"read_at": read_at, "status": "in_flight", "note": IN_FLIGHT.format(tag=tag)}
    # The refund is paid to the depositor on the source chain, in a later bundle.
    with public_cause(public[0], near.chain.chain_id):
        near_head = near.pin_block(tag)
        span = log_span(public[0])
        refund = read_refunds(near, pools[0], deposit, near_head, refund_from, span)
    refund = wrapped_refund(near.chain.chain_id, deposit, refund)
    read_at["source_block"] = near_head.number
    return {"read_at": read_at, "status": "unfilled", "refund": refund}


def named_deposit(reader, pool: str, destination: int, args, span=None):
    """The deposit a command names, by reference or by its transaction, read at the
    source chain's latest block.

    The operator holds the deposit's receipt, and a finalized block trails it by
    minutes. The deposit decides no outcome: it only says what to look for, and the
    outcome is read at the block the command names.
    """
    from almanak.connectors.across.relay import parse_reference

    head = reader.pin_block("latest")
    if args.deposit_tx is not None:
        return deposit_of(reader, pool, destination, args.deposit_tx)
    reference = parse_reference(args.reference)
    return reference, read_deposit(reader, pool, destination, reference, head, span)


def status_reader(args, chain, public_rpc: str | None):
    """One chain's reader of a status read: the command's one transport, or the public
    endpoint the caller named for that chain, which gives the public proof first."""
    from almanak import ax

    if args.public_rpc is None:
        return ax.observation_reader(args, chain)
    if public_rpc is None:
        # Never the source endpoint's name for the destination chain.
        raise ValueError(DESTINATION_REQUIRED)
    with proof_cause(public_rpc, chain):
        return ax.public_reader(chain, public_rpc)


def named(error: DataError, note: str):
    """The same refusal with a note after its own words. Its class, kind, code and
    whatever else it carries, such as a throttle's requested wait, are unchanged."""
    error.args = (f"{error}; {note}",)
    return error


PROOF_FAILED = (
    "the public endpoint {name} on {chain} did not give the proof every public endpoint"
    " must: its chain number, the pinned genesis block and a recent head. Nothing was"
    " read through it"
)


@contextmanager
def proof_cause(name: str | None, chain):
    """Name the public endpoint and its chain when its proof fails. A bridge command
    proves two endpoints, and a bare `stale` or `chain_mismatch` says neither which."""
    try:
        yield
    except DataError as error:
        if name is None:
            raise
        raise named(error, PROOF_FAILED.format(name=name, chain=chain)) from None


PUBLIC_LIMIT = (
    "read through the public endpoint {name} on {chain}: a public endpoint promises"
    " neither state of an older block nor event logs, and can refuse such a read or"
    " leave it unanswered. Nothing is reported from a partial read. Run the command"
    " again, name another endpoint listed for the chain, or name a provider key with"
    " --env-file"
)


@contextmanager
def public_cause(name: str | None, chain):
    """Name the public endpoint and its chain on a read it refused.

    A status read asks for state at a block that can be minutes old and for log pages
    of the scan's full span. A provider key serves both; a public endpoint may not, and
    its bare refusal names neither the endpoint nor the chain of the two the command
    reads. The kind and the endpoint's own words are kept.
    """
    try:
        yield
    except DataError as error:
        # A contract's own revert is an answer, not a limit of the endpoint.
        if name is None or error.reverted or error.kind not in PROVIDER_REFUSALS:
            raise
        raise named(error, PUBLIC_LIMIT.format(name=name, chain=chain)) from None


def status(args) -> dict:
    from almanak.caip import ChainId
    from almanak.connectors.across.domain import spoke_pool
    from almanak.gateway.evm import chain_number

    source, destination = ChainId(args.chain), ChainId(args.destination_chain)
    pools = (spoke_pool(source).address, spoke_pool(destination).address)
    names = (args.public_rpc, args.destination_public_rpc)
    with (
        status_reader(args, source, names[0]) as near,
        status_reader(args, destination, names[1]) as far,
    ):
        with public_cause(names[0], source):
            reference, deposit = named_deposit(
                near, pools[0], chain_number(destination), args, log_span(names[0])
            )
            native = sent_as_value(near, pools[0], deposit)
        result = outcome(
            near, far, pools, deposit, reference, args.block, args.refund_from_block, names
        )
    return {
        "product": "bridge_status",
        "connector": args.connector,
        "reference": reference.text,
        "source_chain": str(source),
        "destination_chain": str(destination),
        "deposit": described(source, deposit, native),
        **result,
    }


def status_arguments(commands) -> None:
    from almanak.cli_arguments import observation_transport
    from almanak.connectors.direct import connectors_for

    text = (
        "what became of one bridge transfer, named by the reference `ax bridge` reported"
        " or by its deposit transaction: arrived with the measured amount, in flight, or"
        " unfilled past its fill deadline with the refund payments the wallet received"
        " since"
    )
    command = commands.add_parser("bridge-status", help=text, description=text)
    command.set_defaults(product="ax-bridge-status")
    command.add_argument(
        "--connector",
        required=True,
        choices=connectors_for("bridge"),
        help="canonical connector ID",
    )
    command.add_argument("--chain", required=True, help="strict CAIP-2 chain the asset left")
    command.add_argument(
        "--destination-chain", required=True, help="strict CAIP-2 chain the asset arrives on"
    )
    named = command.add_mutually_exclusive_group(required=True)
    named.add_argument("--reference", help="the transfer reference `ax bridge` reported")
    named.add_argument(
        "--deposit-tx",
        type=transaction_hash,
        help="the deposit's transaction hash on the source chain, when the reference is"
        " lost; the result states the reference",
    )
    command.add_argument(
        "--refund-from-block",
        type=block_number,
        help="source block a refund scan starts at, to continue one that did not reach"
        " the block read (default: the last block not after the fill deadline)",
    )
    # A tag only: the command reads two chains, and one number names no block of both.
    command.add_argument(
        "--block",
        choices=("finalized", "latest"),
        default="finalized",
        help="the block of each chain the outcome is read at; the deposit itself is"
        " found at the source chain's latest block (default: finalized, which trails"
        " the head by minutes, so a recent fill reads as in_flight until then)",
    )
    # A provider key or the read gateway serves both chains. A public endpoint serves
    # one, so the caller names a second for the destination chain.
    observation_transport(command)
    destination_public_rpc_option(command)


def block_number(value: str) -> int:
    import argparse

    if not (value.isascii() and value.isdigit()):
        raise argparse.ArgumentTypeError("expected a block number")
    return int(value)
