"""The direct bridge commands of `almanak ax`, and the read-only status of one transfer.

`ax bridge` and `ax revoke-bridge` run through `ax_actions` as every direct action does.
What differs is here: a route names two chains, so the deployment is resolved on the
chain it sends from and completed on the destination chain with a second reader from the
same env file. After that the deployment has one chain. Nothing is read or signed on the
destination chain while the action runs.

A bridge asks the venue's own quote API, which a public RPC endpoint cannot stand in
for, and it reads two chains, so these commands take `--env-file` only.

`ax bridge-status` reads what became of one transfer, named by the reference `ax bridge`
reported. It keeps no state: the deposit is read again from the source SpokePool and
held to the reference's relay hash, and everything else follows from that deposit. It
plans and signs nothing.
"""

from dataclasses import dataclass

from almanak.gateway.ethereum import DataError

# Pages of the source chain a refund scan reads at most in one command. A refund is paid
# in a later bundle, hours after the fill deadline; the scan says where it stopped, and
# `--refund-from-block` continues it.
MAXIMUM_REFUND_PAGES = 200
PUBLIC_REFUSAL = (
    "a bridge reads two chains and asks the venue's quote API; it takes --env-file,"
    " never --public-rpc"
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
        action.set_defaults(amount=None, max_fee_bps=0, recipient=None)
        return
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


def validate(parser, args) -> None:
    if args.public_rpc is not None:
        parser.error(PUBLIC_REFUSAL)


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
    return resolution(args.connector).MarketSelection.model_validate_json(json.dumps(values))


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


def completed(connector: str, env_file, resolved, recipient: str, on_source: bool):
    """The route completed on its destination chain, at that chain's own anchor, and
    held to the reviewed SpokePool pins on both chains. The recipient is checked there
    too, under `require_receivable`."""
    from almanak.ax_actions import resolution
    from almanak.gateway.read_endpoints import verified_reader

    resolver = resolution(connector)
    selection = resolved.selection
    with verified_reader(selection.destination_chain, env_file) as reader:
        prove_genesis(reader)
        block = reader.pin_block("latest")
        route = resolver.complete_market(reader, resolved, block)
        on_destination = contract_code(reader, recipient, block)
    resolver.admit_route(route)
    chains = (selection.chain, selection.destination_chain)
    require_receivable(recipient, on_source, on_destination, chains)
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


def deployment(env_file, request, action, wallet, provider, endpoint, limits):
    """The operator deployment of one wallet along one route, on the chain it leaves."""
    from almanak.ax_actions import configuration, resolution
    from almanak.connectors.direct import Wallet, binding
    from almanak.gateway.ethereum import EthereumReader
    from almanak.models import Chain

    connector = request.connector
    chain = Chain(chain_id=request.chain, environment="mainnet")
    account = Wallet(account=wallet["account"], address=wallet["address"].lower())
    stated = None if action.recipient is None else action.recipient.lower()
    with EthereumReader(chain, endpoint) as reader:
        block = reader.pin_block("latest")
        resolved = resolution(connector).resolve_market(reader, request, block)
        on_source = stated is not None and contract_code(reader, stated, block)
    if action.action == "bridge" and stated is not None:
        route = completed(connector, env_file, resolved, stated, on_source)
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


def pages(reader, first: int, last: int, limit: int | None = None):
    """Bounded block ranges from `first` to `last`, each pinned at both ends."""
    from almanak.connectors.across.settlement_reader import MAXIMUM_EVENT_BLOCK_SPAN

    count = 0
    while first <= last and (limit is None or count < limit):
        end = min(first + MAXIMUM_EVENT_BLOCK_SPAN - 1, last)
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


def read_deposit(reader, pool, destination: int, reference, head) -> Deposit:
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
    for start, end in pages(reader, first.number, last.number):
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


def read_arrival(reader, pool, origin: int, deposit: Deposit, reference, head) -> dict | None:
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
    for start, end in pages(reader, first.number, last.number):
        rows = chain.fill_logs(reader, pool, origin, reference.deposit_id, start, end)
        found = settlement.matching_fill(rows, reference.relay_hash, destination)
        if found is None:
            continue
        row, fill = found
        _, paid = settlement.paid_by_fill(
            reader, deposit.recipient, deposit.output_token, row, fill
        )
        return {
            "transaction": row["transactionHash"],
            "block": int(row["blockNumber"], 16),
            "received": str(paid),
            "fill_type": FILL_TYPES[fill["fill_type"]],
        }
    raise DataError(
        "integrity",
        "the destination SpokePool holds the relay as filled, but no fill of it is in the"
        " blocks between its quote and its fill deadline; nothing is reported as received",
    )


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


def read_refunds(reader, pool: str, deposit: Deposit, head, start: int | None) -> dict:
    """Refund-leaf payments of the sent asset to the depositor since the fill deadline.

    Nothing here says which deposit a payment returns; see `UNATTRIBUTED`.
    """
    from almanak.connectors.across import settlement_reader as chain

    # A deposit can still fill in a block stamped with its deadline, so no refund of it is
    # in that block or an earlier one. A stated cursor continues a scan and never precedes it.
    expired = chain.block_at_or_before(reader, deposit.fill_deadline, head).number + 1
    start = expired if start is None else max(start, expired)
    # An empty range, a start past the head, has scanned up to the block before it.
    payments: list[dict] = []
    scanned_to = start - 1
    seen: set[str] = set()
    for first, end in pages(reader, start, head.number, MAXIMUM_REFUND_PAGES):
        rows = chain.refund_logs(reader, deposit.input_token, pool, deposit.depositor, first, end)
        # A payment record covers its whole transaction, so each is decoded once.
        fresh = {row["transactionHash"]: row for row in rows if row["transactionHash"] not in seen}
        seen.update(fresh)
        payments += [p for row in fresh.values() if (p := leaf_payment(reader, pool, deposit, row))]
        scanned_to = end.number
    held = chain.deferred_refund(reader, pool, deposit.input_token, deposit.depositor, head)
    return {
        "payments": payments,
        "scanned": {"from": start, "to": scanned_to},
        # Whether the scan reached the block read; otherwise continue from `to` + 1.
        "complete": scanned_to == head.number,
        # What the SpokePool holds for the wallet because a refund transfer to it failed.
        "deferred_in_spoke_pool": str(held),
        "note": UNATTRIBUTED,
    }


def described(chain_id, deposit: Deposit) -> dict:
    return {
        "transaction": deposit.transaction,
        "block": deposit.block,
        "depositor": deposit.depositor,
        "recipient": deposit.recipient,
        "sent": {"asset": f"{chain_id}/erc20:{deposit.input_token}", "amount": str(deposit.sent)},
        "offered": str(deposit.offered),
        "fill_deadline": deposit.fill_deadline,
    }


IN_FLIGHT = (
    "No fill is in the destination chain's {tag} block or before it. A fill mined in a"
    " later block is not read yet: a finalized block trails the head by minutes."
)


def outcome(near, far, pools, deposit: Deposit, reference, tag: str, refund_from) -> dict:
    """What became of the deposit as of each chain's `tag` block. A received amount
    appears only when a fill was measured; an open transfer states none, never zero.

    Unfilled: the SpokePool reverts a fill of a relay whose fill deadline is before its
    block's timestamp, so a block past the deadline that holds the relay as not filled
    shows that no fill can follow.
    """
    from almanak.gateway.evm import chain_number

    far_head = far.pin_block(tag)
    read_at = {"block": tag, "destination_block": far_head.number}
    origin = chain_number(near.chain.chain_id)
    arrival = read_arrival(far, pools[1], origin, deposit, reference, far_head)
    if arrival is not None:
        return {"read_at": read_at, "status": "arrived", "arrival": arrival}
    if far_head.timestamp <= deposit.fill_deadline:
        return {"read_at": read_at, "status": "in_flight", "note": IN_FLIGHT.format(tag=tag)}
    # The refund is paid to the depositor on the source chain, in a later bundle.
    near_head = near.pin_block(tag)
    refund = read_refunds(near, pools[0], deposit, near_head, refund_from)
    read_at["source_block"] = near_head.number
    return {"read_at": read_at, "status": "unfilled", "refund": refund}


def named_deposit(reader, pool: str, destination: int, args):
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
    return reference, read_deposit(reader, pool, destination, reference, head)


def status(args) -> dict:
    from almanak.ax import observation_reader
    from almanak.caip import ChainId
    from almanak.connectors.across.domain import spoke_pool
    from almanak.gateway.evm import chain_number

    source, destination = ChainId(args.chain), ChainId(args.destination_chain)
    pools = (spoke_pool(source).address, spoke_pool(destination).address)
    with observation_reader(args, source) as near, observation_reader(args, destination) as far:
        reference, deposit = named_deposit(near, pools[0], chain_number(destination), args)
        result = outcome(near, far, pools, deposit, reference, args.block, args.refund_from_block)
    return {
        "product": "bridge_status",
        "connector": args.connector,
        "reference": reference.text,
        "source_chain": str(source),
        "destination_chain": str(destination),
        "deposit": described(source, deposit),
        **result,
    }


def status_arguments(commands) -> None:
    from pathlib import Path

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
    # A provider key or the read gateway, which serve both chains. No listed public
    # endpoint does, so none is offered.
    command.set_defaults(public_rpc=None)
    transport = command.add_mutually_exclusive_group()
    transport.add_argument(
        "--env-file", type=Path, help="private provider credentials read by this process"
    )
    transport.add_argument(
        "--gateway-url", help="read-only observation gateway on loopback (service read-gateway)"
    )
    command.add_argument(
        "--gateway-auth-file", type=Path, help="authentication token file for --gateway-url"
    )


def block_number(value: str) -> int:
    import argparse

    if not (value.isascii() and value.isdigit()):
        raise argparse.ArgumentTypeError("expected a block number")
    return int(value)
