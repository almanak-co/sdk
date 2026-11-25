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
    import argparse

    from almanak.models import ZERO_ADDRESS, address

    try:
        parsed = address(value)
    except ValueError as error:
        raise argparse.ArgumentTypeError(f"expected an EVM address: {error}") from None
    if parsed == ZERO_ADDRESS:
        raise argparse.ArgumentTypeError("expected a nonzero EVM address")
    return parsed


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


def completed(connector: str, env_file, resolved):
    """The route completed on its destination chain, at that chain's own anchor, and
    held to the reviewed SpokePool pins on both chains."""
    from almanak.ax_actions import resolution
    from almanak.gateway.read_endpoints import verified_reader

    resolver = resolution(connector)
    with verified_reader(resolved.selection.destination_chain, env_file) as reader:
        prove_genesis(reader)
        route = resolver.complete_market(reader, resolved, reader.pin_block("latest"))
    resolver.admit_route(route)
    return route


def deployment(env_file, request, action, wallet, provider, endpoint, limits):
    """The operator deployment of one wallet along one route, on the chain it leaves."""
    from almanak.ax_actions import configuration, resolution
    from almanak.connectors.direct import Wallet, binding
    from almanak.gateway.ethereum import EthereumReader
    from almanak.models import Chain

    connector = request.connector
    chain = Chain(chain_id=request.chain, environment="mainnet")
    account = Wallet(account=wallet["account"], address=wallet["address"].lower())
    with EthereumReader(chain, endpoint) as reader:
        block = reader.pin_block("latest")
        resolved = resolution(connector).resolve_market(reader, request, block)
    route = completed(connector, env_file, resolved)
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
    input_token, output_token, sent, offered, _quoted_at, fill_deadline = data[:6]
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
                return deposit
    raise DataError(
        "not_found",
        "no deposit with this reference left the source chain for the destination chain"
        " in the blocks its quote allows, as of the block read; check the chains and the"
        " reference, or read at a later block",
    )


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
    return {
        "transaction": row["transactionHash"],
        "block": int(row["blockNumber"], 16),
        "amounts": [str(amount) for amount in named],
        "equals_sent": deposit.sent in named,
    }


def read_refunds(reader, pool: str, deposit: Deposit, head, start: int | None) -> dict:
    """Refund-leaf payments of the sent asset to the depositor since the fill deadline.

    Nothing here says which deposit a payment returns; see `UNATTRIBUTED`.
    """
    from almanak.connectors.across import settlement_reader as chain

    if start is None:
        start = chain.block_at_or_before(reader, deposit.fill_deadline, head).number
    payments, scanned_to = [], None
    for first, end in pages(reader, start, head.number, MAXIMUM_REFUND_PAGES):
        rows = chain.refund_logs(reader, deposit.input_token, pool, deposit.depositor, first, end)
        payments += [p for row in rows if (p := leaf_payment(reader, pool, deposit, row))]
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


def outcome(near, far, pools, deposit: Deposit, reference, heads, refund_from) -> dict:
    """What became of the deposit as of the two blocks read. A received amount appears
    only when a fill was measured; an open transfer states none, never zero."""
    from almanak.gateway.evm import chain_number

    near_head, far_head = heads
    origin = chain_number(near.chain.chain_id)
    arrival = read_arrival(far, pools[1], origin, deposit, reference, far_head)
    if arrival is not None:
        return {"status": "arrived", "arrival": arrival}
    if far_head.timestamp <= deposit.fill_deadline:
        return {"status": "in_flight"}
    # Past the fill deadline with no fill: no fill can follow. The refund is paid to the
    # depositor on the source chain, in a later bundle.
    refund = read_refunds(near, pools[0], deposit, near_head, refund_from)
    return {"status": "unfilled", "refund": refund}


def status(args) -> dict:
    from almanak.ax import observation_reader
    from almanak.caip import ChainId
    from almanak.connectors.across.domain import spoke_pool
    from almanak.connectors.across.relay import parse_reference
    from almanak.gateway.evm import chain_number

    source, destination = ChainId(args.chain), ChainId(args.destination_chain)
    pools = (spoke_pool(source).address, spoke_pool(destination).address)
    reference = parse_reference(args.reference)
    with observation_reader(args, source) as near, observation_reader(args, destination) as far:
        heads = (near.pin_block(args.block), far.pin_block(args.block))
        deposit = read_deposit(near, pools[0], chain_number(destination), reference, heads[0])
        result = outcome(near, far, pools, deposit, reference, heads, args.refund_from_block)
    return {
        "product": "bridge_status",
        "connector": args.connector,
        "reference": args.reference,
        "source_chain": str(source),
        "destination_chain": str(destination),
        "read_at": {"source_block": heads[0].number, "destination_block": heads[1].number},
        "deposit": described(source, deposit),
        **result,
    }


def status_arguments(commands) -> None:
    from pathlib import Path

    from almanak.cli_arguments import block_option
    from almanak.connectors.direct import connectors_for

    text = (
        "what became of one bridge transfer, by the reference `ax bridge` reported:"
        " arrived with the measured amount, in flight, or unfilled past its fill deadline"
        " with the refund payments the wallet received since"
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
    command.add_argument(
        "--reference", required=True, help="the transfer reference `ax bridge` reported"
    )
    command.add_argument(
        "--refund-from-block",
        type=block_number,
        help="source block a refund scan starts at, to continue one that did not reach"
        " the block read (default: the last block not after the fill deadline)",
    )
    block_option(command)
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
