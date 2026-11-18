"""Pinned market quotes and durable, account-scoped gas admission."""

from almanak.chains import evm_network
from almanak.connectors.composition import connector_for
from almanak.gateway.environments import origin_chain
from almanak.gateway.ethereum import DataError
from almanak.gateway.production_config import execution_gas
from almanak.state import FeeQuote


def quoted_fees(reader, policy, block, tx, *, estimate=None, node_gas_price=None):
    if estimate is None:
        selector = {"blockHash": block.hash, "requireCanonical": True}
        estimate = int(reader._rpc("eth_estimateGas", [tx, selector]), 16)
    price, quote = quoted_price(reader, policy, block, node_gas_price=node_gas_price)
    gas = (estimate * (10000 + policy.gas_limit_buffer_bps) + 9999) // 10000
    return {"gas_estimate": estimate, "gas": gas, "gas_price": price, "fee_quote": quote}


def fee_history_block(reader, block) -> int:
    """The block whose fee history prices a transaction pinned at `block`.

    On a chain whose large blocks keep their own base fee, a quote pinned on a large
    block reads its parent: the latest small block, whose next base fee is the next
    small block's, where a transaction within the small-block gas limit lands. The
    parent hash binds that block to the pin, so re-preparation, signing and
    replacement at the same pin read the same history.
    """
    if not evm_network(block.chain.chain_id).separate_large_block_fees or block.number == 0:
        return block.number
    pinned = reader._rpc("eth_getBlockByNumber", [hex(block.number), False])
    parent = reader._rpc("eth_getBlockByNumber", [hex(block.number - 1), False])
    try:
        if pinned["hash"] != block.hash or parent["hash"] != pinned["parentHash"]:
            raise DataError("reorganization", "gas quote block changed during preparation")
        large = int(pinned["gasLimit"], 16) > int(parent["gasLimit"], 16)
    except (KeyError, TypeError, ValueError):
        raise DataError("malformed", "block header is incomplete or invalid") from None
    return block.number - 1 if large else block.number


def quoted_price(reader, policy, block, *, node_gas_price=None):
    number = fee_history_block(reader, block)
    history = reader._rpc("eth_feeHistory", ["0x1", hex(number), [policy.reward_percentile]])
    quote = decode_quote(history, block, policy.base_fee_basis, history_block=number)
    if reader.pin_block(block.number) != block:
        raise DataError("reorganization", "gas quote block changed during preparation")
    price = (quote.market_base_fee * policy.base_fee_multiplier_bps + 9999) // 10000
    price += quote.priority_fee
    if not evm_network(policy.asset.chain).fee_history_reports_tips:
        if node_gas_price is None:
            # eth_gasPrice is the node's current suggestion, not a block-addressable
            # observation; the retained quote records the admitted value so plan,
            # signing and replacement revalidation reuse it instead of a moving suggestion.
            node_gas_price = int(reader._rpc("eth_gasPrice", []), 16)
            if reader.pin_block(block.number) != block:
                raise DataError("reorganization", "gas quote block changed during preparation")
        quote = quote.model_copy(update={"node_gas_price": node_gas_price})
        price = max(price, node_gas_price)
    return price, quote


def window_bid(reader, policy, block, blocks):
    """The highest legacy bid this policy would have made over `blocks` blocks ending at `block`.

    For each block i of the window, bid_i = ceil(base_i x base_fee_multiplier_bps / 10000)
    + reward_i at the policy's reward_percentile, where base_i is the next block's base fee
    for the next_block basis and block i's own for observed_block; the result is the
    largest bid_i. On a chain whose fee history omits tips the node's eth_gasPrice floors
    it, as it floors a single-block quote. It sizes check findings only and never prices a
    transaction.
    """
    params = [hex(blocks), hex(block.number), [policy.reward_percentile]]
    bid = decode_window(reader._rpc("eth_feeHistory", params), block, blocks, policy)
    if not evm_network(policy.asset.chain).fee_history_reports_tips:
        bid = max(bid, int(reader._rpc("eth_gasPrice", []), 16))
    if reader.pin_block(block.number) != block:
        raise DataError("reorganization", "gas window block changed during the check")
    return bid


def decode_window(history, block, blocks, policy):
    try:
        rewards = history["reward"]
        if int(history["oldestBlock"], 16) != block.number - blocks + 1 or len(rewards) != blocks:
            raise ValueError("different fee-history window")
        if any(len(reward) != 1 for reward in rewards):
            raise ValueError("unexpected reward percentiles")
        fees = [int(fee, 16) for fee in history["baseFeePerGas"]]
        bases = window_bases(fees, blocks, policy.base_fee_basis)
        multiplier = policy.base_fee_multiplier_bps
        return max(
            (base * multiplier + 9999) // 10000 + int(reward[0], 16)
            for base, reward in zip(bases, rewards, strict=True)
        )
    except (KeyError, IndexError, TypeError, ValueError):
        raise DataError("malformed", "fee history window is incomplete or inconsistent") from None


def window_bases(fees, blocks, basis):
    """Each window block's base fee under `basis`; a node may omit the next block's fee
    when only observed fees are used, as for a single-block quote."""
    if basis == "next_block" and len(fees) == blocks + 1:
        return fees[1:]
    if basis == "observed_block" and len(fees) in (blocks, blocks + 1):
        return fees[:blocks]
    raise ValueError("fee history does not provide the explicitly selected basis")


def pinned_gas_price(reader, block):
    """The legacy price of execution without a gas policy, from the pinned block alone.

    Re-preparation at the same block must reproduce the plan, so the node's moving
    eth_gasPrice suggestion is not read. The higher of the block's and the next block's
    base fee, raised by one block of maximum EIP-1559 increase, needs no tip to be included.
    A base fee that rises more than one block before broadcast is refused by the node.
    """
    number = fee_history_block(reader, block)
    history = reader._rpc("eth_feeHistory", ["0x1", hex(number), []])
    try:
        if int(history["oldestBlock"], 16) != number:
            raise ValueError("different fee-history block")
        fees = [int(fee, 16) for fee in history["baseFeePerGas"]]
        if len(fees) != 2:
            raise ValueError("unexpected fee-history window")
    except (KeyError, TypeError, ValueError):
        raise DataError("malformed", "fee history is incomplete or inconsistent") from None
    if reader.pin_block(block.number) != block:
        raise DataError("reorganization", "gas price block changed during preparation")
    return (max(fees) * 9 + 7) // 8


def decode_quote(history, block, basis="next_block", *, history_block=None):
    # The quote stays identified by its pinned block; its history may be the pin's
    # parent (`fee_history_block`).
    try:
        if int(history["oldestBlock"], 16) != (
            block.number if history_block is None else history_block
        ):
            raise ValueError("different fee-history block")
        if len(history["reward"]) != 1:
            raise ValueError("unexpected fee-history window")
        if len(history["reward"][0]) != 1:
            raise ValueError("unexpected reward percentiles")
        return FeeQuote(
            block=block,
            priority_fee=int(history["reward"][0][0], 16),
            **base_fee_observation(history["baseFeePerGas"], basis),
        )
    except (KeyError, IndexError, TypeError, ValueError):
        raise DataError("malformed", "fee history is incomplete or inconsistent") from None


def base_fee_observation(fees, basis):
    if basis == "observed_block" and len(fees) in (1, 2):
        return {"observed_base_fee": int(fees[0], 16)}
    if basis == "next_block" and len(fees) == 2:
        return {"next_base_fee": int(fees[1], 16)}
    raise ValueError("fee history does not provide the explicitly selected basis")


def validate_fee_limits(policy, gas, price, op_exposure=0):
    """Refuse a transaction outside the author's gas policy, naming the values compared.

    Gas policy values and market quotes are not secrets; the author needs them to choose
    workable ceilings, so each refusal carries them to the coordinator's status.
    """
    if not 0 < gas <= policy.max_gas_limit:
        raise DataError(
            "gas_limit",
            f"estimated gas exceeds the configured gas limit: {gas} gas including the "
            f"{policy.gas_limit_buffer_bps} bps gas_limit_buffer_bps, "
            f"max_gas_limit is {policy.max_gas_limit}",
        )
    if not 0 < price <= policy.max_gas_price_wei:
        raise DataError(
            "gas_price",
            f"market gas quote exceeds the configured price ceiling: quoted {price} wei, "
            f"max_gas_price_wei is {policy.max_gas_price_wei}",
        )
    cost = gas * price + op_exposure
    if cost > policy.max_transaction_fee.amount:
        # A quote, like the price: a lower price or L1 data fee can admit the same gas.
        raise DataError(
            "gas_fee_quote",
            f"transaction maximum cost exceeds its fee ceiling: {gas} gas at {price} wei"
            + (f" plus {op_exposure} L1 data and operator fees" if op_exposure else "")
            + f" costs up to {cost}, max_transaction_fee is {policy.max_transaction_fee.amount} "
            f"{policy.asset} base units",
        )


def validate_budget(state, operation, balance, gas, price, op_exposure=0):
    admitted = execution_gas(state.config)
    if admitted is None:
        raise ValueError("gas budget validation requires admitted execution policy")
    policy = admitted.policy(operation.account)
    validate_fee_limits(policy, gas, price, op_exposure)
    payer = state.config.accounts.binding(operation.account).profile.submitter
    spent = sum(
        o.gas_paid.amount
        for o in state.outcomes
        if getattr(o, "gas_paid", None) is not None
        and state.config.accounts.binding(o.operation.account).profile.submitter == payer
    )
    reserve = policy.exit_reserve.amount if increases_exposure(state.config, operation) else 0
    pending = state.pending
    candidate_fee = gas * price + op_exposure
    if pending is not None and pending.family is not None:
        entries = (
            *pending.family.candidates,
            *((pending.family.proposal,) if pending.family.proposal else ()),
        )
        candidate_fee = max(
            candidate_fee,
            *(gas * entry.gas_price + entry.fee_quote.additional_fee_exposure for entry in entries),
        )
    required = candidate_fee + reserve
    native_value = pending.execution.value if pending is not None and pending.execution else 0
    if policy.total_budget is not None and spent + required > policy.total_budget.amount:
        raise DataError(
            "gas_budget",
            "remaining budget cannot cover execution and exit reserve: "
            f"{spent} already spent + {candidate_fee} maximum fee + {reserve} exit_reserve "
            f"exceeds total_budget {policy.total_budget.amount} ({policy.asset} base units)",
        )
    if required + native_value > balance.amount:
        raise DataError(
            "gas_budget",
            "payer balance cannot cover execution and exit reserve: "
            f"{candidate_fee} maximum fee + {reserve} exit_reserve + {native_value} value "
            f"exceeds the payer's {balance.amount} ({policy.asset} base units)",
        )


def increases_exposure(config, operation):
    return connector_for(config).execution.increases_exposure(config, operation)


def validate_receipt_cost(config, pending, receipt):
    gas = int(receipt["gasUsed"], 16)
    price = int(receipt["effectiveGasPrice"], 16)
    chain = origin_chain(config, pending.operation.target.fund.chain)
    # Nitro can refund part of a legacy price bid. The admitted bid still
    # bounds payer exposure; Ethereum legacy receipts retain exact-price checks.
    valid_price = (
        0 <= price <= pending.gas_price
        if evm_network(chain.chain_id).legacy_price_is_ceiling
        else price == pending.gas_price
    )
    if not 0 < gas <= pending.gas or not valid_price:
        raise DataError("inconsistent", "receipt gas differs from the admitted legacy transaction")
