"""GM receipt accounting correlates exact accepted inputs, minted shares and share burns."""

from almanak.asset_amounts import AssetAmounts
from almanak.connectors.gmx_v2.liquidity_abi import vault
from almanak.connectors.gmx_v2.liquidity_operations import (
    CancelLiquidityRequest,
    CreateDeposit,
    CreateWithdrawal,
)
from almanak.connectors.gmx_v2.liquidity_reader import handler
from almanak.connectors.gmx_v2.receipts import (
    TRANSFER,
    check_values,
    event_evidence,
    one_protocol_event,
)
from almanak.connectors.gmx_v2.requests import Request
from almanak.dex_intents import LiquidityBudget
from almanak.gateway.ethereum import DataError
from almanak.gateway.evm_logs import data, events, topic_address
from almanak.models import ZERO_ADDRESS, AssetAmount


def completed_request(pending, receipt, block):
    operation, target = pending.operation, pending.operation.target
    kind = operation.request_kind
    name = kind.title() + (
        "Cancelled" if isinstance(operation, CancelLiquidityRequest) else "Created"
    )
    event = one_protocol_event(receipt, target, name, handler(target, kind))
    if isinstance(operation, CancelLiquidityRequest):
        previous = pending.before.request
        if previous is None or (previous.key, previous.kind) != (operation.request_key, kind):
            raise ValueError("GM cancellation requires its exact retained request key and kind")
        return previous.append(event_evidence(event, block, receipt, "cancelled"))
    validate_created(event, operation)
    escrow = (
        operation.amounts.root
        if isinstance(operation, CreateDeposit)
        else (operation.market_tokens,)
    )
    return Request(
        fund=target.fund,
        allocation=target.allocation,
        decision_identity=pending.decision.identity,
        kind=kind,
        key=event.topics[0],
        market=target.market.share_asset,
        escrow=escrow,
        execution_fee=operation.execution_fee,
        admission_deadline=operation.admission_deadline,
        events=(event_evidence(event, block, receipt, "accepted"),),
    )


def validate_created(event, operation):
    target = operation.target
    addresses = {
        "account": target.fund.address,
        "receiver": target.fund.address,
        "callbackContract": ZERO_ADDRESS,
        "uiFeeReceiver": ZERO_ADDRESS,
        "market": target.market.share_asset.reference,
    }
    numbers = {
        "uiFeeFactor": 0,
        "executionFee": operation.execution_fee.amount,
        "callbackGasLimit": 0,
        "srcChainId": 0,
    }
    if isinstance(operation, CreateDeposit):
        addresses.update(
            initialLongToken=target.market.long_asset.reference,
            initialShortToken=target.market.short_asset.reference,
        )
        numbers.update(
            initialLongTokenAmount=operation.amounts.root[0].amount,
            initialShortTokenAmount=operation.amounts.root[1].amount,
            minMarketTokens=operation.minimum_market_tokens.amount,
            depositType=0,
        )
    else:
        numbers.update(
            marketTokenAmount=operation.market_tokens.amount,
            minLongTokenAmount=operation.minimum_outputs.root[0].amount,
            minShortTokenAmount=operation.minimum_outputs.root[1].amount,
            withdrawalType=0,
        )
    check_values(event, "address", addresses)
    check_values(event, "uint256", numbers)
    check_values(event, "bool", {"shouldUnwrapNativeToken": False})
    if (
        event.array("address", "longTokenSwapPath")
        or event.array("address", "shortTokenSwapPath")
        or event.array("bytes32", "dataList")
    ):
        raise DataError("receipt", "GM creation must retain empty routes and local-only behavior")


def economic_operation(decision, request):
    intent, target = decision.intent, decision.intent.target
    common = {
        "target": target,
        "execution_fee": request.execution_fee,
        "admission_deadline": intent.deadline,
    }
    if isinstance(intent, LiquidityBudget):
        minimum = request.events[0].event().value("uint256", "minMarketTokens")
        return CreateDeposit(
            **common,
            amounts=AssetAmounts((intent.maximum0, intent.maximum1)),
            minimum_market_tokens=AssetAmount(asset=target.market.share_asset, amount=minimum),
        )
    return CreateWithdrawal(
        **common,
        market_tokens=request.escrow[0],
        minimum_outputs=AssetAmounts((intent.minimum0, intent.minimum1)),
    )


def validate_request(operation, request):
    escrow = (
        operation.amounts.root
        if isinstance(operation, CreateDeposit)
        else (operation.market_tokens,)
    )
    if (request.kind, request.escrow, request.execution_fee, request.admission_deadline) != (
        operation.request_kind,
        escrow,
        operation.execution_fee,
        operation.admission_deadline,
    ):
        raise DataError(
            "receipt", "GM request changed its retained escrow, fee or admission deadline"
        )


def minted(operation, request, receipt):
    target = operation.target
    event = one_protocol_event(receipt, target, "DepositExecuted", handler(target, "deposit"))
    check_values(event, "address", {"account": target.fund.address})
    check_values(event, "bytes32", {"key": request.key})
    check_values(
        event,
        "uint256",
        {
            "longTokenAmount": operation.amounts.root[0].amount,
            "shortTokenAmount": operation.amounts.root[1].amount,
            "swapPricingType": 3,
        },
    )
    shares = event.value("uint256", "receivedMarketTokens")
    if shares < operation.minimum_market_tokens.amount:
        raise DataError("receipt", "GM minted shares are below the admitted share-asset minimum")
    return shares


def share_effect(operation, request, receipt):
    target, status = operation.target, request.status
    expected, pair, delta = 0, None, 0
    if isinstance(operation, CreateDeposit) and status == "executed":
        expected = minted(operation, request, receipt)
        pair, delta = (ZERO_ADDRESS, target.fund.address), expected
    elif isinstance(operation, CreateWithdrawal):
        expected, pair, delta = withdrawal_share_effect(operation, status)
    total = share_transfers(receipt, target, pair)
    if total != expected:
        raise DataError("receipt", "GM mint, escrow, burn or refund differs from its exact request")
    return delta


def withdrawal_share_effect(operation, status):
    target, amount = operation.target, operation.market_tokens.amount
    store = target.market.contracts.withdrawal_vault
    if status == "accepted":
        return amount, (target.fund.address, store), -amount
    if status == "executed":
        return amount, (store, ZERO_ADDRESS), 0
    if status == "cancelled":
        return amount, (store, target.fund.address), amount
    return 0, None, 0


def share_transfers(receipt, target, pair):
    total = 0
    for log in events(receipt, target.market.share_asset.reference, TRANSFER):
        if len(log["topics"]) != 3 or tuple(topic_address(t) for t in log["topics"][1:]) != pair:
            raise DataError(
                "receipt", "GM share transfer changed its admitted mint, escrow or burn path"
            )
        total += data(log, ("uint256",))[0]
    return total


def validate_cash(operation, request, receipt, assets):
    validate_request(operation, request)
    expected = expected_cash(operation, request)
    if request.status == "executed" and isinstance(operation, CreateWithdrawal):
        event = one_protocol_event(
            receipt, operation.target, "WithdrawalExecuted", handler(operation.target, "withdrawal")
        )
        check_values(event, "uint256", {"swapPricingType": 4})
        check_values(event, "bytes32", {"key": request.key})
        check_values(event, "address", {"account": operation.target.fund.address})
        for minimum in operation.minimum_outputs.root:
            actual = next(value.amount for value in assets if value.key.asset == minimum.asset)
            if actual < minimum.amount:
                raise DataError(
                    "receipt", "GM withdrawal output is below its admitted asset minimum"
                )
            expected[minimum.asset] = actual
    if any(
        value.amount != expected.get(value.key.asset, 0)
        for value in assets
        if value.key.asset.namespace != "slip44"
    ):
        raise DataError(
            "receipt", "GM currency effects differ from exact request escrow or outputs"
        )
    validate_currency_paths(operation, request, receipt)


def expected_cash(operation, request):
    if request.status == "accepted":
        return {
            value.asset: -value.amount
            for value in operation.inputs
            if value.asset != operation.target.market.share_asset
        }
    if request.status == "cancelled" and isinstance(operation, CreateDeposit):
        return {value.asset: value.amount for value in operation.amounts.root}
    return {}


def validate_currency_paths(operation, request, receipt):
    target = operation.target
    pair = None
    if request.status == "accepted":
        pair = (target.fund.address, vault(target, request.kind))
    elif request.status == "cancelled" and request.kind == "deposit":
        pair = (vault(target, request.kind), target.fund.address)
    elif request.status == "executed" and request.kind == "withdrawal":
        pair = (target.market.share_asset.reference, target.fund.address)
    for asset in dict.fromkeys((*target.assets, target.market.wrapped_native)):
        for log in events(receipt, asset.reference, TRANSFER):
            parties = tuple(topic_address(topic) for topic in log["topics"][1:])
            if target.fund.address in parties and parties != pair:
                raise DataError(
                    "receipt", "GM currency transfer differs from its admitted vault or market path"
                )
