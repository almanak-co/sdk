"""A synthetic GMX chain that answers canonical reads from invented DataStore state.

Nothing here is captured chain evidence. The reader records every request and
response exactly as the historical gateway does, so tests can prove that a
reader's canonical requests re-decode through the provider-free replay path.
"""

from eth_abi import encode
from eth_utils import keccak

from almanak.backtest.dataset import RawEvidence
from almanak.backtest.history import ReadEvidence, ReadRequest
from almanak.connectors.gmx_v2.economic_position_reader import SIDE_RISK_KEYS
from almanak.connectors.gmx_v2.economic_valuation_reader import IMPACT_FIELDS, SIDE_FIELDS
from almanak.connectors.gmx_v2.oracle_reader import key
from almanak.gateway.ethereum import DataError, EthereumReader
from almanak.gateway.evm import chain_number
from almanak.models import ZERO_ADDRESS
from tests import gmx_synthetic_frames as synthetic

HOLDING = "0x" + "6a" * 20
REFERRAL_STORAGE = "0x" + "6b" * 20
AFFILIATE = "0x" + "6c" * 20
REFERRAL_CODE = b"\x6d" * 32
GETTERS = {
    keccak(text=f"get{kind}(bytes32)")[:4]: (kind, output, default)
    for kind, output, default in (
        ("Uint", "uint256", 0),
        ("Int", "int256", 0),
        ("Bool", "bool", False),
        ("Bytes32", "bytes32", bytes(32)),
        ("Address", "address", ZERO_ADDRESS),
    )
}


class SyntheticReader(EthereumReader):
    """Serve invented state through the exact RPC shapes the readers request."""

    def __init__(self, market, blocks):
        super().__init__(market.chain, "http://unused.invalid")
        self.market = market
        self.headers = {block.number: block for block in blocks}
        self.store: dict[bytes, tuple[str, object]] = {}
        self.answers: dict[tuple[str, str, str | None], str] = {}
        self.cache: dict[str, ReadEvidence] = {}
        self.used: set[str] = set()
        self.provider_attempts = 0

    def set(self, kind, name, types=(), values=(), *, value):
        self.store[key(name, types, values)] = (kind, value)

    def answer(self, to, signature, inputs, args, outputs, result, block=None):
        data = "0x" + (keccak(text=signature)[:4] + encode(inputs, args)).hex()
        anchor = None if block is None else block.hash
        self.answers[(to, data, anchor)] = "0x" + encode(outputs, result).hex()

    def _rpc(self, method, params, *, allow_none=False):
        request = ReadRequest(chain=self.chain, method=method, params=tuple(params))
        if request.key not in self.cache:
            self.provider_attempts += 1
            result = self.respond(method, params)
            self.cache[request.key] = ReadEvidence(request=request, result=result, retrieved_at=1.0)
        self.used.add(request.key)
        return self.cache[request.key].result

    def header(self, block):
        return {"number": hex(block.number), "hash": block.hash, "timestamp": hex(block.timestamp)}

    def respond(self, method, params):
        if method == "eth_chainId":
            return hex(chain_number(self.chain.chain_id))
        if method == "eth_getBlockByNumber":
            selector = params[0]
            number = max(self.headers) if selector == "finalized" else int(selector, 16)
            return self.header(self.headers[number])
        if method == "eth_call":
            return self.answer_call(params[0]["to"], params[0]["data"], params[1]["blockHash"])
        raise DataError("unavailable", f"synthetic chain does not answer {method}")

    def answer_call(self, to, data, block_hash):
        if block_hash not in {block.hash for block in self.headers.values()}:
            raise DataError("unavailable", "synthetic call anchored outside retained headers")
        raw = bytes.fromhex(data[2:])
        if to == self.market.contracts.data_store and raw[:4] in GETTERS:
            kind, output, default = GETTERS[raw[:4]]
            stored_kind, value = self.store.get(raw[4:36], (kind, default))
            if stored_kind != kind:
                raise DataError("malformed", "synthetic DataStore key read through another getter")
            return "0x" + encode((output,), (value,)).hex()
        for anchor in (block_hash, None):
            if (to, data, anchor) in self.answers:
                return self.answers[(to, data, anchor)]
        raise DataError("unavailable", f"synthetic chain has no answer for {to} {data[:10]}")

    def evidence(self):
        return tuple(RawEvidence(body=self.cache[k].model_dump_json()) for k in sorted(self.used))


class Retained:
    """The minimal dataset surface ReplayReader needs: retained raw responses by digest."""

    def __init__(self, raw):
        self.raw = {value.digest: value for value in raw}

    def evidence(self, digest):
        return self.raw[digest]


def populate(reader, opportunity):
    """Write every DataStore key and contract answer the perpetual readers consult."""
    market = opportunity.risk.market
    share, long, short, index = (
        market.share_asset.reference,
        market.long_asset.reference,
        market.short_asset.reference,
        market.index_asset.reference,
    )
    reader.answer(
        market.contracts.reader,
        "getMarket(address,address)",
        ("address", "address"),
        (market.contracts.data_store, share),
        ("(address,address,address,address)",),
        ((share, index, long, short),),
    )
    accounting, spot = opportunity.accounting, opportunity.accounting.spot
    for currency in (spot.long, spot.short):
        token = currency.price.asset.reference
        reader.set(
            "Uint",
            "POOL_AMOUNT",
            ("address", "address"),
            (share, token),
            value=currency.pool_amount,
        )
        reader.set(
            "Uint",
            "SWAP_IMPACT_POOL_AMOUNT",
            ("address", "address"),
            (share, token),
            value=currency.impact_pool_amount,
        )
    for positive, factor in (
        (True, spot.impact.positive_factor),
        (False, spot.impact.negative_factor),
    ):
        reader.set(
            "Uint", "SWAP_IMPACT_FACTOR", ("address", "bool"), (share, positive), value=factor
        )
    reader.set(
        "Uint", "SWAP_IMPACT_EXPONENT_FACTOR", ("address",), (share,), value=spot.impact.exponent
    )
    for positive, factor in ((True, spot.fees.positive_factor), (False, spot.fees.negative_factor)):
        reader.set("Uint", "SWAP_FEE_FACTOR", ("address", "bool"), (share, positive), value=factor)
    reader.set("Uint", "SWAP_FEE_RECEIVER_FACTOR", value=spot.fees.receiver_factor)
    for is_long, side, interest in (
        (True, accounting.long, opportunity.funding.long_interest),
        (False, accounting.short, opportunity.funding.short_interest),
    ):
        for field, name in SIDE_FIELDS.items():
            reader.set(
                "Uint", name, ("address", "bool"), (share, is_long), value=getattr(side, field)
            )
        reader.set(
            "Uint",
            "MAX_PNL_FACTOR",
            ("bytes32", "address", "bool"),
            (key("MAX_PNL_FACTOR_FOR_TRADERS"), share, is_long),
            value=side.maximum_pnl_factor,
        )
        tokens_long = side.open_interest_tokens * 3 // 5
        for token, usd, tokens in (
            (long, interest.long, tokens_long),
            (short, interest.short, side.open_interest_tokens - tokens_long),
        ):
            arguments = ("address", "address", "bool"), (share, token, is_long)
            reader.set("Uint", "OPEN_INTEREST", *arguments, value=usd)
            reader.set("Uint", "OPEN_INTEREST_IN_TOKENS", *arguments, value=tokens)
    reader.set(
        "Bool",
        "SKIP_BORROWING_FEE_FOR_SMALLER_SIDE",
        value=accounting.skip_borrowing_for_smaller_side,
    )
    reader.set(
        "Bool",
        "USE_OPEN_INTEREST_IN_TOKENS_FOR_BALANCE",
        value=accounting.use_open_interest_tokens_for_balance,
    )
    reader.set(
        "Uint", "BORROWING_FEE_RECEIVER_FACTOR", value=accounting.borrowing_fee_receiver_factor
    )
    for field, name in IMPACT_FIELDS.items():
        reader.set("Uint", name, ("address",), (share,), value=getattr(accounting.impact, field))
    reader.answer(share, "totalSupply()", (), (), ("uint256",), (accounting.share_supply,))
    pricing, impact = opportunity.impact.pricing, opportunity.impact
    for name, positive, negative in (
        ("POSITION_IMPACT_FACTOR", pricing.impact.positive_factor, pricing.impact.negative_factor),
        (
            "POSITION_IMPACT_EXPONENT_FACTOR",
            pricing.impact.positive_exponent,
            pricing.impact.negative_exponent,
        ),
        (
            "MAX_POSITION_IMPACT_FACTOR",
            pricing.impact.maximum_positive_factor,
            impact.maximum_negative_factor,
        ),
    ):
        reader.set("Uint", name, ("address", "bool"), (share, True), value=positive)
        reader.set("Uint", name, ("address", "bool"), (share, False), value=negative)
    reader.set(
        "Uint",
        "MAX_LENDABLE_IMPACT_FACTOR",
        ("address",),
        (share,),
        value=impact.maximum_lendable_factor,
    )
    reader.set(
        "Uint",
        "MAX_LENDABLE_IMPACT_USD",
        ("address",),
        (share,),
        value=impact.maximum_lendable_usd_e30,
    )
    populate_funding(reader, opportunity, share, long, short)
    populate_fees_and_risk(reader, opportunity, share)
    for token in (opportunity.solvency.long, opportunity.solvency.short):
        reader.answer(
            token.asset.reference,
            "balanceOf(address)",
            ("address",),
            (share,),
            ("uint256",),
            (token.balance,),
        )
    return reader


def populate_funding(reader, opportunity, share, long, short):
    funding = opportunity.funding
    names = {
        "factor": "FUNDING_FACTOR",
        "exponent": "FUNDING_EXPONENT_FACTOR",
        "increase_per_second": "FUNDING_INCREASE_FACTOR_PER_SECOND",
        "minimum_increase_per_second": "MIN_FUNDING_INCREASE_RATE_PER_SECOND",
        "decrease_per_second": "FUNDING_DECREASE_FACTOR_PER_SECOND",
        "stable_threshold": "THRESHOLD_FOR_STABLE_FUNDING",
        "decrease_threshold": "THRESHOLD_FOR_DECREASE_FUNDING",
    }
    for field, name in names.items():
        reader.set("Uint", name, ("address",), (share,), value=getattr(funding.parameters, field))
    for longs_pay in (True, False):
        bounds = funding.parameters.bounds(longs_pay)
        for field, name in (
            ("minimum_per_second", "MIN_FUNDING_FACTOR_PER_SECOND"),
            ("maximum_per_second", "MAX_FUNDING_FACTOR_PER_SECOND"),
        ):
            reader.set(
                "Uint", name, ("address", "bool"), (share, longs_pay), value=getattr(bounds, field)
            )
    reader.set("Uint", "FUNDING_UPDATED_AT", ("address",), (share,), value=funding.updated_at)
    reader.set(
        "Int",
        "SAVED_FUNDING_FACTOR_PER_SECOND",
        ("address",),
        (share,),
        value=funding.saved_per_second,
    )
    for side, is_long in (("long", True), ("short", False)):
        for field, name in (
            ("payments", "FUNDING_FEE_AMOUNT_PER_SIZE"),
            ("claims", "CLAIMABLE_FUNDING_AMOUNT_PER_SIZE"),
        ):
            values = getattr(opportunity.funding_indexes, f"{side}_{field}")
            for token, amount in ((long, values.long), (short, values.short)):
                reader.set(
                    "Uint",
                    name,
                    ("address", "address", "bool"),
                    (share, token, is_long),
                    value=amount,
                )


def populate_fees_and_risk(reader, opportunity, share):
    fees, risk, market = opportunity.fees, opportunity.risk, opportunity.risk.market
    reader.set(
        "Uint",
        "POSITION_FEE_FACTOR",
        ("address", "bool"),
        (share, True),
        value=fees.improved.position_factor,
    )
    reader.set(
        "Uint",
        "POSITION_FEE_FACTOR",
        ("address", "bool"),
        (share, False),
        value=fees.worsened.position_factor,
    )
    reader.set("Uint", "POSITION_FEE_RECEIVER_FACTOR", value=fees.improved.position_receiver_factor)
    reader.set(
        "Uint", "LIQUIDATION_FEE_RECEIVER_FACTOR", value=fees.improved.liquidation_receiver_factor
    )
    reader.set(
        "Uint",
        "LIQUIDATION_FEE_FACTOR",
        ("address",),
        (share,),
        value=fees.improved.liquidation_factor,
    )
    reader.answer(
        market.contracts.order_handler, "referralStorage()", (), (), ("address",), (ZERO_ADDRESS,)
    )
    if risk.holding_address_available:
        reader.set("Address", "HOLDING_ADDRESS", value=HOLDING)
    for name, value in (
        ("REQUEST_EXPIRATION_TIME", risk.request_expiration_seconds),
        ("MAX_ORACLE_PRICE_AGE", risk.maximum_oracle_age_seconds),
        ("MIN_POSITION_SIZE_USD", risk.minimum_position_usd_e30),
        ("MIN_COLLATERAL_USD", risk.minimum_collateral_usd_e30),
    ):
        reader.set("Uint", name, value=value)
    for name, value in (
        ("MIN_COLLATERAL_FACTOR", risk.minimum_collateral_factor),
        ("MIN_COLLATERAL_FACTOR_FOR_LIQUIDATION", risk.liquidation_collateral_factor),
        ("MAX_POSITION_IMPACT_FACTOR_FOR_LIQUIDATIONS", risk.maximum_liquidation_impact_factor),
    ):
        reader.set("Uint", name, ("address",), (share,), value=value)
    for is_long, side in ((True, risk.long), (False, risk.short)):
        for field, name in SIDE_RISK_KEYS.items():
            reader.set(
                "Uint", name, ("address", "bool"), (share, is_long), value=getattr(side, field)
            )


def register_referral(reader, target, *, rebate, share, custom=0, minimum, pro_tier=0, pro=0):
    """Register a synthetic referral code and pro tier for the target's fund address."""
    market = reader.market
    reader.answer(
        market.contracts.order_handler,
        "referralStorage()",
        (),
        (),
        ("address",),
        (REFERRAL_STORAGE,),
    )
    reader.answer(
        REFERRAL_STORAGE,
        "traderReferralCodes(address)",
        ("address",),
        (target.fund.address,),
        ("bytes32",),
        (REFERRAL_CODE,),
    )
    reader.answer(
        REFERRAL_STORAGE,
        "codeOwners(bytes32)",
        ("bytes32",),
        (REFERRAL_CODE,),
        ("address",),
        (AFFILIATE,),
    )
    reader.answer(
        REFERRAL_STORAGE, "referrerTiers(address)", ("address",), (AFFILIATE,), ("uint256",), (1,)
    )
    reader.answer(
        REFERRAL_STORAGE,
        "tiers(uint256)",
        ("uint256",),
        (1,),
        ("uint256", "uint256"),
        (rebate, share),
    )
    reader.answer(
        REFERRAL_STORAGE,
        "referrerDiscountShares(address)",
        ("address",),
        (AFFILIATE,),
        ("uint256",),
        (custom,),
    )
    reader.set("Uint", "MIN_AFFILIATE_REWARD_FACTOR", ("uint256",), (1,), value=minimum)
    if pro_tier:
        reader.set("Uint", "PRO_TRADER_TIER", ("address",), (target.fund.address,), value=pro_tier)
        reader.set("Uint", "PRO_DISCOUNT_FACTOR", ("uint256",), (pro_tier,), value=pro)


def perpetual_chain(name="arbitrum"):
    """A synthetic chain whose reads reproduce ``gmx_synthetic_frames.perpetual_frame``."""
    target = synthetic.perpetual_target(name)
    at = synthetic.block(target.market)
    following = at.model_copy(
        update={"number": at.number + 1, "hash": "0x" + "ac" * 32, "timestamp": at.timestamp + 1}
    )
    reader = SyntheticReader(target.market, (at, following))
    populate(reader, synthetic.perpetual_opportunity(target.market, at))
    return reader, target, at
