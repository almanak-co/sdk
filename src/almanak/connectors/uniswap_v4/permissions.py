"""Built-in Roles conditions over canonical, correlated periphery ABI templates.

Roles 2.1 uses one type tree for array members and OR branches. Uniswap action
arrays contain heterogeneous ABI blobs. Inspect their normalized wire words:
all pointers, lengths, actions, currencies and recipients are fixed; only the
explicitly declared numeric fields vary. No custom condition or wildcard call.
"""

from eth_abi import decode, encode
from eth_utils import keccak

from almanak.connectors.uniswap_v4 import abi
from almanak.connectors.uniswap_v4.authority import capability_codec
from almanak.connectors.uniswap_v4.models import Capabilities, Range
from almanak.connectors.v3_pool_domain import MAX_TICK, MIN_TICK
from almanak.gateway.permission_models import PermissionManifest
from almanak.gateway.roles_conditions import (
    Condition,
    FunctionGrant,
    any_amount,
    bounded,
    combine,
    either,
    equal,
    matches,
    positive,
    signed_at_least,
    signed_at_most,
)
from almanak.models import AssetAmount

MAXIMUM = 2**128 - 1
# Outside ordinary offsets and sizes. Ambiguous occurrences fail compilation,
# so a marker collision cannot widen an unintended ABI field.
MARKERS = tuple(int.from_bytes(keccak(text=f"uniswap-variable-{i}")[:16], "big") for i in range(5))


def bytes_equal(value: bytes):
    return Condition(parameter=2, operator=16, value="0x" + encode(("bytes",), (value,)).hex())


def encoded(*children):
    return matches(*children, parameter=6)


def array(*children):
    return matches(*children, parameter=4)


def zero_or(maximum):
    """Zero or a value up to a protocol width: call sanity, never an author amount."""
    return either(equal("uint256", 0), bounded(maximum))


def words_condition(payload: bytes, variables, positional=None):
    words = [int.from_bytes(payload[i : i + 32], "big") for i in range(0, len(payload), 32)]
    conditions = [equal("uint256", word) for word in words]
    for marker, replacements in variables.items():
        indices = [i for i, word in enumerate(words) if word == marker]
        if len(indices) != len(replacements):
            raise ValueError("ambiguous variable positions in Uniswap ABI template")
        for index, condition in zip(indices, replacements, strict=True):
            conditions[index] = condition
    for index, condition in (positional or {}).items():
        conditions[index] = condition
    return encoded(*conditions)


def grant(target, signature, condition, explanation):
    return FunctionGrant(
        target=target, signature=signature, condition=condition, explanation=(explanation,)
    )


def approval_grants(target):
    spenders = [target.contracts.router]
    if target.liquidity_enabled:
        spenders.append(target.contracts.position_manager)
    grants = []
    for asset in target.assets:
        grants.append(
            grant(
                asset.reference,
                "approve(address,uint256)",
                matches(equal("address", target.contracts.permit2), any_amount()),
                "ERC-20 allowance of any amount, including zero cleanup, to Permit2 only.",
            )
        )
        for spender in spenders:
            grants.append(
                grant(
                    target.contracts.permit2,
                    "approve(address,address,uint160,uint48)",
                    matches(
                        equal("address", asset.reference),
                        equal("address", spender),
                        any_amount(),
                        zero_or(2**48 - 1),
                    ),
                    "Permit2 allowance of any amount for this pool currency to the configured "
                    "router or position manager only; expiry freshness is a gateway check.",
                )
            )
    return grants


def swap_grant(target, direction):
    asset = target.assets[0 if direction else 1]
    call = abi.swap(target, AssetAmount(asset=asset, amount=MARKERS[0]), MARKERS[1], 1)
    commands, inputs, _ = decode(("bytes", "bytes[]", "uint256"), bytes.fromhex(call.data[10:]))
    condition = matches(bytes_equal(commands), array(*router_inputs(inputs)), bounded(MAXIMUM))
    return grant(
        target.contracts.router,
        abi.ROUTER_EXECUTE,
        condition,
        "One exact-input swap in the declared pool with fixed settlement currencies, paying "
        "the Safe. Correlated quote minima are gateway checks.",
    )


def router_inputs(inputs):
    """Word conditions per router input, carrying no author amount.

    The first input-amount word is the uint128 amountIn, or a native leg's Permit2 pull, and
    keeps the uint128 sanity bound; a later one, the settlement maximum, is any nonzero amount.
    Minima are nonzero uint128. An input without a variable is fixed byte for byte: its Dynamic
    type is compatible with the first input's encoded words.
    """
    children, first = [], True
    for payload in inputs:
        words = [int.from_bytes(payload[i : i + 32], "big") for i in range(0, len(payload), 32)]
        amounts, minima = words.count(MARKERS[0]), words.count(MARKERS[1])
        if not amounts and not minima:
            children.append(bytes_equal(payload))
            continue
        variables = {}
        if amounts:
            variables[MARKERS[0]] = tuple(
                bounded(MAXIMUM) if first and i == 0 else positive() for i in range(amounts)
            )
            first = False
        if minima:
            variables[MARKERS[1]] = (bounded(MAXIMUM),) * minima
        children.append(words_condition(payload, variables))
    if first:
        raise ValueError("Uniswap swap template carries no input amount")
    return children


def unlock_payload(call):
    payload, _ = decode(("bytes", "uint256"), bytes.fromhex(call.data[10:]))
    return payload


def word_values(payload):
    return [int.from_bytes(payload[i : i + 32], "big") for i in range(0, len(payload), 32)]


def modify_grant(target, call, variables, explanation, positional=None):
    return grant(
        target.contracts.position_manager,
        abi.MODIFY,
        matches(words_condition(unlock_payload(call), variables, positional), bounded(MAXIMUM)),
        explanation,
    )


def tick_words(target):
    """The unlockData word indices of tickLower and tickUpper in the mint template.

    Two templates that differ only in their ticks differ in exactly those two words;
    the extremal ranges are valid ticks for every pool and never equal each other.
    """
    payloads = [
        unlock_payload(abi.mint(target, MARKERS[0], MARKERS[1], MARKERS[2], 1, selected=bounds))
        for bounds in (Range(lower=-887272, upper=887272), Range(lower=-887270, upper=887270))
    ]
    if len(payloads[0]) != len(payloads[1]):
        raise ValueError("mint templates differ in layout, not only in ticks")
    differing = [
        index
        for index in range(0, len(payloads[0]), 32)
        if payloads[0][index : index + 32] != payloads[1][index : index + 32]
    ]
    if len(differing) != 2:
        raise ValueError("mint template does not isolate exactly two tick words")
    return differing[0] // 32, differing[1] // 32


def mint_grant(target):
    """A mint in the exact pool owned by the Safe, at any ticks and deposit amounts.

    Ticks are not scope (blueprint 27 section 4.3): the configured range or band is an
    economic choice the gateway enforces before submission, so a changed band needs no
    new permissions. The tick words admit the protocol's whole tick domain, which the
    pool enforces itself.
    """
    lower, upper = tick_words(target)
    template = Range(lower=MIN_TICK, upper=MAX_TICK)
    call = abi.mint(target, MARKERS[0], MARKERS[1], MARKERS[2], 1, selected=template)
    # A native leg's maximum is also the wrapped native the PositionManager pulls and unwraps.
    words = word_values(unlock_payload(call))
    variables = {
        MARKERS[0]: (bounded(MAXIMUM),),
        MARKERS[1]: (any_amount(),) * words.count(MARKERS[1]),
        MARKERS[2]: (any_amount(),) * words.count(MARKERS[2]),
    }
    return modify_grant(
        target,
        call,
        variables,
        "Liquidity mint in the exact pool, owned by the Safe, at any ticks and deposit "
        "amounts. The configured range or band, position width and tick ordering are "
        "gateway checks.",
        positional={lower: signed_at_least(MIN_TICK), upper: signed_at_most(MAX_TICK)},
    )


def exit_grants(target):
    token = Condition(parameter=1, operator=17, value="0x" + bytes(32).hex())
    explanation = (
        "Broad NFT exit: compatible NFTs owned or approved to this Safe; exact template "
        "and proceeds to Safe. Gateway additionally enforces retained NFT ownership."
    )
    return [
        modify_grant(
            target, abi.collect(target, MARKERS[0], 1), {MARKERS[0]: (token,)}, explanation
        ),
        modify_grant(
            target,
            abi.close(target, MARKERS[0], MARKERS[1], MARKERS[2], 1),
            {
                MARKERS[0]: (token,),
                MARKERS[1]: (zero_or(MAXIMUM),),
                MARKERS[2]: (zero_or(MAXIMUM),),
            },
            explanation,
        ),
    ]


def derive(profile, capabilities):
    grants = []
    for capability in capabilities.requirements:
        target = capability.target
        if target.fund != profile.fund:
            continue
        grants.extend(approval_grants(target))
        grants.extend(swap_grant(target, direction) for direction in (True, False))
        if target.liquidity_enabled:
            grants.append(mint_grant(target))
            grants.extend(exit_grants(target))
    return combine(grants)


class UniswapPermissions:
    codec = capability_codec()
    manifest_type = PermissionManifest[Capabilities]
    derive = staticmethod(derive)
    runtime_checks = (
        "Quote freshness, slippage, expiry and exact action/settlement amount correlation.",
        "Attributed inventory, the configured range or band, and exact retained NFT, "
        "pool and range ownership.",
        "Holding timers, operation dependencies and spending-authority cleanup.",
        "LP exits have a broader on-chain token-ID grant than gateway admission.",
    )

    @staticmethod
    def identities(config, account, reader):
        from almanak.connectors.uniswap_v4.deployments import identities

        return identities(config, account, reader)
