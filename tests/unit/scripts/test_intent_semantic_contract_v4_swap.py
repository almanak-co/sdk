"""Positive and negative controls for the Uniswap V4 swap semantic contracts.

Every payload here is synthetic, so these prove what the validator refuses on a
given log set -- not that any real receipt is authentic. The shapes are taken
from a sealed Robinhood receipt: the pool pays native, the wrapper mints to the
router, and the router forwards to the wallet.

The two profiles are deliberately separate. ``v4_swap.v1`` is ERC-20 only and
requires the pool's currencies to be exactly the wallet's assets; because
WRAP_ETH is 1:1, relaxing that would let a native swap pass as a swap in the
wrapped pool of the same pair. ``v4_swap_route.v1`` covers the converting route
and proves the conversion instead of assuming it.
"""

from __future__ import annotations

from typing import Any

import pytest
from eth_abi import encode
from eth_utils import keccak

from qa_lab.intent_semantic_contract import validate_semantic_contract

CHAIN = "robinhood"
ZERO = "0x" + "00" * 20
WETH = "0x0bd7d308f8e1639fab988df18a8011f41eacad73"
USDG = "0x5fc5360d0400a0fd4f2af552add042d716f1d168"
POOL_MANAGER = "0x8366a39cc670b4001a1121b8f6a443a643e40951"
ROUTER = "0x8876789976decbfcbbbe364623c63652db8c0904"
PERMIT2 = "0x000000000022d473030f116ddee9f6b43ac78ba3"
ACCOUNT = "0x" + "11" * 20
STRANGER = "0x" + "33" * 20

POOL_KEY_TYPE = "(address,address,uint24,int24,address)"
TRANSFER_TOPIC = "0x" + keccak(text="Transfer(address,address,uint256)").hex()
SWAP_TOPIC = "0x" + keccak(text="Swap(bytes32,address,int128,int128,uint160,uint128,int24,uint24)").hex()
DEPOSIT_TOPIC = "0x" + keccak(text="Deposit(address,uint256)").hex()
EXECUTE_SELECTOR = keccak(text="execute(bytes,bytes[],uint256)")[:4].hex()

ERC20_KEY = (WETH, USDG, 3000, 60, ZERO)
NATIVE_KEY = (ZERO, USDG, 3000, 60, ZERO)

AMOUNT_IN = 10_000_000
POOL_PAYOUT = 3_994_199_473_556_767
# 0.80 * quote: what a router leaking max_slippage forwards instead.
MIN_OUT = 3_195_359_578_845_413


def pool_id(key: tuple[Any, ...]) -> str:
    return "0x" + keccak(encode([POOL_KEY_TYPE], [key])).hex()


def _topic(address: str) -> str:
    return "0x" + "00" * 12 + address[2:]


def _twos(value: int) -> int:
    return (value + (1 << 256)) % (1 << 256)


def router_call(
    key: tuple[Any, ...],
    minimum_out: int,
    *,
    amount_in: int = AMOUNT_IN,
    commands: tuple[int, ...] = (0x10, 0x0B),
    min_hop_price_x36: int | None = 0,
) -> dict[str, Any]:
    # USDG is currency1 in both pools, so a USDG-in swap is one-for-zero -- the
    # connector derives this the same way, from the input's position in the key.
    zero_for_one = USDG == key[0]
    # Robinhood's router is a fork whose ExactInputSingleParams carries an extra
    # uint256 minHopPriceX36; min_hop_price_x36=None builds the upstream shape,
    # which this router cannot decode.
    if min_hop_price_x36 is None:
        swap = encode(
            [f"({POOL_KEY_TYPE},bool,uint128,uint128,bytes)"],
            [(key, zero_for_one, amount_in, minimum_out, b"")],
        )
    else:
        swap = encode(
            [f"({POOL_KEY_TYPE},bool,uint128,uint128,uint256,bytes)"],
            [(key, zero_for_one, amount_in, minimum_out, min_hop_price_x36, b"")],
        )
    inner = encode(["bytes", "bytes[]"], [bytes([0x06, 0x0B, 0x0E]), [swap]])
    outer = encode(["bytes", "bytes[]", "uint256"], [bytes(commands), [inner], 2_000_000_000])
    return {"to": ROUTER, "value": 0, "data": "0x" + EXECUTE_SELECTOR + outer.hex()}


def transfer(token: str, source: str, destination: str, amount: int) -> dict[str, Any]:
    return {
        "address": token,
        "topics": [TRANSFER_TOPIC, _topic(source), _topic(destination)],
        "data": "0x" + format(amount, "064x"),
    }


def deposit(destination: str, amount: int) -> dict[str, Any]:
    return {
        "address": WETH,
        "topics": [DEPOSIT_TOPIC, _topic(destination)],
        "data": "0x" + format(amount, "064x"),
    }


def mint(destination: str, amount: int) -> dict[str, Any]:
    """Robinhood's WETH wraps by minting and emits no Deposit event."""
    return transfer(WETH, ZERO, destination, amount)


def swap_log(key: tuple[Any, ...], amount0: int, amount1: int, *, sender: str = ROUTER) -> dict[str, Any]:
    return {
        "address": POOL_MANAGER,
        "topics": [SWAP_TOPIC, pool_id(key), _topic(sender)],
        "data": "0x" + format(_twos(amount0), "064x") + format(_twos(amount1), "064x") + "00" * 32 * 4,
    }


def payload(
    profile: str, logs: list[dict[str, Any]], *, target_reference: str = "WETH", **overrides: Any
) -> dict[str, Any]:
    contract: dict[str, Any] = {
        "schema_version": 1,
        "profile": profile,
        "intent": "SWAP",
        "account": ACCOUNT,
        "asset_address": USDG,
        "asset_decimals": 6,
        "output_asset_address": WETH,
        "output_asset_decimals": 18,
        "resource_address": POOL_MANAGER,
        "permit2_address": PERMIT2,
        "wrapper_address": WETH,
        "requested_amount_raw": str(AMOUNT_IN),
        "parser_amount_raw": str(AMOUNT_IN),
        "wallet_before_raw": str(AMOUNT_IN),
        "wallet_after_raw": "0",
        "output_wallet_before_raw": "0",
    }
    contract.update(overrides)
    return {
        "chain": CHAIN,
        "protocol": "uniswap_v4",
        "intent": "SWAP",
        "raw_receipt": {"logs": logs},
        "source_request": {
            "schema_version": 1,
            "captured_by": "compiler_observer",
            "intent": "SWAP",
            "asset_reference": "USDG",
            "target_asset_reference": target_reference,
            "amount": "10",
        },
        "semantic_contract": contract,
    }


ERC20_CONTRACT = {
    "currency0": WETH,
    "currency1": USDG,
    "fee_tier": 3000,
    "tick_spacing": 60,
    "hooks": ZERO,
    "pool_id": pool_id(ERC20_KEY),
    "parser_output_amount_raw": str(POOL_PAYOUT),
    "output_wallet_after_raw": str(POOL_PAYOUT),
    "compiled_calls": [router_call(ERC20_KEY, MIN_OUT)],
}

ROUTE_CONTRACT = {
    "currency0": ZERO,
    "currency1": USDG,
    "fee_tier": 3000,
    "tick_spacing": 60,
    "hooks": ZERO,
    "pool_id": pool_id(NATIVE_KEY),
    "compiled_calls": [router_call(NATIVE_KEY, MIN_OUT)],
}

ERC20_SWAP = swap_log(ERC20_KEY, POOL_PAYOUT, -AMOUNT_IN)
ROUTE_SWAP = swap_log(NATIVE_KEY, POOL_PAYOUT, -AMOUNT_IN)


def test_erc20_pool_swap_is_rederived_from_the_pool_managers_own_event() -> None:
    result = validate_semantic_contract(
        payload(
            "v4_swap.v1",
            [
                ERC20_SWAP,
                transfer(USDG, ACCOUNT, POOL_MANAGER, AMOUNT_IN),
                transfer(WETH, POOL_MANAGER, ACCOUNT, POOL_PAYOUT),
            ],
            **ERC20_CONTRACT,
        ),
        expected_profile="v4_swap.v1",
    )
    assert result["status"] == "VERIFIED"
    assert result["facts"]["pool_id"] == pool_id(ERC20_KEY)


@pytest.mark.parametrize(
    ("name", "logs", "overrides", "message"),
    [
        (
            "output removed again in the same receipt",
            [
                ERC20_SWAP,
                transfer(USDG, ACCOUNT, POOL_MANAGER, AMOUNT_IN),
                transfer(WETH, POOL_MANAGER, ACCOUNT, POOL_PAYOUT),
                transfer(WETH, ACCOUNT, STRANGER, POOL_PAYOUT),
            ],
            {},
            "net wallet flow",
        ),
        (
            "swap unlocked by an arbitrary caller",
            [
                swap_log(ERC20_KEY, POOL_PAYOUT, -AMOUNT_IN, sender=STRANGER),
                transfer(USDG, ACCOUNT, POOL_MANAGER, AMOUNT_IN),
                transfer(WETH, POOL_MANAGER, ACCOUNT, POOL_PAYOUT),
            ],
            {},
            "committed UniversalRouter",
        ),
        (
            "input never reaches the pool",
            [
                ERC20_SWAP,
                transfer(USDG, ACCOUNT, STRANGER, AMOUNT_IN),
                transfer(WETH, POOL_MANAGER, ACCOUNT, POOL_PAYOUT),
            ],
            {},
            "uncommitted counterparty",
        ),
        (
            "compiled calldata routed a different pool",
            [
                ERC20_SWAP,
                transfer(USDG, ACCOUNT, POOL_MANAGER, AMOUNT_IN),
                transfer(WETH, POOL_MANAGER, ACCOUNT, POOL_PAYOUT),
            ],
            {"compiled_calls": [router_call(NATIVE_KEY, MIN_OUT)]},
            "routed a different pool",
        ),
    ],
)
def test_erc20_pool_swap_refuses(
    name: str, logs: list[dict[str, Any]], overrides: dict[str, Any], message: str
) -> None:
    contract = {**ERC20_CONTRACT, **overrides}
    with pytest.raises(ValueError, match=message):
        validate_semantic_contract(payload("v4_swap.v1", logs, **contract), expected_profile="v4_swap.v1")


@pytest.mark.parametrize("wrap", [deposit, mint], ids=["deposit-event-wrapper", "mint-transfer-wrapper"])
def test_converting_route_verifies_when_the_whole_payout_is_delivered(wrap: Any) -> None:
    """Both wrapper dialects: canonical WETH9 emits Deposit, Robinhood's mints."""
    result = validate_semantic_contract(
        payload(
            "v4_swap_route.v1",
            [
                ROUTE_SWAP,
                transfer(USDG, ACCOUNT, POOL_MANAGER, AMOUNT_IN),
                wrap(ROUTER, POOL_PAYOUT),
                transfer(WETH, ROUTER, ACCOUNT, POOL_PAYOUT),
            ],
            **ROUTE_CONTRACT,
            parser_output_amount_raw=str(POOL_PAYOUT),
            output_wallet_after_raw=str(POOL_PAYOUT),
        ),
        expected_profile="v4_swap_route.v1",
    )
    assert result["status"] == "VERIFIED"
    assert result["facts"]["pool_native_payout_raw"] == str(POOL_PAYOUT)


@pytest.mark.parametrize("wrap", [deposit, mint], ids=["deposit-event-wrapper", "mint-transfer-wrapper"])
def test_converting_route_refuses_a_router_that_forwards_only_the_minimum(wrap: Any) -> None:
    """The live defect: the wallet is credited amountOutMinimum, not the payout.

    Stated as an equality on purpose. An inequality against the enforced minimum
    is satisfied by a router that keeps the difference, which is the shape this
    profile exists to see.
    """
    with pytest.raises(ValueError, match="did not conserve the pool payout"):
        validate_semantic_contract(
            payload(
                "v4_swap_route.v1",
                [
                    ROUTE_SWAP,
                    transfer(USDG, ACCOUNT, POOL_MANAGER, AMOUNT_IN),
                    wrap(ROUTER, MIN_OUT),
                    transfer(WETH, ROUTER, ACCOUNT, MIN_OUT),
                ],
                **ROUTE_CONTRACT,
                parser_output_amount_raw=str(POOL_PAYOUT),
                output_wallet_after_raw=str(MIN_OUT),
            ),
            expected_profile="v4_swap_route.v1",
        )


def test_converting_route_refuses_a_native_sweep_that_delivers_no_wrapped_asset() -> None:
    """Sweeping native instead of wrapping satisfies no WETH-out intent."""
    with pytest.raises(ValueError, match="did not conserve the pool payout"):
        validate_semantic_contract(
            payload(
                "v4_swap_route.v1",
                [ROUTE_SWAP, transfer(USDG, ACCOUNT, POOL_MANAGER, AMOUNT_IN)],
                **ROUTE_CONTRACT,
                parser_output_amount_raw=str(POOL_PAYOUT),
                output_wallet_after_raw=str(POOL_PAYOUT),
            ),
            expected_profile="v4_swap_route.v1",
        )


def test_converting_route_refuses_a_wrapped_payout_delivered_by_a_stranger() -> None:
    """Conservation is an equality over totals, so a wash pair nets the same.

    Without binding the delivered side's counterparties, the wallet's gain need
    not have come from this pool at all.
    """
    with pytest.raises(ValueError, match="uncommitted counterparty"):
        validate_semantic_contract(
            payload(
                "v4_swap_route.v1",
                [
                    ROUTE_SWAP,
                    transfer(USDG, ACCOUNT, POOL_MANAGER, AMOUNT_IN),
                    mint(ROUTER, POOL_PAYOUT),
                    transfer(WETH, ROUTER, STRANGER, POOL_PAYOUT),
                    transfer(WETH, STRANGER, ACCOUNT, POOL_PAYOUT),
                ],
                **ROUTE_CONTRACT,
                parser_output_amount_raw=str(POOL_PAYOUT),
                output_wallet_after_raw=str(POOL_PAYOUT),
            ),
            expected_profile="v4_swap_route.v1",
        )


def test_converting_route_refuses_a_parser_that_disagrees_with_the_pool_event() -> None:
    with pytest.raises(ValueError, match="parser measurement"):
        validate_semantic_contract(
            payload(
                "v4_swap_route.v1",
                [
                    ROUTE_SWAP,
                    transfer(USDG, ACCOUNT, POOL_MANAGER, AMOUNT_IN),
                    mint(ROUTER, POOL_PAYOUT),
                    transfer(WETH, ROUTER, ACCOUNT, POOL_PAYOUT),
                ],
                **ROUTE_CONTRACT,
                parser_output_amount_raw=str(MIN_OUT),
                output_wallet_after_raw=str(POOL_PAYOUT),
            ),
            expected_profile="v4_swap_route.v1",
        )


def test_erc20_profile_refuses_a_converting_route() -> None:
    """A native swap must not be admissible as a swap in the wrapped pool.

    WRAP_ETH is 1:1, so without the currency-set equality the two are
    indistinguishable by amount alone.
    """
    with pytest.raises(ValueError, match="do not describe the swapped pair"):
        validate_semantic_contract(
            payload(
                "v4_swap.v1",
                [
                    ROUTE_SWAP,
                    transfer(USDG, ACCOUNT, POOL_MANAGER, AMOUNT_IN),
                    mint(ROUTER, POOL_PAYOUT),
                    transfer(WETH, ROUTER, ACCOUNT, POOL_PAYOUT),
                ],
                **ROUTE_CONTRACT,
                parser_output_amount_raw=str(POOL_PAYOUT),
                output_wallet_after_raw=str(POOL_PAYOUT),
            ),
            expected_profile="v4_swap.v1",
        )


def test_compiled_call_must_match_the_sealed_direction_and_size() -> None:
    """The routed pool alone is not the trade: direction and size are part of it."""
    logs = [
        ROUTE_SWAP,
        transfer(USDG, ACCOUNT, POOL_MANAGER, AMOUNT_IN),
        mint(ROUTER, POOL_PAYOUT),
        transfer(WETH, ROUTER, ACCOUNT, POOL_PAYOUT),
    ]
    contract = {
        **ROUTE_CONTRACT,
        "parser_output_amount_raw": str(POOL_PAYOUT),
        "output_wallet_after_raw": str(POOL_PAYOUT),
        "compiled_calls": [router_call(NATIVE_KEY, MIN_OUT, amount_in=AMOUNT_IN * 2)],
    }
    with pytest.raises(ValueError, match="does not spend the compiler-observed request"):
        validate_semantic_contract(payload("v4_swap_route.v1", logs, **contract), expected_profile="v4_swap_route.v1")


def test_a_contract_naming_its_own_permit2_is_refused() -> None:
    """The allowed-counterparty set may not be built from producer input.

    Otherwise a payload names the counterparty it wants authorised, and the set
    permits exactly the transfer it exists to constrain.
    """
    logs = [
        ERC20_SWAP,
        transfer(USDG, ACCOUNT, POOL_MANAGER, AMOUNT_IN),
        transfer(WETH, POOL_MANAGER, ACCOUNT, POOL_PAYOUT),
    ]
    with pytest.raises(ValueError, match="not the committed deployment"):
        validate_semantic_contract(
            payload("v4_swap.v1", logs, **{**ERC20_CONTRACT, "permit2_address": STRANGER}),
            expected_profile="v4_swap.v1",
        )


def test_a_recorded_profile_that_disagrees_with_the_declared_contract_is_refused() -> None:
    """The cell declares its contract; the run may not substitute another.

    A run that recorded whichever profile matched the route it happened to take
    would drop the conversion proof the moment the route stopped converting, and
    the cell would still seal green.
    """
    logs = [
        ERC20_SWAP,
        transfer(USDG, ACCOUNT, POOL_MANAGER, AMOUNT_IN),
        transfer(WETH, POOL_MANAGER, ACCOUNT, POOL_PAYOUT),
    ]
    with pytest.raises(ValueError, match="must use 'v4_swap_route.v1' schema v1"):
        validate_semantic_contract(payload("v4_swap.v1", logs, **ERC20_CONTRACT), expected_profile="v4_swap_route.v1")


def test_route_profile_refuses_an_all_erc20_swap() -> None:
    """The converse of the ERC-20 profile's refusal of a converting route.

    Without it the conversion contract could be satisfied by a receipt that
    never converted, and the payout-conservation chain would prove nothing.
    """
    logs = [
        ERC20_SWAP,
        transfer(USDG, ACCOUNT, POOL_MANAGER, AMOUNT_IN),
        transfer(WETH, POOL_MANAGER, ACCOUNT, POOL_PAYOUT),
    ]
    with pytest.raises(ValueError, match="requires the pool's native currency leg"):
        validate_semantic_contract(
            payload("v4_swap_route.v1", logs, **ERC20_CONTRACT),
            expected_profile="v4_swap_route.v1",
        )


def test_route_profile_refuses_a_self_named_wrapper() -> None:
    """A payload may not nominate the contract that proves its own conversion.

    The wrapper joins the allowed-counterparty set and carries the deposit term
    of the conservation chain, so a payload free to name it can present a 1:1
    mint of an unrelated ERC-20 as the native conversion and satisfy every
    remaining term. Checking it against the declared output asset is no check at
    all: both sides come from the producer.
    """
    impostor = "0x" + "ab" * 20
    logs = [
        ROUTE_SWAP,
        transfer(USDG, ACCOUNT, POOL_MANAGER, AMOUNT_IN),
        transfer(impostor, ZERO, ROUTER, POOL_PAYOUT),
        transfer(impostor, ROUTER, ACCOUNT, POOL_PAYOUT),
    ]
    with pytest.raises(ValueError, match="names a wrapper that is not"):
        validate_semantic_contract(
            payload(
                "v4_swap_route.v1",
                logs,
                target_reference=impostor,
                **{**ROUTE_CONTRACT, "output_asset_address": impostor, "wrapper_address": impostor},
                parser_output_amount_raw=str(POOL_PAYOUT),
                output_wallet_after_raw=str(POOL_PAYOUT),
            ),
            expected_profile="v4_swap_route.v1",
        )


def test_router_batch_that_does_not_dispatch_a_v4_swap_is_refused() -> None:
    """The swap must be found by the command byte the router dispatches on.

    Reading a fixed input slot proves only that some payload is shaped like a
    swap. Here the batch carries a swap-shaped input under a WRAP_ETH command,
    so nothing in it would ever reach the PoolManager.
    """
    logs = [
        ERC20_SWAP,
        transfer(USDG, ACCOUNT, POOL_MANAGER, AMOUNT_IN),
        transfer(WETH, POOL_MANAGER, ACCOUNT, POOL_PAYOUT),
    ]
    with pytest.raises(ValueError, match="exactly one V4_SWAP router command; found 0"):
        validate_semantic_contract(
            payload(
                "v4_swap.v1",
                logs,
                **{**ERC20_CONTRACT, "compiled_calls": [router_call(ERC20_KEY, MIN_OUT, commands=(0x0B, 0x0B))]},
            ),
            expected_profile="v4_swap.v1",
        )


def test_a_v4_swap_command_permitted_to_revert_is_refused() -> None:
    """A command the batch tolerates failing cannot carry an execution proof.

    The high bit of a UniversalRouter command byte lets that command revert
    without failing the batch, so the surrounding receipt can look complete
    while the swap never happened.
    """
    logs = [
        ERC20_SWAP,
        transfer(USDG, ACCOUNT, POOL_MANAGER, AMOUNT_IN),
        transfer(WETH, POOL_MANAGER, ACCOUNT, POOL_PAYOUT),
    ]
    with pytest.raises(ValueError, match="permitted to revert"):
        validate_semantic_contract(
            payload(
                "v4_swap.v1",
                logs,
                **{**ERC20_CONTRACT, "compiled_calls": [router_call(ERC20_KEY, MIN_OUT, commands=(0x90, 0x0B))]},
            ),
            expected_profile="v4_swap.v1",
        )


def test_a_swap_struct_the_committed_router_cannot_decode_is_refused() -> None:
    """The upstream shape is not merely a different encoding here -- it is a payload
    this router would trap on, so it must not be read with the other schema."""
    logs = [
        ROUTE_SWAP,
        transfer(USDG, ACCOUNT, POOL_MANAGER, AMOUNT_IN),
        mint(ROUTER, POOL_PAYOUT),
        transfer(WETH, ROUTER, ACCOUNT, POOL_PAYOUT),
    ]
    contract = {
        **ROUTE_CONTRACT,
        "parser_output_amount_raw": str(POOL_PAYOUT),
        "output_wallet_after_raw": str(POOL_PAYOUT),
        "compiled_calls": [router_call(NATIVE_KEY, MIN_OUT, min_hop_price_x36=None)],
    }
    with pytest.raises(ValueError, match="not a decodable V4 swap"):
        validate_semantic_contract(payload("v4_swap_route.v1", logs, **contract), expected_profile="v4_swap_route.v1")


def test_a_second_price_bound_beyond_amount_out_minimum_is_refused() -> None:
    """amountOutMinimum is what this proof reports the router was told to enforce;
    a non-zero per-hop price is a second bound in different units that it does not describe."""
    logs = [
        ROUTE_SWAP,
        transfer(USDG, ACCOUNT, POOL_MANAGER, AMOUNT_IN),
        mint(ROUTER, POOL_PAYOUT),
        transfer(WETH, ROUTER, ACCOUNT, POOL_PAYOUT),
    ]
    contract = {
        **ROUTE_CONTRACT,
        "parser_output_amount_raw": str(POOL_PAYOUT),
        "output_wallet_after_raw": str(POOL_PAYOUT),
        "compiled_calls": [router_call(NATIVE_KEY, MIN_OUT, min_hop_price_x36=1)],
    }
    with pytest.raises(ValueError, match="non-zero per-hop price bound"):
        validate_semantic_contract(payload("v4_swap_route.v1", logs, **contract), expected_profile="v4_swap_route.v1")
