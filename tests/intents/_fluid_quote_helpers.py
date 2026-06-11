"""Independent Fluid resolver-quote helpers for intent tests.

The money-safety invariant for a DEX connector is that the compiled
``min_amount_out`` is actually derived from a real on-chain quote with the
intent's slippage bound applied — a compiler that emitted a placeholder
(``min_out = 1``) would still "pass" an execution test as long as the swap
landed. These helpers re-quote the swap through Fluid's deployed
``DexReservesResolver`` directly (raw ``eth_call`` against the deterministic
deployment address, no connector code involved) so the test can compare the
compiled ``min_amount_out`` against an independent source.

Quotes are deterministic for a given fork state, and compilation performs
read-only calls, so a re-quote taken after compile but before execution sees
the exact pool state the compiler quoted against.
"""

from decimal import Decimal

from web3 import Web3

# Deterministic on every Fluid chain — pinned independently of connector
# constants on purpose (Phase-0 report, VIB-5028,
# docs/internal/qa/fluid-protocol-validation-2026-06-10.md §addresses).
FLUID_DEX_RESERVES_RESOLVER = "0x05Bd8269A20C472b148246De20E6852091BF16Ff"

_ESTIMATE_SWAP_IN_ABI = [
    {
        "name": "estimateSwapIn",
        "type": "function",
        "stateMutability": "payable",
        "inputs": [
            {"name": "dex_", "type": "address"},
            {"name": "swap0to1_", "type": "bool"},
            {"name": "amountIn_", "type": "uint256"},
            {"name": "amountOutMin_", "type": "uint256"},
        ],
        "outputs": [{"name": "amountOut_", "type": "uint256"}],
    }
]

# How far the compiler's slippage basis may sit BELOW the resolver quote
# before we call it a bug. The compiler floors min_amount_out on the SAFER of
# (oracle estimate, resolver quote); when the price oracle marks the pair
# slightly below the pool's own price the basis is the oracle estimate, so
# exact equality with the resolver formula is not guaranteed. 5% comfortably
# covers oracle-vs-pool divergence on the majors used in these tests while
# still rejecting placeholder floors outright (a min_out of 1 is ~10 orders
# of magnitude below the bound).
ORACLE_DIVERGENCE_ALLOWANCE = Decimal("0.05")


def fluid_resolver_quote(web3: Web3, pool_address: str, swap0to1: bool, amount_in: int) -> int:
    """Quote ``amount_in`` through the deployed DexReservesResolver."""
    resolver = web3.eth.contract(
        address=Web3.to_checksum_address(FLUID_DEX_RESERVES_RESOLVER),
        abi=_ESTIMATE_SWAP_IN_ABI,
    )
    return int(resolver.functions.estimateSwapIn(Web3.to_checksum_address(pool_address), swap0to1, amount_in, 0).call())


def assert_min_out_quote_derived(
    min_amount_out: int,
    independent_quote: int,
    max_slippage: Decimal,
) -> None:
    """Assert the compiled ``min_amount_out`` is the slippage-bounded quote.

    Upper bound: the compiler may never promise MORE than the quote allows
    (``quote * (1 - max_slippage)``) — a higher floor would revert on-chain.
    Lower bound: the floor must sit within ``ORACLE_DIVERGENCE_ALLOWANCE`` of
    that same formula — placeholders and forgotten-quote bugs land orders of
    magnitude below and fail loudly here.
    """
    assert independent_quote > 0, "independent resolver quote must be positive"
    quote_floor = int(Decimal(independent_quote) * (Decimal("1") - max_slippage))
    lower_bound = int(Decimal(quote_floor) * (Decimal("1") - ORACLE_DIVERGENCE_ALLOWANCE))
    assert min_amount_out <= quote_floor, (
        f"min_amount_out ({min_amount_out}) exceeds the slippage-bounded resolver quote "
        f"({quote_floor} = {independent_quote} * (1 - {max_slippage})) — over-promising, would revert"
    )
    assert min_amount_out >= lower_bound, (
        f"min_amount_out ({min_amount_out}) is far below the slippage-bounded resolver quote "
        f"({quote_floor}; allowed floor {lower_bound}) — not derived from the on-chain quote"
    )
