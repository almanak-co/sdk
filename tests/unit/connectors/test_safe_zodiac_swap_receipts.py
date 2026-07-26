"""Safe / Zodiac receipt parsing for the SWAP connectors (VIB-6043).

Under Safe + Zodiac Roles execution — the only execution mode the hosted
platform runs — the transaction ``from`` is the **agent EOA** (it signs
``execTransactionWithRole`` on the Roles modifier) while every ERC-20
``Transfer`` moves on the **Safe**. Parsers that took the trading wallet from
``receipt["from"]`` matched no transfer at all, so token discovery returned
``("", "")``, decimals resolved to ``None`` and amount extraction returned
``None`` — which surfaced either as a ``CriticalAccountingError`` (Curve: money
on-chain, zero ledger rows, circuit breaker) or as a success ledger row with
EMPTY amounts (PancakeSwap V3: Empty != Zero violation).

Every test here asserts a **measured** amount, never merely "not None": the
whole failure class was "the row exists but the numbers are missing".

Three shapes per connector:

* ``…_zodiac_stamped`` — the production path: a Zodiac receipt (tx ``from`` =
  agent EOA, Transfers on the Safe, ``ExecutionFromModuleSuccess`` emitted by
  the Safe) carrying the framework stamp the ``ResultEnricher`` writes from
  ``ExecutionContext.wallet_address``.
* ``…_zodiac_unstamped_is_unmeasured`` — the control. The SAME receipt without
  a stamp must NOT resolve to the Safe: deriving it from the Safe's own events
  would be authentication by event signature (any contract can emit a
  colliding topic0), so the resolver refuses and the amounts stay unmeasured
  rather than becoming attacker-choosable. This is what makes the stamped
  cases discriminating.
* ``…_eoa_unchanged`` — plain EOA receipt still extracts exactly as before.
"""

from decimal import Decimal
from typing import Any

import pytest

from almanak.connectors._strategy_base.base.receipt_wallet import stamp_trading_wallet

# Safe 20a and its agent EOA — the exact pair the platform QA Safe lane deploys
# (tests/platform/scripts/safe_bootstrap.py).
AGENT_EOA = "0x42657d7c6Fe1bC3FB0af01a702b88cC31A93661b"
SAFE = "0x4c373c8D5c486F601874EF02A2Cc19b5F4E9e837"

# Real Arbitrum tokens so decimals resolve through the static token table
# (6 / 18) instead of being mocked.
USDT_ARB = "0xfd086bc7cd5c481dcc9c85ebe478a1c0b69fcbb9"
USDC_ARB = "0xaf88d065e77c8cc2239327c5edb3a432268e5831"
WETH_ARB = "0x82af49447d8a07e3bd95bd0d56f35241523fbab1"

POOL = "0x7f90122bf0700f9e7e1f688fe926940e8839f353"

TRANSFER_TOPIC = "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"
EXECUTION_FROM_MODULE_SUCCESS_TOPIC = "0x6895c13664aa4f67288b25d7a21d7aaa34916e355fb9b6fae0a139a9085becb8"

# 50 USDT in, 0.026577044991779670 WETH out — the exact leg sizes the Safe-lane
# sweep executed on curve:arbitrum (VIB-6043 evidence).
AMOUNT_IN_RAW = 50_000_000
AMOUNT_OUT_RAW = 26_577_044_991_779_670
AMOUNT_IN_HUMAN = Decimal("50")
AMOUNT_OUT_HUMAN = Decimal("0.02657704499177967")


def _topic_addr(address: str) -> str:
    return "0x" + "0" * 24 + address[2:].lower()


def transfer_log(token: str, sender: str, recipient: str, amount: int, index: int) -> dict[str, Any]:
    """ERC-20 Transfer log."""
    return {
        "address": token,
        "topics": [TRANSFER_TOPIC, _topic_addr(sender), _topic_addr(recipient)],
        "data": f"0x{amount:064x}",
        "logIndex": index,
    }


def module_success_log(index: int, safe: str = SAFE) -> dict[str, Any]:
    """``ExecutionFromModuleSuccess(address module)`` — emitted BY the Safe."""
    return {
        "address": safe,
        "topics": [EXECUTION_FROM_MODULE_SUCCESS_TOPIC, _topic_addr("0x1111111111111111111111111111111111111111")],
        "data": "0x",
        "logIndex": index,
    }


def receipt(logs: list[dict[str, Any]], sender: str = AGENT_EOA) -> dict[str, Any]:
    return {
        "from": sender,
        "status": 1,
        "transactionHash": "0x" + "ab" * 32,
        "blockNumber": 487748138,
        "gasUsed": 321_004,
        "logs": logs,
    }


def money_legs(wallet: str, start_index: int = 0) -> list[dict[str, Any]]:
    """The two ERC-20 legs of a USDT -> WETH swap, moving on ``wallet``."""
    return [
        transfer_log(USDT_ARB, wallet, POOL, AMOUNT_IN_RAW, start_index),
        transfer_log(WETH_ARB, POOL, wallet, AMOUNT_OUT_RAW, start_index + 1),
    ]


# ---------------------------------------------------------------------------
# Curve — the fail-closed flavour (CriticalAccountingError, zero ledger rows)
# ---------------------------------------------------------------------------


def curve_parser():
    from almanak.connectors.curve.receipt_parser import CurveReceiptParser

    return CurveReceiptParser(chain="arbitrum")


def curve_token_exchange_log(buyer: str, index: int = 2) -> dict[str, Any]:
    """``TokenExchange(address indexed buyer, int128, uint256, int128, uint256)``."""
    data = f"{0:064x}{AMOUNT_IN_RAW:064x}{1:064x}{AMOUNT_OUT_RAW:064x}"
    return {
        "address": POOL,
        "topics": [
            "0x8b3e96f2b889fa771c53c981b40daf005f63f637f1869f707052d15a3dd97140",
            _topic_addr(buyer),
        ],
        "data": f"0x{data}",
        "logIndex": index,
    }


def _assert_curve_amounts(amounts: Any) -> None:
    assert amounts is not None, "curve extraction returned None — the VIB-6043 blackout"
    assert amounts.amount_in == AMOUNT_IN_RAW
    assert amounts.amount_out == AMOUNT_OUT_RAW
    assert amounts.amount_in_decimal == AMOUNT_IN_HUMAN
    assert amounts.amount_out_decimal == AMOUNT_OUT_HUMAN
    assert amounts.token_in == USDT_ARB
    assert amounts.token_out == WETH_ARB


def test_curve_zodiac_stamped_with_safe_log():
    logs = [*money_legs(SAFE), curve_token_exchange_log(SAFE), module_success_log(3)]
    _assert_curve_amounts(curve_parser().extract_swap_amounts(stamp_trading_wallet(receipt(logs), SAFE)))


def test_curve_zodiac_stamped():
    logs = [*money_legs(SAFE), curve_token_exchange_log(SAFE)]
    stamped = stamp_trading_wallet(receipt(logs), SAFE)
    _assert_curve_amounts(curve_parser().extract_swap_amounts(stamped))


def test_curve_eoa_unchanged():
    logs = [*money_legs(AGENT_EOA), curve_token_exchange_log(AGENT_EOA)]
    _assert_curve_amounts(curve_parser().extract_swap_amounts(receipt(logs)))


def test_curve_zodiac_without_wallet_resolution_is_unmeasured():
    """Control: with the Safe legs but NO way to learn the Safe, nothing is measurable.

    Proves the tests above pass *because* the wallet resolves, not because the
    parser found the amounts some other way (Empty != Zero — the parser must
    return None rather than invent numbers).
    """
    logs = [*money_legs(SAFE), curve_token_exchange_log(SAFE)]
    assert curve_parser().extract_swap_amounts(receipt(logs)) is None


# ---------------------------------------------------------------------------
# PancakeSwap V3 — the fail-open flavour (success row with EMPTY amounts)
# ---------------------------------------------------------------------------


def pancake_parser():
    from almanak.connectors.pancakeswap_v3.receipt_parser import PancakeSwapV3ReceiptParser

    return PancakeSwapV3ReceiptParser(chain="arbitrum")


def pancake_swap_log(index: int = 2, wallet: str = SAFE) -> dict[str, Any]:
    """PancakeSwap V3 ``Swap`` log — only its presence gates extraction."""
    data = (
        f"{(1 << 256) - AMOUNT_IN_RAW:064x}"  # amount0 (negative, two's complement)
        f"{AMOUNT_OUT_RAW:064x}"  # amount1
        f"{0:064x}{0:064x}{0:064x}{0:064x}"  # sqrtPriceX96, liquidity, tick, protocolFees
    )
    return {
        "address": POOL,
        "topics": [
            "0x19b47279256b2a23a1665c810c8d55a1758940ee09377d4f8d26497a3577dc83",
            _topic_addr(wallet),
            _topic_addr(wallet),
        ],
        "data": f"0x{data}",
        "logIndex": index,
    }


def _assert_pancake_amounts(amounts: Any) -> None:
    assert amounts is not None, "pancakeswap_v3 extraction returned None"
    assert amounts.amount_in == AMOUNT_IN_RAW
    assert amounts.amount_out == AMOUNT_OUT_RAW
    assert amounts.amount_in_decimal == AMOUNT_IN_HUMAN
    assert amounts.amount_out_decimal == AMOUNT_OUT_HUMAN


def test_pancakeswap_v3_zodiac_stamped_with_safe_log():
    logs = [*money_legs(SAFE), pancake_swap_log(), module_success_log(3)]
    _assert_pancake_amounts(pancake_parser().extract_swap_amounts(stamp_trading_wallet(receipt(logs), SAFE)))


def test_pancakeswap_v3_zodiac_stamped():
    logs = [*money_legs(SAFE), pancake_swap_log()]
    stamped = stamp_trading_wallet(receipt(logs), SAFE)
    _assert_pancake_amounts(pancake_parser().extract_swap_amounts(stamped))


def test_pancakeswap_v3_eoa_unchanged():
    logs = [*money_legs(AGENT_EOA), pancake_swap_log(wallet=AGENT_EOA)]
    _assert_pancake_amounts(pancake_parser().extract_swap_amounts(receipt(logs)))


def test_pancakeswap_v3_zodiac_without_wallet_resolution_is_unmeasured():
    logs = [*money_legs(SAFE), pancake_swap_log()]
    assert pancake_parser().extract_swap_amounts(receipt(logs)) is None


# ---------------------------------------------------------------------------
# Enso — no protocol Swap event at all; the wallet IS the only anchor
# ---------------------------------------------------------------------------


def enso_parser():
    from almanak.connectors.enso.receipt_parser import EnsoReceiptParser

    return EnsoReceiptParser(chain="arbitrum")


def _assert_enso_amounts(amounts: Any) -> None:
    assert amounts is not None, "enso extraction returned None"
    assert amounts.amount_in == AMOUNT_IN_RAW
    assert amounts.amount_out == AMOUNT_OUT_RAW


def test_enso_zodiac_stamped_with_safe_log():
    logs = [*money_legs(SAFE), module_success_log(2)]
    _assert_enso_amounts(enso_parser().extract_swap_amounts(stamp_trading_wallet(receipt(logs), SAFE)))


def test_enso_zodiac_unstamped_is_unmeasured():
    logs = [*money_legs(SAFE), module_success_log(2)]
    assert enso_parser().extract_swap_amounts(receipt(logs)) is None


def test_enso_zodiac_stamped():
    stamped = stamp_trading_wallet(receipt(money_legs(SAFE)), SAFE)
    _assert_enso_amounts(enso_parser().extract_swap_amounts(stamped))


def test_enso_eoa_unchanged():
    _assert_enso_amounts(enso_parser().extract_swap_amounts(receipt(money_legs(AGENT_EOA))))


def test_enso_zodiac_without_wallet_resolution_is_unmeasured():
    assert enso_parser().extract_swap_amounts(receipt(money_legs(SAFE))) is None


# ---------------------------------------------------------------------------
# SushiSwap V3 / Aerodrome — Shape-B parsers (amounts from the Swap event,
# token addresses + decimals from the wallet-keyed Transfer scan)
# ---------------------------------------------------------------------------


def sushi_parser():
    from almanak.connectors.sushiswap_v3.receipt_parser import SushiSwapV3ReceiptParser

    return SushiSwapV3ReceiptParser(chain="arbitrum")


def v3_swap_log(topic: str, index: int = 2, wallet: str = SAFE) -> dict[str, Any]:
    # Uniswap-V3 pool-delta convention: amount0 > 0 = the pool received token0
    # (the user paid it in); amount1 < 0 = the pool paid token1 out.
    data = (
        f"{AMOUNT_IN_RAW:064x}"  # amount0 = +amount_in (token0 in)
        f"{(1 << 256) - AMOUNT_OUT_RAW:064x}"  # amount1 = -amount_out (token1 out)
        f"{0:064x}{0:064x}{0:064x}"
    )
    return {
        "address": POOL,
        "topics": [topic, _topic_addr(wallet), _topic_addr(wallet)],
        "data": f"0x{data}",
        "logIndex": index,
    }


SUSHI_SWAP_TOPIC = "0xc42079f94a6350d7e6235f29174924f928cc2ac818eb64fed8004e115fbcca67"


def _assert_sushi_amounts(amounts: Any) -> None:
    assert amounts is not None, "sushiswap_v3 extraction returned None"
    assert amounts.amount_in == AMOUNT_IN_RAW
    assert amounts.amount_out == AMOUNT_OUT_RAW
    assert amounts.amount_in_decimal == AMOUNT_IN_HUMAN
    assert amounts.amount_out_decimal == AMOUNT_OUT_HUMAN


def test_sushiswap_v3_zodiac_stamped_with_safe_log():
    logs = [*money_legs(SAFE), v3_swap_log(SUSHI_SWAP_TOPIC), module_success_log(3)]
    _assert_sushi_amounts(sushi_parser().extract_swap_amounts(stamp_trading_wallet(receipt(logs), SAFE)))


def test_sushiswap_v3_zodiac_stamped():
    logs = [*money_legs(SAFE), v3_swap_log(SUSHI_SWAP_TOPIC)]
    _assert_sushi_amounts(sushi_parser().extract_swap_amounts(stamp_trading_wallet(receipt(logs), SAFE)))


def test_sushiswap_v3_eoa_unchanged():
    logs = [*money_legs(AGENT_EOA), v3_swap_log(SUSHI_SWAP_TOPIC, wallet=AGENT_EOA)]
    _assert_sushi_amounts(sushi_parser().extract_swap_amounts(receipt(logs)))


def aerodrome_parser():
    from almanak.connectors.aerodrome.receipt_parser import AerodromeReceiptParser

    return AerodromeReceiptParser(chain="base")


AERODROME_SWAP_TOPIC = "0xb3e2773606abfd36b5bd91394b3a54d1398336c65005baf7bf7a05efeffaf75b"

# Base-chain tokens for the Aerodrome cell.
USDC_BASE = "0x833589fcd6edb6e08f4c7c32d4f71b54bda02913"
WETH_BASE = "0x4200000000000000000000000000000000000006"


def aerodrome_money_legs(wallet: str) -> list[dict[str, Any]]:
    return [
        transfer_log(USDC_BASE, wallet, POOL, AMOUNT_IN_RAW, 0),
        transfer_log(WETH_BASE, POOL, wallet, AMOUNT_OUT_RAW, 1),
    ]


def aerodrome_swap_log(index: int = 2, wallet: str = SAFE) -> dict[str, Any]:
    """Solidly-style ``Swap(sender, to, amount0In, amount1In, amount0Out, amount1Out)``."""
    data = f"{AMOUNT_IN_RAW:064x}{0:064x}{0:064x}{AMOUNT_OUT_RAW:064x}"
    return {
        "address": POOL,
        "topics": [AERODROME_SWAP_TOPIC, _topic_addr(wallet), _topic_addr(wallet)],
        "data": f"0x{data}",
        "logIndex": index,
    }


def _assert_aerodrome_amounts(amounts: Any) -> None:
    assert amounts is not None, "aerodrome extraction returned None"
    assert amounts.amount_in == AMOUNT_IN_RAW
    assert amounts.amount_out == AMOUNT_OUT_RAW
    assert amounts.amount_in_decimal == AMOUNT_IN_HUMAN
    assert amounts.amount_out_decimal == AMOUNT_OUT_HUMAN


def test_aerodrome_zodiac_stamped_with_safe_log():
    logs = [*aerodrome_money_legs(SAFE), aerodrome_swap_log(), module_success_log(3)]
    _assert_aerodrome_amounts(aerodrome_parser().extract_swap_amounts(stamp_trading_wallet(receipt(logs), SAFE)))


def test_aerodrome_zodiac_stamped():
    logs = [*aerodrome_money_legs(SAFE), aerodrome_swap_log()]
    _assert_aerodrome_amounts(aerodrome_parser().extract_swap_amounts(stamp_trading_wallet(receipt(logs), SAFE)))


def test_aerodrome_eoa_unchanged():
    logs = [*aerodrome_money_legs(AGENT_EOA), aerodrome_swap_log(wallet=AGENT_EOA)]
    _assert_aerodrome_amounts(aerodrome_parser().extract_swap_amounts(receipt(logs)))


def test_sushiswap_v3_zodiac_without_wallet_resolution_is_unmeasured():
    """No stamp, no Safe log: decimals are unresolvable, so no amounts are booked.

    Aerodrome is deliberately NOT covered here: it has a pool-anchored
    (sender-independent) fallback for single-swap receipts, so it degrades
    rather than failing — which is why aerodrome passed the Safe-lane sweep
    while curve and pancakeswap_v3 did not.
    """
    logs = [*money_legs(SAFE), v3_swap_log(SUSHI_SWAP_TOPIC)]
    assert sushi_parser().extract_swap_amounts(receipt(logs)) is None


def test_aerodrome_pool_anchored_fallback_still_measures_without_wallet_resolution():
    """Aerodrome degrades gracefully where curve/pancakeswap_v3 could not.

    With no stamp and no Safe log the wallet is unresolvable, but Aerodrome has
    a pool-anchored (sender-independent) path for single-swap receipts. Pinning
    it here documents WHY aerodrome passed the Safe-lane sweep pre-fix while
    curve and pancakeswap_v3 did not — and guards the fallback from silently
    disappearing.
    """
    logs = [*aerodrome_money_legs(SAFE), aerodrome_swap_log()]
    _assert_aerodrome_amounts(aerodrome_parser().extract_swap_amounts(receipt(logs)))


# ---------------------------------------------------------------------------
# Controls: a Safe log in the receipt must NOT be treated as identity
# ---------------------------------------------------------------------------


def test_curve_zodiac_unstamped_is_unmeasured():
    logs = [*money_legs(SAFE), curve_token_exchange_log(SAFE), module_success_log(3)]
    assert curve_parser().extract_swap_amounts(receipt(logs)) is None


def test_pancakeswap_v3_zodiac_unstamped_is_unmeasured():
    logs = [*money_legs(SAFE), pancake_swap_log(), module_success_log(3)]
    assert pancake_parser().extract_swap_amounts(receipt(logs)) is None


def test_sushiswap_v3_zodiac_unstamped_is_unmeasured():
    logs = [*money_legs(SAFE), v3_swap_log(SUSHI_SWAP_TOPIC), module_success_log(3)]
    assert sushi_parser().extract_swap_amounts(receipt(logs)) is None
