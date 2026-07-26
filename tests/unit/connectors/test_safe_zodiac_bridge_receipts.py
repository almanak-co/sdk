"""Safe / Zodiac receipt parsing for the BRIDGE + Fluid connectors (VIB-6043).

Companion to ``test_safe_zodiac_swap_receipts.py``, covering the four parsers
whose money-leg discovery is keyed on the trading wallet outside the plain
swap family:

* ``across``  — ``_find_wallet_deposit_transfer`` (the wallet -> SpokePool scan
  that ``extract_bridge_data`` falls back to when the ``V3FundsDeposited``
  event is absent or undecodable; it is the ONLY source of both the amount and
  the source token on that path).
* ``stargate`` — ``_find_wallet_deposit_transfer``. ``OFTSent`` carries the
  amount but **never** the source token address, so this scan is the only
  source of the token — and therefore of the decimals.
* ``lifi``     — ``extract_bridge_data``. With no wallet-outgoing Transfer the
  parser falls through to the native-asset branch, i.e. an ERC-20 bridge is
  booked as a native deposit with ``source_token_address = None``.
* ``_fluid_core`` — ``_receipt_wallet`` -> ``_extract_swap_token_addresses``,
  the secondary wallet-keyed leg match used for receipts whose Transfers do
  not touch Fluid's canonical Liquidity layer.

Under Safe + Zodiac Roles execution — the only mode the hosted platform runs —
``receipt["from"]`` is the **agent EOA** (it signs ``execTransactionWithRole``
on the Roles modifier) while every ERC-20 ``Transfer`` moves on the **Safe**.
Keying the scan on ``receipt["from"]`` therefore matched nothing.

Every test asserts a **measured** value (an amount and/or the token identity),
never merely "is not None": the failure class being pinned is "the row exists
but the numbers are missing" (Empty != Zero).

Three shapes per connector, plus a control:

* ``…_zodiac_stamped_with_safe_log`` — the production path: a realistic Zodiac
  receipt (Safe log present) carrying the framework stamp the ``ResultEnricher``
  writes. The Safe log is NOT what identifies the Safe — an event signature is
  not authentication — the stamp is.
* ``…_zodiac_stamped_without_safe_log`` — same receipt with the Safe log
  REMOVED and ``stamp_trading_wallet`` applied instead (the ``ResultEnricher``
  path).
* ``…_eoa_receipt_unchanged`` — plain EOA receipt extracts exactly as before.
* ``…_without_wallet_resolution_*`` — control pinning the pre-fix behaviour, so
  the tests above pass *because* the wallet resolves rather than by accident.

Decimals resolve through the static token table (real chain / token / contract
addresses, imported from the connectors' own constants) — nothing is mocked.
"""

from decimal import Decimal
from typing import Any

from almanak.connectors._fluid_core.receipt_parser import (
    ERC721_TRANSFER_TOPIC,
    SWAP_TOPIC,
    FluidReceiptParser,
)
from almanak.connectors._strategy_base.base.receipt_wallet import stamp_trading_wallet
from almanak.connectors.across.adapter import ACROSS_SPOKE_POOL_ADDRESSES
from almanak.connectors.across.receipt_parser import AcrossReceiptParser
from almanak.connectors.lifi.client import LIFI_DIAMOND_ADDRESS
from almanak.connectors.lifi.receipt_parser import (
    LIFI_TRANSFER_STARTED_TOPIC,
    LiFiReceiptParser,
)
from almanak.connectors.stargate.adapter import (
    STARGATE_CHAIN_IDS,
    STARGATE_ROUTER_ADDRESSES,
)
from almanak.connectors.stargate.receipt_parser import (
    OFT_SENT_TOPIC,
    StargateReceiptParser,
)

# Safe 20a and its agent EOA — the exact pair the platform QA Safe lane deploys
# (tests/platform/scripts/safe_bootstrap.py).
AGENT_EOA = "0x42657d7c6Fe1bC3FB0af01a702b88cC31A93661b"
SAFE = "0x4c373c8D5c486F601874EF02A2Cc19b5F4E9e837"

# Real Arbitrum tokens (the Safe lane's chain).
USDC_ARB = "0xaf88d065e77c8cc2239327c5edb3a432268e5831"
USDT_ARB = "0xfd086bc7cd5c481dcc9c85ebe478a1c0b69fcbb9"

TRANSFER_TOPIC = "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"
EXECUTION_FROM_MODULE_SUCCESS_TOPIC = "0x6895c13664aa4f67288b25d7a21d7aaa34916e355fb9b6fae0a139a9085becb8"

# 50 USDC bridged; the quote's destination estimate is 49.95 USDC.
AMOUNT_IN_RAW = 50_000_000
AMOUNT_IN_HUMAN = Decimal("50")
EXPECTED_OUT_HUMAN = "49.95"


def _topic_addr(address: str) -> str:
    return "0x" + "0" * 24 + address[2:].lower()


def _word(value: int) -> str:
    return f"{value:064x}"


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
        "transactionHash": "0x" + "cd" * 32,
        "blockNumber": 487_748_138,
        "gasUsed": 288_417,
        "logs": logs,
    }


# ---------------------------------------------------------------------------
# Across — the wallet -> SpokePool Transfer is the whole measurement
# ---------------------------------------------------------------------------
#
# ``extract_bridge_data`` reads ``V3FundsDeposited`` first; the wallet-keyed
# scan is the documented fallback for SpokePool versions whose deposit event
# this parser cannot decode. On that path the Transfer supplies BOTH the amount
# and the source token, so a wallet that resolves to the agent EOA yields
# "not an Across receipt" -> None -> no ledger row for a bridge that moved
# real money.

ARBITRUM_SPOKE_POOL = ACROSS_SPOKE_POOL_ADDRESSES[42161]


def across_parser() -> AcrossReceiptParser:
    return AcrossReceiptParser(chain="arbitrum")


def across_logs(wallet: str) -> list[dict[str, Any]]:
    return [transfer_log(USDC_ARB, wallet, ARBITRUM_SPOKE_POOL, AMOUNT_IN_RAW, 0)]


def across_extract(receipt_dict: dict[str, Any]) -> Any:
    return across_parser().extract_bridge_data(
        receipt_dict,
        from_chain="arbitrum",
        to_chain="base",
        token="USDC",
        amount="50",
        bridge="across",
        expected_amount_out=EXPECTED_OUT_HUMAN,
    )


def _assert_across_bridge(data: Any) -> None:
    assert data is not None, "across extraction returned None — the VIB-6043 blackout"
    assert data.amount_sent_raw == AMOUNT_IN_RAW
    assert data.amount_sent == AMOUNT_IN_HUMAN
    assert data.source_token_address == USDC_ARB
    assert data.source_chain == "arbitrum"
    assert data.destination_chain == "base"
    assert data.token_symbol == "USDC"
    assert data.bridge_name == "across"
    assert data.expected_amount_out == Decimal(EXPECTED_OUT_HUMAN)


def test_across_zodiac_stamped_with_safe_log():
    logs = [*across_logs(SAFE), module_success_log(1)]
    _assert_across_bridge(across_extract(stamp_trading_wallet(receipt(logs), SAFE)))


def test_across_zodiac_stamped_without_safe_log():
    stamped = stamp_trading_wallet(receipt(across_logs(SAFE)), SAFE)
    _assert_across_bridge(across_extract(stamped))


def test_across_eoa_receipt_unchanged():
    _assert_across_bridge(across_extract(receipt(across_logs(AGENT_EOA), sender=AGENT_EOA)))


def test_across_zodiac_without_wallet_resolution_is_unmeasured():
    """Control: Safe legs, no stamp and no Safe log -> nothing is measurable.

    Pins the pre-fix outcome (Empty != Zero: no BridgeData at all rather than a
    fabricated one) and proves the tests above pass because the wallet
    resolves, not because the parser found the amount some other way.
    """
    assert across_extract(receipt(across_logs(SAFE))) is None


# ---------------------------------------------------------------------------
# Stargate — OFTSent carries the amount but never the token address
# ---------------------------------------------------------------------------

STARGATE_USDC_POOL_ARB = STARGATE_ROUTER_ADDRESSES[42161]["USDC"]
BASE_ENDPOINT_ID = STARGATE_CHAIN_IDS["base"]

AMOUNT_RECEIVED_RAW = 49_950_000


def stargate_parser() -> StargateReceiptParser:
    return StargateReceiptParser(chain="arbitrum")


def oft_sent_log(index: int) -> dict[str, Any]:
    """``OFTSent(bytes32 indexed guid, uint32 dstEid, address indexed from, uint256, uint256)``."""
    return {
        "address": STARGATE_USDC_POOL_ARB,
        "topics": [
            OFT_SENT_TOPIC,
            "0x" + "11" * 32,  # guid
            _topic_addr(SAFE),  # fromAddress — the Safe, not the agent EOA
        ],
        "data": "0x" + _word(BASE_ENDPOINT_ID) + _word(AMOUNT_IN_RAW) + _word(AMOUNT_RECEIVED_RAW),
        "logIndex": index,
    }


def stargate_logs(wallet: str) -> list[dict[str, Any]]:
    return [
        transfer_log(USDC_ARB, wallet, STARGATE_USDC_POOL_ARB, AMOUNT_IN_RAW, 0),
        oft_sent_log(1),
    ]


def stargate_extract(receipt_dict: dict[str, Any]) -> Any:
    return stargate_parser().extract_bridge_data(
        receipt_dict,
        from_chain="arbitrum",
        to_chain="base",
        token="USDC",
        amount="50",
        bridge="stargate",
        expected_amount_out=EXPECTED_OUT_HUMAN,
    )


def _assert_stargate_bridge(data: Any) -> None:
    assert data is not None, "stargate extraction returned None"
    assert data.amount_sent_raw == AMOUNT_IN_RAW
    assert data.amount_sent == AMOUNT_IN_HUMAN
    assert data.source_token_address == USDC_ARB
    assert data.source_chain == "arbitrum"
    assert data.destination_chain == "base"
    assert data.bridge_name == "stargate"


def test_stargate_zodiac_stamped_with_safe_log():
    logs = [*stargate_logs(SAFE), module_success_log(2)]
    _assert_stargate_bridge(stargate_extract(stamp_trading_wallet(receipt(logs), SAFE)))


def test_stargate_zodiac_stamped_without_safe_log():
    stamped = stamp_trading_wallet(receipt(stargate_logs(SAFE)), SAFE)
    _assert_stargate_bridge(stargate_extract(stamped))


def test_stargate_eoa_receipt_unchanged():
    _assert_stargate_bridge(stargate_extract(receipt(stargate_logs(AGENT_EOA), sender=AGENT_EOA)))


def test_stargate_zodiac_without_wallet_resolution_loses_the_source_token():
    """Control: without wallet resolution the source token is never measured.

    ``OFTSent`` still yields the amount, so the pre-fix parser wrote a bridge
    row whose ``source_token_address`` was empty — the token identity that
    decimals (and every downstream valuation) hang off.
    """
    data = stargate_extract(receipt(stargate_logs(SAFE)))
    assert data is not None
    assert data.source_token_address is None


# ---------------------------------------------------------------------------
# LiFi — no wallet-outgoing Transfer means the ERC-20 bridge reads as native
# ---------------------------------------------------------------------------


def lifi_parser() -> LiFiReceiptParser:
    return LiFiReceiptParser(chain="arbitrum")


def lifi_transfer_started_log(index: int) -> dict[str, Any]:
    """``LiFiTransferStarted(ILiFi.BridgeData)`` — emitted by the Diamond's facet."""
    return {
        "address": LIFI_DIAMOND_ADDRESS,
        "topics": [LIFI_TRANSFER_STARTED_TOPIC],
        "data": "0x" + _word(0),
        "logIndex": index,
    }


def lifi_logs(wallet: str) -> list[dict[str, Any]]:
    return [
        transfer_log(USDC_ARB, wallet, LIFI_DIAMOND_ADDRESS, AMOUNT_IN_RAW, 0),
        lifi_transfer_started_log(1),
    ]


def lifi_extract(receipt_dict: dict[str, Any]) -> Any:
    return lifi_parser().extract_bridge_data(
        receipt_dict,
        from_chain="arbitrum",
        to_chain="base",
        token="USDC",
        amount="50",
        bridge="lifi",
        expected_amount_out=EXPECTED_OUT_HUMAN,
    )


def _assert_lifi_bridge(data: Any) -> None:
    assert data is not None, "lifi extraction returned None"
    assert data.amount_sent_raw == AMOUNT_IN_RAW
    assert data.amount_sent == AMOUNT_IN_HUMAN
    assert data.source_token_address == USDC_ARB
    assert data.source_chain == "arbitrum"
    assert data.destination_chain == "base"
    assert data.bridge_name == "lifi"


def test_lifi_zodiac_stamped_with_safe_log():
    logs = [*lifi_logs(SAFE), module_success_log(2)]
    _assert_lifi_bridge(lifi_extract(stamp_trading_wallet(receipt(logs), SAFE)))


def test_lifi_zodiac_stamped_without_safe_log():
    stamped = stamp_trading_wallet(receipt(lifi_logs(SAFE)), SAFE)
    _assert_lifi_bridge(lifi_extract(stamped))


def test_lifi_eoa_receipt_unchanged():
    _assert_lifi_bridge(lifi_extract(receipt(lifi_logs(AGENT_EOA), sender=AGENT_EOA)))


def test_lifi_zodiac_without_wallet_resolution_misreads_erc20_as_native():
    """Control: the pre-fix failure mode — an ERC-20 bridge booked as native.

    With no resolvable wallet there is no wallet-outgoing Transfer, so the
    parser takes the native-asset branch: the amount comes from the compiler's
    quote hint (not the receipt) and the source token is never identified.
    """
    data = lifi_extract(receipt(lifi_logs(SAFE)))
    assert data is not None
    assert data.source_token_address is None


# ---------------------------------------------------------------------------
# Fluid DEX — wallet-keyed leg match for non-Liquidity-custodied receipts
# ---------------------------------------------------------------------------
#
# ``_extract_swap_token_addresses`` matches Transfer counterparties against
# Fluid's canonical Liquidity layer first (sender-independent, covered by
# tests/unit/connectors/fluid/test_receipt_parser.py). The wallet-keyed match
# below it is the documented secondary signal for receipts that do NOT involve
# that address — and it is the branch VIB-6043 broke under Zodiac: with the
# wallet resolving to the agent EOA, neither leg matches, both token addresses
# stay None, decimals are unresolvable and ``extract_swap_amounts`` fails
# closed on a swap that really happened.

# Real Fluid DEX USDC/USDT pool on Arbitrum (Phase-0 verification V1.2,
# docs/internal/qa/fluid-protocol-validation-2026-06-10.md).
FLUID_USDC_USDT_POOL = "0x3C0441B42195F4aD6aa9a0978E06096ea616CDa7"

FLUID_AMOUNT_OUT_RAW = 49_975_000
FLUID_AMOUNT_OUT_HUMAN = Decimal("49.975")

assert ERC721_TRANSFER_TOPIC == TRANSFER_TOPIC  # same keccak; Fluid reuses the constant


def fluid_parser() -> FluidReceiptParser:
    return FluidReceiptParser(chain="arbitrum")


def fluid_swap_log(recipient: str, index: int) -> dict[str, Any]:
    """``Swap(bool swap0to1, uint256 amountIn, uint256 amountOut, address to)``."""
    data = "0x" + _word(1) + _word(AMOUNT_IN_RAW) + _word(FLUID_AMOUNT_OUT_RAW) + "0" * 24 + recipient[2:].lower()
    return {"address": FLUID_USDC_USDT_POOL, "topics": [SWAP_TOPIC], "data": data, "logIndex": index}


def fluid_logs(wallet: str) -> list[dict[str, Any]]:
    return [
        transfer_log(USDC_ARB, wallet, FLUID_USDC_USDT_POOL, AMOUNT_IN_RAW, 0),
        fluid_swap_log(wallet, 1),
        transfer_log(USDT_ARB, FLUID_USDC_USDT_POOL, wallet, FLUID_AMOUNT_OUT_RAW, 2),
    ]


def _assert_fluid_amounts(amounts: Any) -> None:
    assert amounts is not None, "fluid extraction returned None — the VIB-6043 fail-closed blackout"
    assert amounts.amount_in == AMOUNT_IN_RAW
    assert amounts.amount_out == FLUID_AMOUNT_OUT_RAW
    assert amounts.amount_in_decimal == AMOUNT_IN_HUMAN
    assert amounts.amount_out_decimal == FLUID_AMOUNT_OUT_HUMAN
    assert amounts.token_in.lower() == USDC_ARB
    assert amounts.token_out.lower() == USDT_ARB


def test_fluid_zodiac_stamped_with_safe_log():
    logs = [*fluid_logs(SAFE), module_success_log(3)]
    _assert_fluid_amounts(fluid_parser().extract_swap_amounts(stamp_trading_wallet(receipt(logs), SAFE)))


def test_fluid_zodiac_stamped_without_safe_log():
    stamped = stamp_trading_wallet(receipt(fluid_logs(SAFE)), SAFE)
    _assert_fluid_amounts(fluid_parser().extract_swap_amounts(stamped))


def test_fluid_eoa_receipt_unchanged():
    logs = fluid_logs(AGENT_EOA)
    _assert_fluid_amounts(fluid_parser().extract_swap_amounts(receipt(logs, sender=AGENT_EOA)))


def test_fluid_zodiac_without_wallet_resolution_is_unmeasured():
    """Control: no stamp, no Safe log -> neither leg is identified -> None."""
    assert fluid_parser().extract_swap_amounts(receipt(fluid_logs(SAFE))) is None
