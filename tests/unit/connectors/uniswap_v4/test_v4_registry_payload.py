"""Tests for the V4 registry-payload methods (VIB-4583).

``UniswapV4ReceiptParser.extract_registry_payload_open`` /
``extract_registry_payload_close`` are the live-runner chokepoint that feeds the
``position_registry`` row. These assert the V4 identity tuple (token_id /
pool_id / position_manager) is serialized into the payload, that Empty ≠ Zero
fail-closed skips return None (never a fabricated identity), and that the close
path threads the OPEN-side tokenId (V4 closes carry no NFT tokenId).
"""

from __future__ import annotations

from almanak.connectors.uniswap_v4.addresses import UNISWAP_V4
from almanak.connectors.uniswap_v4.receipt_parser import EVENT_TOPICS, UniswapV4ReceiptParser
from almanak.connectors.uniswap_v4.sdk import PoolKey, _pad_int24, _pad_uint

# ----- OPEN fixture (Base USDC/WETH mint) -----------------------------------

OPEN_CHAIN = "base"
BASE_PM = UNISWAP_V4[OPEN_CHAIN]["position_manager"]
BASE_POOL_MANAGER = UNISWAP_V4[OPEN_CHAIN]["pool_manager"]
WALLET = "0x1111111111111111111111111111111111111111"
USDC_BASE = "0x833589fcd6edb6e08f4c7c32d4f71b54bda02913"
WETH_BASE = "0x4200000000000000000000000000000000000006"
TOKEN_ID = 4242
POOL_ID = "0x" + format(0xABCD, "064x")
TICK_LOWER = -887220
TICK_UPPER = -100
LIQUIDITY_DELTA = 10**15


def _hex32(value: int) -> str:
    return "0x" + format(value, "064x")


def _modify_liquidity_log(*, pool_id: str, sender: str, tick_lower: int, tick_upper: int, liquidity_delta: int, salt: str, pool_manager: str) -> dict:
    sender_padded = "0x" + sender.lower().replace("0x", "").zfill(64)
    ld = (1 << 256) + liquidity_delta if liquidity_delta < 0 else liquidity_delta
    data = (
        "0x"
        + format((1 << 256) + tick_lower if tick_lower < 0 else tick_lower, "064x")
        + format((1 << 256) + tick_upper if tick_upper < 0 else tick_upper, "064x")
        + format(ld, "064x")
        + salt.lower().replace("0x", "").zfill(64)
    )
    return {"address": pool_manager, "topics": [EVENT_TOPICS["ModifyLiquidity"], pool_id, sender_padded], "data": data}


def _erc721_mint_log(*, position_manager: str, wallet: str, token_id: int) -> dict:
    return {
        "address": position_manager,
        "topics": [
            EVENT_TOPICS["Transfer"],
            "0x" + "0" * 64,
            "0x" + wallet.lower().replace("0x", "").zfill(64),
            "0x" + format(token_id, "064x"),
        ],
        "data": "0x",
    }


def _erc20_transfer_log(*, token: str, from_addr: str, to_addr: str, amount: int) -> dict:
    return {
        "address": token,
        "topics": [
            EVENT_TOPICS["Transfer"],
            "0x" + from_addr.lower().replace("0x", "").zfill(64),
            "0x" + to_addr.lower().replace("0x", "").zfill(64),
        ],
        "data": _hex32(amount),
    }


def _canonical_mint_receipt() -> dict:
    salt = _hex32(TOKEN_ID)
    return {
        "transactionHash": "0xabc",
        "logs": [
            _modify_liquidity_log(
                pool_id=POOL_ID, sender=BASE_PM, tick_lower=TICK_LOWER, tick_upper=TICK_UPPER,
                liquidity_delta=LIQUIDITY_DELTA, salt=salt, pool_manager=BASE_POOL_MANAGER,
            ),
            _erc721_mint_log(position_manager=BASE_PM, wallet=WALLET, token_id=TOKEN_ID),
            _erc20_transfer_log(token=USDC_BASE, from_addr=WALLET, to_addr=BASE_POOL_MANAGER, amount=1_000_000_000),
            _erc20_transfer_log(token=WETH_BASE, from_addr=WALLET, to_addr=BASE_POOL_MANAGER, amount=5 * 10**17),
        ],
    }


# ----- CLOSE fixture (Arbitrum WETH/USDC burn) ------------------------------

CLOSE_CHAIN = "arbitrum"
CLOSE_POOL_MANAGER = "0x000000000004444c5dc75cB358380D2e3dE08A90"
CLOSE_PM = "0xbD216513d74C8cf14cf4747E6AaA6420FF64ee9e"
CLOSE_WALLET = "0x1234567890abcdef1234567890abcdef12345678"
A_USDC = "0xaf88d065e77c8cc2239327c5edb3a432268e5831"
A_WETH = "0x82af49447d8a07e3bd95bd0d56f35241523fbab1"
CLOSE_POOL_ID = "0x" + "be" * 32
CLOSE_POOL_KEY = PoolKey(currency0=A_USDC, currency1=A_WETH, fee=500, tick_spacing=10)


def _burn_log(*, liquidity_delta: int = -500_000) -> dict:
    data = "0x" + _pad_int24(-60000) + _pad_int24(60000) + _pad_uint((1 << 256) + liquidity_delta) + "0" * 64
    return {
        "address": CLOSE_POOL_MANAGER,
        "topics": [EVENT_TOPICS["ModifyLiquidity"], CLOSE_POOL_ID, "0x" + "00" * 12 + CLOSE_PM.lower().replace("0x", "")],
        "data": data,
    }


def _close_transfer(*, token: str, amount: int) -> dict:
    return {
        "address": token,
        "topics": [
            EVENT_TOPICS["Transfer"],
            "0x" + "00" * 12 + CLOSE_POOL_MANAGER.lower().replace("0x", ""),
            "0x" + "00" * 12 + CLOSE_WALLET.lower().replace("0x", ""),
        ],
        "data": "0x" + _pad_uint(amount),
    }


def _close_receipt() -> dict:
    return {
        "transactionHash": "0xclose",
        "logs": [
            _burn_log(liquidity_delta=-500_000),
            _close_transfer(token=A_WETH, amount=10**18),
            _close_transfer(token=A_USDC, amount=2_000_000_000),
        ],
    }


def _close_parser() -> UniswapV4ReceiptParser:
    return UniswapV4ReceiptParser(
        chain=CLOSE_CHAIN,
        pool_manager_address=CLOSE_POOL_MANAGER,
        position_manager_address=CLOSE_PM,
        pool_key_lookup=lambda pid, chain: CLOSE_POOL_KEY,
    )


# =============================================================================
# OPEN payload
# =============================================================================


def test_open_payload_carries_v4_identity_tuple() -> None:
    parser = UniswapV4ReceiptParser(chain=OPEN_CHAIN)
    payload = parser.extract_registry_payload_open(_canonical_mint_receipt())
    assert payload is not None
    assert payload["token_id"] == str(TOKEN_ID)
    assert payload["pool_id"] == POOL_ID.lower()
    assert payload["position_manager"] == BASE_PM.lower()
    assert payload["tick_lower"] == TICK_LOWER
    assert payload["tick_upper"] == TICK_UPPER
    # No V3 keys leak in.
    assert "pool_address" not in payload
    assert "nft_manager_addr" not in payload


def test_open_payload_carries_fee_tier_when_positive() -> None:
    parser = UniswapV4ReceiptParser(chain=OPEN_CHAIN)
    payload = parser.extract_registry_payload_open(_canonical_mint_receipt(), fee_tier=500)
    assert payload is not None
    assert payload["fee_tier"] == 500
    # fee_tier=0 / None must NOT add the key (Empty ≠ Zero).
    p2 = parser.extract_registry_payload_open(_canonical_mint_receipt(), fee_tier=0)
    assert p2 is not None and "fee_tier" not in p2


def test_open_payload_none_when_not_an_open() -> None:
    parser = UniswapV4ReceiptParser(chain=OPEN_CHAIN)
    assert parser.extract_registry_payload_open({"logs": []}) is None


def test_open_payload_none_when_position_manager_unknown() -> None:
    """Fail-closed: no V4 PositionManager configured → None, never fabricated."""
    parser = UniswapV4ReceiptParser(chain=OPEN_CHAIN, position_manager_address="")
    # Force the parser to have no PM (simulate an unsupported chain).
    parser.position_manager = ""
    assert parser.extract_registry_payload_open(_canonical_mint_receipt()) is None


# =============================================================================
# CLOSE payload
# =============================================================================


def test_close_payload_threads_open_token_id() -> None:
    parser = _close_parser()
    open_payload = {"token_id": "777", "pool_id": CLOSE_POOL_ID.lower(), "position_manager": CLOSE_PM.lower(), "tick_lower": -60000, "tick_upper": 60000, "liquidity": "999"}
    payload = parser.extract_registry_payload_close(_close_receipt(), open_payload=open_payload)
    assert payload is not None
    assert payload["token_id"] == "777"  # threaded from OPEN (close receipt has no tokenId)
    assert payload["pool_id"] == CLOSE_POOL_ID.lower()
    assert payload["amount0_close"] == str(10**18)
    assert payload["amount1_close"] == str(2_000_000_000)
    # OPEN-time ticks/liquidity merged in.
    assert payload["tick_lower"] == -60000
    assert payload["liquidity"] == "999"


def test_close_payload_none_without_open_token_id() -> None:
    """V4 closes carry no receipt tokenId; without an OPEN row carrying one we
    cannot build the identity hash → None (Empty ≠ Zero, never fabricate)."""
    parser = _close_parser()
    assert parser.extract_registry_payload_close(_close_receipt(), open_payload=None) is None
    assert parser.extract_registry_payload_close(_close_receipt(), open_payload={"pool_id": CLOSE_POOL_ID}) is None


def test_close_payload_none_on_pool_mismatch() -> None:
    """A threaded OPEN row for a DIFFERENT pool → refuse the close (wrong row)."""
    parser = _close_parser()
    open_payload = {"token_id": "777", "pool_id": "0x" + "11" * 32}
    assert parser.extract_registry_payload_close(_close_receipt(), open_payload=open_payload) is None
