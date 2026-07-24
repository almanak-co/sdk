"""Real-fork regression: CurveReceiptParser decodes Polygon frxUSD/USDT NG events (VIB-5551).

These fixtures are REAL logs captured on a Polygon Anvil fork (publicnode,
fork block 90788018, 2026-07-24) from the live "FrxUSD USDT0 v1" StableSwap NG
pool ``0x5BC930b8f81F4cEEE3E3527159C3bDF453BcaAe9`` — the pool that replaces
the aave-type am3pool (frozen Aave V2 backing, VIB-5551) as the Polygon Curve
LP/swap representative.

The ops were executed with the EXACT calldata the adapter emits:
``exchange(int128,int128,uint256,uint256)`` (0x3df02124),
NG dynamic-array ``add_liquidity(uint256[],uint256)`` (0xb72df5de) and
``remove_liquidity(uint256,uint256[])`` (0xd40ddb8c). This locks:

* NG dynamic-array ``AddLiquidity`` / ``RemoveLiquidity`` decode
  (``AddLiquidityDyn`` / ``RemoveLiquidityDyn`` topics),
* plain int128 ``TokenExchange`` decode on an NG pool,
* accounting ``coin_symbols`` resolution from the CURVE_POOLS registry entry
  (``['USDT', 'frxUSD']``) — the surface the dead am3pool address once nulled
  (VIB-5434),
* NG "LP token IS the pool" mint/burn Transfer extraction.

Provenance (real on-fork txs):
- SWAP     exchange(0,1,100e6,0):              tx 0x2f8f7b2f…d1f8cb4f (block 90788021)
- LP_OPEN  add_liquidity([10e6,10e18],0):      tx 0x4f3a78b8…4e15b60c (block 90788022)
- LP_CLOSE remove_liquidity(19.9211…e18,[0,0]): tx 0xc0114a1c…54c2f0b4e (block 90788024)

Full round-trip evidence: tests/reports/vib-5551-polygon-frxusd-usdt-realfork.md.
"""

from decimal import Decimal
from unittest.mock import patch

from almanak.connectors.curve.receipt_parser import CurveEventType, CurveReceiptParser

# NG pool contract — also the LP token (StableSwap NG). Matches
# CURVE_POOLS["polygon"]["frxusd_usdt"]["address"].
POOL = "0x5bc930b8f81f4ceee3e3527159c3bdf453bcaae9"
USDT = "0xc2132d05d31c914a87c6611c10748aeb04b58e8f"
FRXUSD = "0x80eede496655fb9047dd39d9f418d5483ed600df"
PROVIDER_TOPIC = "0x000000000000000000000000f39fd6e51aad88f6f4ce6ab8827279cfffb92266"
POOL_TOPIC = "0x0000000000000000000000005bc930b8f81f4ceee3e3527159c3bdf453bcaae9"
ZERO_TOPIC = "0x" + "00" * 32
TRANSFER_TOPIC = "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"

# --- SWAP: TokenExchange(address indexed buyer, int128 sold_id, uint256
# tokens_sold, int128 bought_id, uint256 tokens_bought). NG StableSwap keeps
# the legacy int128 topic0 0x8b3e96f2…7140.
TOKEN_EXCHANGE_TOPIC0 = "0x8b3e96f2b889fa771c53c981b40daf005f63f637f1869f707052d15a3dd97140"
SWAP_TOKENS_SOLD = 100_000_000  # 100 USDT (6 dp)
SWAP_TOKENS_BOUGHT = 99_984_203_839_149_248_513  # 99.9842… frxUSD (18 dp)
TOKEN_EXCHANGE_DATA = (
    "0x"
    "0000000000000000000000000000000000000000000000000000000000000000"  # sold_id = 0 (USDT)
    "0000000000000000000000000000000000000000000000000000000005f5e100"  # tokens_sold = 100e6
    "0000000000000000000000000000000000000000000000000000000000000001"  # bought_id = 1 (frxUSD)
    "0000000000000000000000000000000000000000000000056b8f3fa716bd1001"  # tokens_bought
)

# --- LP_OPEN: NG dynamic-array AddLiquidity(address indexed provider,
# uint256[] token_amounts, uint256[] fees, uint256 invariant, uint256
# token_supply) — topic0 0x189c623b…22a2 (``AddLiquidityDyn``).
ADD_LIQUIDITY_DYN_TOPIC0 = "0x189c623b666b1b45b83d7178f39b8c087cb09774317ca2f53c2d3c3726f222a2"
ADD_AMOUNTS = [10_000_000, 10_000_000_000_000_000_000]  # 10 USDT / 10 frxUSD
ADD_LIQUIDITY_DATA = (
    "0x"
    "0000000000000000000000000000000000000000000000000000000000000080"  # offset token_amounts
    "00000000000000000000000000000000000000000000000000000000000000e0"  # offset fees
    "0000000000000000000000000000000000000000000013981342a3f500d871a9"  # invariant
    "0000000000000000000000000000000000000000000013844df0c14464fd749f"  # token_supply
    "0000000000000000000000000000000000000000000000000000000000000002"  # token_amounts.length
    "0000000000000000000000000000000000000000000000000000000000989680"  # amounts[0] = 10e6 USDT
    "0000000000000000000000000000000000000000000000008ac7230489e80000"  # amounts[1] = 10e18 frxUSD
    "0000000000000000000000000000000000000000000000000000000000000002"  # fees.length
    "0000000000000000000000000000000000000000000000000000000000000013"  # fees[0]
    "0000000000000000000000000000000000000000000000000000122d7ad5545f"  # fees[1]
)
# NG LP mint Transfer (from 0x0 to wallet) emitted by the POOL address itself.
LP_MINTED_RAW = 19_921_135_463_314_271_074  # 19.9211… LP (18 dp)
LP_MINT_DATA = "0x000000000000000000000000000000000000000000000001147617283d48ab62"

# --- LP_CLOSE: NG dynamic-array RemoveLiquidity(address indexed provider,
# uint256[] token_amounts, uint256[] fees, uint256 token_supply) — topic0
# 0x347ad828…80ea (``RemoveLiquidityDyn``). Proportional close → 2 coins;
# the pool emitted an EMPTY fees array.
REMOVE_LIQUIDITY_DYN_TOPIC0 = "0x347ad828e58cbe534d8f6b67985d791360756b18f0d95fd9f197a66cc46480ea"
REMOVE_AMOUNTS = [9_800_184, 10_199_784_772_849_163_378]  # USDT (6 dp) / frxUSD (18 dp)
REMOVE_LIQUIDITY_DATA = (
    "0x"
    "0000000000000000000000000000000000000000000000000000000000000060"  # offset token_amounts
    "00000000000000000000000000000000000000000000000000000000000000c0"  # offset fees
    "000000000000000000000000000000000000000000001383397aaa1c27b4c93d"  # token_supply
    "0000000000000000000000000000000000000000000000000000000000000002"  # token_amounts.length
    "00000000000000000000000000000000000000000000000000000000009589f8"  # amounts[0] = USDT
    "0000000000000000000000000000000000000000000000008d8cea35cb1cdc72"  # amounts[1] = frxUSD
    "0000000000000000000000000000000000000000000000000000000000000000"  # fees.length = 0
)
LP_BURN_DATA = "0x000000000000000000000000000000000000000000000001147617283d48ab62"


def _swap_receipt() -> dict:
    return {
        "status": 1,
        "transactionHash": "0x2f8f7b2f726f2d704951b4b682d0b1e2ee1e7e8ac819ad71e1279b96d1f8cb4f",
        "blockNumber": 90788021,
        "gasUsed": 171111,
        "from": "0xf39fd6e51aad88f6f4ce6ab8827279cfffb92266",
        "logs": [
            {
                "address": USDT,
                "topics": [TRANSFER_TOPIC, PROVIDER_TOPIC, POOL_TOPIC],
                "data": "0x0000000000000000000000000000000000000000000000000000000005f5e100",
                "logIndex": 0,
            },
            {
                "address": FRXUSD,
                "topics": [TRANSFER_TOPIC, POOL_TOPIC, PROVIDER_TOPIC],
                "data": "0x0000000000000000000000000000000000000000000000056b8f3fa716bd1001",
                "logIndex": 2,
            },
            {
                "address": POOL,
                "topics": [TOKEN_EXCHANGE_TOPIC0, PROVIDER_TOPIC],
                "data": TOKEN_EXCHANGE_DATA,
                "logIndex": 3,
            },
        ],
    }


def _add_receipt() -> dict:
    return {
        "status": 1,
        "transactionHash": "0x4f3a78b80268e566808d330c398a99a6d54b9b9e66be25efa9e737364e15b60c",
        "blockNumber": 90788022,
        "gasUsed": 226002,
        "from": "0xf39fd6e51aad88f6f4ce6ab8827279cfffb92266",
        "logs": [
            {
                # NG: the POOL is its own LP token — mint Transfer from 0x0.
                "address": POOL,
                "topics": [TRANSFER_TOPIC, ZERO_TOPIC, PROVIDER_TOPIC],
                "data": LP_MINT_DATA,
                "logIndex": 4,
            },
            {
                "address": POOL,
                "topics": [ADD_LIQUIDITY_DYN_TOPIC0, PROVIDER_TOPIC],
                "data": ADD_LIQUIDITY_DATA,
                "logIndex": 5,
            },
        ],
    }


def _remove_receipt() -> dict:
    return {
        "status": 1,
        "transactionHash": "0xc0114a1cf0f504bb98a2cc53b05228f3f454825483c0ada9d7885f954c2f0b4e",
        "blockNumber": 90788024,
        "gasUsed": 152950,
        "from": "0xf39fd6e51aad88f6f4ce6ab8827279cfffb92266",
        "logs": [
            {
                # NG: LP burn Transfer to 0x0 emitted by the pool itself.
                "address": POOL,
                "topics": [TRANSFER_TOPIC, PROVIDER_TOPIC, ZERO_TOPIC],
                "data": LP_BURN_DATA,
                "logIndex": 2,
            },
            {
                "address": POOL,
                "topics": [REMOVE_LIQUIDITY_DYN_TOPIC0, PROVIDER_TOPIC],
                "data": REMOVE_LIQUIDITY_DATA,
                "logIndex": 3,
            },
        ],
    }


class TestFrxusdUsdtRealLogDecode:
    """frxUSD/USDT (StableSwap NG, 2-coin) swap/open/close decode through CurveReceiptParser."""

    def test_token_exchange_decodes(self):
        parser = CurveReceiptParser(chain="polygon")
        result = parser.parse_receipt(_swap_receipt())
        assert result.success
        swaps = [e for e in result.events if e.event_type == CurveEventType.TOKEN_EXCHANGE]
        assert len(swaps) == 1, "exactly one TokenExchange event expected"
        ev = swaps[0]
        assert ev.contract_address.lower() == POOL
        assert ev.data["sold_id"] == 0
        assert ev.data["tokens_sold"] == SWAP_TOKENS_SOLD
        assert ev.data["bought_id"] == 1
        assert ev.data["tokens_bought"] == SWAP_TOKENS_BOUGHT

    def test_add_liquidity_dyn_decodes(self):
        parser = CurveReceiptParser(chain="polygon")
        result = parser.parse_receipt(_add_receipt())
        assert result.success
        add_events = [e for e in result.events if e.event_type == CurveEventType.ADD_LIQUIDITY]
        assert len(add_events) == 1, "exactly one AddLiquidity event expected"
        ev = add_events[0]
        assert ev.event_name == "AddLiquidityDyn"
        assert ev.contract_address.lower() == POOL
        assert ev.data["token_amounts"] == ADD_AMOUNTS
        assert len(ev.data["fees"]) == 2

    def test_lp_open_data_pool_and_symbols(self):
        parser = CurveReceiptParser(chain="polygon")
        open_data = parser.extract_lp_open_data(_add_receipt())
        assert open_data is not None
        assert open_data.pool_address == POOL
        assert open_data.amount0 == ADD_AMOUNTS[0]
        assert open_data.amount1 == ADD_AMOUNTS[1]
        # Accounting coin-symbol resolution from the registry entry: a Curve
        # LP_OPEN ledger row carries no token0/token1, so accounting prices each
        # coin through these symbols (both CURVE_USD_STABLE_SYMBOLS members).
        assert open_data.coin_symbols == ["USDT", "frxUSD"]

    def test_lp_tokens_received(self):
        parser = CurveReceiptParser(chain="polygon")
        # Curve LP tokens are 18 dp by protocol invariant; force the static-only
        # fallback so this stays hermetic (no gateway round-trip).
        with patch.object(parser, "_resolve_decimals", return_value=None):
            minted = parser.extract_lp_tokens_received(_add_receipt())
        assert minted == Decimal(LP_MINTED_RAW) / Decimal(10**18)

    def test_remove_liquidity_dyn_decodes_two_coins(self):
        parser = CurveReceiptParser(chain="polygon")
        result = parser.parse_receipt(_remove_receipt())
        assert result.success
        rm_events = [e for e in result.events if e.event_type == CurveEventType.REMOVE_LIQUIDITY]
        assert len(rm_events) == 1
        ev = rm_events[0]
        assert ev.event_name == "RemoveLiquidityDyn"
        assert ev.contract_address.lower() == POOL
        # Proportional close returns both coins (the intent test's len==2 assertion).
        assert ev.data["token_amounts"] == REMOVE_AMOUNTS

    def test_lp_close_data_maps_two_coins(self):
        parser = CurveReceiptParser(chain="polygon")
        close_data = parser.extract_lp_close_data(_remove_receipt())
        assert close_data is not None
        assert close_data.pool_address == POOL
        assert close_data.amount0_collected == REMOVE_AMOUNTS[0]
        assert close_data.amount1_collected == REMOVE_AMOUNTS[1]
        assert close_data.coin_symbols == ["USDT", "frxUSD"]
