"""Real-fork regression: CurveReceiptParser decodes Polygon am3pool LP events (VIB-5434).

These fixtures are REAL logs captured on a Polygon Anvil fork (Alchemy archive,
block 89425611, 2026-06-30) from the live am3pool aave-type StableSwap pool
``0x445FE580eF8d70FF569aB36e80c647af338db351``. The underlying-deposit flow is
frozen on current forks (Aave V2 Polygon ``VL_RESERVE_FROZEN`` — see VIB-5551),
so the open/close were executed via the aToken-direct path
(``use_underlying=False``), which emits the SAME pool ``AddLiquidity`` /
``RemoveLiquidity`` events the underlying path would.

This is the receipt-parser deliverable for VIB-5434: it proves the parser already
decodes am3pool's events (the VIB-4307 "missing signatures" xfail reason was
STALE — every am3pool topic0 was already in ``EVENT_TOPICS``). The intent test in
``tests/intents/polygon/test_curve_lp.py`` stays strict-xfail'd for the SEPARATE
execution blocker (Aave V2 frozen, VIB-5551), not for any parser gap.

Provenance (real on-fork txs):
- LP_OPEN  add_liquidity([100 amDAI,0,0],0,false): tx 0x3be9ed60…b1f1913a
- LP_CLOSE remove_liquidity(LP,[0,0,0],false):     tx 0x6db800b0…be060013
"""

from decimal import Decimal
from unittest.mock import patch

from almanak.connectors.curve.receipt_parser import CurveEventType, CurveReceiptParser

# Real am3pool pool contract (the AddLiquidity/RemoveLiquidity emitter) — matches
# CURVE_POOLS["polygon"]["3pool"]["address"] (corrected in VIB-5434).
AM3POOL = "0x445fe580ef8d70ff569ab36e80c647af338db351"
AM3CRV_LP = "0xe7a24ef0c5e95ffb0f6684b813a78f2a3ad7d171"
PROVIDER_TOPIC = "0x000000000000000000000000f39fd6e51aad88f6f4ce6ab8827279cfffb92266"
ZERO_TOPIC = "0x" + "00" * 32
TRANSFER_TOPIC = "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"

# Real AddLiquidity3 log (topic0 0x423f6495…df0d): provider(indexed),
# amounts[3]=[100e18,0,0], fees[3], invariant, token_supply.
ADD_LIQUIDITY_TOPIC0 = "0x423f6495a08fc652425cf4ed0d1f9e37e571d9b9529b1c1c23cce780b2e7df0d"
ADD_LIQUIDITY_DATA = (
    "0x"
    "0000000000000000000000000000000000000000000000056bc75e2d63100000"  # amounts[0] = 100e18 (DAI)
    "0000000000000000000000000000000000000000000000000000000000000000"  # amounts[1] = 0 (USDC.e)
    "0000000000000000000000000000000000000000000000000000000000000000"  # amounts[2] = 0 (USDT)
    "0000000000000000000000000000000000000000000000000000db5874362b37"  # fees[0]
    "0000000000000000000000000000000000000000000000000000000000001a83"  # fees[1]
    "0000000000000000000000000000000000000000000000000000000000001488"  # fees[2]
    "00000000000000000000000000000000000000000001f72c13cb144fe1cbf7cd"  # invariant
    "00000000000000000000000000000000000000000001b762e2508de75ee48764"  # token_supply
)
# Real LP mint Transfer (from 0x0 to wallet): 87309190543053990718 am3CRV (18 dp).
LP_MINTED_RAW = 87309190543053990718
LP_MINT_DATA = "0x000000000000000000000000000000000000000000000004bba88e3709a0ab3e"

# Real RemoveLiquidity3 log (topic0 0xa49d4cf0…252d): provider(indexed),
# amounts[3], fees[3]=[0,0,0], token_supply. Proportional close → all 3 coins.
REMOVE_LIQUIDITY_TOPIC0 = "0xa49d4cf02656aebf8c771f5a8585638a2a15ee6c97cf7205d4208ed7c1df252d"
REMOVE_AMOUNTS = [35762604797064604931, 36191732, 28029909]  # amDAI(18) / amUSDC(6) / amUSDT(6)
REMOVE_LIQUIDITY_DATA = (
    "0x"
    "000000000000000000000000000000000000000000000001f04e4b95a3b76d03"  # amounts[0] = 35762604797064604931
    "0000000000000000000000000000000000000000000000000000000002283df4"  # amounts[1] = 36191732
    "0000000000000000000000000000000000000000000000000000000001abb3d5"  # amounts[2] = 28029909
    "0000000000000000000000000000000000000000000000000000000000000000"  # fees[0] = 0
    "0000000000000000000000000000000000000000000000000000000000000000"  # fees[1] = 0
    "0000000000000000000000000000000000000000000000000000000000000000"  # fees[2] = 0
    "00000000000000000000000000000000000000000001b75e26a7ffb05543dc26"  # token_supply
)


def _add_receipt() -> dict:
    return {
        "status": 1,
        "transactionHash": "0x3be9ed60600edc31aeb155fc2851d6ab78afc85914a1e712bdc1b7e1b1f1913a",
        "blockNumber": 89425615,
        "gasUsed": 365752,
        "from": "0xf39fd6e51aad88f6f4ce6ab8827279cfffb92266",
        "logs": [
            {
                "address": AM3CRV_LP,
                "topics": [TRANSFER_TOPIC, ZERO_TOPIC, PROVIDER_TOPIC],
                "data": LP_MINT_DATA,
                "logIndex": 3,
            },
            {
                "address": AM3POOL,
                "topics": [ADD_LIQUIDITY_TOPIC0, PROVIDER_TOPIC],
                "data": ADD_LIQUIDITY_DATA,
                "logIndex": 4,
            },
        ],
    }


def _remove_receipt() -> dict:
    return {
        "status": 1,
        "transactionHash": "0x6db800b0ea08e63499831503c3d79c0ffa211a24b1ebe45a9c325b72be060013",
        "blockNumber": 89425616,
        "gasUsed": 250000,
        "from": "0xf39fd6e51aad88f6f4ce6ab8827279cfffb92266",
        "logs": [
            {
                "address": AM3POOL,
                "topics": [REMOVE_LIQUIDITY_TOPIC0, PROVIDER_TOPIC],
                "data": REMOVE_LIQUIDITY_DATA,
                "logIndex": 4,
            },
        ],
    }


class TestAm3poolRealLogDecode:
    """am3pool (aave-type, 3-coin) open/close decode through CurveReceiptParser."""

    def test_add_liquidity3_decodes(self):
        parser = CurveReceiptParser(chain="polygon")
        result = parser.parse_receipt(_add_receipt())
        assert result.success
        add_events = [e for e in result.events if e.event_type == CurveEventType.ADD_LIQUIDITY]
        assert len(add_events) == 1, "exactly one AddLiquidity event expected"
        ev = add_events[0]
        assert ev.event_name == "AddLiquidity3"
        assert ev.contract_address.lower() == AM3POOL
        # 3-coin amounts in DAI / USDC.e / USDT order (single-sided DAI deposit).
        assert ev.data["token_amounts"] == [100_000_000_000_000_000_000, 0, 0]
        assert len(ev.data["fees"]) == 3

    def test_lp_open_data_pool_and_symbols(self):
        parser = CurveReceiptParser(chain="polygon")
        open_data = parser.extract_lp_open_data(_add_receipt())
        assert open_data is not None
        assert open_data.pool_address == AM3POOL
        assert open_data.amount0 == 100_000_000_000_000_000_000
        assert open_data.amount1 == 0
        # The registry-address fix (VIB-5434) restores coin-symbol resolution:
        # a Curve LP_OPEN ledger row carries no token0/token1, so accounting needs
        # these to price each coin. Was None while the registry held the dead address.
        assert open_data.coin_symbols == ["DAI", "USDC.e", "USDT"]

    def test_lp_tokens_received(self):
        parser = CurveReceiptParser(chain="polygon")
        # Curve LP tokens are 18 dp by protocol invariant; force the static-only
        # fallback so this stays hermetic (no gateway round-trip).
        with patch.object(parser, "_resolve_decimals", return_value=None):
            minted = parser.extract_lp_tokens_received(_add_receipt())
        assert minted == Decimal(LP_MINTED_RAW) / Decimal(10**18)

    def test_remove_liquidity3_decodes_three_coins(self):
        parser = CurveReceiptParser(chain="polygon")
        result = parser.parse_receipt(_remove_receipt())
        assert result.success
        rm_events = [e for e in result.events if e.event_type == CurveEventType.REMOVE_LIQUIDITY]
        assert len(rm_events) == 1
        ev = rm_events[0]
        assert ev.event_name == "RemoveLiquidity3"
        assert ev.contract_address.lower() == AM3POOL
        # Proportional close returns all 3 coins (the intent test's len==3 assertion).
        assert ev.data["token_amounts"] == REMOVE_AMOUNTS
        assert ev.data["fees"] == [0, 0, 0]

    def test_lp_close_data_maps_three_coins(self):
        parser = CurveReceiptParser(chain="polygon")
        close_data = parser.extract_lp_close_data(_remove_receipt())
        assert close_data is not None
        assert close_data.pool_address == AM3POOL
        # DAI / USDC.e land on amount0 / amount1; USDT (coin index 2) on additional_amounts.
        assert close_data.amount0_collected == REMOVE_AMOUNTS[0]
        assert close_data.amount1_collected == REMOVE_AMOUNTS[1]
        assert close_data.additional_amounts == {2: REMOVE_AMOUNTS[2]}
        assert close_data.coin_symbols == ["DAI", "USDC.e", "USDT"]
