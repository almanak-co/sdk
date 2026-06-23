"""Tests for Curve Receipt Parser (Refactored)."""

from decimal import Decimal
from types import SimpleNamespace

from almanak.connectors.curve.receipt_parser import (
    CurveEventType,
    CurveReceiptParser,
)

# Test data
POOL_ADDRESS = "0xbebc44782c7db0a1a60cb6fe97d0b483032ff1c7"
USER_ADDRESS = "0x742d35cc6634c0532925a3b844bc454e4438f44e"


def create_token_exchange_log(buyer, sold_id, tokens_sold, bought_id, tokens_bought):
    """Create TokenExchange log with int128 token IDs."""

    # Convert signed int128 to two's complement if negative
    def int128_to_hex(value):
        if value < 0:
            value = (1 << 128) + value
        return f"{value:064x}"

    data = int128_to_hex(sold_id) + f"{tokens_sold:064x}" + int128_to_hex(bought_id) + f"{tokens_bought:064x}"

    return {
        "address": POOL_ADDRESS,
        "topics": [
            "0x8b3e96f2b889fa771c53c981b40daf005f63f637f1869f707052d15a3dd97140",
            f"0x000000000000000000000000{buyer[2:].lower()}",
        ],
        "data": f"0x{data}",
        "logIndex": 0,
    }


def create_add_liquidity_2_log(provider, amounts, fees, invariant, supply):
    """Create AddLiquidity log for 2-coin pool."""
    data = f"{amounts[0]:064x}{amounts[1]:064x}" + f"{fees[0]:064x}{fees[1]:064x}" + f"{invariant:064x}{supply:064x}"

    return {
        "address": POOL_ADDRESS,
        "topics": [
            "0x26f55a85081d24974e85c6c00045d0f0453991e95873f52bff0d21af4079a768",
            f"0x000000000000000000000000{provider[2:].lower()}",
        ],
        "data": f"0x{data}",
        "logIndex": 1,
    }


def create_remove_liquidity_2_log(provider, amounts, fees, supply):
    """Create RemoveLiquidity log for 2-coin pool."""
    data = f"{amounts[0]:064x}{amounts[1]:064x}" + f"{fees[0]:064x}{fees[1]:064x}" + f"{supply:064x}"

    return {
        "address": POOL_ADDRESS,
        "topics": [
            "0x7c363854ccf79623411f8995b362bce5eddff18c927edc6f5dbbb5e05819a82c",
            f"0x000000000000000000000000{provider[2:].lower()}",
        ],
        "data": f"0x{data}",
        "logIndex": 2,
    }


def create_add_liquidity_3_log(provider, amounts, fees, invariant, supply, pool=POOL_ADDRESS):
    """Create AddLiquidity log for a 3-coin NG pool (e.g. 3pool DAI/USDC/USDT).

    Signature: AddLiquidity(address,uint256[3],uint256[3],uint256,uint256).
    """
    data = "".join(f"{a:064x}" for a in amounts) + "".join(f"{f:064x}" for f in fees) + f"{invariant:064x}{supply:064x}"
    return {
        "address": pool,
        "topics": [
            "0x423f6495a08fc652425cf4ed0d1f9e37e571d9b9529b1c1c23cce780b2e7df0d",
            f"0x000000000000000000000000{provider[2:].lower()}",
        ],
        "data": f"0x{data}",
        "logIndex": 1,
    }


class TestCurveReceiptParser:
    """Tests for CurveReceiptParser."""

    def test_parse_token_exchange(self):
        """Test parsing TokenExchange event."""
        parser = CurveReceiptParser(chain="ethereum")

        sold_id = 0  # DAI
        bought_id = 1  # USDC
        tokens_sold = 1_000_000_000_000_000_000_000  # 1000 DAI
        tokens_bought = 999_000_000  # 999 USDC

        receipt = {
            "transactionHash": "0x123",
            "blockNumber": 12345,
            "status": 1,
            "logs": [create_token_exchange_log(USER_ADDRESS, sold_id, tokens_sold, bought_id, tokens_bought)],
            "gasUsed": 150000,
        }

        result = parser.parse_receipt(receipt)

        assert result.success is True
        assert len(result.events) == 1
        assert result.events[0].event_type == CurveEventType.TOKEN_EXCHANGE
        assert result.events[0].data["buyer"] == USER_ADDRESS.lower()
        assert result.events[0].data["sold_id"] == sold_id
        assert result.events[0].data["tokens_sold"] == tokens_sold
        assert result.events[0].data["bought_id"] == bought_id
        assert result.events[0].data["tokens_bought"] == tokens_bought

        # Check swap_events
        assert len(result.swap_events) == 1
        assert result.swap_events[0].sold_id == sold_id
        assert result.swap_events[0].bought_id == bought_id

    def test_parse_token_exchange_3pool(self):
        """Test parsing TokenExchange for 3-coin pool."""
        parser = CurveReceiptParser()

        # 3-coin pool indices (DAI, USDC, USDT)
        sold_id = 1  # USDC
        bought_id = 2  # USDT
        tokens_sold = 1_000_000_000  # 1000 USDC
        tokens_bought = 999_500_000  # 999.5 USDT

        receipt = {
            "transactionHash": "0x456",
            "blockNumber": 12346,
            "status": 1,
            "logs": [create_token_exchange_log(USER_ADDRESS, sold_id, tokens_sold, bought_id, tokens_bought)],
            "gasUsed": 150000,
        }

        result = parser.parse_receipt(receipt)

        assert result.success is True
        assert result.events[0].data["sold_id"] == sold_id
        assert result.events[0].data["bought_id"] == bought_id
        assert result.swap_events[0].tokens_sold == tokens_sold
        assert result.swap_events[0].tokens_bought == tokens_bought

    def test_parse_token_exchange_underlying(self):
        """Test parsing TokenExchangeUnderlying event."""
        parser = CurveReceiptParser()

        # Define test values
        sold_id = 1
        bought_id = 2
        tokens_sold = 1_000_000_000_000_000_000_000  # 1000 tokens (18 decimals)
        tokens_bought = 999_000_000  # 999 tokens (6 decimals)

        # Convert signed int128 to two's complement if negative
        def int128_to_hex(value):
            if value < 0:
                value = (1 << 128) + value
            return f"{value:064x}"

        # Construct properly formatted data (exactly 256 hex chars)
        data = int128_to_hex(sold_id) + f"{tokens_sold:064x}" + int128_to_hex(bought_id) + f"{tokens_bought:064x}"

        receipt = {
            "transactionHash": "0x789",
            "blockNumber": 12347,
            "status": 1,
            "logs": [
                {
                    "address": POOL_ADDRESS,
                    "topics": [
                        "0xd013ca23e77a65003c2c659c5442c00c805371b7fc1ebd4c206c41d1536bd90b",
                        f"0x000000000000000000000000{USER_ADDRESS[2:].lower()}",
                    ],
                    "data": f"0x{data}",
                    "logIndex": 0,
                }
            ],
            "gasUsed": 150000,
        }

        result = parser.parse_receipt(receipt)

        assert result.success is True
        assert result.events[0].event_type == CurveEventType.TOKEN_EXCHANGE_UNDERLYING
        assert result.events[0].data["buyer"] == USER_ADDRESS.lower()
        assert result.events[0].data["sold_id"] == sold_id
        assert result.events[0].data["tokens_sold"] == tokens_sold
        assert result.events[0].data["bought_id"] == bought_id
        assert result.events[0].data["tokens_bought"] == tokens_bought

    def test_parse_add_liquidity_2pool(self):
        """Test parsing AddLiquidity event for 2-coin pool."""
        parser = CurveReceiptParser()

        amounts = [1_000_000, 2_000_000]
        fees = [100, 200]
        invariant = 3_000_000
        supply = 2_900_000

        receipt = {
            "transactionHash": "0xabc",
            "blockNumber": 12348,
            "status": 1,
            "logs": [create_add_liquidity_2_log(USER_ADDRESS, amounts, fees, invariant, supply)],
            "gasUsed": 200000,
        }

        result = parser.parse_receipt(receipt)

        assert result.success is True
        assert len(result.events) == 1
        assert result.events[0].event_type == CurveEventType.ADD_LIQUIDITY
        assert result.events[0].data["provider"] == USER_ADDRESS.lower()
        assert result.events[0].data["token_amounts"] == amounts
        assert result.events[0].data["fees"] == fees
        assert result.events[0].data["invariant"] == invariant
        assert result.events[0].data["token_supply"] == supply

    def test_parse_remove_liquidity_2pool(self):
        """Test parsing RemoveLiquidity event for 2-coin pool."""
        parser = CurveReceiptParser()

        amounts = [1_000_000, 2_000_000]
        fees = [100, 200]
        supply = 2_700_000

        receipt = {
            "transactionHash": "0xdef",
            "blockNumber": 12349,
            "status": 1,
            "logs": [create_remove_liquidity_2_log(USER_ADDRESS, amounts, fees, supply)],
            "gasUsed": 200000,
        }

        result = parser.parse_receipt(receipt)

        assert result.success is True
        assert result.events[0].event_type == CurveEventType.REMOVE_LIQUIDITY
        assert result.events[0].data["token_amounts"] == amounts
        assert result.events[0].data["fees"] == fees
        assert result.events[0].data["token_supply"] == supply

    def test_failed_transaction(self):
        """Test handling failed transactions."""
        parser = CurveReceiptParser()

        receipt = {
            "transactionHash": "0x111",
            "blockNumber": 12350,
            "status": 0,
            "logs": [create_token_exchange_log(USER_ADDRESS, 0, 1000000, 1, 999000)],
            "gasUsed": 50000,
        }

        result = parser.parse_receipt(receipt)

        assert result.success is True
        assert result.transaction_success is False
        assert result.error == "Transaction reverted"

    def test_failed_transaction_with_empty_logs(self):
        """Regression for issue #2064: early-revert receipt (status=0, logs=[])
        must surface the revert via ``error``."""
        parser = CurveReceiptParser()

        receipt = {
            "transactionHash": "0x111",
            "blockNumber": 12350,
            "status": 0,
            "logs": [],
            "gasUsed": 50000,
        }

        result = parser.parse_receipt(receipt)

        assert result.success is True
        assert result.transaction_success is False
        assert result.error == "Transaction reverted"

    def test_empty_logs(self):
        """Test parsing receipt with no logs."""
        parser = CurveReceiptParser()

        receipt = {
            "transactionHash": "0x222",
            "blockNumber": 12351,
            "status": 1,
            "logs": [],
            "gasUsed": 21000,
        }

        result = parser.parse_receipt(receipt)

        assert result.success is True
        assert len(result.events) == 0

    def test_backward_compatibility(self):
        """Test backward compatibility methods."""
        parser = CurveReceiptParser()

        token_exchange_topic = "0x8b3e96f2b889fa771c53c981b40daf005f63f637f1869f707052d15a3dd97140"
        assert parser.is_curve_event(token_exchange_topic) is True
        assert parser.get_event_type(token_exchange_topic) == CurveEventType.TOKEN_EXCHANGE

        unknown_topic = "0x9999999999999999999999999999999999999999999999999999999999999999"
        assert parser.is_curve_event(unknown_topic) is False
        assert parser.get_event_type(unknown_topic) == CurveEventType.UNKNOWN

    def test_bytes_transaction_hash(self):
        """Test handling bytes transaction hash."""
        parser = CurveReceiptParser()

        receipt = {
            "transactionHash": b"\x12\x34\x56\x78",
            "blockNumber": 12352,
            "status": 1,
            "logs": [create_token_exchange_log(USER_ADDRESS, 0, 1000000, 1, 999000)],
            "gasUsed": 150000,
        }

        result = parser.parse_receipt(receipt)

        assert result.success is True
        assert result.transaction_hash == "0x12345678"


class TestCurvePrimitiveMoneyLegs:
    """VIB-3587 — Curve LP_OPEN declares funded coin legs (no fabricated 0-leg).

    Exercises the REAL declared-leg path end-to-end: the actual
    ``CurveReceiptParser.extract_primitive_money_legs`` joined to the actual
    ledger dispatcher ``_extract_from_declared_legs`` (no ``SimpleNamespace``
    stand-ins). 3pool on Ethereum is DAI (idx 0, 18dp) / USDC (idx 1, 6dp) /
    USDT (idx 2, 6dp), so the amounts double as a decimals-scaling check.
    """

    # Ethereum 3pool (matches CURVE_POOLS["ethereum"]["3pool"]["address"]).
    POOL = "0xbEbc44782C7dB0a1A60Cb6fe97d0b483032FF1C7"
    PROVIDER = "0x742d35cc6634c0532925a3b844bc454e4438f44e"

    def _receipt(self, amounts):
        log = create_add_liquidity_3_log(
            self.PROVIDER, amounts, fees=[0, 0, 0], invariant=1, supply=10**18, pool=self.POOL
        )
        return {"status": 1, "transactionHash": "0x" + "ab" * 32, "logs": [log]}

    def _tuple_for(self, amounts):
        from almanak.framework.observability.ledger import _extract_from_declared_legs

        parser = CurveReceiptParser(chain="ethereum")
        legs = parser.extract_primitive_money_legs(self._receipt(amounts))
        assert legs is not None
        token_in, token_out, amount_in, amount_out, _eff, _slip = _extract_from_declared_legs(legs)
        return legs, token_in, token_out, amount_in, amount_out

    def test_single_sided_usdc_no_fabricated_zero_leg(self):
        """Single-sided USDC (coin idx 1): only USDC is declared; DAI/USDT absent.

        The legacy ``amount0``/``amount1`` two-slot guess persisted a fabricated
        ``amount_in='0'`` for the unfunded DAI (coin idx 0) AND lost USDC's
        symbol (3pool's pool string has no ``/`` to parse). The declared-leg path
        puts USDC on ``token_in``/``amount_in`` and leaves ``token_out`` empty —
        no fabricated zero leg.
        """
        legs, token_in, token_out, amount_in, amount_out = self._tuple_for([0, 100_000_000, 0])

        # Exactly one declared leg — the funded coin. Unfunded coins are ABSENT
        # (Empty != Zero), not measured-zero legs.
        assert len(legs.legs) == 1
        assert legs.legs[0].token == "USDC"
        assert legs.legs[0].amount.is_measured
        assert legs.legs[0].amount.value == Decimal("100")

        # Funded coin lands on the IN slot; OUT stays empty (no fabricated zero).
        assert token_in == "USDC"
        assert amount_in == "100"
        assert token_out == ""
        assert amount_out == ""

    def test_single_sided_non_leading_coin_usdt_idx2(self):
        """Single-sided USDT (coin idx 2): the legacy path dropped this entirely.

        ``amount0``/``amount1`` only ever carry coins 0/1, so a deposit of coin
        index 2+ was invisible to the ledger. The declared-leg path surfaces it.
        """
        legs, token_in, token_out, amount_in, amount_out = self._tuple_for([0, 0, 50_000_000])

        assert len(legs.legs) == 1
        assert legs.legs[0].token == "USDT"
        assert token_in == "USDT"
        assert amount_in == "50"
        assert token_out == ""
        assert amount_out == ""

    def test_two_sided_dai_usdc_lane_symmetric(self):
        """Two-sided DAI+USDC stays lane-symmetric with the legacy projection."""
        legs, token_in, token_out, amount_in, amount_out = self._tuple_for([10 * 10**18, 10 * 10**6, 0])

        assert [leg.token for leg in legs.legs] == ["DAI", "USDC"]
        assert token_in == "DAI"
        assert amount_in == "10"
        assert token_out == "USDC"
        assert amount_out == "10"

    def test_no_add_liquidity_event_returns_none(self):
        """A receipt with no AddLiquidity event declares no legs (legacy fallback)."""
        parser = CurveReceiptParser(chain="ethereum")
        receipt = {
            "status": 1,
            "transactionHash": "0x" + "cd" * 32,
            "logs": [create_token_exchange_log(USER_ADDRESS, 0, 1000000, 1, 999000)],
        }
        assert parser.extract_primitive_money_legs(receipt) is None

    def test_unknown_pool_returns_none(self):
        """An AddLiquidity from a pool absent from CURVE_POOLS yields no legs.

        Without the pool-coin address map we cannot bind an amount to the coin it
        funds, so the parser returns None and the legacy two-slot path is used.
        """
        parser = CurveReceiptParser(chain="ethereum")
        unknown_pool = "0x0000000000000000000000000000000000000bad"
        log = create_add_liquidity_3_log(
            self.PROVIDER, [0, 1_000_000, 0], fees=[0, 0, 0], invariant=1, supply=10**18, pool=unknown_pool
        )
        receipt = {"status": 1, "transactionHash": "0x" + "ef" * 32, "logs": [log]}
        assert parser.extract_primitive_money_legs(receipt) is None

    def test_funded_amount_beyond_known_coins_falls_back_to_legacy(self, monkeypatch):
        """A funded ``token_amounts`` slot with no bound pool coin → legacy fallback.

        Guards a stale / truncated ``CURVE_POOLS.coin_addresses`` (fewer coins than
        the pool actually has): the funded coin at the unbound index must NOT be
        silently dropped from the declared legs. Since declared legs bypass the
        legacy path, the parser returns ``None`` so the legacy two-slot extraction
        runs instead of declaring a lossy subset.
        """
        from almanak.connectors.curve import adapter

        # Truncate 3pool to its first TWO coins (DAI, USDC) — drop USDT (idx 2).
        pools = adapter.CURVE_POOLS
        truncated = {
            chain: {
                name: (
                    {**data, "coin_addresses": list(data.get("coin_addresses", []))[:2]}
                    if str(data.get("address", "")).lower() == self.POOL.lower()
                    else data
                )
                for name, data in chain_pools.items()
            }
            for chain, chain_pools in pools.items()
        }
        monkeypatch.setattr(adapter, "CURVE_POOLS", truncated)

        parser = CurveReceiptParser(chain="ethereum")
        # USDT (idx 2) is funded but unbound to a known coin → fall back to legacy.
        assert parser.extract_primitive_money_legs(self._receipt([0, 0, 50_000_000])) is None

        # A deposit confined to the known coins still declares legs normally.
        legs = parser.extract_primitive_money_legs(self._receipt([0, 100_000_000, 0]))
        assert legs is not None
        assert [leg.token for leg in legs.legs] == ["USDC"]

    def test_resolver_miss_keeps_coin_address_identity(self, monkeypatch):
        """When the static symbol resolver misses, the leg keeps the coin ADDRESS.

        ``coin_address`` is pool-ordered chain-truth — emitting ``""`` would
        discard a known token identity (Empty != Zero). The leg falls back to the
        lowercased address (the ledger treats ``token`` opaquely) instead.
        """
        from almanak.framework.data import tokens

        class _MissResolver:
            def resolve(self, *_args, **_kwargs):
                # Symbol unknown, but decimals still resolvable (USDC = 6dp).
                return SimpleNamespace(symbol="", decimals=6)

        monkeypatch.setattr(tokens, "get_token_resolver", lambda: _MissResolver())

        parser = CurveReceiptParser(chain="ethereum")
        legs = parser.extract_primitive_money_legs(self._receipt([0, 100_000_000, 0]))
        assert legs is not None
        assert len(legs.legs) == 1
        # USDC's pool-coin address (3pool idx 1), lowercased — NOT "".
        assert legs.legs[0].token == "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48"
        assert legs.legs[0].amount.is_measured
        assert legs.legs[0].amount.value == Decimal("100")
