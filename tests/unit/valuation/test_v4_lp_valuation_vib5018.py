"""VIB-5018 / VIB-4586 — Uniswap V4 LP valuation path.

Regression + design tests for the identity-faithful V4 LP valuation path in
``PortfolioValuer``.

Background (the "$289M bug"): a Uniswap V4 LP position has a tokenId on the V4
``PositionManager``, NOT on the V3 ``NonfungiblePositionManager``. The generic
LP repricer (``_reprice_lp_on_chain_enriched``) reads ``positions(uint256)`` on
the V3 PM, which for a V4 tokenId returns an unrelated NFT (or garbage). That
corrupted BOTH token identity (``token0_symbol="link"`` on a WETH/USDC pool) AND
amount scaling (~10^7), producing ``value_usd=$289.6M`` for a ~$5 position — at
HIGH confidence.

The gateway exposes ``LookupV4PoolKey`` (pool_id → PoolKey identity) but no
boundary-compliant V4 PositionManager liquidity reader, and the V4 strategy
reports no liquidity/ticks on its open positions. So the V4 valuation re-marks
the receipt-parsed OPEN amounts (from the Layer-3 ``position_events`` row) at
current prices — identity-faithful, order-of-magnitude correct, ESTIMATED.

These tests pin:

1. V4 routes to the dedicated path, never the V3 reader (no "link", no 10^7).
2. Correct token identity / amounts / USD value (within an order of magnitude of
   the true ~$5) from the OPEN amounts re-marked at current price.
3. Confidence is ESTIMATED (not HIGH) for the approximate V4 path.
4. When the V4 path genuinely cannot value (no OPEN event / no identity / no
   price), the snapshot confidence drops to UNAVAILABLE — never a wrong value at
   HIGH (VIB-4584).
"""

from decimal import Decimal
from unittest.mock import MagicMock, patch

import pytest

from almanak.framework.portfolio.models import ValueConfidence
from almanak.framework.teardown.models import PositionInfo, PositionType
from almanak.framework.valuation.portfolio_valuer import PortfolioValuer

# Known-state OPEN row lifted from the frozen lp_v4 baseline (WETH/USDC, ~$4.88).
# position_events OPEN: token0=WETH token1=USDC amount0=1042527846772824 (wei,
# 18dp) amount1=2600197 (wei, 6dp).
_AMOUNT0_WEI = 1042527846772824  # ~0.001042 WETH
_AMOUNT1_WEI = 2600197  # ~2.600197 USDC
_POOL_ID = "0x1d8c55f347727c0fb4f5e1b65cdb93639e0c7102580a7d345e1144cd5a718f54"
_WETH_ADDR = "0x4200000000000000000000000000000000000006"  # WETH on base
_USDC_ADDR = "0x833589fcd6edb6e08f4c7c32d4f71b54bda02913"  # USDC on base


def _resolved(symbol: str, address: str, decimals: int):
    r = MagicMock()
    r.symbol = symbol
    r.address = address
    r.decimals = decimals
    return r


def _patch_resolver(symbol_by_addr: dict[str, tuple[str, int]]):
    """Patch get_token_resolver: resolve(addr|symbol) -> resolved; get_decimals(chain, symbol)."""
    by_symbol = dict(symbol_by_addr.values())
    resolver = MagicMock()

    def _resolve(token, chain):
        lower = token.lower() if isinstance(token, str) else token
        if lower in symbol_by_addr:
            sym, dec = symbol_by_addr[lower]
            return _resolved(sym, lower, dec)
        if token in by_symbol:  # symbol passthrough
            return _resolved(token, "", by_symbol[token])
        return None

    def _get_decimals(chain, symbol):
        if symbol in by_symbol:
            return by_symbol[symbol]
        raise KeyError(symbol)

    resolver.resolve = _resolve
    resolver.get_decimals = _get_decimals
    return patch("almanak.framework.data.tokens.get_token_resolver", return_value=resolver)


def _market(prices: dict[str, Decimal]):
    market = MagicMock()

    def _price(token, quote="USD", *, chain=None):
        # Matches the real MarketSnapshot.price signature (chain is keyword-only,
        # VIB-5722): the valuer threads chain= into every price read.
        if token in prices:
            return prices[token]
        raise ValueError(f"No price for {token}")

    market.price = _price
    return market


def _v4_position(details: dict | None = None) -> PositionInfo:
    base = {
        "pool_address": _POOL_ID,
        "fee_tier": 3000,
        "token0": "WETH",
        "token1": "USDC",
    }
    if details:
        base.update(details)
    return PositionInfo(
        position_type=PositionType.LP,
        position_id="2350913",
        chain="base",
        protocol="uniswap_v4",
        value_usd=Decimal("0"),
        details=base,
    )


def _valuer_with_open_event(
    *,
    gateway_client=None,
    amount0_wei=_AMOUNT0_WEI,
    amount1_wei=_AMOUNT1_WEI,
    token0="WETH",
    token1="USDC",
    has_open=True,
):
    """A PortfolioValuer whose accounting store returns a V4 LP OPEN event."""
    valuer = PortfolioValuer(gateway_client=gateway_client)
    store = MagicMock()
    if has_open:
        store.get_position_events_sync.return_value = [
            {
                "token0": token0,
                "token1": token1,
                "amount0": str(amount0_wei),
                "amount1": str(amount1_wei),
                "value_usd": "4.878330709561607",
                "timestamp": "2026-05-17T15:01:57+00:00",
                "ledger_entry_id": "led-1",
            }
        ]
    else:
        store.get_position_events_sync.return_value = []
    valuer._accounting_store = store
    valuer._deployment_id = "dep-test"
    return valuer


def _pool_key():
    pk = MagicMock()
    pk.currency0 = _WETH_ADDR
    pk.currency1 = _USDC_ADDR
    pk.fee = 3000
    pk.tick_spacing = 60
    pk.hooks = "0x0000000000000000000000000000000000000000"
    return pk


class TestV4IdentityFaithful:
    """The V4 path resolves correct identity / amounts / value — never the V3 garbage."""

    def test_open_amounts_drive_correct_identity_and_value(self):
        valuer = _valuer_with_open_event(gateway_client=MagicMock())
        position = _v4_position()
        market = _market({"WETH": Decimal("2500"), "USDC": Decimal("1")})
        resolver = {_WETH_ADDR: ("WETH", 18), _USDC_ADDR: ("USDC", 6)}

        with (
            _patch_resolver(resolver),
            patch.object(valuer, "_resolve_v4_pool_key", return_value=_pool_key()),
        ):
            value_usd, details, repriced = valuer._reprice_position_enriched(position, "base", market)

        assert repriced is True
        # Identity is correct — NOT the "link"/"WETH" V3-read corruption.
        assert details["token0_symbol"] == "WETH"
        assert details["token1_symbol"] == "USDC"
        # Amounts are human-scaled, not the 10^7 garbage.
        assert Decimal(details["amount0"]) == Decimal(_AMOUNT0_WEI) / Decimal(10**18)
        assert Decimal(details["amount1"]) == Decimal(_AMOUNT1_WEI) / Decimal(10**6)
        # USD value: 0.001042*2500 + 2.6002 ≈ $5.21. Within an order of magnitude
        # of the true ~$4.88, and FAR from the $289.6M corruption.
        assert Decimal("0.5") < value_usd < Decimal("50")
        assert details["valuation_source"] == "v4_open_amounts"
        assert details["valuation_status"] == "estimated"

    def test_snapshot_confidence_is_estimated_not_high(self):
        valuer = _valuer_with_open_event(gateway_client=MagicMock())
        position = _v4_position()
        market = _market({"WETH": Decimal("2500"), "USDC": Decimal("1")})
        resolver = {_WETH_ADDR: ("WETH", 18), _USDC_ADDR: ("USDC", 6)}

        with (
            _patch_resolver(resolver),
            patch.object(valuer, "_resolve_v4_pool_key", return_value=_pool_key()),
        ):
            _, details, _ = valuer._reprice_position_enriched(position, "base", market)
            conf = valuer._determine_value_confidence(
                positions=[MagicMock(details=details)],
                wallet_balances=[],
                positions_unavailable=False,
                wallet_data_incomplete=False,
            )
        assert conf == ValueConfidence.ESTIMATED

    def test_uses_open_event_symbols_when_gateway_unavailable(self):
        """No gateway PoolKey → identity from the OPEN event symbols (still correct)."""
        valuer = _valuer_with_open_event(gateway_client=None)
        position = _v4_position()
        market = _market({"WETH": Decimal("2500"), "USDC": Decimal("1")})
        resolver = {_WETH_ADDR: ("WETH", 18), _USDC_ADDR: ("USDC", 6)}

        with _patch_resolver(resolver):
            value_usd, details, repriced = valuer._reprice_position_enriched(position, "base", market)

        assert repriced is True
        assert details["token0_symbol"] == "WETH"
        assert details["token1_symbol"] == "USDC"
        assert Decimal("0.5") < value_usd < Decimal("50")
        assert details["valuation_status"] == "estimated"


class TestV4DoesNotUseV3Reader:
    """The V4 path must never invoke the V3 LPPositionReader.read_position."""

    def test_v3_reader_never_called_for_v4(self):
        valuer = _valuer_with_open_event(gateway_client=MagicMock())
        position = _v4_position()
        market = _market({"WETH": Decimal("2500"), "USDC": Decimal("1")})
        resolver = {_WETH_ADDR: ("WETH", 18), _USDC_ADDR: ("USDC", 6)}

        with (
            _patch_resolver(resolver),
            patch.object(valuer, "_resolve_v4_pool_key", return_value=_pool_key()),
            patch.object(valuer._lp_reader, "read_position") as read_position,
        ):
            valuer._reprice_position_enriched(position, "base", market)

        read_position.assert_not_called()


class TestV4NoPathUnavailable:
    """VIB-4584 — no value source → UNAVAILABLE, never a wrong value at HIGH."""

    def test_no_open_event_no_value_flags_no_path(self):
        valuer = _valuer_with_open_event(gateway_client=None, has_open=False)
        position = _v4_position()
        market = _market({"WETH": Decimal("2500"), "USDC": Decimal("1")})

        with _patch_resolver({}):
            value_usd, details, repriced = valuer._reprice_position_enriched(position, "base", market)

        assert repriced is False
        assert value_usd == Decimal("0")
        conf = valuer._determine_value_confidence(
            positions=[MagicMock(details={**details, "valuation_status": "no_path"})],
            wallet_balances=[],
            positions_unavailable=True,
            wallet_data_incomplete=False,
        )
        assert conf == ValueConfidence.UNAVAILABLE

    def test_missing_price_flags_no_path_not_high(self):
        valuer = _valuer_with_open_event(gateway_client=MagicMock())
        position = _v4_position()
        market = _market({"USDC": Decimal("1")})  # WETH price missing
        resolver = {_WETH_ADDR: ("WETH", 18), _USDC_ADDR: ("USDC", 6)}

        with (
            _patch_resolver(resolver),
            patch.object(valuer, "_resolve_v4_pool_key", return_value=_pool_key()),
        ):
            value_usd, _details, repriced = valuer._reprice_position_enriched(position, "base", market)

        assert repriced is False  # no price → no value → no_path
        assert value_usd == Decimal("0")

    def test_positive_reported_value_trusted_as_estimated(self):
        """A strategy-asserted positive value is trusted (ESTIMATED), never dropped."""
        valuer = _valuer_with_open_event(gateway_client=None, has_open=False)
        position = _v4_position()
        position.value_usd = Decimal("12.34")
        market = _market({})

        with _patch_resolver({}):
            value_usd, details, repriced = valuer._reprice_position_enriched(position, "base", market)

        assert repriced is True
        assert value_usd == Decimal("12.34")
        assert details.get("valuation_status") == "estimated"


class TestV4OpenAmountsEmptyVsZero:
    """Empty != Zero in the OPEN-amount read."""

    def test_unparseable_amount_is_no_path(self):
        valuer = _valuer_with_open_event(gateway_client=None)
        valuer._accounting_store.get_position_events_sync.return_value = [
            {"token0": "WETH", "token1": "USDC", "amount0": "", "amount1": "2600197"}
        ]
        position = _v4_position()
        market = _market({"WETH": Decimal("2500"), "USDC": Decimal("1")})
        with _patch_resolver({_WETH_ADDR: ("WETH", 18), _USDC_ADDR: ("USDC", 6)}):
            value_usd, _details, repriced = valuer._reprice_position_enriched(position, "base", market)
        assert repriced is False
        assert value_usd == Decimal("0")

    def test_zero_amounts_are_measured_zero(self):
        valuer = _valuer_with_open_event(gateway_client=None, amount0_wei=0, amount1_wei=0)
        position = _v4_position()
        market = _market({"WETH": Decimal("2500"), "USDC": Decimal("1")})
        with _patch_resolver({_WETH_ADDR: ("WETH", 18), _USDC_ADDR: ("USDC", 6)}):
            value_usd, details, repriced = valuer._reprice_position_enriched(position, "base", market)
        assert repriced is True
        assert value_usd == Decimal("0")
        assert details["valuation_status"] == "estimated"


class TestV4DefensiveBranches:
    """Cover the no-value-source defensive branches (decimals / cache / gateway errors)."""

    def test_unknown_decimals_flags_no_path(self):
        valuer = _valuer_with_open_event(gateway_client=None)
        position = _v4_position()
        market = _market({"WETH": Decimal("2500"), "USDC": Decimal("1")})
        # Resolver knows the symbols (resolve) but get_decimals raises for WETH.
        resolver = MagicMock()
        resolver.resolve = lambda token, chain: None  # symbols come from OPEN event

        def _get_decimals(chain, symbol):
            if symbol == "USDC":
                return 6
            raise KeyError(symbol)

        resolver.get_decimals = _get_decimals
        with patch("almanak.framework.data.tokens.get_token_resolver", return_value=resolver):
            value_usd, _details, repriced = valuer._reprice_position_enriched(position, "base", market)
        assert repriced is False
        assert value_usd == Decimal("0")

    def test_recent_open_events_cache_hit(self):
        """Same-iteration in-memory cache is used without an accounting-store read."""
        valuer = PortfolioValuer(gateway_client=None)
        # No accounting store; only the runner-side recent-open cache.
        valuer._accounting_store = None
        valuer._deployment_id = "dep-test"
        valuer._recent_open_events = {
            ("2350913", "LP"): {
                "token0": "WETH",
                "token1": "USDC",
                "amount0": str(_AMOUNT0_WEI),
                "amount1": str(_AMOUNT1_WEI),
            }
        }
        position = _v4_position()
        market = _market({"WETH": Decimal("2500"), "USDC": Decimal("1")})
        with _patch_resolver({_WETH_ADDR: ("WETH", 18), _USDC_ADDR: ("USDC", 6)}):
            value_usd, details, repriced = valuer._reprice_position_enriched(position, "base", market)
        assert repriced is True
        assert Decimal("0.5") < value_usd < Decimal("50")
        assert details["valuation_source"] == "v4_open_amounts"

    def test_cache_hit_without_amounts_falls_through_to_store(self):
        """VIB-5018 live regression: the runner cache dict carries token0/token1/
        ticks/liquidity but NO amount0/amount1 (see strategy_runner
        _update_recent_open_events_cache). A cache hit lacking amounts MUST fall
        through to the store query (which has them), producing ESTIMATED — NOT
        no_path. This is the exact failure the live Anvil-Base re-baseline caught
        (value_usd="0", warning at portfolio_valuer.py:1152) that the original
        unit suite missed because it always seeded the cache WITH amounts.
        """
        valuer = PortfolioValuer(gateway_client=None)
        # Cache hit shaped like the real runner cache PRE-fix: no amount0/amount1.
        valuer._recent_open_events = {
            ("2350913", "LP"): {
                "token0": "WETH",
                "token1": "USDC",
                "tick_lower": -203460,
                "tick_upper": -201480,
                "liquidity": "1002136843936",
                "value_usd": "3.901860",
                # NOTE: no amount0 / amount1 — the live blind spot.
            }
        }
        # Store DOES carry the full OPEN row with amounts (live position_events).
        store = MagicMock()
        store.get_position_events_sync.return_value = [
            {
                "token0": "WETH",
                "token1": "USDC",
                "amount0": "1088710760429413",  # ~0.001088 WETH
                "amount1": "2131992",  # ~2.131992 USDC
            }
        ]
        valuer._accounting_store = store
        valuer._deployment_id = "dep-test"

        position = _v4_position()
        market = _market({"WETH": Decimal("2500"), "USDC": Decimal("1")})
        with _patch_resolver({_WETH_ADDR: ("WETH", 18), _USDC_ADDR: ("USDC", 6)}):
            value_usd, details, repriced = valuer._reprice_position_enriched(position, "base", market)

        # Must NOT be no_path — the ESTIMATED path fires from the store amounts.
        assert repriced is True
        assert details["valuation_status"] == "estimated"
        assert details["valuation_source"] == "v4_open_amounts"
        assert details["token0_symbol"] == "WETH"
        assert details["token1_symbol"] == "USDC"
        # amount0 = 1088710760429413 / 1e18 ≈ 0.001088; value ≈ 0.001088*2500 + 2.132 ≈ $4.85.
        assert Decimal(details["amount0"]) == Decimal("1088710760429413") / Decimal(10**18)
        assert Decimal("0.5") < value_usd < Decimal("50")
        # The store WAS consulted (cache was insufficient).
        store.get_position_events_sync.assert_called_once()

    def test_complete_cache_hit_skips_store(self):
        """A cache hit WITH amounts is self-sufficient — no store round-trip
        (the VIB-5018 cache-stamp complement keeps the fast path)."""
        valuer = PortfolioValuer(gateway_client=None)
        valuer._recent_open_events = {
            ("2350913", "LP"): {
                "token0": "WETH",
                "token1": "USDC",
                "amount0": str(_AMOUNT0_WEI),
                "amount1": str(_AMOUNT1_WEI),
            }
        }
        store = MagicMock()
        valuer._accounting_store = store
        valuer._deployment_id = "dep-test"
        position = _v4_position()
        market = _market({"WETH": Decimal("2500"), "USDC": Decimal("1")})
        with _patch_resolver({_WETH_ADDR: ("WETH", 18), _USDC_ADDR: ("USDC", 6)}):
            value_usd, details, repriced = valuer._reprice_position_enriched(position, "base", market)
        assert repriced is True
        assert details["valuation_source"] == "v4_open_amounts"
        assert Decimal("0.5") < value_usd < Decimal("50")
        store.get_position_events_sync.assert_not_called()

    def test_gateway_lookup_exception_falls_back_to_open_symbols(self):
        """A raising gateway PoolKey lookup must not crash — fall back to OPEN symbols."""
        valuer = _valuer_with_open_event(gateway_client=MagicMock())
        position = _v4_position()
        market = _market({"WETH": Decimal("2500"), "USDC": Decimal("1")})
        with (
            _patch_resolver({_WETH_ADDR: ("WETH", 18), _USDC_ADDR: ("USDC", 6)}),
            patch(
                "almanak.connectors.uniswap_v4.gateway_pool_key_client.make_sync_pool_key_lookup",
                side_effect=RuntimeError("boom"),
            ),
        ):
            value_usd, details, repriced = valuer._reprice_position_enriched(position, "base", market)
        assert repriced is True
        assert details["token0_symbol"] == "WETH"
        assert Decimal("0.5") < value_usd < Decimal("50")

    def test_accounting_store_read_exception_is_no_path(self):
        valuer = PortfolioValuer(gateway_client=None)
        store = MagicMock()
        store.get_position_events_sync.side_effect = RuntimeError("db down")
        valuer._accounting_store = store
        valuer._deployment_id = "dep-test"
        position = _v4_position()
        market = _market({"WETH": Decimal("2500"), "USDC": Decimal("1")})
        with _patch_resolver({_WETH_ADDR: ("WETH", 18), _USDC_ADDR: ("USDC", 6)}):
            value_usd, _details, repriced = valuer._reprice_position_enriched(position, "base", market)
        assert repriced is False
        assert value_usd == Decimal("0")


class TestIsV4LpPosition:
    """V4 routing discriminator is by DATA SHAPE (64-hex pool_id), not protocol name."""

    def test_v4_pool_id_shape_detected(self):
        assert PortfolioValuer._is_v4_lp_position(_v4_position()) is True

    def test_v3_contract_address_shape_not_v4(self):
        # 40-hex pool contract address (V3 shape) → NOT routed to the V4 path.
        position = _v4_position({"pool_address": _WETH_ADDR, "pool_id": None, "pool": None})
        assert PortfolioValuer._is_v4_lp_position(position) is False

    def test_no_pool_identity_not_v4(self):
        position = _v4_position({"pool_address": None, "pool_id": None, "pool": None})
        assert PortfolioValuer._is_v4_lp_position(position) is False


class TestExtractV4PoolId:
    """pool_id extraction: 64-hex accepted, 40-hex (EVM address) rejected."""

    def test_accepts_64_hex_pool_id(self):
        position = _v4_position()
        assert PortfolioValuer._extract_v4_pool_id(position) == _POOL_ID.lower().removeprefix("0x")

    def test_rejects_evm_address_shape(self):
        position = _v4_position({"pool_address": _WETH_ADDR, "pool_id": None, "pool": None})
        assert PortfolioValuer._extract_v4_pool_id(position) is None


class TestCoerceInt:
    """Empty != Zero in liquidity/amount coercion."""

    @pytest.mark.parametrize(
        ("value", "expected"),
        [
            (None, None),
            ("", None),
            ("not-a-number", None),
            ("0", 0),
            (0, 0),
            ("12345", 12345),
            (-230400, -230400),
        ],
    )
    def test_coerce(self, value, expected):
        assert PortfolioValuer._coerce_int(value) == expected


class TestV4SymbolOrdering:
    """pr-audit Important #1 — partial PoolKey resolution must NOT splice a
    user-order ``details`` symbol into a sorted ``currency0<currency1`` slot."""

    def test_partial_poolkey_resolution_falls_back_to_open_pair(self):
        """If only currency0 resolves from its address, fall back to the
        (canonically-sorted) OPEN-event symbol PAIR — never (resolved0, spliced1).
        """
        valuer = PortfolioValuer(gateway_client=MagicMock())
        # details["token1"] is a WRONG/user-order symbol; if the code spliced it
        # in, the test would see "WRONG" instead of the OPEN-event "USDC".
        position = _v4_position({"token1": "WRONG"})
        # Resolver knows currency0 (WETH) but NOT currency1 (USDC) address.
        with (
            _patch_resolver({_WETH_ADDR: ("WETH", 18)}),
            patch.object(valuer, "_resolve_v4_pool_key", return_value=_pool_key()),
        ):
            sym0, sym1 = valuer._resolve_v4_symbols(position, "base", "WETH", "USDC")
        # Falls back to the OPEN pair as a unit — not (WETH, "WRONG"/USDC-spliced).
        assert (sym0, sym1) == ("WETH", "USDC")

    def test_both_currencies_resolve_uses_onchain_pair(self):
        valuer = PortfolioValuer(gateway_client=MagicMock())
        position = _v4_position()
        with (
            _patch_resolver({_WETH_ADDR: ("WETH", 18), _USDC_ADDR: ("USDC", 6)}),
            patch.object(valuer, "_resolve_v4_pool_key", return_value=_pool_key()),
        ):
            sym0, sym1 = valuer._resolve_v4_symbols(position, "base", "open0", "open1")
        # On-chain addresses win when BOTH resolve (authoritative, sorted).
        assert (sym0, sym1) == ("WETH", "USDC")

    def test_symbol_from_address_has_no_details_fallback(self):
        # A resolver miss returns None (NOT a strategy-metadata splice).
        with _patch_resolver({}):
            assert PortfolioValuer._symbol_from_address(_WETH_ADDR, "base") is None
        with _patch_resolver({_WETH_ADDR: ("WETH", 18)}):
            assert PortfolioValuer._symbol_from_address(_WETH_ADDR, "base") == "WETH"


class TestHostedHydrationPayload:
    """Codex P2 / pr-audit #2 — the boot/hosted hydration projection must carry
    amount0/amount1 so a restarted hosted V4 LP values ESTIMATED, not UNAVAILABLE."""

    def test_open_event_payload_carries_amounts(self):
        from almanak.framework.runner._run_loop_helpers import _open_event_payload

        payload = _open_event_payload(
            {
                "token0": "WETH",
                "token1": "USDC",
                "amount0": "1088710760429413",
                "amount1": "2131992",
                "liquidity": "1002136843936",
            }
        )
        assert payload["amount0"] == "1088710760429413"
        assert payload["amount1"] == "2131992"

    def test_open_event_payload_empty_not_zero(self):
        from almanak.framework.runner._run_loop_helpers import _open_event_payload

        # Measured zero preserved; absent stays "" (unmeasured → store fall-through).
        zero = _open_event_payload({"amount0": "0", "amount1": 0})
        assert zero["amount0"] == "0"
        assert zero["amount1"] == "0"
        absent = _open_event_payload({})
        assert absent["amount0"] == ""
        assert absent["amount1"] == ""


# ===========================================================================
# VIB-5024 — live on-chain read → HIGH confidence (ESTIMATED → HIGH upgrade)
# ===========================================================================


def _live_state(
    *,
    liquidity=1002136843936,
    tick_lower=-887220,
    tick_upper=887220,
    current_tick=-50,
    sqrt_price_x96=79228162514264337593543950336,
    pool_id=_POOL_ID,
    tokens_owed0=0,
    tokens_owed1=0,
):
    """A real V4PositionState as the gateway_client.query_v4_position_state would return."""
    from almanak.framework.gateway_client import V4PositionState

    return V4PositionState(
        liquidity=liquidity,
        tick_lower=tick_lower,
        tick_upper=tick_upper,
        current_tick=current_tick,
        sqrt_price_x96=sqrt_price_x96,
        pool_id=pool_id,
        tokens_owed0=tokens_owed0,
        tokens_owed1=tokens_owed1,
    )


def _gateway_with_live_state(state):
    """A gateway_client mock whose ``query_v4_position_state`` returns ``state``.

    Crucially stubbed at the gateway_client boundary (NOT above the
    ``_reprice_v4_lp_live`` seam) so the connector-backed registry reader and the
    valuer's concentrated-liquidity math run end-to-end (the #2678 lesson:
    a fix that is inert in the live path passes unit tests stubbed too high)."""
    from unittest.mock import MagicMock

    gw = MagicMock()
    gw.query_v4_position_state.return_value = state
    return gw


class TestV4LiveOnChainHigh:
    """VIB-5024 — a successful live gateway read values the position at HIGH."""

    def test_live_read_drives_high_confidence(self):
        state = _live_state()
        gateway = _gateway_with_live_state(state)
        valuer = _valuer_with_open_event(gateway_client=gateway)
        position = _v4_position()
        market = _market({"WETH": Decimal("2500"), "USDC": Decimal("1")})
        resolver = {_WETH_ADDR: ("WETH", 18), _USDC_ADDR: ("USDC", 6)}

        with (
            _patch_resolver(resolver),
            patch.object(valuer, "_resolve_v4_pool_key", return_value=_pool_key()),
        ):
            value_usd, details, repriced = valuer._reprice_position_enriched(position, "base", market)

        # The live boundary read actually fired (end-to-end, not stubbed above it).
        gateway.query_v4_position_state.assert_called_once()
        assert details["valuation_source"] == "v4_on_chain"
        assert details["valuation_status"] == "onchain"  # NOT "estimated"
        assert repriced is True
        # Amounts come from live liquidity + tick math, not the OPEN amounts.
        assert details["token0_symbol"] == "WETH"
        assert details["token1_symbol"] == "USDC"
        assert "liquidity" in details and details["liquidity"] == str(state.liquidity)
        assert value_usd > 0

        conf = valuer._determine_value_confidence(
            positions=[MagicMock(details=details)],
            wallet_balances=[],
            positions_unavailable=False,
            wallet_data_incomplete=False,
        )
        assert conf == ValueConfidence.HIGH

    def test_high_value_includes_uncollected_fees(self):
        """V3 parity: HIGH value = principal + uncollected fees (tokens_owed0/1)."""
        # 1 WETH owed (1e18 @ $2500) + 1000 USDC owed (1e9 @ $1) = $3500 in fees.
        owed0, owed1 = 10**18, 1000 * 10**6
        state_no_fees = _live_state(tokens_owed0=0, tokens_owed1=0)
        state_fees = _live_state(tokens_owed0=owed0, tokens_owed1=owed1)
        market = _market({"WETH": Decimal("2500"), "USDC": Decimal("1")})
        resolver = {_WETH_ADDR: ("WETH", 18), _USDC_ADDR: ("USDC", 6)}

        def _value(state):
            valuer = _valuer_with_open_event(gateway_client=_gateway_with_live_state(state))
            with (
                _patch_resolver(resolver),
                patch.object(valuer, "_resolve_v4_pool_key", return_value=_pool_key()),
            ):
                v, details, _ = valuer._reprice_position_enriched(_v4_position(), "base", market)
            return v, details

        base_value, base_details = _value(state_no_fees)
        fee_value, fee_details = _value(state_fees)

        assert base_details["fees_usd"] == "0"
        assert fee_details["tokens_owed0"] == str(owed0)
        assert fee_details["tokens_owed1"] == str(owed1)
        # principal identical; the delta is exactly the fee component ($3500).
        assert Decimal(fee_details["fees_usd"]) == Decimal("3500")
        assert fee_value - base_value == Decimal("3500")

    def test_live_read_uses_exact_tick_math(self):
        """A narrow in-range position values via sqrtPriceX96 tick math (exact)."""
        # current_tick=0 in [-100, 100] → in range, mix of both tokens.
        state = _live_state(
            liquidity=10**18,
            tick_lower=-100,
            tick_upper=100,
            current_tick=0,
            sqrt_price_x96=79228162514264337593543950336,  # tick≈0
        )
        gateway = _gateway_with_live_state(state)
        valuer = _valuer_with_open_event(gateway_client=gateway)
        position = _v4_position()
        market = _market({"WETH": Decimal("2500"), "USDC": Decimal("1")})
        resolver = {_WETH_ADDR: ("WETH", 18), _USDC_ADDR: ("USDC", 6)}

        with (
            _patch_resolver(resolver),
            patch.object(valuer, "_resolve_v4_pool_key", return_value=_pool_key()),
        ):
            _value, details, _repriced = valuer._reprice_position_enriched(position, "base", market)

        assert details["in_range"] is True
        assert Decimal(details["amount0"]) > 0
        assert Decimal(details["amount1"]) > 0


class TestV4LiveReadFallsBackToEstimated:
    """When the live read is unavailable, fall back to the ESTIMATED OPEN-amount path."""

    def test_live_read_failure_falls_back_to_estimated(self):
        # Gateway returns None (partial / failed on-chain read).
        gateway = _gateway_with_live_state(None)
        valuer = _valuer_with_open_event(gateway_client=gateway)
        position = _v4_position()
        market = _market({"WETH": Decimal("2500"), "USDC": Decimal("1")})
        resolver = {_WETH_ADDR: ("WETH", 18), _USDC_ADDR: ("USDC", 6)}

        with (
            _patch_resolver(resolver),
            patch.object(valuer, "_resolve_v4_pool_key", return_value=_pool_key()),
        ):
            value_usd, details, repriced = valuer._reprice_position_enriched(position, "base", market)

        gateway.query_v4_position_state.assert_called_once()
        assert repriced is True
        # Fell back to the ESTIMATED OPEN-amount re-mark — still correct, still ESTIMATED.
        assert details["valuation_source"] == "v4_open_amounts"
        assert details["valuation_status"] == "estimated"
        assert Decimal("0.5") < value_usd < Decimal("50")

    def test_onchain_poolid_mismatch_falls_back_to_estimated(self):
        """Stored pool_id != on-chain pool_id → never value at HIGH (identity guard).

        The gateway returns the AUTHORITATIVE pool_id (keccak of the tokenId's
        PoolKey); symbols are resolved from the position's STORED pool_id. A
        divergence means live amounts would pair with wrong-pool symbols — the
        $289M identity-bug class — so the live tier must decline and fall back to
        ESTIMATED rather than emit HIGH.
        """
        # Live read succeeds but reports a DIFFERENT pool than the stored one.
        state = _live_state(pool_id="0x" + "ab" * 32)
        gateway = _gateway_with_live_state(state)
        valuer = _valuer_with_open_event(gateway_client=gateway)
        position = _v4_position()  # stored pool_id == _POOL_ID (≠ the live state's)
        market = _market({"WETH": Decimal("2500"), "USDC": Decimal("1")})
        resolver = {_WETH_ADDR: ("WETH", 18), _USDC_ADDR: ("USDC", 6)}

        with (
            _patch_resolver(resolver),
            patch.object(valuer, "_resolve_v4_pool_key", return_value=_pool_key()),
        ):
            value_usd, details, repriced = valuer._reprice_position_enriched(position, "base", market)

        gateway.query_v4_position_state.assert_called_once()  # live read fired
        assert repriced is True
        # Declined HIGH on the identity divergence; used the ESTIMATED path instead.
        assert details["valuation_status"] == "estimated"
        assert details["valuation_source"] == "v4_open_amounts"
        assert value_usd > 0

    def test_live_read_failure_no_open_event_is_unavailable(self):
        """Neither live read nor OPEN amounts → no_path → UNAVAILABLE (never wrong-HIGH)."""
        gateway = _gateway_with_live_state(None)
        valuer = _valuer_with_open_event(gateway_client=gateway, has_open=False)
        position = _v4_position()
        market = _market({"WETH": Decimal("2500"), "USDC": Decimal("1")})

        with (
            _patch_resolver({}),
            patch.object(valuer, "_resolve_v4_pool_key", return_value=None),
        ):
            value_usd, details, repriced = valuer._reprice_position_enriched(position, "base", market)

        assert repriced is False
        assert value_usd == Decimal("0")
        conf = valuer._determine_value_confidence(
            positions=[MagicMock(details={**details, "valuation_status": "no_path"})],
            wallet_balances=[],
            positions_unavailable=True,
            wallet_data_incomplete=False,
        )
        assert conf == ValueConfidence.UNAVAILABLE

    def test_no_gateway_client_uses_estimated(self):
        """No gateway → live tier short-circuits → ESTIMATED OPEN-amount path."""
        valuer = _valuer_with_open_event(gateway_client=None)
        position = _v4_position()
        market = _market({"WETH": Decimal("2500"), "USDC": Decimal("1")})
        resolver = {_WETH_ADDR: ("WETH", 18), _USDC_ADDR: ("USDC", 6)}

        with _patch_resolver(resolver):
            _value, details, repriced = valuer._reprice_position_enriched(position, "base", market)

        assert repriced is True
        assert details["valuation_status"] == "estimated"
