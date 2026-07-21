"""Unit tests for LP, Perp, and Vault accounting builders (VIB-3515/3516/3517)."""

from __future__ import annotations

import json
from decimal import Decimal
from unittest.mock import MagicMock

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_intent(intent_type_str: str, **kwargs):
    intent = MagicMock()
    it = MagicMock()
    it.value = intent_type_str
    intent.intent_type = it
    intent.protocol = kwargs.get("protocol", "aerodrome")
    intent.pool = kwargs.get("pool", "USDC/DAI/0xpooladdr")
    intent.token0 = kwargs.get("token0", "USDC")
    intent.token1 = kwargs.get("token1", "DAI")
    intent.token0_decimals = kwargs.get("token0_decimals", 6)
    intent.token1_decimals = kwargs.get("token1_decimals", 18)
    intent.market = kwargs.get("market", "ETH/USD")
    intent.collateral_token = kwargs.get("collateral_token", "USDC")
    intent.size_usd = kwargs.get("size_usd", Decimal("1000"))
    intent.collateral_amount = kwargs.get("collateral_amount", Decimal("100"))
    intent.is_long = kwargs.get("is_long", True)
    intent.leverage = kwargs.get("leverage", Decimal("10"))
    intent.vault_address = kwargs.get("vault_address", "0xvaultaddr")
    intent.amount = kwargs.get("amount", Decimal("500"))
    return intent


def _make_result(tx_hash: str = "0xtxhash", lp_open_data=None, lp_close_data=None):
    result = MagicMock()
    result.tx_hash = tx_hash
    result.transaction_results = []
    result.lp_open_data = lp_open_data
    result.lp_close_data = lp_close_data
    result.extracted_data = {}
    return result


_COMMON_KWARGS = {
    "deployment_id": "strat-1",
    "cycle_id": "cycle-1",
    "execution_mode": "paper",
    "chain": "base",
    "wallet_address": "0xwallet",
}


# ---------------------------------------------------------------------------
# LP accounting builder
# ---------------------------------------------------------------------------


class TestLPAccountingBuilder:
    def test_returns_none_for_non_lp_intent(self) -> None:
        from almanak.framework.accounting.lp_accounting import build_lp_accounting_event

        intent = _make_intent("SWAP")
        result = build_lp_accounting_event(intent=intent, result=_make_result(), **_COMMON_KWARGS)
        assert result is None

    def test_lp_open_event_built(self) -> None:
        from almanak.framework.accounting.lp_accounting import build_lp_accounting_event

        lp_open = MagicMock()
        lp_open.amount0 = 100_000_000  # 100 USDC (6 decimals)
        lp_open.amount1 = 100 * 10**18  # 100 DAI (18 decimals)

        intent = _make_intent("LP_OPEN", protocol="aerodrome", pool="USDC/DAI/0xpool")
        result_obj = _make_result(lp_open_data=lp_open)

        event = build_lp_accounting_event(intent=intent, result=result_obj, **_COMMON_KWARGS)

        assert event is not None
        assert event.event_type == "LP_OPEN"
        # pool="USDC/DAI/0xpool" → last segment starts with "0x" → extracted address
        assert event.pool_address == "0xpool"
        assert event.token0 == "USDC"
        assert event.token1 == "DAI"
        assert event.amount0 == Decimal("100")  # 100_000_000 / 10^6
        assert event.amount1 == Decimal("100")  # 100e18 / 10^18
        assert "lp:" in event.position_key

    def test_lp_close_event_built(self) -> None:
        from almanak.framework.accounting.lp_accounting import build_lp_accounting_event

        lp_close = MagicMock()
        lp_close.amount0_collected = 95_000_000  # 95 USDC
        lp_close.amount1_collected = 95 * 10**18
        lp_close.fees0 = 500_000
        lp_close.fees1 = 5 * 10**17

        intent = _make_intent("LP_CLOSE", protocol="uniswap_v3", pool="USDC/DAI/0xpool2")
        result_obj = _make_result(lp_close_data=lp_close)

        event = build_lp_accounting_event(intent=intent, result=result_obj, **_COMMON_KWARGS)

        assert event is not None
        assert event.event_type == "LP_CLOSE"
        assert event.amount0 is not None
        assert event.fees0_collected is not None

    def test_payload_json_roundtrip(self) -> None:
        from almanak.framework.accounting.lp_accounting import build_lp_accounting_event

        intent = _make_intent("LP_OPEN", protocol="aerodrome", pool="USDC/DAI/0xpool")
        event = build_lp_accounting_event(intent=intent, result=_make_result(), **_COMMON_KWARGS)

        assert event is not None
        payload = json.loads(event.to_payload_json())
        assert payload["event_type"] == "LP_OPEN"
        assert "position_key" in payload

    def test_identity_fields(self) -> None:
        from almanak.framework.accounting.lp_accounting import build_lp_accounting_event

        intent = _make_intent("LP_OPEN")
        event = build_lp_accounting_event(intent=intent, result=_make_result(), **_COMMON_KWARGS)

        assert event is not None
        assert event.identity.deployment_id == "strat-1"
        assert event.identity.chain == "base"

    def test_token0_token1_parsed_from_pool_string_when_bare_attrs_missing(self) -> None:
        """VIB-3584: token0/token1 are parsed from pool string when intent lacks bare attrs."""
        from almanak.framework.accounting.lp_accounting import build_lp_accounting_event

        intent = MagicMock()
        it = MagicMock()
        it.value = "LP_OPEN"
        intent.intent_type = it
        intent.protocol = "uniswap_v3"
        intent.pool = "WETH/USDC/500"
        # Simulate LP intents that don't expose bare token0/token1 (returns None)
        del intent.token0
        del intent.token1
        del intent.token_a
        del intent.token_b
        intent.token0_decimals = None
        intent.token1_decimals = None

        event = build_lp_accounting_event(intent=intent, result=_make_result(), **_COMMON_KWARGS)
        assert event is not None
        assert event.token0 == "WETH"
        assert event.token1 == "USDC"

    def test_cost_basis_usd_computed_from_price_oracle(self) -> None:
        """VIB-3583: cost_basis_usd is computed when price_oracle is provided."""
        from almanak.framework.accounting.lp_accounting import build_lp_accounting_event

        lp_open = MagicMock()
        lp_open.amount0 = 1_000_000  # 1 USDC (6 decimals)
        lp_open.amount1 = 1 * 10**18  # 1 WETH (18 decimals)

        intent = _make_intent(
            "LP_OPEN",
            protocol="uniswap_v3",
            pool="USDC/WETH/3000",
            token0="USDC",
            token1="WETH",
            token0_decimals=6,
            token1_decimals=18,
        )
        result_obj = _make_result(lp_open_data=lp_open)

        price_oracle = {"USDC": Decimal("1.0"), "WETH": Decimal("2500.0")}
        event = build_lp_accounting_event(intent=intent, result=result_obj, price_oracle=price_oracle, **_COMMON_KWARGS)

        assert event is not None
        # 1 USDC * 1.0 + 1 WETH * 2500.0 = 2501.0
        assert event.cost_basis_usd == Decimal("2501.0")

    def test_cost_basis_usd_none_without_price_oracle(self) -> None:
        """VIB-3583: cost_basis_usd stays None when no price_oracle provided."""
        from almanak.framework.accounting.lp_accounting import build_lp_accounting_event

        intent = _make_intent("LP_OPEN", protocol="aerodrome", pool="USDC/DAI/0xpool")
        event = build_lp_accounting_event(intent=intent, result=_make_result(), **_COMMON_KWARGS)

        assert event is not None
        assert event.cost_basis_usd is None

    def test_cost_basis_usd_none_when_one_leg_price_missing(self) -> None:
        """Partial cost basis silently undercounts — must return None if any leg lacks a price."""
        from almanak.framework.accounting.lp_accounting import build_lp_accounting_event

        lp_open = MagicMock()
        lp_open.amount0 = 1_000_000  # 1 USDC
        lp_open.amount1 = 1 * 10**18  # 1 LONGTOKEN

        intent = _make_intent(
            "LP_OPEN",
            protocol="uniswap_v3",
            pool="USDC/LONGTOKEN/3000",
            token0="USDC",
            token1="LONGTOKEN",
            token0_decimals=6,
            token1_decimals=18,
        )
        result_obj = _make_result(lp_open_data=lp_open)

        # Only USDC has a price; LONGTOKEN is unlisted
        price_oracle = {"USDC": Decimal("1.0")}
        event = build_lp_accounting_event(intent=intent, result=result_obj, price_oracle=price_oracle, **_COMMON_KWARGS)

        assert event is not None
        assert event.cost_basis_usd is None  # partial total would mislead PnL math

    def test_cost_basis_usd_none_when_decimals_assumed(self) -> None:
        """cost_basis_usd must be None when token decimals are assumed (18) — amounts may be off 1e12."""
        from almanak.framework.accounting.lp_accounting import build_lp_accounting_event

        lp_open = MagicMock()
        lp_open.amount0 = 1_000_000  # Would be 1 USDC at 6 dec, but 0.000001 at assumed 18
        lp_open.amount1 = 1 * 10**18

        intent = MagicMock()
        it = MagicMock()
        it.value = "LP_OPEN"
        intent.intent_type = it
        intent.protocol = "uniswap_v3"
        intent.pool = "USDC/WETH/3000"
        del intent.token0
        del intent.token1
        del intent.token_a
        del intent.token_b
        # Simulate no decimal info — all decimal attrs must be explicitly None to trigger assumed_decimals
        intent.token0_decimals = None
        intent.token1_decimals = None
        intent.token_a_decimals = None
        intent.token_b_decimals = None

        price_oracle = {"USDC": Decimal("1.0"), "WETH": Decimal("2500.0")}
        event = build_lp_accounting_event(
            intent=intent, result=_make_result(lp_open_data=lp_open), price_oracle=price_oracle, **_COMMON_KWARGS
        )

        assert event is not None
        assert event.cost_basis_usd is None  # assumed decimals → skip cost basis


# ---------------------------------------------------------------------------
# Perp accounting builder
# ---------------------------------------------------------------------------


class TestPerpAccountingBuilder:
    def test_returns_none_for_non_perp_intent(self) -> None:
        from almanak.framework.accounting.perp_accounting import build_perp_accounting_event

        intent = _make_intent("LP_OPEN")
        result = build_perp_accounting_event(intent=intent, result=_make_result(), **_COMMON_KWARGS)
        assert result is None

    def test_perp_open_event_built(self) -> None:
        from almanak.framework.accounting.perp_accounting import build_perp_accounting_event

        intent = _make_intent("PERP_OPEN", protocol="gmx_v2", market="ETH/USD")
        event = build_perp_accounting_event(intent=intent, result=_make_result(), **_COMMON_KWARGS)

        assert event is not None
        assert event.event_type == "PERP_OPEN"
        assert event.market == "ETH/USD"
        assert event.collateral_token == "USDC"
        assert event.size_usd == Decimal("1000")
        assert event.is_long is True
        assert event.leverage == Decimal("10")
        assert "perp:" in event.position_key

    def test_perp_close_event_built(self) -> None:
        from almanak.framework.accounting.perp_accounting import build_perp_accounting_event

        intent = _make_intent("PERP_CLOSE", protocol="gmx_v2")
        event = build_perp_accounting_event(intent=intent, result=_make_result(), **_COMMON_KWARGS)

        assert event is not None
        assert event.event_type == "PERP_CLOSE"
        assert event.realized_pnl_usd is None

    def test_payload_json_roundtrip(self) -> None:
        from almanak.framework.accounting.perp_accounting import build_perp_accounting_event

        intent = _make_intent("PERP_OPEN", protocol="gmx_v2")
        event = build_perp_accounting_event(intent=intent, result=_make_result(), **_COMMON_KWARGS)

        assert event is not None
        payload = json.loads(event.to_payload_json())
        assert payload["event_type"] == "PERP_OPEN"
        assert payload["market"] == "ETH/USD"
        # VIB-5941: the accounting payload now emits the canonical schema key
        # ``size`` (was the non-schema ``size_usd``), plus intent-known ``is_long``,
        # and the perp primitive contract bumped to v2.
        assert payload["size"] == "1000"
        assert "size_usd" not in payload
        assert payload["is_long"] is True
        assert payload["primitive_version"] == 2


# ---------------------------------------------------------------------------
# Vault accounting builder
# ---------------------------------------------------------------------------


class TestVaultAccountingBuilder:
    def test_returns_none_for_non_vault_intent(self) -> None:
        from almanak.framework.accounting.vault_accounting import build_vault_accounting_event

        intent = _make_intent("SWAP")
        result = build_vault_accounting_event(intent=intent, result=_make_result(), **_COMMON_KWARGS)
        assert result is None

    def test_vault_deposit_event_built(self) -> None:
        from almanak.framework.accounting.vault_accounting import build_vault_accounting_event

        intent = _make_intent("VAULT_DEPOSIT", protocol="metamorpho", vault_address="0xvault1", amount=Decimal("500"))
        event = build_vault_accounting_event(intent=intent, result=_make_result(), **_COMMON_KWARGS)

        assert event is not None
        assert event.event_type == "VAULT_DEPOSIT"
        assert event.vault_address == "0xvault1"
        assert event.assets_amount == Decimal("500")
        assert event.shares_amount is None
        assert "vault:" in event.position_key

    def test_vault_redeem_event_built(self) -> None:
        from almanak.framework.accounting.vault_accounting import build_vault_accounting_event

        intent = _make_intent("VAULT_REDEEM", protocol="metamorpho")
        event = build_vault_accounting_event(intent=intent, result=_make_result(), **_COMMON_KWARGS)

        assert event is not None
        assert event.event_type == "VAULT_WITHDRAW"

    def test_amount_all_string_ignored(self) -> None:
        from almanak.framework.accounting.vault_accounting import build_vault_accounting_event

        intent = _make_intent("VAULT_DEPOSIT", amount="all")
        event = build_vault_accounting_event(intent=intent, result=_make_result(), **_COMMON_KWARGS)

        assert event is not None
        assert event.assets_amount is None  # "all" → not convertible to Decimal

    def test_payload_json_roundtrip(self) -> None:
        from almanak.framework.accounting.vault_accounting import build_vault_accounting_event

        intent = _make_intent("VAULT_DEPOSIT", vault_address="0xvault2", amount=Decimal("250"))
        event = build_vault_accounting_event(intent=intent, result=_make_result(), **_COMMON_KWARGS)

        assert event is not None
        payload = json.loads(event.to_payload_json())
        assert payload["event_type"] == "VAULT_DEPOSIT"
        assert payload["assets_amount"] == "250"


# ---------------------------------------------------------------------------
# Aerodrome stable pool state persistence
# ---------------------------------------------------------------------------


class TestAerodromeStablePoolPersistence:
    def _make_strategy(self):
        import sys
        from pathlib import Path

        strategy_dir = Path(__file__).parent.parent.parent / "strategies/incubating/aerodrome_stable_pool_lp"
        sys.path.insert(0, str(strategy_dir))
        try:
            import importlib.util

            spec = importlib.util.spec_from_file_location("strategy", strategy_dir / "strategy.py")
            mod = importlib.util.module_from_spec(spec)  # type: ignore[arg-type]
            spec.loader.exec_module(mod)  # type: ignore[union-attr]
            return mod.AerodromeStablePoolLPStrategy
        finally:
            sys.path.pop(0)

    def test_get_persistent_state_includes_has_position(self) -> None:
        StratCls = self._make_strategy()
        strat = MagicMock(spec=StratCls)
        strat._has_position = True
        strat._lp_opened_count = 3

        # Call get_persistent_state via the unbound method
        state = StratCls.get_persistent_state(strat)

        assert state["has_position"] is True
        assert state["lp_opened_count"] == 3

    def test_load_persistent_state_restores_has_position(self) -> None:
        StratCls = self._make_strategy()
        # Use a real object so attribute assignments from load_persistent_state are observable.
        strat = StratCls.__new__(StratCls)
        strat._has_position = False
        strat._lp_opened_count = 0

        StratCls.load_persistent_state(strat, {"has_position": True, "lp_opened_count": 2})

        assert strat._has_position is True
        assert strat._lp_opened_count == 2

    def test_load_persistent_state_empty_dict_safe(self) -> None:
        StratCls = self._make_strategy()
        strat = StratCls.__new__(StratCls)
        strat._has_position = True
        strat._lp_opened_count = 5

        StratCls.load_persistent_state(strat, {})

        # Empty dict should default to False/0, not raise
        assert strat._has_position is False
        assert strat._lp_opened_count == 0
