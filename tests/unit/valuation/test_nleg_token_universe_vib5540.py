"""VIB-5540 (Seam A) — portfolio-snapshot N-coin token universe.

The portfolio snapshot must price every coin of every open / recently-closed
position, not just the config-derived ``_get_tracked_tokens()`` allowlist.
Without the union, an N-coin venue that returns coins the strategy never named
in config (a Curve 3pool close returns DAI+USDC+USDT; a tricrypto close returns
WBTC) drops those coins from wallet equity → the wallet-method PnL is short by
their value and G6 reports a spurious gap.

These are pure-function tests over the two new helpers — no live chain, no
gateway.
"""

from __future__ import annotations

import json

from almanak.framework.valuation.portfolio_valuer import PortfolioValuer


def _lp_event(coin_symbols: list[str] | None, event_type: str = "LP_CLOSE") -> dict:
    payload = {"event_type": event_type}
    if coin_symbols is not None:
        payload["coin_symbols"] = coin_symbols
    return {
        "id": f"id-{event_type}-{'-'.join(str(c) for c in (coin_symbols or []))}",
        "deployment_id": "d1",
        "event_type": event_type,
        "position_key": "curve-3pool",
        "payload_json": json.dumps(payload),
    }


class TestPositionCoinSymbols:
    def test_extracts_deduped_coin_symbols_from_prefetched_events(self) -> None:
        v = PortfolioValuer()
        # OPEN + CLOSE both carry the 3-coin universe → deduped to 3, first-seen order.
        v._snapshot_events_flat = [
            _lp_event(["DAI", "USDC", "USDT"], "LP_OPEN"),
            _lp_event(["DAI", "USDC", "USDT"], "LP_CLOSE"),
        ]
        assert v._position_coin_symbols() == ["DAI", "USDC", "USDT"]

    def test_no_prefetch_returns_empty(self) -> None:
        v = PortfolioValuer()
        v._snapshot_events_flat = None
        assert v._position_coin_symbols() == []

    def test_events_without_coin_symbols_contribute_nothing(self) -> None:
        v = PortfolioValuer()
        v._snapshot_events_flat = [
            {"id": "1", "event_type": "SWAP", "payload_json": json.dumps({"event_type": "SWAP"})},
            _lp_event(None, "LP_OPEN"),
        ]
        assert v._position_coin_symbols() == []

    def test_malformed_payload_degrades_to_empty_never_raises(self) -> None:
        v = PortfolioValuer()
        v._snapshot_events_flat = [
            {"id": "1", "payload_json": "{not valid json"},
            {"id": "2", "payload_json": None},
            {"id": "3", "payload_json": ""},
            "not-a-dict",  # non-dict event is skipped, not crashed on
            _lp_event(["WBTC", "WETH", "USDT"], "LP_CLOSE"),
        ]
        # The one valid event still contributes; the junk is skipped.
        assert v._position_coin_symbols() == ["WBTC", "WETH", "USDT"]

    def test_dict_payload_json_supported(self) -> None:
        # Some backends hand back an already-decoded dict rather than a JSON str.
        v = PortfolioValuer()
        v._snapshot_events_flat = [
            {"id": "1", "payload_json": {"event_type": "LP_CLOSE", "coin_symbols": ["DAI", "USDC"]}},
        ]
        assert v._position_coin_symbols() == ["DAI", "USDC"]

    def test_empty_and_none_coin_entries_skipped(self) -> None:
        v = PortfolioValuer()
        v._snapshot_events_flat = [
            _lp_event(["DAI", "", None, "USDT"], "LP_CLOSE"),  # type: ignore[list-item]
        ]
        assert v._position_coin_symbols() == ["DAI", "USDT"]


class TestUnionTrackedWithPositionCoins:
    def test_config_tokens_kept_at_head_extra_coins_appended(self) -> None:
        v = PortfolioValuer()
        v._snapshot_events_flat = [_lp_event(["DAI", "USDC", "USDT"], "LP_CLOSE")]
        # Config allowlist tracks only USDC; the union adds DAI + USDT.
        result = v._union_tracked_with_position_coins(["USDC"])
        assert result[0] == "USDC"  # config token keeps its head position
        assert set(result) == {"USDC", "DAI", "USDT"}
        assert result == ["USDC", "DAI", "USDT"]

    def test_case_insensitive_dedup_no_duplicate(self) -> None:
        v = PortfolioValuer()
        v._snapshot_events_flat = [_lp_event(["dai", "USDC"], "LP_CLOSE")]
        # "USDC" already tracked (any case) → not duplicated; "dai" is new.
        result = v._union_tracked_with_position_coins(["USDC"])
        assert result == ["USDC", "dai"]

    def test_no_extra_coins_returns_base_unchanged(self) -> None:
        v = PortfolioValuer()
        v._snapshot_events_flat = None
        base = ["USDC", "WETH"]
        result = v._union_tracked_with_position_coins(base)
        assert result == base
        assert result is not base  # returns a copy, never mutates the caller's list

    def test_degrades_to_base_on_missing_prefetch(self) -> None:
        v = PortfolioValuer()
        # No _snapshot_events_flat attr set on a fresh instance beyond the default.
        v._snapshot_events_flat = None
        assert v._union_tracked_with_position_coins(["USDC"]) == ["USDC"]
