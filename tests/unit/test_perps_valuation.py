"""Framework-side perp valuation tests (VIB-4930 PR-3, venue-agnostic).

After the behaviour flip, the framework perp read+value path routes through the
connector-published ``PerpsReadSpec`` / ``PerpsReadRegistry`` (mirroring the
lending-read seam). The framework reader owns only the gateway ``_rpc_stub.Call``
round-trip; metadata + valuation are reached through the registry by
``position.protocol``. These tests pin:

- ``PerpsPositionReader`` (gateway-routed, registry-dispatched) read behaviour,
  including the ``DirectRpcAdapter`` paper-trading transport end-to-end.
- ``PortfolioValuer._reprice_perps_on_chain`` / ``_reprice_perps_on_chain_enriched``
  money-path output against a *real* GMX market (the registry resolves the
  metadata + value; only the reader + ``market.price`` are stubbed).
- The ``Empty≠Zero`` seam: ``ok=False`` and a measured-empty book both fall back
  to the strategy-reported value rather than fabricating a zero.
- ``PositionDiscoveryService`` perps discovery over the registry's venues.
- A static self-containment guard: the framework perp path names no connector.

The relocated GMX mark-to-market math tests live in
``tests/unit/connectors/gmx_v2/test_perps_read.py``; the GMX read/value/metadata
parity pins live there too.
"""

import ast
from decimal import Decimal
from pathlib import Path
from unittest.mock import MagicMock

import pytest
from eth_abi import encode as abi_encode
from eth_utils import to_checksum_address

from almanak.connectors._strategy_base.perps_read_base import PerpsReadResult
from almanak.connectors.gmx_v2 import perps_read as gmx_perps
from almanak.framework.teardown.models import PositionInfo, PositionType
from almanak.framework.valuation.perps_position_reader import (
    PerpsPositionOnChain,
    PerpsPositionReader,
)

# Real GMX arbitrum ETH/USD market: symbol "ETH", 18-decimal index token. The
# real ``PerpsReadRegistry`` resolves its metadata + valuation, so integration
# tests exercise the genuine connector math, not a mock.
_ETH_MARKET = "0x70d95587d40A2caf56bd97485aB3Eec10Bee6336"
_USDC = "0xaf88d065e77c8cC2239327C5EDb3A432268e5831"


def _on_chain_eth_long() -> PerpsPositionOnChain:
    """A matching ETH long: size 10k USD, 5 ETH, 2000 USDC collateral."""
    return PerpsPositionOnChain(
        account="0xWallet",
        market=_ETH_MARKET,
        collateral_token=_USDC,
        size_in_usd=10_000 * 10**30,
        size_in_tokens=5 * 10**18,
        collateral_amount=2000 * 10**6,
        is_long=True,
        borrowing_factor=0,
        funding_fee_amount_per_size=0,
        increased_at_time=0,
        decreased_at_time=0,
    )


# =============================================================================
# PerpsPositionOnChain (re-exported from _strategy_base)
# =============================================================================


class TestPerpsPositionOnChain:
    def test_is_active(self):
        pos = _on_chain_eth_long()
        assert pos.is_active is True

    def test_inactive_zero_size(self):
        pos = PerpsPositionOnChain(
            account="0x1234",
            market="0xmarket",
            collateral_token="0xusdc",
            size_in_usd=0,
            size_in_tokens=0,
            collateral_amount=0,
            is_long=True,
            borrowing_factor=0,
            funding_fee_amount_per_size=0,
            increased_at_time=0,
            decreased_at_time=0,
        )
        assert pos.is_active is False

    def test_position_key_long(self):
        pos = PerpsPositionOnChain(
            account="0x1234",
            market="0xABCD",
            collateral_token="0xUSDC",
            size_in_usd=1,
            size_in_tokens=1,
            collateral_amount=1,
            is_long=True,
            borrowing_factor=0,
            funding_fee_amount_per_size=0,
            increased_at_time=0,
            decreased_at_time=0,
        )
        assert pos.position_key == "gmx-0xabcd-0xusdc-long"

    def test_position_key_short(self):
        pos = PerpsPositionOnChain(
            account="0x1234",
            market="0xABCD",
            collateral_token="0xUSDC",
            size_in_usd=1,
            size_in_tokens=1,
            collateral_amount=1,
            is_long=False,
            borrowing_factor=0,
            funding_fee_amount_per_size=0,
            increased_at_time=0,
            decreased_at_time=0,
        )
        assert pos.position_key == "gmx-0xabcd-0xusdc-short"


# =============================================================================
# PerpsPositionReader (gateway-routed, registry-dispatched)
# =============================================================================


class TestPerpsPositionReader:
    def test_no_gateway_returns_unmeasured(self):
        """No gateway client => ok=False (unmeasured), never a fabricated empty."""
        reader = PerpsPositionReader()
        result = reader.read_positions("arbitrum", "0x1234", protocol="gmx_v2")
        assert isinstance(result, PerpsReadResult)
        assert result.ok is False
        assert result.positions == ()

    def test_unresolved_protocol_returns_unmeasured(self):
        """A gateway is present but the venue/chain doesn't resolve => ok=False."""
        reader = PerpsPositionReader(gateway_client=MagicMock())
        result = reader.read_positions("arbitrum", "0x1234", protocol="not_a_perp_venue")
        assert result.ok is False
        assert result.positions == ()

    def test_from_gateway_client_none(self):
        reader = PerpsPositionReader.from_gateway_client(None)
        assert reader._gateway is None

    def test_from_gateway_client_stores_what_is_passed(self):
        """The wrapper stores whatever transport is passed (GatewayClient or adapter)."""
        sentinel = MagicMock()
        reader = PerpsPositionReader.from_gateway_client(sentinel)
        assert reader._gateway is sentinel

    def test_direct_rpc_adapter_transport_decodes_end_to_end(self):
        """Paper-trading transport: a DirectRpcAdapter-shaped client whose
        ``_rpc_stub.Call`` returns an encoded ``getAccountPositions`` blob must
        decode end-to-end through the framework reader's ``eth_call``."""
        account = to_checksum_address("0x" + "11" * 20)
        # One active ETH long, encoded in the Reader's Position.Props[] ABI shape.
        props = (
            (account, to_checksum_address(_ETH_MARKET), to_checksum_address(_USDC)),
            (10**31, 10**18, 10**6, 7, 9, 11, 13, 100, 200, 1_700_000_000, 1_700_000_001),
            (True,),
        )
        blob = "0x" + abi_encode([gmx_perps._GET_ACCOUNT_POSITIONS_OUTPUT], [[props]]).hex()

        class _Stub:
            def Call(self, request, timeout=None):  # noqa: N802 — mirrors gateway proto
                resp = MagicMock()
                resp.success = True
                # The framework reader json.loads(response.result); the adapter
                # JSON-encodes the hex string the same way.
                import json

                resp.result = json.dumps(blob)
                return resp

        client = MagicMock()
        client._rpc_stub = _Stub()
        client.config.timeout = 10

        reader = PerpsPositionReader.from_gateway_client(client)
        result = reader.read_positions("arbitrum", account, protocol="gmx_v2")

        assert result.ok is True
        assert len(result.positions) == 1
        pos = result.positions[0]
        assert pos.market == to_checksum_address(_ETH_MARKET)
        assert pos.is_long is True
        assert pos.size_in_usd == 10**31

    def test_failed_eth_call_is_unmeasured(self):
        """A reverted/failed gateway call reduces to ok=False (unmeasured)."""

        class _Stub:
            def Call(self, request, timeout=None):  # noqa: N802 — mirrors gateway proto
                resp = MagicMock()
                resp.success = False
                resp.error = "execution reverted"
                return resp

        client = MagicMock()
        client._rpc_stub = _Stub()
        client.config.timeout = 10

        reader = PerpsPositionReader.from_gateway_client(client)
        wallet = to_checksum_address("0x" + "11" * 20)
        result = reader.read_positions("arbitrum", wallet, protocol="gmx_v2")
        assert result.ok is False
        assert result.positions == ()


# =============================================================================
# PortfolioValuer perps integration — real registry, stubbed reader + prices
# =============================================================================


class TestPortfolioValuerPerpsIntegration:
    """``_reprice_perps_on_chain`` / ``_enriched`` end-to-end against a real GMX market."""

    def _make_valuer(self):
        from almanak.framework.valuation.portfolio_valuer import PortfolioValuer

        return PortfolioValuer(gateway_client=None)

    def _make_perp_position(self):
        return PositionInfo(
            position_type=PositionType.PERP,
            position_id="gmx-eth-usdc-long",
            chain="arbitrum",
            protocol="gmx_v2",
            value_usd=Decimal("2500"),  # Strategy-reported fallback
            details={
                "market": _ETH_MARKET,
                "collateral_token": _USDC,
                "is_long": True,
                "wallet_address": "0xWallet",
            },
        )

    @staticmethod
    def _market_at(eth_price: Decimal):
        market = MagicMock()
        market.price.side_effect = lambda token: {
            "ETH": eth_price,
            "USDC": Decimal("1"),
        }.get(token, Decimal("0"))
        return market

    def test_reprice_perps_no_wallet(self):
        """No wallet_address in details => returns None (fallback)."""
        valuer = self._make_valuer()
        pos = PositionInfo(
            position_type=PositionType.PERP,
            position_id="test",
            chain="arbitrum",
            protocol="gmx_v2",
            value_usd=Decimal("1000"),
            details={},
        )
        result = valuer._reprice_perps_on_chain(pos, "arbitrum", MagicMock())
        assert result is None

    def test_reprice_perps_no_matching_position(self):
        """Reader returns a position for a different market => no match => None."""
        valuer = self._make_valuer()
        pos = self._make_perp_position()
        different_pos = PerpsPositionOnChain(
            account="0xWallet",
            market="0xDIFFERENT",
            collateral_token=_USDC,
            size_in_usd=10_000 * 10**30,
            size_in_tokens=5 * 10**18,
            collateral_amount=2000 * 10**6,
            is_long=True,
            borrowing_factor=0,
            funding_fee_amount_per_size=0,
            increased_at_time=0,
            decreased_at_time=0,
        )
        valuer._perps_reader = MagicMock()
        valuer._perps_reader.read_positions.return_value = PerpsReadResult(positions=(different_pos,), ok=True)
        result = valuer._reprice_perps_on_chain(pos, "arbitrum", MagicMock())
        assert result is None

    def test_reprice_perps_success(self):
        """Successful repricing returns the connector's mark-to-market net value."""
        valuer = self._make_valuer()
        pos = self._make_perp_position()
        valuer._perps_reader = MagicMock()
        valuer._perps_reader.read_positions.return_value = PerpsReadResult(positions=(_on_chain_eth_long(),), ok=True)
        # Collateral decimals come from the framework token resolver; stub it.
        from almanak.framework.valuation.portfolio_valuer import PortfolioValuer

        with (
            pytest.MonkeyPatch.context() as mp,
        ):
            mp.setattr(PortfolioValuer, "_resolve_token_symbol", lambda self, addr, p, k: "USDC")
            mp.setattr(PortfolioValuer, "_get_token_decimals", lambda self, sym, chain: 6)
            result = valuer._reprice_perps_on_chain(pos, "arbitrum", self._market_at(Decimal("2200")))

        assert result is not None
        # Entry = 10000/5 = 2000, mark = 2200, tokens = 5 => PnL = 1000
        # Net = 2000 collateral + 1000 pnl = 3000
        assert result == Decimal("3000")

    def test_reprice_perps_matches_legacy_market_address_shape(self):
        """A perp keyed under the legacy ``market_address`` detail (no ``market``)
        still reprices — the match reads both shapes, mirroring
        ``_canonical_position_key`` (CodeRabbit #2595). Without the fallback,
        ``market_address`` is ignored => no match => stale strategy value kept.
        """
        from almanak.framework.valuation.portfolio_valuer import PortfolioValuer

        valuer = self._make_valuer()
        pos = PositionInfo(
            position_type=PositionType.PERP,
            position_id="gmx-eth-usdc-long",
            chain="arbitrum",
            protocol="gmx_v2",
            value_usd=Decimal("2500"),
            details={
                "market_address": _ETH_MARKET,  # legacy shape, NOT "market"
                "collateral_token": _USDC,
                "is_long": True,
                "wallet_address": "0xWallet",
            },
        )
        valuer._perps_reader = MagicMock()
        valuer._perps_reader.read_positions.return_value = PerpsReadResult(positions=(_on_chain_eth_long(),), ok=True)
        with pytest.MonkeyPatch.context() as mp:
            mp.setattr(PortfolioValuer, "_resolve_token_symbol", lambda self, addr, p, k: "USDC")
            mp.setattr(PortfolioValuer, "_get_token_decimals", lambda self, sym, chain: 6)
            result = valuer._reprice_perps_on_chain(pos, "arbitrum", self._market_at(Decimal("2200")))

        assert result == Decimal("3000")  # repriced via market_address, not the $2500 fallback

    def test_reprice_perps_enriched_success(self):
        """The enriched path returns the same net value plus the enriched dict."""
        valuer = self._make_valuer()
        pos = self._make_perp_position()
        valuer._perps_reader = MagicMock()
        valuer._perps_reader.read_positions.return_value = PerpsReadResult(positions=(_on_chain_eth_long(),), ok=True)
        from almanak.framework.valuation.portfolio_valuer import PortfolioValuer

        with pytest.MonkeyPatch.context() as mp:
            mp.setattr(PortfolioValuer, "_resolve_token_symbol", lambda self, addr, p, k: "USDC")
            mp.setattr(PortfolioValuer, "_get_token_decimals", lambda self, sym, chain: 6)
            out = valuer._reprice_perps_on_chain_enriched(pos, "arbitrum", self._market_at(Decimal("2200")))

        assert out is not None
        net, enriched = out
        assert net == Decimal("3000")
        assert enriched["market"] == _ETH_MARKET
        assert enriched["is_long"] is True
        assert enriched["size_usd"] == "10000"
        assert enriched["unrealized_pnl_usd"] == "1000"
        assert enriched["valuation_source"] == "on_chain"

    def test_reprice_perps_unmeasured_read_falls_back(self):
        """Empty≠Zero: reader ok=False => None (caller keeps strategy value)."""
        valuer = self._make_valuer()
        pos = self._make_perp_position()
        valuer._perps_reader = MagicMock()
        valuer._perps_reader.read_positions.return_value = PerpsReadResult(positions=(), ok=False)
        result = valuer._reprice_perps_on_chain(pos, "arbitrum", MagicMock())
        assert result is None

    def test_reprice_perps_measured_empty_book_falls_back(self):
        """A measured-empty book (ok=True, no positions) also yields None (no match)."""
        valuer = self._make_valuer()
        pos = self._make_perp_position()
        valuer._perps_reader = MagicMock()
        valuer._perps_reader.read_positions.return_value = PerpsReadResult(positions=(), ok=True)
        result = valuer._reprice_perps_on_chain(pos, "arbitrum", MagicMock())
        assert result is None

    def test_reprice_position_delegates_to_perps(self):
        """_reprice_position dispatches PERP to _reprice_perps_on_chain."""
        from unittest.mock import patch

        valuer = self._make_valuer()
        pos = self._make_perp_position()
        market = MagicMock()

        with patch.object(valuer, "_reprice_perps_on_chain", return_value=Decimal("3000")) as mock_reprice:
            result = valuer._reprice_position(pos, "arbitrum", market)

        mock_reprice.assert_called_once_with(pos, "arbitrum", market)
        assert result == Decimal("3000")

    def test_reprice_position_perps_fallback(self):
        """PERP fallback to strategy-reported value when repricing returns None."""
        from unittest.mock import patch

        valuer = self._make_valuer()
        pos = self._make_perp_position()  # value_usd=2500
        market = MagicMock()

        with patch.object(valuer, "_reprice_perps_on_chain", return_value=None):
            result = valuer._reprice_position(pos, "arbitrum", market)

        assert result == Decimal("2500")

    def test_is_long_missing_returns_none(self):
        """Audit B2 (rewritten): missing is_long forces fallback, not a silent default."""
        valuer = self._make_valuer()
        pos = PositionInfo(
            position_type=PositionType.PERP,
            position_id="gmx-test",
            chain="arbitrum",
            protocol="gmx_v2",
            value_usd=Decimal("1000"),
            details={
                "market": _ETH_MARKET,
                "wallet_address": "0xWallet",
                # No "is_long" field!
            },
        )
        valuer._perps_reader = MagicMock()
        valuer._perps_reader.read_positions.return_value = PerpsReadResult(positions=(_on_chain_eth_long(),), ok=True)
        result = valuer._reprice_perps_on_chain(pos, "arbitrum", MagicMock())
        assert result is None  # Forces fallback to strategy-reported value

    # --- _value_matched_perp shared-helper guards (VIB-4930 dedup refactor) ---
    # Both reprice methods are now thin wrappers over ``_value_matched_perp``;
    # these pin the fail-closed guards inside that single shared helper so the
    # de-duplication keeps every money-critical None path covered.

    def _perp_reader_with_eth_long(self, valuer):
        valuer._perps_reader = MagicMock()
        valuer._perps_reader.read_positions.return_value = PerpsReadResult(positions=(_on_chain_eth_long(),), ok=True)

    def test_value_matched_perp_zero_mark_price_returns_none(self):
        """Guard: a non-positive index (mark) price fails closed to None."""
        valuer = self._make_valuer()
        pos = self._make_perp_position()
        self._perp_reader_with_eth_long(valuer)
        from almanak.framework.valuation.portfolio_valuer import PortfolioValuer

        # market.price("ETH") -> 0 => mark_price <= 0 guard trips.
        market = MagicMock()
        market.price.side_effect = lambda token: {"ETH": Decimal("0"), "USDC": Decimal("1")}.get(token, Decimal("0"))
        with pytest.MonkeyPatch.context() as mp:
            mp.setattr(PortfolioValuer, "_resolve_token_symbol", lambda self, addr, p, k: "USDC")
            mp.setattr(PortfolioValuer, "_get_token_decimals", lambda self, sym, chain: 6)
            assert valuer._value_matched_perp(pos, "arbitrum", market) is None
            # Both wrappers fail closed through the same guard.
            assert valuer._reprice_perps_on_chain(pos, "arbitrum", market) is None
            assert valuer._reprice_perps_on_chain_enriched(pos, "arbitrum", market) is None

    def test_value_matched_perp_zero_collateral_price_returns_none(self):
        """Guard: a non-positive collateral price fails closed to None."""
        valuer = self._make_valuer()
        pos = self._make_perp_position()
        self._perp_reader_with_eth_long(valuer)
        from almanak.framework.valuation.portfolio_valuer import PortfolioValuer

        # ETH prices fine, USDC collateral price is 0 => collateral guard trips.
        market = MagicMock()
        market.price.side_effect = lambda token: {"ETH": Decimal("2200"), "USDC": Decimal("0")}.get(token, Decimal("0"))
        with pytest.MonkeyPatch.context() as mp:
            mp.setattr(PortfolioValuer, "_resolve_token_symbol", lambda self, addr, p, k: "USDC")
            mp.setattr(PortfolioValuer, "_get_token_decimals", lambda self, sym, chain: 6)
            assert valuer._value_matched_perp(pos, "arbitrum", market) is None

    def test_value_matched_perp_unresolved_collateral_symbol_returns_none(self):
        """Guard: collateral symbol that won't resolve fails closed to None."""
        valuer = self._make_valuer()
        pos = self._make_perp_position()
        self._perp_reader_with_eth_long(valuer)
        from almanak.framework.valuation.portfolio_valuer import PortfolioValuer

        with pytest.MonkeyPatch.context() as mp:
            # _resolve_token_symbol -> None trips the guard before any price call.
            mp.setattr(PortfolioValuer, "_resolve_token_symbol", lambda self, addr, p, k: None)
            assert valuer._value_matched_perp(pos, "arbitrum", self._market_at(Decimal("2200"))) is None

    def test_value_matched_perp_unknown_collateral_decimals_returns_none(self):
        """Guard: unknown collateral decimals fails closed (never defaults to 18)."""
        valuer = self._make_valuer()
        pos = self._make_perp_position()
        self._perp_reader_with_eth_long(valuer)
        from almanak.framework.valuation.portfolio_valuer import PortfolioValuer

        with pytest.MonkeyPatch.context() as mp:
            mp.setattr(PortfolioValuer, "_resolve_token_symbol", lambda self, addr, p, k: "USDC")
            mp.setattr(PortfolioValuer, "_get_token_decimals", lambda self, sym, chain: None)
            assert valuer._value_matched_perp(pos, "arbitrum", self._market_at(Decimal("2200"))) is None

    def test_value_matched_perp_success_returns_position_value(self):
        """Happy path: the shared helper returns the connector's PerpsPositionValue
        whose ``net_value_usd`` both wrappers consume (entry 2000, mark 2200 =>
        collateral 2000 + pnl 1000 = 3000)."""
        valuer = self._make_valuer()
        pos = self._make_perp_position()
        self._perp_reader_with_eth_long(valuer)
        from almanak.framework.valuation.portfolio_valuer import PortfolioValuer

        with pytest.MonkeyPatch.context() as mp:
            mp.setattr(PortfolioValuer, "_resolve_token_symbol", lambda self, addr, p, k: "USDC")
            mp.setattr(PortfolioValuer, "_get_token_decimals", lambda self, sym, chain: 6)
            valued = valuer._value_matched_perp(pos, "arbitrum", self._market_at(Decimal("2200")))

        assert valued is not None
        assert valued.net_value_usd == Decimal("3000")
        assert valued.unrealized_pnl_usd == Decimal("1000")


# =============================================================================
# PositionDiscoveryService perps discovery (registry-driven)
# =============================================================================


class TestPositionDiscoveryPerps:
    def test_has_perps_protocol(self):
        from almanak.framework.valuation.position_discovery import _has_perps_protocol

        assert _has_perps_protocol(["gmx_v2"]) is True
        assert _has_perps_protocol(["GMX_V2"]) is True
        assert _has_perps_protocol(["gmx"]) is True
        assert _has_perps_protocol(["uniswap_v3"]) is False
        assert _has_perps_protocol([]) is False

    def test_discover_perps_creates_positions(self):
        from almanak.framework.valuation.position_discovery import (
            DiscoveryConfig,
            PositionDiscoveryService,
        )

        service = PositionDiscoveryService(gateway_client=None)
        service._perps_reader = MagicMock()
        # Discovery iterates EVERY registered perp venue (gmx_v2 + aster_perps as
        # of VIB-4930 PR-4); on Arbitrum only GMX is deployed, so the reader is
        # protocol-aware here — GMX returns the ETH long, the BSC-only venue
        # reads ``ok=False`` (its resolve_plan is None off-chain). This mirrors
        # the real multi-venue scan instead of returning positions for every
        # venue indiscriminately.
        service._perps_reader.read_positions.side_effect = lambda *, chain, wallet_address, protocol: (
            PerpsReadResult(positions=(_on_chain_eth_long(),), ok=True)
            if protocol == "gmx_v2"
            else PerpsReadResult(positions=(), ok=False)
        )

        config = DiscoveryConfig(
            chain="arbitrum",
            wallet_address="0xWallet",
            protocols=["gmx_v2"],
            tracked_tokens=["USDC", "ETH"],
        )
        result = service.discover(config)

        assert result.perps_scanned is True
        assert len(result.positions) == 1
        pos = result.positions[0]
        assert pos.position_type.value == "PERP"
        assert pos.protocol == "gmx_v2"
        assert pos.position_id == "gmx-" + _ETH_MARKET.lower() + "-" + _USDC.lower() + "-long"
        assert pos.details["is_long"] is True
        assert pos.details["wallet_address"] == "0xWallet"
        assert pos.details["market"] == _ETH_MARKET
        assert pos.details["collateral_token"] == _USDC
        assert pos.details["side"] == "long"
        # Repriced later by the portfolio valuer.
        assert pos.value_usd == Decimal("0")

    def test_discover_no_perps_protocol_skips(self):
        from almanak.framework.valuation.position_discovery import (
            DiscoveryConfig,
            PositionDiscoveryService,
        )

        service = PositionDiscoveryService(gateway_client=None)
        service._perps_reader = MagicMock()

        config = DiscoveryConfig(
            chain="arbitrum",
            wallet_address="0xWallet",
            protocols=["uniswap_v3"],
            tracked_tokens=["ETH"],
        )
        result = service.discover(config)

        assert result.perps_scanned is False
        service._perps_reader.read_positions.assert_not_called()

    def test_discover_perps_reader_exception_records_error(self):
        from almanak.framework.valuation.position_discovery import (
            DiscoveryConfig,
            PositionDiscoveryService,
        )

        service = PositionDiscoveryService(gateway_client=None)
        service._perps_reader = MagicMock()

        # Per-venue error handling: only gmx_v2's read raises; the other
        # registered venue (BSC-only, off-chain here) reads ok=False and is
        # skipped silently. Exactly one error is recorded — for the one venue
        # that raised — proving the except is scoped per venue, not global.
        def _read(*, chain, wallet_address, protocol):
            if protocol == "gmx_v2":
                raise RuntimeError("boom")
            return PerpsReadResult(positions=(), ok=False)

        service._perps_reader.read_positions.side_effect = _read

        config = DiscoveryConfig(
            chain="arbitrum",
            wallet_address="0xWallet",
            protocols=["gmx_v2"],
        )
        result = service.discover(config)

        assert result.perps_scanned is True
        assert len(result.errors) == 1
        assert "boom" in result.errors[0]
        assert "gmx_v2" in result.errors[0]

    def test_discover_perps_not_deployed_skips_silently(self):
        """A chain where NO perp venue is deployed skips silently — no error, no positions.

        With the real registry, every venue's ``resolve_plan`` returns ``None``
        on a chain where its reader/data-store address is absent from
        ``AddressRegistry`` (here ``ethereum`` — neither ``gmx_v2`` nor
        ``aster_perps`` is deployed). Discovery skips each silently BEFORE
        issuing a read (the not-deployed gate), so no error is recorded and no
        phantom-empty position is emitted. Mirrors the lending precedent
        (``_scan_lending_protocol`` ``continue``s silently on an unresolved
        reserve). The reader is asserted never called: not-deployed venues are
        provably empty without an on-chain round trip.
        """
        from almanak.framework.valuation.position_discovery import (
            DiscoveryConfig,
            PositionDiscoveryService,
        )

        service = PositionDiscoveryService(gateway_client=None)
        service._perps_reader = MagicMock()

        config = DiscoveryConfig(
            chain="ethereum",
            wallet_address="0xWallet",
            protocols=["gmx_v2"],
        )
        result = service.discover(config)

        assert result.perps_scanned is True
        assert result.positions == []  # No phantom-empty positions.
        assert result.errors == []  # Silent skip — not deployed, not a failure.
        # Not-deployed venues are gated out by resolve_plan before any read.
        service._perps_reader.read_positions.assert_not_called()

    def test_discover_perps_deployed_but_read_fails_records_error(self):
        """A DEPLOYED venue whose read returns ``ok=False`` records an error.

        ``arbitrum`` has ``gmx_v2`` deployed (``resolve_plan`` resolves), so an
        ``ok=False`` read there is a genuine gateway/RPC/decode failure — NOT a
        not-deployed signal — and MUST be surfaced, not swallowed. The other
        registered venue (``aster_perps``) is not deployed on Arbitrum
        (``resolve_plan`` is ``None``) and is therefore skipped silently. Exactly
        one error is recorded, and it names the deployed venue that failed.
        """
        from almanak.framework.valuation.position_discovery import (
            DiscoveryConfig,
            PositionDiscoveryService,
        )

        service = PositionDiscoveryService(gateway_client=None)
        service._perps_reader = MagicMock()
        service._perps_reader.read_positions.return_value = PerpsReadResult(positions=(), ok=False)

        config = DiscoveryConfig(
            chain="arbitrum",
            wallet_address="0xWallet",
            protocols=["gmx_v2"],
        )
        result = service.discover(config)

        assert result.perps_scanned is True
        assert result.positions == []  # ok=False emits nothing.
        assert len(result.errors) == 1  # Only the deployed venue that failed.
        assert "gmx_v2" in result.errors[0]

    def test_discover_perps_registered_venue_not_on_chain_no_error(self):
        """Regression: a registered venue NOT deployed on the scanned chain
        yields no error and no positions.

        When a perp venue's reader/data-store address is absent from
        ``AddressRegistry`` for ``config.chain``, ``resolve_plan`` returns
        ``None`` (the not-deployed gate) and discovery skips the venue BEFORE
        any read. With the real registry, scanning ``ethereum`` hits this exact
        path for every registered venue (neither ``gmx_v2`` nor ``aster_perps``
        is deployed there). Discovery must treat this as "nothing here", not as
        a failure worth surfacing — matching the lending behaviour for an
        unresolved reserve.
        """
        from almanak.framework.valuation.position_discovery import (
            DiscoveryConfig,
            PositionDiscoveryService,
        )

        service = PositionDiscoveryService(gateway_client=None)
        service._perps_reader = MagicMock()
        # An off-chain venue is gated out by resolve_plan -> None; the reader is
        # never reached. Stub it anyway to prove no read is needed.
        service._perps_reader.read_positions.return_value = PerpsReadResult(positions=(), ok=False)

        config = DiscoveryConfig(
            chain="ethereum",
            wallet_address="0xWallet",
            protocols=["gmx_v2"],
        )
        result = service.discover(config)

        assert result.perps_scanned is True
        perp_positions = [p for p in result.positions if p.position_type == PositionType.PERP]
        assert perp_positions == []  # No positions for an off-chain venue.
        assert result.errors == []  # No spurious cross-venue error.
        service._perps_reader.read_positions.assert_not_called()

    def test_discover_perps_measured_empty_book_no_error(self):
        """A measured-empty book (ok=True) emits nothing and records no error."""
        from almanak.framework.valuation.position_discovery import (
            DiscoveryConfig,
            PositionDiscoveryService,
        )

        service = PositionDiscoveryService(gateway_client=None)
        service._perps_reader = MagicMock()
        service._perps_reader.read_positions.return_value = PerpsReadResult(positions=(), ok=True)

        config = DiscoveryConfig(
            chain="arbitrum",
            wallet_address="0xWallet",
            protocols=["gmx_v2"],
        )
        result = service.discover(config)

        assert result.perps_scanned is True
        assert result.positions == []
        assert result.errors == []


# =============================================================================
# Audit fix regression — perps dedup (P2), rewritten against the new flow
# =============================================================================


class TestAuditFixes:
    def test_perps_dedup_skips_discovered_when_strategy_reports(self):
        """Fix P2: discovery perps skipped when the strategy already reports a PERP
        for the same protocol (no double-count)."""
        from unittest.mock import patch

        from almanak.framework.valuation.portfolio_valuer import PortfolioValuer

        valuer = PortfolioValuer(gateway_client=None)

        strategy_pos = PositionInfo(
            position_type=PositionType.PERP,
            position_id="gmx-ETH/USD-usdc-perp",  # Strategy's custom format
            chain="arbitrum",
            protocol="gmx_v2",
            value_usd=Decimal("2500"),
            details={"market": "0x70d95587", "is_long": True, "wallet_address": "0xW"},
        )
        discovered_pos = PositionInfo(
            position_type=PositionType.PERP,
            position_id="gmx-0x70d95587-0xusdc-long",  # Discovery format
            chain="arbitrum",
            protocol="gmx_v2",
            value_usd=Decimal("0"),
            details={},
        )

        with patch.object(valuer, "_get_strategy_positions", return_value=([strategy_pos], False)):
            mock_discovery = MagicMock()
            mock_result = MagicMock()
            mock_result.positions = [discovered_pos]
            mock_result.errors = []
            mock_discovery.discover.return_value = mock_result
            valuer._discovery = mock_discovery

            with patch.object(valuer, "_build_discovery_config", return_value=MagicMock()):
                market = MagicMock()
                market.price.return_value = Decimal("2000")
                market.balance.return_value = Decimal("0")

                strategy = MagicMock()
                strategy.deployment_id = "test"
                strategy.chain = "arbitrum"
                strategy._get_tracked_tokens.return_value = []

                positions, total, incomplete = valuer._get_positions(strategy, market, {})

        # Only the strategy's position survives (no double-count).
        assert len(positions) == 1
        assert positions[0].details.get("market") == "0x70d95587"

    def test_perps_dedup_collapses_alias_across_sources(self):
        """Fix P1: a strategy-reported perp alias and a discovery-stamped canonical
        name for the SAME venue collapse to ONE position (no double-count).

        ``pancakeswap_perps`` is the deprecated alias for ``aster_perps`` (PCS
        Perps is broker id=2 on the Aster Diamond). Before the dedup key
        canonicalised perp aliases, a strategy reporting ``pancakeswap_perps``
        and discovery stamping ``aster_perps`` for the same (chain, market,
        is_long) keyed distinctly and BOTH survived, double-counting the perp.
        The canonical key must now collapse them onto the discovery row.
        """
        from unittest.mock import patch

        from almanak.framework.valuation.portfolio_valuer import PortfolioValuer

        valuer = PortfolioValuer(gateway_client=None)

        strategy_pos = PositionInfo(
            position_type=PositionType.PERP,
            position_id="pcs-ETH/USD-usdc-perp",  # Strategy's custom format
            chain="bsc",
            protocol="pancakeswap_perps",  # Deprecated alias for aster_perps
            value_usd=Decimal("2500"),
            details={"market": "0xMARKET", "is_long": True, "wallet_address": "0xW"},
        )
        discovered_pos = PositionInfo(
            position_type=PositionType.PERP,
            position_id="aster-0xmarket-0xusdc-long",  # Discovery format
            chain="bsc",
            protocol="aster_perps",  # Canonical name
            value_usd=Decimal("0"),
            details={"market": "0xMARKET", "is_long": True},
        )

        with patch.object(valuer, "_get_strategy_positions", return_value=([strategy_pos], False)):
            mock_discovery = MagicMock()
            mock_result = MagicMock()
            mock_result.positions = [discovered_pos]
            mock_result.errors = []
            mock_discovery.discover.return_value = mock_result
            valuer._discovery = mock_discovery

            with patch.object(valuer, "_build_discovery_config", return_value=MagicMock()):
                market = MagicMock()
                market.price.return_value = Decimal("2000")
                market.balance.return_value = Decimal("0")

                strategy = MagicMock()
                strategy.deployment_id = "test"
                strategy.chain = "bsc"
                strategy._get_tracked_tokens.return_value = []

                positions, total, incomplete = valuer._get_positions(strategy, market, {})

        # Alias + canonical for the same venue/market/side collapse to ONE.
        assert len(positions) == 1


# =============================================================================
# Static self-containment: the framework perp path names no connector
# =============================================================================


def test_framework_valuation_perp_path_imports_no_connector():
    """No module under ``almanak/framework/valuation/`` may import a perp
    connector — that knowledge lives behind ``PerpsReadRegistry``. The strongest
    coupling signal (``CONNECTOR_IMPORT`` in the self-containment audit), checked
    strictly across every valuation module.
    """
    repo_root = Path(__file__).resolve().parents[2]
    valuation_dir = repo_root / "almanak" / "framework" / "valuation"
    forbidden_imports = ("almanak.connectors.gmx_v2", "almanak.connectors.aster_perps")

    offenders: list[str] = []
    for py in sorted(valuation_dir.rglob("*.py")):
        rel = py.relative_to(repo_root).as_posix()
        tree = ast.parse(py.read_text(), filename=str(py))
        for node in ast.walk(tree):
            if isinstance(node, ast.ImportFrom) and node.module:
                for bad in forbidden_imports:
                    if node.module == bad or node.module.startswith(bad + "."):
                        offenders.append(f"{rel}:{node.lineno} imports {node.module}")
            elif isinstance(node, ast.Import):
                for alias in node.names:
                    for bad in forbidden_imports:
                        if alias.name == bad or alias.name.startswith(bad + "."):
                            offenders.append(f"{rel}:{node.lineno} imports {alias.name}")

    assert not offenders, "valuation modules must import no perp connector:\n" + "\n".join(offenders)


def test_framework_valuation_perp_path_names_no_venue():
    """No module under ``almanak/framework/valuation/`` may hardcode a perp venue
    slug as a live string literal on the read/value path. After the VIB-4930 flip
    the valuer + discovery reach metadata + valuation through
    ``PerpsReadRegistry`` by ``position.protocol`` (discovery iterates
    ``supported_protocols()``), and the reader takes ``protocol`` as a required
    arg — so no valuation module names a venue at all. Comments and docstrings
    are ignored (only live ``Constant`` str nodes are scanned); this test file is
    not part of the scan.
    """
    repo_root = Path(__file__).resolve().parents[2]
    valuation_dir = repo_root / "almanak" / "framework" / "valuation"
    forbidden_strings = ("gmx_v2", "aster_perps")

    offenders: list[str] = []
    for py in sorted(valuation_dir.rglob("*.py")):
        rel = py.relative_to(repo_root).as_posix()
        tree = ast.parse(py.read_text(), filename=str(py))

        # Skip docstrings (Constant str nodes at module/def/class body[0]); they
        # may name a venue in prose without coupling live code to it.
        docstring_ids = set()
        for node in ast.walk(tree):
            if isinstance(node, ast.Module | ast.FunctionDef | ast.AsyncFunctionDef | ast.ClassDef):
                if (
                    ast.get_docstring(node, clean=False) is not None
                    and node.body
                    and isinstance(node.body[0], ast.Expr)
                ):
                    docstring_ids.add(id(node.body[0].value))
        for node in ast.walk(tree):
            if isinstance(node, ast.Constant) and isinstance(node.value, str) and id(node) not in docstring_ids:
                for bad in forbidden_strings:
                    if bad in node.value:
                        offenders.append(f"{rel}:{node.lineno} string literal contains {bad!r}: {node.value!r}")

    assert not offenders, "valuation modules must name no perp venue:\n" + "\n".join(offenders)

    # The reader names no venue of its own: ``protocol`` is required (no default).
    import inspect

    from almanak.framework.valuation import perps_position_reader

    sig = inspect.signature(perps_position_reader.PerpsPositionReader.read_positions)
    assert sig.parameters["protocol"].default is inspect.Parameter.empty
