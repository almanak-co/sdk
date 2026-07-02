"""Stablecoin de-peg cross-check wired into spot + lending USD marks (VIB-4868).

`check_peg_divergence` is the pool-agnostic, Empty != Zero de-peg primitive. Before
VIB-4868 its only valuer call site was the Curve-LP NAV path; spot wallet valuation
and lending asset legs marked a stablecoin straight off the gateway oracle, so a
depegged stable was reported at its broken price (or $1) at HIGH confidence on the
common path. These tests pin the new invariant: a HELD stablecoin whose INDEPENDENT
oracle price has broken its $1 peg degrades the mark to UNAVAILABLE with a typed
reason, while the healthy-peg path and all volatile (non-stable) tokens are unchanged.
"""

from datetime import UTC, datetime
from decimal import Decimal
from unittest.mock import MagicMock, PropertyMock, patch

from almanak.framework.portfolio.models import ValueConfidence
from almanak.framework.teardown.models import PositionInfo, PositionType, TeardownPositionSummary
from almanak.framework.valuation.lending_position_reader import LendingPositionOnChain
from almanak.framework.valuation.portfolio_valuer import PortfolioValuer

USDC = "0xaf88d065e77c8cC2239327C5EDb3A432268e5831"


def _make_strategy(tracked_tokens, deployment_id="depeg-strat", chain="arbitrum"):
    strategy = MagicMock()
    type(strategy).deployment_id = PropertyMock(return_value=deployment_id)
    type(strategy).chain = PropertyMock(return_value=chain)
    type(strategy).wallet_address = PropertyMock(return_value="0x1234567890123456789012345678901234567890")
    strategy._get_tracked_tokens.return_value = tracked_tokens
    strategy.get_open_positions.return_value = TeardownPositionSummary(
        deployment_id=deployment_id,
        timestamp=datetime.now(UTC),
        positions=[],
    )
    return strategy


def _make_market(prices, balances):
    market = MagicMock()

    def mock_price(token, quote="USD"):
        if token in prices:
            return prices[token]
        raise ValueError(f"No price for {token}")

    def mock_balance(token):
        if token in balances:
            result = MagicMock()
            result.balance = balances[token]
            return result
        raise ValueError(f"No balance for {token}")

    market.price = mock_price
    market.balance = mock_balance
    return market


# ---------------------------------------------------------------------------
# _stable_peg_check helper — the shared seam
# ---------------------------------------------------------------------------


class TestStablePegCheckHelper:
    def test_non_stablecoin_returns_none(self):
        """A volatile token is not peg-bound — no cross-check applies (priced normally)."""
        valuer = PortfolioValuer()
        assert valuer._stable_peg_check("WETH", Decimal("3500")) is None
        assert valuer._stable_peg_check("ARB", Decimal("0.40")) is None
        assert valuer._stable_peg_check(None, Decimal("1")) is None

    def test_yield_bearing_wrappers_and_soft_pegs_return_none(self):
        """The gate is HARD-$1 cash equivalents only — NOT the broad STABLECOINS set.

        Yield-bearing ERC-4626 wrappers (sDAI/sUSDe) correctly price WELL above $1 from
        accrued yield, and soft-pegs (FRAX/crvUSD/GHO/LUSD/USDe) legitimately float
        100-300 bps off $1. Gating the hard-$1 check on them would blackout a healthy
        NAV every iteration. They MUST be excluded → return None (priced normally).
        """
        valuer = PortfolioValuer()
        # Yield-bearing wrappers at their real appreciated prices.
        assert valuer._stable_peg_check("sUSDe", Decimal("1.25")) is None
        assert valuer._stable_peg_check("SUSDE", Decimal("1.5")) is None
        assert valuer._stable_peg_check("sDAI", Decimal("1.15")) is None
        assert valuer._stable_peg_check("SDAI", Decimal("1.18")) is None
        # Soft-pegs floating off $1 under normal stress.
        for soft in ("FRAX", "CRVUSD", "GHO", "LUSD", "USDE"):
            assert valuer._stable_peg_check(soft, Decimal("0.97")) is None, soft

    def test_healthy_stable_is_ok(self):
        """A stablecoin within the threshold of $1 passes the cross-check."""
        valuer = PortfolioValuer()
        peg = valuer._stable_peg_check("USDC", Decimal("1.0"))
        assert peg is not None
        assert peg.ok is True
        # case-insensitive symbol match
        assert valuer._stable_peg_check("usdc", Decimal("0.999")).ok is True

    def test_depegged_stable_fails(self):
        """A stablecoin > threshold off $1 is a depeg, not oracle noise."""
        valuer = PortfolioValuer()
        peg = valuer._stable_peg_check("USDC", Decimal("0.90"))
        assert peg is not None
        assert peg.ok is False
        assert peg.reason == "depeg_divergence"
        assert peg.max_divergence_bps == 1000  # |0.90 - 1| / 1 = 1000 bps


# ---------------------------------------------------------------------------
# Spot wallet valuation
# ---------------------------------------------------------------------------


class TestSpotStableDepeg:
    def test_held_depegged_stable_degrades_to_unavailable(self):
        """A held stablecoin marked off-peg degrades the WHOLE snapshot to UNAVAILABLE."""
        valuer = PortfolioValuer()
        strategy = _make_strategy(tracked_tokens=["ETH", "USDC"])
        market = _make_market(
            prices={"ETH": Decimal("3500"), "USDC": Decimal("0.85")},  # USDC depegged 1500 bps
            balances={"ETH": Decimal("2"), "USDC": Decimal("5000")},
        )

        snapshot = valuer.value(strategy, market)

        assert snapshot.value_confidence == ValueConfidence.UNAVAILABLE
        assert snapshot.snapshot_metadata.get("stable_depeg") == {"USDC": "1500bps"}

    def test_healthy_stable_stays_high_no_metadata(self):
        """A healthy-peg stablecoin leaves confidence HIGH and stamps NO depeg metadata."""
        valuer = PortfolioValuer()
        strategy = _make_strategy(tracked_tokens=["ETH", "USDC"])
        market = _make_market(
            prices={"ETH": Decimal("3500"), "USDC": Decimal("1.0")},
            balances={"ETH": Decimal("2"), "USDC": Decimal("5000")},
        )

        snapshot = valuer.value(strategy, market)

        assert snapshot.value_confidence == ValueConfidence.HIGH
        assert "stable_depeg" not in snapshot.snapshot_metadata

    def test_depegged_stable_not_held_does_not_degrade(self):
        """A depegged stable with ZERO balance is not held — no mark, no degrade."""
        valuer = PortfolioValuer()
        strategy = _make_strategy(tracked_tokens=["ETH", "USDC"])
        market = _make_market(
            prices={"ETH": Decimal("3500"), "USDC": Decimal("0.50")},  # depegged but...
            balances={"ETH": Decimal("2"), "USDC": Decimal("0")},  # ...not held
        )

        snapshot = valuer.value(strategy, market)

        assert snapshot.value_confidence == ValueConfidence.HIGH
        assert "stable_depeg" not in snapshot.snapshot_metadata

    def test_volatile_token_far_from_one_does_not_false_fire(self):
        """A non-stable token priced far from $1 must NOT trip the de-peg check."""
        valuer = PortfolioValuer()
        strategy = _make_strategy(tracked_tokens=["ETH", "ARB"])
        market = _make_market(
            prices={"ETH": Decimal("3500"), "ARB": Decimal("0.40")},  # 60% off $1 — but volatile
            balances={"ETH": Decimal("2"), "ARB": Decimal("100")},
        )

        snapshot = valuer.value(strategy, market)

        assert snapshot.value_confidence == ValueConfidence.HIGH
        assert "stable_depeg" not in snapshot.snapshot_metadata

    def test_held_susde_at_appreciated_price_does_not_degrade(self):
        """A held yield-bearing sUSDe correctly priced ~$1.25 must NOT blacken the NAV.

        This is the audit blocker: sUSDe is in the broad STABLECOINS set but is a
        yield-bearing wrapper that appreciates far past $1. Gating on STABLECOINS would
        flip a healthy Ethena-staking strategy to UNAVAILABLE every iteration.
        """
        valuer = PortfolioValuer()
        strategy = _make_strategy(tracked_tokens=["ETH", "sUSDe"])
        market = _make_market(
            prices={"ETH": Decimal("3500"), "sUSDe": Decimal("1.25")},  # real appreciated price
            balances={"ETH": Decimal("2"), "sUSDe": Decimal("5000")},
        )

        snapshot = valuer.value(strategy, market)

        assert snapshot.value_confidence == ValueConfidence.HIGH
        assert "stable_depeg" not in snapshot.snapshot_metadata
        # The appreciated mark is still counted (5000 * 1.25 = 6250 + 2*3500).
        assert snapshot.wallet_total_value_usd == Decimal("13250")

    def test_held_sdai_at_appreciated_price_does_not_degrade(self):
        """A held yield-bearing sDAI correctly priced ~$1.15 must NOT blacken the NAV."""
        valuer = PortfolioValuer()
        strategy = _make_strategy(tracked_tokens=["ETH", "sDAI"])
        market = _make_market(
            prices={"ETH": Decimal("3500"), "sDAI": Decimal("1.15")},  # accrued DSR value
            balances={"ETH": Decimal("2"), "sDAI": Decimal("5000")},
        )

        snapshot = valuer.value(strategy, market)

        assert snapshot.value_confidence == ValueConfidence.HIGH
        assert "stable_depeg" not in snapshot.snapshot_metadata

    def test_address_form_depegged_stable_degrades(self):
        """A depegged stablecoin balance keyed by ADDRESS (not symbol) MUST still degrade.

        CodeRabbit finding: a raw address never matches the symbol-keyed gate, so an
        address-keyed USDC balance would bypass the check. The valuer must resolve the
        address to its symbol before gating.
        """
        valuer = PortfolioValuer()
        strategy = _make_strategy(tracked_tokens=["ETH", USDC])
        market = _make_market(
            prices={"ETH": Decimal("3500"), USDC: Decimal("0.85")},  # depegged 1500 bps
            balances={"ETH": Decimal("2"), USDC: Decimal("5000")},
        )

        with patch.object(PortfolioValuer, "_symbol_from_address", return_value="USDC"):
            snapshot = valuer.value(strategy, market)

        assert snapshot.value_confidence == ValueConfidence.UNAVAILABLE
        # Keyed by the wallet's own balance key (the address) so it maps to the row.
        assert snapshot.snapshot_metadata.get("stable_depeg") == {USDC: "1500bps"}

    def test_address_form_healthy_stable_stays_high(self):
        """An address-keyed stablecoin holding its peg values normally (no false-fire)."""
        valuer = PortfolioValuer()
        strategy = _make_strategy(tracked_tokens=["ETH", USDC])
        market = _make_market(
            prices={"ETH": Decimal("3500"), USDC: Decimal("1.0")},
            balances={"ETH": Decimal("2"), USDC: Decimal("5000")},
        )

        with patch.object(PortfolioValuer, "_symbol_from_address", return_value="USDC"):
            snapshot = valuer.value(strategy, market)

        assert snapshot.value_confidence == ValueConfidence.HIGH
        assert "stable_depeg" not in snapshot.snapshot_metadata

    def test_unresolvable_address_not_treated_as_stable(self):
        """Empty≠Zero: an UNRESOLVABLE address is NOT fabricated as a stable → no degrade.

        An off-$1 price on a token whose address can't be resolved to a known stable
        symbol must not trip the depeg gate (it is priced on the normal spot path).
        """
        valuer = PortfolioValuer()
        unknown = "0x00000000000000000000000000000000deadbeef"
        strategy = _make_strategy(tracked_tokens=["ETH", unknown])
        market = _make_market(
            prices={"ETH": Decimal("3500"), unknown: Decimal("0.85")},
            balances={"ETH": Decimal("2"), unknown: Decimal("5000")},
        )

        with patch.object(PortfolioValuer, "_symbol_from_address", return_value=None):
            snapshot = valuer.value(strategy, market)

        assert snapshot.value_confidence == ValueConfidence.HIGH
        assert "stable_depeg" not in snapshot.snapshot_metadata


# ---------------------------------------------------------------------------
# Lending asset USD marks
# ---------------------------------------------------------------------------


def _lending_position(position_type, asset, asset_address=USDC):
    return PositionInfo(
        position_type=getattr(PositionType, position_type),
        position_id="lend-depeg",
        chain="arbitrum",
        protocol="aave_v3",
        value_usd=Decimal("999"),
        details={
            "asset_address": asset_address,
            "wallet": "0x1234567890abcdef1234567890abcdef12345678",
            "asset": asset,
        },
    )


class TestLendingStableDepeg:
    def _valuer_with_reader(self, on_chain):
        valuer = PortfolioValuer(gateway_client=MagicMock())
        valuer._lending_reader = MagicMock()
        valuer._lending_reader.read_position.return_value = on_chain
        return valuer

    def test_depegged_stable_supply_marks_no_path(self):
        """A SUPPLY of a depegged stablecoin degrades to no_path (UNAVAILABLE), not $1 par."""
        on_chain = LendingPositionOnChain(
            asset_address=USDC,
            current_atoken_balance=5_000_000_000,  # 5000 USDC supplied
            current_stable_debt=0,
            current_variable_debt=0,
            liquidity_rate=0,
            usage_as_collateral_enabled=True,
        )
        valuer = self._valuer_with_reader(on_chain)
        market = MagicMock()
        market.price.return_value = Decimal("0.80")  # USDC depegged 2000 bps

        result = valuer._reprice_lending_on_chain_enriched(_lending_position("SUPPLY", "USDC"), "arbitrum", market)

        assert result is not None
        value_usd, details = result
        assert value_usd == Decimal("0")
        assert details["valuation_status"] == "no_path"
        assert details["unavailable_reason"] == "lending_oracle_depeg_divergence"
        assert details["depeg_divergence_bps"] == "2000"
        assert details["mark_unmeasured"] is True

    def test_healthy_stable_supply_values_normally(self):
        """A healthy-peg stablecoin supply marks at par with NO degrade marker."""
        on_chain = LendingPositionOnChain(
            asset_address=USDC,
            current_atoken_balance=5_000_000_000,  # 5000 USDC
            current_stable_debt=0,
            current_variable_debt=1_000_000_000,  # 1000 USDC debt
            liquidity_rate=0,
            usage_as_collateral_enabled=True,
        )
        valuer = self._valuer_with_reader(on_chain)
        market = MagicMock()
        market.price.return_value = Decimal("1.0")

        with patch.object(PortfolioValuer, "_get_token_decimals", return_value=6):
            result = valuer._reprice_lending_on_chain_enriched(_lending_position("SUPPLY", "USDC"), "arbitrum", market)

        assert result is not None
        value_usd, details = result
        assert value_usd == Decimal("4000")  # 5000 supply - 1000 debt
        assert details.get("valuation_status") != "no_path"
        assert "unavailable_reason" not in details

    def test_volatile_lending_asset_skips_check(self):
        """A non-stable lending asset (WETH) priced far from $1 is NOT de-peg degraded."""
        on_chain = LendingPositionOnChain(
            asset_address="0x82aF49447D8a07e3bd95BD0d56f35241523fBab1",
            current_atoken_balance=1_000_000_000_000_000_000,  # 1 WETH supplied
            current_stable_debt=0,
            current_variable_debt=0,
            liquidity_rate=0,
            usage_as_collateral_enabled=True,
        )
        valuer = self._valuer_with_reader(on_chain)
        market = MagicMock()
        market.price.return_value = Decimal("3500")  # far from $1 but WETH is volatile

        with patch.object(PortfolioValuer, "_get_token_decimals", return_value=18):
            result = valuer._reprice_lending_on_chain_enriched(
                _lending_position("SUPPLY", "WETH", asset_address="0x82aF49447D8a07e3bd95BD0d56f35241523fBab1"),
                "arbitrum",
                market,
            )

        assert result is not None
        value_usd, details = result
        assert value_usd == Decimal("3500")  # 1 WETH * $3500, valued normally
        assert details.get("valuation_status") != "no_path"

    def test_yield_bearing_wrapper_supply_not_degraded(self):
        """A SUPPLY of a yield-bearing wrapper (sUSDe ~$1.25) must value normally, NOT no_path.

        sUSDe is in the broad STABLECOINS set but appreciates past $1 by design — the
        lending gate must use the hard-$1 cash-equivalent set, so it is priced normally.
        """
        on_chain = LendingPositionOnChain(
            asset_address="0x211Cc4DD073734dA055fbF44a2b4667d5E5fE5d2",  # sUSDe
            current_atoken_balance=5_000_000_000_000_000_000_000,  # 5000 sUSDe (18 dp)
            current_stable_debt=0,
            current_variable_debt=0,
            liquidity_rate=0,
            usage_as_collateral_enabled=True,
        )
        valuer = self._valuer_with_reader(on_chain)
        market = MagicMock()
        market.price.return_value = Decimal("1.25")  # real appreciated sUSDe price

        with patch.object(PortfolioValuer, "_get_token_decimals", return_value=18):
            result = valuer._reprice_lending_on_chain_enriched(
                _lending_position("SUPPLY", "sUSDe", asset_address="0x211Cc4DD073734dA055fbF44a2b4667d5E5fE5d2"),
                "arbitrum",
                market,
            )

        assert result is not None
        value_usd, details = result
        assert value_usd == Decimal("6250")  # 5000 sUSDe * $1.25, valued normally (not degraded)
        assert details.get("valuation_status") != "no_path"
        assert "unavailable_reason" not in details
