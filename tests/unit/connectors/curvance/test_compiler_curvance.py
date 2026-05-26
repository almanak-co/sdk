"""Compile-layer unit tests for the Curvance Monad branches.

The Anvil oracle CAUTION fork artefact (documented in
``tests/intents/monad/test_curvance_lending.py``) blocks BORROW / REPAY /
WITHDRAW from real on-chain execution. These tests close the compile-layer gap
by:

- Asserting calldata/selector/target correctness for each Curvance compile
  branch (BORROW, REPAY, WITHDRAW with both withdraw_all and asset-amount
  flavours, SUPPLY).
- Verifying the new fail-fast guards: market_id ↔ token mismatch,
  ``use_as_collateral=False``, and ``withdraw_all`` without an available share
  balance.

The tests do not require Anvil, an RPC, or a gateway client.
"""

from __future__ import annotations

from decimal import Decimal

import pytest

from almanak.connectors.curvance import CURVANCE_MARKETS
from almanak.framework.intents import (
    BorrowIntent,
    RepayIntent,
    SupplyIntent,
    WithdrawIntent,
)
from almanak.framework.intents.compiler import IntentCompiler
from almanak.framework.intents.compiler_models import CompilationStatus

CHAIN = "monad"
WALLET = "0x54776446Aa29Fc49d152B4850bD410eA1E4d24bF"
WMON_USDC_MARKET = "0xa6A2A92F126b79Ee0804845ee6B52899b4491093"
WBTC_USDC_MARKET_LOWER = next(
    (mid for mid, info in CURVANCE_MARKETS["monad"].items() if info.collateral_symbol.upper() == "WBTC"),
    None,
)


@pytest.fixture
def price_oracle() -> dict[str, Decimal]:
    return {
        "WMON": Decimal("3.0"),
        "USDC": Decimal("1.0"),
        "WBTC": Decimal("60000"),
        "WETH": Decimal("3000"),
    }


@pytest.fixture
def compiler(price_oracle: dict[str, Decimal]) -> IntentCompiler:
    # No rpc_url and no gateway_client → balance queries return None,
    # which is exactly what we want for the withdraw_all guard test.
    return IntentCompiler(chain=CHAIN, wallet_address=WALLET, price_oracle=price_oracle)


# -----------------------------------------------------------------------------
# SUPPLY
# -----------------------------------------------------------------------------


class TestSupplyCompile:
    def test_supply_collateral_happy_path(self, compiler: IntentCompiler) -> None:
        intent = SupplyIntent(
            protocol="curvance",
            token="WMON",
            amount=Decimal("1.0"),
            use_as_collateral=True,
            market_id=WMON_USDC_MARKET,
            chain=CHAIN,
        )
        result = compiler.compile(intent)
        assert result.status == CompilationStatus.SUCCESS, f"Compile failed: {result.error}"
        assert result.action_bundle is not None
        # approve(WMON, cWMON) + depositAsCollateral(amount, receiver)
        txs = result.action_bundle.transactions
        assert len(txs) == 2, f"Expected approve + depositAsCollateral, got {len(txs)}"
        approve_tx, supply_tx = txs
        assert approve_tx["data"].startswith("0x095ea7b3")  # approve(address,uint256)
        # supply tx must hit the market's collateral cToken with depositAsCollateral selector
        market = CURVANCE_MARKETS[CHAIN][WMON_USDC_MARKET.lower()]
        assert supply_tx["to"].lower() == market.collateral_ctoken.lower()
        assert supply_tx["data"].startswith("0x2f4a61d9")  # depositAsCollateral(uint256,address)

    def test_supply_lend_only_rejected(self, compiler: IntentCompiler) -> None:
        """``use_as_collateral=False`` must be rejected (lend-only deposit not wired).

        The user-visible safeguard fires at the intent vocabulary layer
        (Pydantic validator); the compile-layer FAILED branch is defense in
        depth in case the intent is constructed without going through the
        validator (e.g., ``model_construct``).
        """
        from pydantic_core import ValidationError

        with pytest.raises(ValidationError, match="use_as_collateral=False"):
            SupplyIntent(
                protocol="curvance",
                token="WMON",
                amount=Decimal("1.0"),
                use_as_collateral=False,
                market_id=WMON_USDC_MARKET,
                chain=CHAIN,
            )

        # Defense-in-depth: same rejection at compile time when bypassing the validator.
        intent = SupplyIntent.model_construct(
            protocol="curvance",
            token="WMON",
            amount=Decimal("1.0"),
            use_as_collateral=False,
            market_id=WMON_USDC_MARKET,
            chain=CHAIN,
        )
        result = compiler.compile(intent)
        assert result.status == CompilationStatus.FAILED
        assert "use_as_collateral=False" in (result.error or "")

    def test_supply_token_mismatch_rejected(self, compiler: IntentCompiler) -> None:
        """Supplying USDC to the WMON-USDC market (where WMON is collateral) must FAIL."""
        intent = SupplyIntent(
            protocol="curvance",
            token="USDC",  # wrong: USDC is the debt asset, not collateral
            amount=Decimal("100"),
            use_as_collateral=True,
            market_id=WMON_USDC_MARKET,
            chain=CHAIN,
        )
        result = compiler.compile(intent)
        assert result.status == CompilationStatus.FAILED
        err = (result.error or "").lower()
        assert "collateral" in err and "usdc" in err


# -----------------------------------------------------------------------------
# BORROW
# -----------------------------------------------------------------------------


class TestBorrowCompile:
    def test_borrow_happy_path(self, compiler: IntentCompiler) -> None:
        intent = BorrowIntent(
            protocol="curvance",
            collateral_token="WMON",
            collateral_amount=Decimal("30"),
            borrow_token="USDC",
            borrow_amount=Decimal("15"),
            market_id=WMON_USDC_MARKET,
            chain=CHAIN,
        )
        result = compiler.compile(intent)
        assert result.status == CompilationStatus.SUCCESS, f"Compile failed: {result.error}"
        assert result.action_bundle is not None
        # Expect approve(WMON, cWMON) + depositAsCollateral + borrow(USDC)
        txs = result.action_bundle.transactions
        assert len(txs) == 3, f"Expected approve + depositAsCollateral + borrow, got {len(txs)}"
        approve_tx, supply_tx, borrow_tx = txs
        market = CURVANCE_MARKETS[CHAIN][WMON_USDC_MARKET.lower()]
        assert approve_tx["data"].startswith("0x095ea7b3")
        assert supply_tx["to"].lower() == market.collateral_ctoken.lower()
        assert supply_tx["data"].startswith("0x2f4a61d9")  # depositAsCollateral
        assert borrow_tx["to"].lower() == market.borrowable_ctoken.lower()
        assert borrow_tx["data"].startswith("0x4b3fd148")  # borrow(uint256,address)

    def test_borrow_market_token_mismatch_rejected(self, compiler: IntentCompiler) -> None:
        """Borrowing WMON from the WMON-USDC market (USDC is the debt asset) must FAIL."""
        intent = BorrowIntent(
            protocol="curvance",
            collateral_token="WMON",
            collateral_amount=Decimal("30"),
            borrow_token="WMON",  # wrong: market debt is USDC
            borrow_amount=Decimal("1"),
            market_id=WMON_USDC_MARKET,
            chain=CHAIN,
        )
        result = compiler.compile(intent)
        assert result.status == CompilationStatus.FAILED
        err = (result.error or "").lower()
        assert "debt asset" in err and "wmon" in err


# -----------------------------------------------------------------------------
# REPAY
# -----------------------------------------------------------------------------


class TestRepayCompile:
    def test_repay_full_uses_zero_sentinel(self, compiler: IntentCompiler) -> None:
        intent = RepayIntent(
            protocol="curvance",
            token="USDC",
            amount=Decimal("0"),
            repay_full=True,
            market_id=WMON_USDC_MARKET,
            chain=CHAIN,
        )
        result = compiler.compile(intent)
        assert result.status == CompilationStatus.SUCCESS, f"Compile failed: {result.error}"
        assert result.action_bundle is not None
        # approve(USDC, bUSDC) MAX, then repay(0)
        txs = result.action_bundle.transactions
        assert len(txs) == 2, f"Expected approve + repay, got {len(txs)}"
        approve_tx, repay_tx = txs
        market = CURVANCE_MARKETS[CHAIN][WMON_USDC_MARKET.lower()]
        assert approve_tx["data"].startswith("0x095ea7b3")
        assert repay_tx["to"].lower() == market.borrowable_ctoken.lower()
        assert repay_tx["data"].startswith("0x371fd8e6")  # repay(uint256)
        # The 32-byte argument must be 0 (full-debt sentinel)
        arg_word = repay_tx["data"][10 : 10 + 64]
        assert int(arg_word, 16) == 0, "repay_full=True must encode 0 (Curvance full-debt sentinel)"

    def test_repay_token_mismatch_rejected(self, compiler: IntentCompiler) -> None:
        intent = RepayIntent(
            protocol="curvance",
            token="WMON",  # wrong: market debt is USDC
            amount=Decimal("1.0"),
            repay_full=False,
            market_id=WMON_USDC_MARKET,
            chain=CHAIN,
        )
        result = compiler.compile(intent)
        assert result.status == CompilationStatus.FAILED
        err = (result.error or "").lower()
        assert "debt asset" in err and "wmon" in err


# -----------------------------------------------------------------------------
# WITHDRAW
# -----------------------------------------------------------------------------


class TestWithdrawCompile:
    def test_withdraw_amount_happy_path(self, compiler: IntentCompiler) -> None:
        """Asset-amount withdraw goes through ``withdrawCollateral(assets, receiver, owner)``."""
        intent = WithdrawIntent(
            protocol="curvance",
            token="WMON",
            amount=Decimal("0.5"),
            withdraw_all=False,
            market_id=WMON_USDC_MARKET,
            chain=CHAIN,
        )
        result = compiler.compile(intent)
        assert result.status == CompilationStatus.SUCCESS, f"Compile failed: {result.error}"
        assert result.action_bundle is not None
        txs = result.action_bundle.transactions
        assert len(txs) == 1
        market = CURVANCE_MARKETS[CHAIN][WMON_USDC_MARKET.lower()]
        assert txs[0]["to"].lower() == market.collateral_ctoken.lower()
        assert txs[0]["data"].startswith("0x72d46ac2")  # withdrawCollateral(uint256,address,address)

    def test_withdraw_all_without_share_balance_rejected(self, compiler: IntentCompiler) -> None:
        """``withdraw_all=True`` requires reading the cToken share balance.

        Without a gateway client / RPC, the balance query returns ``None`` and
        the compiler MUST fail fast — every full-withdraw intent would otherwise
        revert at execution time.
        """
        intent = WithdrawIntent(
            protocol="curvance",
            token="WMON",
            amount=Decimal("0"),
            withdraw_all=True,
            market_id=WMON_USDC_MARKET,
            chain=CHAIN,
        )
        result = compiler.compile(intent)
        assert result.status == CompilationStatus.FAILED
        err = (result.error or "").lower()
        assert "share balance" in err

    def test_withdraw_token_mismatch_rejected(self, compiler: IntentCompiler) -> None:
        intent = WithdrawIntent(
            protocol="curvance",
            token="USDC",  # wrong: WMON-USDC market collateral is WMON
            amount=Decimal("1.0"),
            withdraw_all=False,
            market_id=WMON_USDC_MARKET,
            chain=CHAIN,
        )
        result = compiler.compile(intent)
        assert result.status == CompilationStatus.FAILED
        err = (result.error or "").lower()
        assert "collateral" in err
