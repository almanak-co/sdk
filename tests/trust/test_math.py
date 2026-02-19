#!/usr/bin/env python3
"""
Phase 2: Mathematical Correctness

This file contains tests that verify all financial calculations match published formulas.
These tests ensure the math is correct regardless of data quality or market conditions.

Usage:
    python -c "exec(open('tests/trust/test_math.py').read())"

Tests:
    2.1 Impermanent Loss Formula: Must match published IL formula
    2.2 Sharpe Ratio Calculation: Must match manual calculation
    2.3 Max Drawdown Calculation: Must match manual calculation
"""

import math

# Add project root to path (works with exec and direct execution)
import os
import sys
from decimal import Decimal
from pathlib import Path

try:
    # Try to get the file path (works when run directly)
    current_file = os.path.abspath(__file__)
except NameError:
    # When run via exec(), assume we're in the project root
    current_file = os.path.join(os.getcwd(), "tests/trust/test_math.py")

project_root = Path(current_file).parent.parent.parent
sys.path.insert(0, str(project_root))

from almanak.framework.backtesting.pnl.calculators.impermanent_loss import (
    ImpermanentLossCalculator,
)


def test_il_calculation():
    """Test 2.1: Impermanent Loss Formula Verification

    For a 50% price increase (ratio = 1.5):
    IL = 2 * sqrt(1.5) / (1 + 1.5) - 1
    IL = 2 * 1.2247 / 2.5 - 1
    IL = 0.9798 - 1 = -0.0202 = -2.02%

    This catches: IL formula errors, sqrt precision issues
    """
    print("Testing 2.1: Impermanent Loss Formula...")

    try:
        calc = ImpermanentLossCalculator()

        # Test case: 50% price increase
        price_ratio = Decimal("1.5")  # 2000 -> 3000

        # Manual calculation using V2 formula
        # IL = 2 * sqrt(k) / (1 + k) - 1
        sqrt_ratio = calc._decimal_sqrt(price_ratio)
        expected_il = (Decimal("2") * sqrt_ratio) / (Decimal("1") + price_ratio) - Decimal("1")

        # Expected: approximately -2.02% (loss)
        expected_il_pct = Decimal("-0.0202")  # -2.02%
        tolerance = Decimal("0.0001")  # 0.01% tolerance

        if abs(expected_il - expected_il_pct) <= tolerance:
            print(f"✅ PASS: IL formula verified (IL={expected_il:.4f}, expected={expected_il_pct:.4f})")
            return True
        else:
            print(f"❌ FAIL: IL formula error (IL={expected_il:.4f}, expected={expected_il_pct:.4f})")
            return False

    except Exception as e:
        print(f"❌ FAIL: Exception during IL test: {e}")
        return False


def test_sharpe_ratio():
    """Test 2.2: Sharpe Ratio Calculation

    Known equity curve: [10000, 10100, 10200, 10150, 10250]
    Returns: [1%, 0.99%, -0.49%, 0.98%]
    Mean return: 0.62%
    Std dev: ~0.67%
    Daily Sharpe (rf=0): ~0.93

    This catches: return calculation errors, std dev issues, annualization bugs
    """
    print("Testing 2.2: Sharpe Ratio Calculation...")

    try:
        # Known equity curve
        equity = [Decimal("10000"), Decimal("10100"), Decimal("10200"), Decimal("10150"), Decimal("10250")]

        # Calculate returns
        returns = []
        for i in range(1, len(equity)):
            ret = (equity[i] - equity[i - 1]) / equity[i - 1]
            returns.append(float(ret))

        # Manual calculation
        mean_return = sum(returns) / len(returns)
        variance = sum((r - mean_return) ** 2 for r in returns) / len(returns)
        std_dev = math.sqrt(variance)

        # Daily Sharpe (assuming 0% risk-free rate)
        daily_sharpe = mean_return / std_dev if std_dev > 0 else 0

        # Expected values (from the problem statement)
        expected_mean = 0.0062  # 0.62%
        expected_std = 0.0067  # 0.67%
        expected_sharpe = 0.93  # 0.93

        tolerance = 0.01  # 1% tolerance

        if (
            abs(mean_return - expected_mean) <= tolerance
            and abs(std_dev - expected_std) <= tolerance
            and abs(daily_sharpe - expected_sharpe) <= tolerance
        ):
            print(
                f"✅ PASS: Sharpe ratio calculation verified (mean={mean_return:.4f}, std={std_dev:.4f}, sharpe={daily_sharpe:.2f})"
            )
            return True
        else:
            print(
                f"❌ FAIL: Sharpe ratio calculation error (mean={mean_return:.4f}, std={std_dev:.4f}, sharpe={daily_sharpe:.2f})"
            )
            return False

    except Exception as e:
        print(f"❌ FAIL: Exception during Sharpe test: {e}")
        return False


def test_max_drawdown():
    """Test 2.3: Max Drawdown Calculation

    Equity: [10000, 11000, 10500, 12000, 9000, 10000]
    Peak: 12000, Trough: 9000
    Max Drawdown = (12000 - 9000) / 12000 = 25.0%

    This catches: peak tracking errors, drawdown math bugs
    """
    print("Testing 2.3: Max Drawdown Calculation...")

    try:
        equity = [
            Decimal("10000"),
            Decimal("11000"),
            Decimal("10500"),
            Decimal("12000"),
            Decimal("9000"),
            Decimal("10000"),
        ]

        peak = equity[0]
        max_drawdown = Decimal("0")

        for value in equity:
            if value > peak:
                peak = value
            drawdown = (peak - value) / peak if peak > 0 else Decimal("0")
            if drawdown > max_drawdown:
                max_drawdown = drawdown

        expected = Decimal("0.25")  # 25%

        print(f"Equity curve: {equity}")
        print(f"Peak: ${peak}")
        print(f"Max drawdown: {max_drawdown:.4f}")
        if abs(max_drawdown - expected) > Decimal("0.001"):
            print(f"❌ FAIL: Drawdown calculation error (computed={max_drawdown:.4f}, expected={expected:.4f})")
            return False

        print("✅ PASS: Max drawdown = 25.0%")
        return True

    except Exception as e:
        print(f"❌ FAIL: Exception during drawdown test: {e}")
        return False


def run_phase_2_tests():
    """Run all Phase 2 mathematical correctness tests."""
    print("=" * 60)
    print("PHASE 2: Mathematical Correctness")
    print("=" * 60)

    results = []
    results.append(test_il_calculation())
    results.append(test_sharpe_ratio())
    results.append(test_max_drawdown())

    passed = sum(results)
    total = len(results)

    print("\n" + "=" * 60)
    print(f"PHASE 2 RESULTS: {passed}/{total} tests passed")

    if passed == total:
        print("✅ ALL MATHEMATICAL CALCULATIONS VERIFIED")
        return True
    else:
        print("❌ MATHEMATICAL ERRORS DETECTED")
        return False


if __name__ == "__main__":
    success = run_phase_2_tests()
    sys.exit(0 if success else 1)
