"""Back-compat shim — the HF-safe unwind now lives in :mod:`lending_unwind` (VIB-5467).

The health-factor-aware ``WITHDRAW → SWAP → REPAY`` staircase was promoted to a
first-class, lending-generic, strategy-callable primitive,
:func:`almanak.framework.teardown.lending_unwind.generate_lending_unwind`, because a
leverage-loop unwind and a plain-borrow unwind are the *same* operation (TD-09 thesis:
"knowing how to tear down = calling the right primitive"). This module preserves the
historical names so existing imports keep working:

* ``generate_leverage_loop_teardown`` → :func:`generate_lending_unwind`
* ``LeverageUnwindError``             → :class:`LendingUnwindError`
* ``hf_safe_withdraw_slice_usd``      (re-exported unchanged)

Prefer importing the canonical names from the package
(``from almanak.framework.teardown import generate_lending_unwind``) in new code.
"""

from __future__ import annotations

from almanak.framework.teardown.lending_unwind import (
    LendingUnwindError,
    generate_lending_unwind,
    hf_safe_withdraw_slice_usd,
)

# Historical aliases (same objects, so ``isinstance`` / ``pytest.raises`` and any
# identity check keep behaving exactly as before).
LeverageUnwindError = LendingUnwindError
generate_leverage_loop_teardown = generate_lending_unwind

__all__ = [
    "LendingUnwindError",
    "LeverageUnwindError",
    "generate_leverage_loop_teardown",
    "generate_lending_unwind",
    "hf_safe_withdraw_slice_usd",
]
