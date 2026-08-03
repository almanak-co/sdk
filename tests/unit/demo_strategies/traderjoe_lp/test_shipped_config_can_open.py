"""VIB-6307 — the shipped demo must be able to open a position at its OWN funding.

The demo's stated defect is that it "cannot complete its own lifecycle as shipped".
Re-pointing the drained pool fixed one blocker; this pins the second, which is
independent of it and was missed on the first pass.

``TraderJoeLPStrategy.decide`` gates on WALLET INVENTORY, not on the configured
deploy size (``strategy.py`` — ``total_usd = token_x_usd + token_y_usd`` read from
``market.balance(...)``, then ``if total_usd < self.min_position_usd: return
Intent.hold(...)``). ``min_position_usd`` was absent from ``config.json``, so it took
the dataclass default of **$100** while the demo funds itself with roughly $10 —
0.005 WAVAX plus $10 USDT. The shipped demo, funded exactly as it specifies, HOLDs
every iteration on mainnet and never opens a position.

WHY NOTHING CAUGHT IT — the finding worth keeping
--------------------------------------------------
``anvil_funding`` is 100 WAVAX + 10,000 USDT, which clears a $100 threshold trivially.
So the Anvil path opens and the mainnet path holds, and ``make test-demo-*`` only ever
exercises the Anvil path. A green Anvil E2E is therefore *consistent with* a demo that
can never open on mainnet — it is not evidence against it.

That asymmetry is a property of every demo, not just this one: ``anvil_funding`` and
``token_funding`` are independent numbers, and only the first is ever executed by the
demo test surface. These assertions compare them, which is the cheapest place to catch
the whole class.

The test deliberately reads the SHIPPED ``config.json`` rather than constructing a
config, so it tracks the artefact a user actually runs.
"""

from __future__ import annotations

import json
from decimal import Decimal
from pathlib import Path

import pytest

import almanak

_CONFIG = Path(almanak.__file__).parent / "demo_strategies" / "traderjoe_lp" / "config.json"

# Deliberately conservative: a WAVAX collapse should not silently flip this test to
# green-by-luck. Real WAVAX has traded far above this; using a floor rather than a
# live price keeps the assertion deterministic and offline.
_WAVAX_FLOOR_USD = Decimal("1.00")


@pytest.fixture(scope="module")
def config() -> dict:
    return json.loads(_CONFIG.read_text())


def _funded_usd_lower_bound(config: dict) -> Decimal:
    """Lower bound on the USD the demo funds itself with, from ``token_funding``.

    ``amount_type: "usd"`` entries contribute their face value. ``token`` entries are
    valued at a conservative floor price, so the bound can only understate — a test
    that passes on the bound passes on the real value.
    """
    total = Decimal("0")
    for entry in config.get("token_funding", []):
        amount = Decimal(str(entry["amount"]))
        if entry.get("amount_type") == "usd":
            total += amount
        elif str(entry.get("symbol", "")).upper() == "WAVAX":
            total += amount * _WAVAX_FLOOR_USD
        # An unpriced token contributes 0 — keeps this a genuine lower bound.
    return total


def test_min_position_usd_is_declared_not_left_to_the_dataclass_default(config: dict) -> None:
    """Absence is the bug: the $100 default is silently wrong for this demo's size."""
    assert "min_position_usd" in config, (
        "config.json must declare min_position_usd — omitting it inherits the $100 "
        "dataclass default, which exceeds this demo's own ~$10 token_funding and "
        "makes decide() HOLD on every iteration"
    )


def test_shipped_funding_clears_the_shipped_open_threshold(config: dict) -> None:
    """The demo must be able to open at its OWN declared funding, on mainnet."""
    funded = _funded_usd_lower_bound(config)
    threshold = Decimal(str(config["min_position_usd"]))
    assert funded > threshold, (
        f"token_funding is worth at least ${funded} but min_position_usd is "
        f"${threshold}: decide() would HOLD forever and the demo could never open a "
        "position on mainnet"
    )


def test_open_threshold_does_not_block_reopening_after_a_close(config: dict) -> None:
    """The threshold must sit below the notional the REOPEN path actually deploys.

    A recentering LP closes and reopens. After a close the wallet holds roughly what
    the position was worth, so a threshold above that lets the demo open exactly once
    and then HOLD for the rest of the run — a subtler null than never opening, and one
    a short smoke test would not surface.

    Sized against ``token_funding``, deliberately, NOT against ``amount_x``/``amount_y``.
    The config amounts drive only the ``force_action`` path; the live reopen deploys
    ``token_*_balance * 0.95`` off WALLET INVENTORY (``strategy.py`` — "Deploy ~95% of
    each balanced side"). Asserting against the config amounts would let this test
    false-pass if a future edit set ``amount_x``/``amount_y`` far above what the demo
    funds itself with: the assertion would still hold while the property it names —
    that a reopen is not blocked — had stopped being true. Raised by Grok on the
    #3577 panel.
    """
    reopen_usd = _funded_usd_lower_bound(config) * Decimal("0.95")
    threshold = Decimal(str(config["min_position_usd"]))
    assert threshold < reopen_usd, (
        f"min_position_usd ${threshold} is not below the ~${reopen_usd} a reopen "
        "deploys (95% of wallet inventory); the strategy could open once and then "
        "refuse to reopen after a drift-close"
    )


def test_anvil_funding_cannot_be_the_only_thing_that_clears_the_threshold(config: dict) -> None:
    """The Anvil/mainnet asymmetry that hid this defect, pinned directly.

    ``anvil_funding`` is orders of magnitude larger than ``token_funding``, so it
    clears any sane threshold and the demo test surface stays green regardless. This
    asserts the MAINNET-shaped funding clears it too — i.e. that a passing Anvil run
    actually implies a working mainnet demo, which is what everyone assumes it means.
    """
    anvil = config.get("anvil_funding", {})
    anvil_stable = Decimal(str(anvil.get("USDT", anvil.get("USDC", 0))))
    threshold = Decimal(str(config["min_position_usd"]))
    funded = _funded_usd_lower_bound(config)

    assert anvil_stable > threshold, "sanity: anvil_funding is expected to clear it trivially"
    assert funded > threshold, (
        "only anvil_funding clears min_position_usd — the Anvil path would open while "
        "the mainnet path HOLDs, and make test-demo-* would never notice"
    )
