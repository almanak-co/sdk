"""Regression guard: AutoRedemptionService must stay deleted.

VIB-3697 removed `almanak/framework/services/auto_redemption.py` because
it accepted a raw private key in `__init__` and called
`Account.sign_transaction(...)` in-process. That violates the CLAUDE.md
hard rule:

    Strategies have no secrets. Signing / submission -> Return an Intent;
    never touch signers from strategy code.

The redemption flow already exists end-to-end via `PredictionRedeemIntent`
-> `PolymarketAdapter._compile_redeem_intent` -> standard execution
orchestrator (gateway-routed signer), so the service was both dangerous
and redundant.

This test fails loudly if anyone reintroduces the file or re-exports the
removed symbols, so the gateway-boundary regression cannot slip back in
silently.
"""

from __future__ import annotations

import importlib
from pathlib import Path

import pytest


def test_auto_redemption_module_does_not_exist() -> None:
    """The source file must remain deleted."""
    # Resolve the path relative to the installed package, not the repo
    # checkout, so the guard works regardless of where the test is run from.
    services_pkg = importlib.import_module("almanak.framework.services")
    services_dir = Path(services_pkg.__file__).resolve().parent  # type: ignore[arg-type]
    auto_redemption_path = services_dir / "auto_redemption.py"

    assert not auto_redemption_path.exists(), (
        f"auto_redemption.py was reintroduced at {auto_redemption_path}. "
        "It accepts a raw private key and signs in-process, which is a "
        "gateway-boundary violation. Use PredictionRedeemIntent instead."
    )


def test_auto_redemption_service_is_not_importable_from_services() -> None:
    """`AutoRedemptionService` must not be re-exported from the services package."""
    from almanak.framework import services as services_pkg

    assert not hasattr(services_pkg, "AutoRedemptionService"), (
        "AutoRedemptionService is re-exported from almanak.framework.services. "
        "It was removed in VIB-3697 because it signs in-process with a raw "
        "private key. Use PredictionRedeemIntent for redemptions."
    )


def test_auto_redemption_module_import_raises() -> None:
    """Direct import of the deleted module must fail with ImportError."""
    with pytest.raises(ImportError):
        importlib.import_module("almanak.framework.services.auto_redemption")


@pytest.mark.parametrize(
    "symbol",
    [
        "AutoRedemptionService",
        "RedemptionStatus",
        "RedemptionAttempt",
        "RedemptionCallback",
        "MarketResolvedEvent",
    ],
)
def test_removed_symbols_are_not_in_services_all(symbol: str) -> None:
    """None of the auto_redemption symbols may live in `services.__all__`."""
    from almanak.framework import services as services_pkg

    all_names: list[str] = list(getattr(services_pkg, "__all__", []))
    assert symbol not in all_names, (
        f"`{symbol}` is in almanak.framework.services.__all__. "
        "It belonged to the removed AutoRedemptionService and must not "
        "come back without re-architecting redemption to go through the "
        "gateway-routed signer (PredictionRedeemIntent already does this)."
    )
