"""Unit tests for the strategy-side flash-loan provider registry (VIB-4837).

Proves the registry the intent compiler now dispatches through:

* the global ``FLASH_LOAN_PROVIDER_REGISTRY`` is populated with the three
  built-in providers in a byte-stable order (the selector candidate order and
  the compiler's ``"Supported providers: …"`` error string depend on it), and
* ``register`` / ``names`` / ``has`` / ``providers`` / ``build`` behave as the
  compiler relies on, including the unknown-provider ``KeyError`` guard.
"""

from __future__ import annotations

import pytest

from almanak.connectors._strategy_base.flash_loan_base import FlashLoanProvider
from almanak.connectors._strategy_base.flash_loan_registry import (
    FlashLoanProviderRegistration,
    FlashLoanProviderRegistry,
)

# Import the *singleton* from the boot file (not the bare definition site): the
# boot module runs ``_register_all()`` at import, so this guarantees the global
# registry is populated regardless of test/worker import order — mirroring how
# the compiler reaches it in production.
from almanak.connectors._strategy_flash_loan_registry import FLASH_LOAN_PROVIDER_REGISTRY


def test_builtin_providers_registered_in_stable_order() -> None:
    # Order is load-bearing: it fixes the selector candidate order and the
    # compiler's unknown-provider error string. Keep it pinned.
    assert FLASH_LOAN_PROVIDER_REGISTRY.names() == ("aave", "balancer", "morpho")


def test_providers_are_fresh_flashloanprovider_instances() -> None:
    providers = FLASH_LOAN_PROVIDER_REGISTRY.providers()
    assert [p.name for p in providers] == ["aave", "balancer", "morpho"]
    assert all(isinstance(p, FlashLoanProvider) for p in providers)
    # A fresh instance per call — selections must not share mutable candidates.
    again = FLASH_LOAN_PROVIDER_REGISTRY.providers()
    assert all(a is not b for a, b in zip(providers, again, strict=True))


def test_has_matches_names() -> None:
    for name in FLASH_LOAN_PROVIDER_REGISTRY.names():
        assert FLASH_LOAN_PROVIDER_REGISTRY.has(name)
    assert not FLASH_LOAN_PROVIDER_REGISTRY.has("not-a-provider")


class _FakeProvider(FlashLoanProvider):
    @property
    def name(self) -> str:
        return "fake"

    def supports(self, chain, token):  # noqa: ANN001, ANN201 - test stub
        return True

    def quote(self, chain, token, amount):  # noqa: ANN001, ANN201 - test stub
        raise NotImplementedError


def test_build_dispatches_to_registered_callable() -> None:
    captured: dict[str, object] = {}

    def _fake_build(compiler, **kwargs):  # noqa: ANN001, ANN003, ANN202 - test stub
        captured["compiler"] = compiler
        captured["kwargs"] = kwargs
        return {"transaction": "ok", "pool_address": "0xpool"}

    registry = FlashLoanProviderRegistry()
    registry.register(FlashLoanProviderRegistration(name="fake", make_provider=_FakeProvider, build=_fake_build))

    result = registry.build(
        "fake",
        "compiler-sentinel",
        token_info="tok",
        amount_wei=123,
        callback_params=b"",
        callback_gas_total=0,
    )

    assert result == {"transaction": "ok", "pool_address": "0xpool"}
    assert captured["compiler"] == "compiler-sentinel"
    assert captured["kwargs"] == {
        "token_info": "tok",
        "amount_wei": 123,
        "callback_params": b"",
        "callback_gas_total": 0,
    }


def test_build_unknown_provider_raises_keyerror() -> None:
    # The compiler validates membership via names()/has() before calling build,
    # so an unregistered name is a programming error — fail loud, never silently
    # build a different provider.
    registry = FlashLoanProviderRegistry()
    with pytest.raises(KeyError):
        registry.build("nope", object())


def test_register_preserves_insertion_order_and_replaces() -> None:
    registry = FlashLoanProviderRegistry()
    registry.register(FlashLoanProviderRegistration(name="a", make_provider=_FakeProvider, build=lambda c, **k: {}))
    registry.register(FlashLoanProviderRegistration(name="b", make_provider=_FakeProvider, build=lambda c, **k: {}))
    assert registry.names() == ("a", "b")
    # Re-registering an existing name replaces in place, order unchanged.
    registry.register(FlashLoanProviderRegistration(name="a", make_provider=_FakeProvider, build=lambda c, **k: {}))
    assert registry.names() == ("a", "b")
