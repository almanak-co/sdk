"""Signature-conformance gate for ``TeardownStateManagerProtocol`` and
``TeardownStateAdapterProtocol`` (VIB-4338).

Background — why this exists:

The Protocols are declared ``@runtime_checkable`` (see
``almanak/framework/teardown/state_manager.py``), but
``isinstance(x, Protocol)`` only checks method *existence*, not signature.
Three concrete implementers exist for ``TeardownStateManagerProtocol``
(``SQLiteTeardownStateManager`` here, ``PostgresTeardownStateManager`` in
``platform-plugins/almanak_platform``, ``GatewayTeardownStateManager`` in
``almanak/framework/teardown/gateway_client.py``) and the gateway server
threads kwargs through to whichever the deployment loaded.

When VIB-4542 added ``positions_closed`` / ``positions_failed`` keyword-only
parameters to the Protocol + SQLite + Gateway-client implementations of
``mark_failed``, the Postgres implementation was missed. The gateway server's
``MarkTeardownFailed`` handler passes both kwargs unconditionally (even as
``None``), so every hosted ``mark_failed`` call raised ``TypeError``, was
caught by ``runner_teardown.py::_safe_mark`` as non-fatal, and silently left
teardown_requests rows stuck at ``status='executing'``. Every subsequent
redeploy resurrected the stuck teardown — the VIB-4338 user pain.

The fix patches the one method. This test guards the *class* of bug: any
future param drift across the three implementers will trip this gate, before
it gets a chance to silently break hosted recovery.

Contract: implementers may be *more permissive* than the Protocol (extra
optional kwargs are fine) but never *less* (every Protocol-required parameter
must exist on every implementer with a compatible kind and default).
"""

from __future__ import annotations

import inspect
from inspect import Parameter, Signature
from typing import Protocol

import pytest

from almanak.framework.teardown import (
    SQLiteTeardownStateAdapter,
    SQLiteTeardownStateManager,
    TeardownStateAdapterProtocol,
    TeardownStateManagerProtocol,
)
from almanak.framework.teardown.gateway_client import GatewayTeardownStateManager

# Postgres implementations live in the optional platform plugin. Skip the
# Postgres parameterizations cleanly when running on a checkout without the
# plugin available (e.g. the public-mirror sync target).
try:
    from almanak_platform.teardown_store import (
        PostgresTeardownStateAdapter,
        PostgresTeardownStateManager,
    )

    _POSTGRES_AVAILABLE = True
except ImportError:  # pragma: no cover - exercised only in mirror builds
    PostgresTeardownStateManager = None  # type: ignore[assignment,misc]
    PostgresTeardownStateAdapter = None  # type: ignore[assignment,misc]
    _POSTGRES_AVAILABLE = False


_KEYWORD_KINDS = {Parameter.KEYWORD_ONLY, Parameter.POSITIONAL_OR_KEYWORD}


def _public_method_names(proto: type[Protocol]) -> list[str]:
    """Return Protocol method names that implementers must satisfy."""
    return [
        name
        for name, value in vars(proto).items()
        if callable(value) and not name.startswith("_")
    ]


def _signature_of(cls: type, method_name: str) -> Signature:
    """``inspect.signature`` minus the bound ``self`` parameter for clarity."""
    sig = inspect.signature(getattr(cls, method_name))
    params = [p for name, p in sig.parameters.items() if name != "self"]
    return sig.replace(parameters=params)


def _params_compatible(impl_sig: Signature, proto_sig: Signature) -> tuple[bool, str]:
    """Check that ``impl_sig`` satisfies every Protocol-declared parameter.

    Implementations are permitted to add extra parameters as long as those
    extras are either keyword-only with defaults or positional-with-defaults
    (i.e. would not break a Protocol-shape caller). Required Protocol params
    must exist on the implementer with the same name AND a compatible kind
    (positional ↔ keyword-or-positional, keyword-only stays keyword-only),
    AND if the Protocol gave a default the implementer must too (so the
    Protocol caller can omit the arg).

    Return (True, '') on success, (False, reason) on failure.
    """
    impl_params = impl_sig.parameters
    for name, proto_p in proto_sig.parameters.items():
        if name not in impl_params:
            return False, f"missing parameter '{name}' (Protocol: {proto_p})"
        impl_p = impl_params[name]

        # Keyword-only must stay keyword-only; otherwise the gateway
        # handler's explicit kwargs-passing breaks.
        if proto_p.kind == Parameter.KEYWORD_ONLY and impl_p.kind != Parameter.KEYWORD_ONLY:
            return (
                False,
                f"parameter '{name}' must be keyword-only (Protocol: KEYWORD_ONLY, impl: {impl_p.kind.name})",
            )

        # Reverse direction: if the Protocol allows positional passing, the
        # implementation must too. A KEYWORD_ONLY impl param cannot satisfy a
        # POSITIONAL_ONLY or POSITIONAL_OR_KEYWORD Protocol param — a caller
        # using the Protocol shape positionally would get TypeError at runtime.
        # Concretely guards ``deployment_id`` (POSITIONAL_OR_KEYWORD in the
        # Protocol, passed positionally by ``runner_teardown._safe_mark`` and
        # the gateway server's ``_strategy_mutation``).
        if (
            proto_p.kind in (Parameter.POSITIONAL_ONLY, Parameter.POSITIONAL_OR_KEYWORD)
            and impl_p.kind == Parameter.KEYWORD_ONLY
        ):
            return (
                False,
                f"parameter '{name}' must support positional passing "
                f"(Protocol: {proto_p.kind.name}, impl: KEYWORD_ONLY)",
            )

        # If Protocol provides a default, impl must too so a caller relying
        # on Protocol shape can omit the arg.
        if proto_p.default is not Parameter.empty and impl_p.default is Parameter.empty:
            return (
                False,
                f"parameter '{name}' lost its default (Protocol default: {proto_p.default!r})",
            )

        # Either side may relax the kind from KEYWORD_ONLY → POSITIONAL_OR_KEYWORD
        # (more permissive), but not the other direction. Already covered above.

    # Verify any extra impl params are safely callable without a value:
    # they must either be VAR_POSITIONAL / VAR_KEYWORD or have a default.
    for name, impl_p in impl_params.items():
        if name in proto_sig.parameters:
            continue
        if impl_p.kind in (Parameter.VAR_POSITIONAL, Parameter.VAR_KEYWORD):
            continue
        if impl_p.default is Parameter.empty:
            return (
                False,
                f"impl adds required parameter '{name}' not in Protocol; would reject Protocol-shape callers",
            )
    return True, ""


# ---------------------------------------------------------------------------
# Manager Protocol — gates the bug-#1 class
# ---------------------------------------------------------------------------

_MANAGER_IMPLEMENTERS = [
    pytest.param(SQLiteTeardownStateManager, id="sqlite"),
    pytest.param(
        PostgresTeardownStateManager,
        id="postgres",
        marks=pytest.mark.skipif(not _POSTGRES_AVAILABLE, reason="almanak_platform plugin not installed"),
    ),
    pytest.param(GatewayTeardownStateManager, id="gateway-client"),
]


@pytest.mark.parametrize("impl_cls", _MANAGER_IMPLEMENTERS)
@pytest.mark.parametrize("method_name", _public_method_names(TeardownStateManagerProtocol))
def test_manager_implements_protocol_signature(impl_cls: type, method_name: str) -> None:
    proto_sig = _signature_of(TeardownStateManagerProtocol, method_name)
    impl_sig = _signature_of(impl_cls, method_name)
    ok, reason = _params_compatible(impl_sig, proto_sig)
    assert ok, (
        f"{impl_cls.__name__}.{method_name} signature drift from "
        f"TeardownStateManagerProtocol.{method_name}: {reason}\n"
        f"  Protocol: {proto_sig}\n"
        f"  Impl:     {impl_sig}"
    )


# ---------------------------------------------------------------------------
# Adapter Protocol — same class of risk, three implementers
# ---------------------------------------------------------------------------

_ADAPTER_IMPLEMENTERS = [
    pytest.param(SQLiteTeardownStateAdapter, id="sqlite"),
    pytest.param(
        PostgresTeardownStateAdapter,
        id="postgres",
        marks=pytest.mark.skipif(not _POSTGRES_AVAILABLE, reason="almanak_platform plugin not installed"),
    ),
]


@pytest.mark.parametrize("impl_cls", _ADAPTER_IMPLEMENTERS)
@pytest.mark.parametrize("method_name", _public_method_names(TeardownStateAdapterProtocol))
def test_adapter_implements_protocol_signature(impl_cls: type, method_name: str) -> None:
    if not hasattr(impl_cls, method_name):
        pytest.fail(
            f"{impl_cls.__name__} is missing required Protocol method '{method_name}'"
        )
    proto_sig = _signature_of(TeardownStateAdapterProtocol, method_name)
    impl_sig = _signature_of(impl_cls, method_name)
    ok, reason = _params_compatible(impl_sig, proto_sig)
    assert ok, (
        f"{impl_cls.__name__}.{method_name} signature drift from "
        f"TeardownStateAdapterProtocol.{method_name}: {reason}\n"
        f"  Protocol: {proto_sig}\n"
        f"  Impl:     {impl_sig}"
    )


# ---------------------------------------------------------------------------
# Regression pin: the exact bug we're fixing.
#
# Asserts ``positions_closed`` and ``positions_failed`` exist as keyword-only
# parameters with default ``None`` on every implementer. Survives a hypothetical
# rename of the parameters in the Protocol with the parametrized tests above,
# but pins the *current* contract that the gateway's ``MarkTeardownFailed``
# server handler depends on.
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("impl_cls", _MANAGER_IMPLEMENTERS)
def test_mark_failed_accepts_positions_kwargs(impl_cls: type) -> None:
    sig = _signature_of(impl_cls, "mark_failed")
    for kw in ("positions_closed", "positions_failed"):
        assert kw in sig.parameters, f"{impl_cls.__name__}.mark_failed must accept '{kw}' (VIB-4542)"
        param = sig.parameters[kw]
        assert param.kind == Parameter.KEYWORD_ONLY, f"{impl_cls.__name__}.mark_failed.{kw} must be keyword-only"
        assert param.default is None, f"{impl_cls.__name__}.mark_failed.{kw} default must be None"
