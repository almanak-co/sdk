"""Click decorators reused by every CLI surface.

Phase 2 (#2099): single source of truth for ``--gateway-host`` and
``--gateway-port`` Click options. Replaces 6 copy-pasted decorator
blocks across CLI surfaces. Canonical env-var names are
``ALMANAK_GATEWAY_HOST`` / ``ALMANAK_GATEWAY_PORT`` (matching the
``GatewaySettings`` env-prefix). Legacy unprefixed ``GATEWAY_HOST`` /
``GATEWAY_PORT`` continue to work for one release; a
``UserWarning`` is emitted at the Click main group when the
legacy names are set without their canonical equivalents.

The warning category is :class:`UserWarning` (not
:class:`DeprecationWarning`) so operators see it on stdout/stderr
without ``-W default`` — Python's default warning filter silences
``DeprecationWarning`` for non-``__main__`` modules, and the SDK is
imported as ``almanak.cli.cli``. ``UserWarning`` is the operator-facing
deprecation channel for that reason.
"""

from __future__ import annotations

import os
import warnings
from collections.abc import Callable
from typing import Any

import click

# Each pair: (legacy unprefixed name, canonical ALMANAK_GATEWAY_* name).
# AUTH_TOKEN / TIMEOUT are not yet exposed as Click options (Phase 4
# territory) but the deprecation warning fires for them too — operators
# moving their .env to canonical names get one consistent migration.
_LEGACY_TO_CANONICAL: tuple[tuple[str, str], ...] = (
    ("GATEWAY_HOST", "ALMANAK_GATEWAY_HOST"),
    ("GATEWAY_PORT", "ALMANAK_GATEWAY_PORT"),
    ("GATEWAY_AUTH_TOKEN", "ALMANAK_GATEWAY_AUTH_TOKEN"),
    ("GATEWAY_TIMEOUT", "ALMANAK_GATEWAY_TIMEOUT"),
)


def warn_legacy_gateway_envvars() -> None:
    """Emit ``UserWarning`` for legacy unprefixed gateway env vars.

    Called once at the Click main group. The warning fires only when the
    legacy name is set AND the canonical name is not — i.e. when the
    legacy name would actually be consulted. This keeps the warning
    silent for operators who have already migrated.

    Uses :class:`UserWarning` so the message reaches operators under
    Python's default warning filter (``DeprecationWarning`` is silenced
    for non-``__main__`` modules, which would render this notice
    invisible until removal).
    """
    for legacy, canonical in _LEGACY_TO_CANONICAL:
        if os.environ.get(legacy) and not os.environ.get(canonical):
            warnings.warn(
                f"{legacy} is deprecated. Use {canonical} instead. "
                "Legacy unprefixed gateway env vars will be removed in a "
                "future release.",
                UserWarning,
                stacklevel=2,
            )


def gateway_client_options(func: Callable[..., Any]) -> Callable[..., Any]:
    """Click decorator: ``--gateway-host`` and ``--gateway-port``.

    Replaces the 6 copy-pasted decorator blocks across CLI surfaces.
    Canonical envvars are ``ALMANAK_GATEWAY_HOST`` / ``ALMANAK_GATEWAY_PORT``;
    legacy ``GATEWAY_HOST`` / ``GATEWAY_PORT`` still work but trigger a
    ``UserWarning`` via :func:`warn_legacy_gateway_envvars` at
    process start.
    """
    func = click.option(
        "--gateway-port",
        type=int,
        default=50051,
        envvar=["ALMANAK_GATEWAY_PORT", "GATEWAY_PORT"],
        show_envvar=True,
        show_default=True,
        help="Gateway gRPC port.",
    )(func)
    func = click.option(
        "--gateway-host",
        type=str,
        # Match ``GatewaySettings.host`` (127.0.0.1) rather than ``localhost``.
        # On IPv6-first hosts ``localhost`` can resolve to ``::1`` while a
        # gateway listening on the default IPv4 loopback would be missed.
        default="127.0.0.1",
        envvar=["ALMANAK_GATEWAY_HOST", "GATEWAY_HOST"],
        show_envvar=True,
        show_default=True,
        help="Gateway gRPC host.",
    )(func)
    return func


__all__ = ["gateway_client_options", "warn_legacy_gateway_envvars"]
