"""Long-lived ManagedGateway entrypoint for sidecar deployments.

Wraps :class:`almanak.gateway.managed.ManagedGateway` as a foreground CLI
process: starts the gateway gRPC server (and its Anvil forks, if configured),
then blocks on SIGTERM/SIGINT and shuts down cleanly.

Designed for use as a multi-container Cloud Run / Kubernetes sidecar where
the test path connects via ``almanak strat test --no-gateway`` to a gateway
that already has its forks warm and its RPC credentials in env. The strategy
container never sees the upstream Alchemy URL — the gateway holds it in this
process's memory.

Differs from ``python -m almanak.gateway.server`` (the production sibling
entrypoint) in one substantive way: this entrypoint owns the Anvil-fork
lifecycle via ManagedGateway, whereas ``server`` expects forks (or live RPCs)
to already be reachable on configured ports. Same gRPC interface in both.

Env vars consumed: any ``ALMANAK_GATEWAY_*`` that :class:`GatewaySettings`
recognises (host, port, chains, network, auth_token, etc.).

Usage::

    python -m almanak.gateway.managed_serve
"""

from __future__ import annotations

import logging
import signal
import sys
import threading

from almanak.core.redaction import install_redaction
from almanak.framework.utils.deployment_banner import emit_gateway_banner
from almanak.gateway.audit import configure_structlog
from almanak.gateway.managed import ManagedGateway

logger = logging.getLogger(__name__)


# Headroom over the per-fork budget so a slow upstream RPC doesn't cause
# Cloud Run to mark the container as crash-looping during initial fork boot.
# Once forks are warm, runtime calls are sub-second.
_SHUTDOWN_TIMEOUT_SECONDS = 30.0


def main() -> int:
    """Entrypoint — run ManagedGateway as a long-lived sidecar process.

    Returns:
        Process exit code: 0 on clean shutdown, 1 on startup or runtime failure.
    """
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    # Centralised secret redaction — matches almanak.gateway.server entrypoint
    # so any privileged URL/token that slips into a log line gets scrubbed
    # before it reaches Cloud Run Logging.
    install_redaction()
    configure_structlog()

    # Fire the deployment-start banner before any other gateway-boot log so
    # users can clearly see where this deployment's logs begin (vs the
    # previous deployment's logs in the same Cloud Logging stream).
    # Banner emission is observability — most failures (e.g. a formatting
    # bug in identity helpers) must not stop the gateway from booting. But
    # ``deployment_id()`` raises ``FatalBootError`` when hosted mode is
    # set with a blank id; that is the hosted-misconfig boot guard and must
    # propagate so the pod refuses to start rather than writing under an
    # empty identity. ``FatalBootError`` is imported lazily to keep
    # ``almanak.framework.deployment`` out of the gateway's module-load
    # closure (enforced by tests/gateway/test_imports_lean.py).
    try:
        emit_gateway_banner(logger)
    except Exception as exc:
        from almanak.framework.deployment.mode import FatalBootError

        if isinstance(exc, FatalBootError):
            raise
        logger.warning(f"Failed to emit deployment-start banner: {exc}")

    # Same dotenv + config-resolution path as ``python -m almanak.gateway.server``.
    # load_config() picks up ALMANAK_GATEWAY_* env vars via the typed pydantic
    # GatewaySettings + any unprefixed legacy fallbacks the config service applies.
    from almanak.config.service import load_config

    config = load_config()
    settings = config.gateway

    # Resolve anvil_chains + anvil_funding the same way ``strat run --network
    # anvil`` does — from the strategy's ``config.json``. This is the existing
    # SDK helper at ``framework/cli/run_helpers.py:_resolve_anvil_chains_and_funding``;
    # we call it so managed_serve stays in lockstep with the CLI's behaviour
    # (chain resolution, default-from-decorator fallback, malformed-config
    # tolerance) without forking a parallel implementation.
    #
    # ``strategy_folder_env`` is the canonical boundary helper for
    # ``ALMANAK_STRATEGY_FOLDER`` — direct ``os.environ.get`` is gated by the
    # check_config_boundary script.
    from almanak.framework.cli.run_helpers import _resolve_anvil_chains_and_funding
    from almanak.framework.local_paths import strategy_folder_env

    strategy_folder = (strategy_folder_env() or "").strip() or "."
    anvil_chains, anvil_funding = _resolve_anvil_chains_and_funding(
        working_dir=strategy_folder,
        config_file=None,
        early_strategy_class=None,
        external_anvil_ports={},
    )
    # In mainnet mode we don't run forks regardless of what config says.
    if settings.network != "anvil":
        anvil_chains = []
        anvil_funding = {}

    logger.info(
        "Starting managed gateway sidecar: grpc_port=%d network=%s anvil_chains=%s anvil_funding_keys=%s",
        settings.grpc_port,
        settings.network,
        anvil_chains or "[]",
        sorted(anvil_funding.keys()) or "[]",
    )

    # Compute fork-startup timeout from the same helper the CLI uses so the
    # budget matches what ``strat run`` would have applied for the same chain
    # set. For mainnet (empty anvil_chains) the helper returns ~10s.
    from almanak.framework.cli._anvil_timeout import compute_anvil_startup_timeout

    startup_timeout = compute_anvil_startup_timeout(anvil_chains)

    mg = ManagedGateway(
        settings=settings,
        anvil_chains=anvil_chains,
        anvil_funding=anvil_funding,
    )

    # SIGTERM is the signal Cloud Run / Kubernetes sends on scale-down or
    # revision replacement; SIGINT covers local debugging via Ctrl-C. Both
    # set the same event so the shutdown path is identical.
    stop_event = threading.Event()

    def _handle_signal(signo: int, frame: object | None = None) -> None:
        del frame  # signal.signal contract, unused here
        logger.info("Received signal %d, initiating shutdown", signo)
        stop_event.set()

    signal.signal(signal.SIGTERM, _handle_signal)
    signal.signal(signal.SIGINT, _handle_signal)

    try:
        mg.start(timeout=startup_timeout)
    except Exception:
        logger.exception("Managed gateway sidecar failed to start")
        # ManagedGateway.start() already cleans up its own partial state on
        # failure (see _cleanup_on_failure) — no extra stop() call needed.
        return 1

    logger.info("Managed gateway sidecar ready on %s:%d", mg.host, mg.port)

    try:
        # Block here until SIGTERM. threading.Event.wait() releases the GIL,
        # so the daemon thread running the gRPC server gets its normal share
        # of CPU.
        stop_event.wait()
    except KeyboardInterrupt:
        # Defensive — signal handler already covers Ctrl-C, but if the wait()
        # call itself raises (rare on cpython, possible on alt runtimes), still
        # fall through to the orderly stop below.
        pass

    logger.info("Stopping managed gateway sidecar")
    try:
        mg.stop(timeout=_SHUTDOWN_TIMEOUT_SECONDS)
    except Exception:
        logger.exception("Error during managed gateway shutdown — exiting anyway")
        return 1

    return 0


if __name__ == "__main__":
    sys.exit(main())
