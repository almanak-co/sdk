"""Strategy identity model for the accounting layer — blueprint 29.

There is **one canonical identifier**, ``deployment_id``, resolved exactly
once at runner boot and immutable for the life of the process:

- ``deployment_id``: stable primary key that survives restarts and
  redeploys of the same deployment. Every deployment-scoped table keys on
  it. In hosted mode it is the platform deployment id (injected as
  ``ALMANAK_DEPLOYMENT_ID``); in local mode it is a pure function of the
  execution wallet + chain.
- ``run_id``: per-process ephemeral UUID4. Forensic event correlation
  only. Never stored as a primary key.

The strategy class name is **deliberately excluded** from ``deployment_id``
and from its hash input (blueprint 29 §1): renaming a class must not fork a
deployment's open positions or accounting history.
"""

import hashlib
import uuid

from almanak.framework.deployment import FatalBootError, deployment_id, is_hosted


def resolve_deployment_id(
    *,
    wallet_address: str = "",
    chain: str = "",
) -> str:
    """Resolve the canonical ``deployment_id`` for this process — once, at boot.

    Two modes, one identity (blueprint 29 §2):

    * **Hosted** (``ALMANAK_IS_HOSTED`` truthy): ``deployment_id`` is the
      platform deployment id, taken verbatim from ``ALMANAK_DEPLOYMENT_ID``.
      A blank id raises :class:`FatalBootError` — a hosted pod with no id
      cannot stamp deployment-scoped rows. The SDK never *computes* a hosted
      identity; the hosted id must be the platform deployment identifier so
      platform-side joins hold.
    * **Local**: ``deployment_id`` is
      ``deployment:{sha256(wallet:chain)[:12]}`` — a pure function of the
      execution wallet and chain. It is stable across restarts and machines,
      forks correctly when wallet or chain changes, and is rename-safe
      (the class name is not an input). If wallet + chain cannot be
      resolved, this raises :class:`FatalBootError` rather than fall back to
      a non-canonical identity — there is no ``--id`` flag and no bare-name
      fallback.

    Args:
        wallet_address: Execution wallet address (EOA or Safe). Local mode
            only; ignored in hosted mode.
        chain: Primary chain (e.g. ``"arbitrum"``), or a comma-joined sorted
            multi-chain signature. Local mode only; ignored in hosted mode.

    Returns:
        The canonical ``deployment_id`` string.

    Raises:
        FatalBootError: hosted with a blank ``ALMANAK_DEPLOYMENT_ID``, or
            local with no resolvable wallet + chain.
    """
    if is_hosted():
        # deployment_id() raises FatalBootError on a blank hosted id.
        hosted_id = deployment_id()
        assert hosted_id is not None  # is_hosted() guarantees a non-None return
        return hosted_id

    wallet = (wallet_address or "").strip().lower()
    resolved_chain = (chain or "").strip().lower()
    if not (wallet and resolved_chain):
        raise FatalBootError(
            "cannot resolve deployment_id: local mode requires a resolved "
            f"execution wallet and chain (wallet={wallet_address!r}, "
            f"chain={chain!r}). The runner refuses to start rather than "
            "fall back to a non-canonical identity."
        )
    key = f"{wallet}:{resolved_chain}"
    short_hash = hashlib.sha256(key.encode()).hexdigest()[:12]
    return f"deployment:{short_hash}"


def generate_run_id() -> str:
    """Generate a fresh per-process run_id (UUID4 hex, 12 chars)."""
    return uuid.uuid4().hex[:12]
