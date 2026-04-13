"""Strategy identity model for the accounting layer.

Three-tier identity:
    - ``strategy_name``: Human/code reference (e.g. "AaveYieldStrategy").
      Immutable, lives in the code.
    - ``deployment_id``: Stable primary key that survives restarts.
      Deterministic hash of (wallet, chain, strategy_name), or user-supplied
      via ``--id``.  All database tables key on this.
    - ``run_id``: Per-process ephemeral UUID4.  Used for forensic event
      correlation only.  Never stored as a primary key.
"""

import hashlib
import logging
import uuid

logger = logging.getLogger(__name__)

# Module-level flag: emit the bare-name deprecation warning at most once per process.
_BARE_NAME_WARNING_EMITTED = False


def resolve_deployment_id(
    *,
    strategy_name: str,
    wallet_address: str = "",
    chain: str = "",
    cli_id: str | None = None,
) -> str:
    """Resolve a stable deployment_id for this strategy instance.

    Precedence:
        1. ``cli_id`` (user-supplied ``--id``) wins unconditionally.
        2. If wallet + chain are available, deterministic
           ``{name}:{hash(wallet+chain+name)[:12]}``.
        3. Fallback: bare ``strategy_name`` with a deprecation warning.
           This keeps backward compatibility for local ``--once`` dev runs
           without a wallet, but will be removed in a future release.

    Args:
        strategy_name: The human-readable strategy name (e.g. class name).
        wallet_address: Execution wallet address (EOA or Safe).
        chain: Primary chain (e.g. "arbitrum").
        cli_id: User-supplied ``--id`` override.

    Returns:
        A stable deployment_id string.
    """
    global _BARE_NAME_WARNING_EMITTED  # noqa: PLW0603

    if cli_id:
        # User override — use exactly as given.
        # If it already has the name: prefix, keep it; otherwise prefix.
        if ":" in cli_id:
            return cli_id
        return f"{strategy_name}:{cli_id}"

    if wallet_address and chain:
        # Deterministic hash from the deployment triple.
        hash_input = f"{wallet_address.lower()}:{chain.lower()}:{strategy_name}"
        short_hash = hashlib.sha256(hash_input.encode()).hexdigest()[:12]
        return f"{strategy_name}:{short_hash}"

    # Fallback: bare name (local dev, no wallet yet).
    # Deprecated — will be removed in a future release.
    if not _BARE_NAME_WARNING_EMITTED:
        logger.warning(
            "deployment_id falling back to bare strategy name '%s' because wallet_address "
            "and chain are not available. This produces a non-canonical identity that cannot "
            "be used for cross-session accounting. Supply --id or ensure wallet+chain are "
            "configured. This fallback will be removed in a future release.",
            strategy_name,
        )
        _BARE_NAME_WARNING_EMITTED = True
    return strategy_name


def validate_deployment_id(deployment_id: str) -> bool:
    """Check whether a deployment_id is canonical (hash-based or user-supplied).

    A canonical deployment_id contains a ``":"`` separator, meaning it was
    produced by the deterministic hash path or by a user-supplied ``--id``.
    Bare strategy names (the deprecated fallback) do not contain ``":"``.

    Returns:
        True if the deployment_id is canonical, False otherwise.
    """
    return ":" in deployment_id


def generate_run_id() -> str:
    """Generate a fresh per-process run_id (UUID4 hex, 12 chars)."""
    return uuid.uuid4().hex[:12]
