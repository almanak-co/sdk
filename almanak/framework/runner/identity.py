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
import uuid


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
        3. Fallback: bare ``strategy_name`` (backward-compatible with
           ``--once`` behavior for local dev without a wallet).

    Args:
        strategy_name: The human-readable strategy name (e.g. class name).
        wallet_address: Execution wallet address (EOA or Safe).
        chain: Primary chain (e.g. "arbitrum").
        cli_id: User-supplied ``--id`` override.

    Returns:
        A stable deployment_id string.
    """
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
    return strategy_name


def generate_run_id() -> str:
    """Generate a fresh per-process run_id (UUID4 hex, 12 chars)."""
    return uuid.uuid4().hex[:12]
