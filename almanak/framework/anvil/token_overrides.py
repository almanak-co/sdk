"""Local token override registry (VIB-2378).

Loads custom token metadata from ``~/.config/almanak/paper_trading_tokens.json``
for use during Anvil fork wallet funding. This avoids contaminating the
production TokenResolver with test-only addresses.

This module lives in ``framework.anvil`` (next to its only consumer,
``fork_manager.fund_tokens``) and depends on the stdlib only. It was moved out
of ``framework.backtesting.paper`` so the anvil funding path no longer imports
the ``backtesting`` package — that package eagerly pulls in ``report_generator``
(which hard-requires ``jinja2``) and the heavy paper-trading engine, neither of
which is needed to fund a fork. See ``framework.backtesting.paper.token_overrides``
for a backward-compatible re-export shim.

File format::

    {
      "ethereum": {
        "swETH": "0xf951E335afb289353dc249e82926178EaC7DEd78",
        "ankrETH": {"address": "0xE95A203B1a91a908F9B9CE46459d101078c2c3cb", "decimals": 18}
      },
      "arbitrum": {
        "CUSTOM": "0x1234..."
      }
    }

Values can be either a plain address string or a dict with ``address`` and
optional ``decimals`` keys.
"""

import json
import logging
from dataclasses import dataclass
from pathlib import Path

logger = logging.getLogger(__name__)

try:
    CONFIG_PATH = Path.home() / ".config" / "almanak" / "paper_trading_tokens.json"
except RuntimeError:
    # Path.home() raises if the home directory can't be resolved (minimal Docker
    # images, some serverless/CI runners). Fall back so this module — which the
    # anvil funding path imports unconditionally — never crashes at import time.
    CONFIG_PATH = Path("/tmp") / ".config" / "almanak" / "paper_trading_tokens.json"


@dataclass(frozen=True)
class TokenOverride:
    """A paper-local token override entry."""

    address: str
    decimals: int | None = None


def load_token_overrides(
    chain: str,
    config_path: Path | None = None,
) -> dict[str, TokenOverride]:
    """Load paper-local token overrides for a specific chain.

    Args:
        chain: Chain name (e.g., "ethereum", "arbitrum").
        config_path: Override path for testing (default: ~/.config/almanak/paper_trading_tokens.json).

    Returns:
        Dict mapping token symbol -> TokenOverride. Empty dict if file missing or malformed.
    """
    path = config_path or CONFIG_PATH

    if not path.exists():
        return {}

    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError) as e:
        logger.warning("[paper-trading] Malformed token override file %s: %s", path, e)
        return {}

    if not isinstance(raw, dict):
        logger.warning("[paper-trading] Token override file must be a JSON object, got %s", type(raw).__name__)
        return {}

    chain_data = raw.get(chain, {})
    if not isinstance(chain_data, dict):
        logger.warning("[paper-trading] Token overrides for chain '%s' must be a JSON object", chain)
        return {}

    overrides: dict[str, TokenOverride] = {}

    for symbol, value in chain_data.items():
        if isinstance(value, str):
            # Plain address string
            overrides[symbol] = TokenOverride(address=value.lower())
        elif isinstance(value, dict):
            address = value.get("address")
            if not isinstance(address, str):
                logger.warning("[paper-trading] Token override '%s' missing valid address, skipping", symbol)
                continue
            decimals = value.get("decimals")
            # Reject non-int, bool, and out-of-range values — anything outside
            # [0, 77] (uint256 has at most 78 decimal digits) would corrupt the
            # token-unit scaling in fund_tokens().
            if decimals is not None and (
                not isinstance(decimals, int) or isinstance(decimals, bool) or not 0 <= decimals <= 77
            ):
                logger.warning(
                    "[paper-trading] Token override '%s' has invalid decimals %r, ignoring", symbol, decimals
                )
                decimals = None
            overrides[symbol] = TokenOverride(address=address.lower(), decimals=decimals)
        else:
            logger.warning(
                "[paper-trading] Token override '%s' has unexpected type %s, skipping", symbol, type(value).__name__
            )

    if overrides:
        logger.info("[paper-trading] Loaded %d token override(s) for chain '%s' from %s", len(overrides), chain, path)

    return overrides
