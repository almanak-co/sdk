"""Solana fork-startup helpers shared by ``strat run`` and ``strat teardown``.

Extracted from ``cli/run.py`` so ``cli/teardown.py`` does not have to import
``run.py`` (which pulls in the full Click command tree, runners, indicator
providers — see VIB-522). Both call sites now share this lightweight module.
"""

from __future__ import annotations

import logging

import click

from almanak.config import connectors_config_from_env

logger = logging.getLogger(__name__)


def get_orca_pool_accounts(strategy_config: dict) -> list[str]:
    """Fetch Orca Whirlpool accounts to pre-clone for local fork testing.

    Returns the pool's vault accounts and all tick array PDAs covering the
    expected LP range (plus one buffer array on each side).

    Returns an empty list on any error so fork startup is never blocked.

    Note: Direct HTTP call to Orca API is acceptable here because this runs
    during CLI fork setup, before the gateway is started.
    """
    if strategy_config.get("protocol") != "orca_whirlpools":
        return []
    pool_address = strategy_config.get("pool_address")
    if not pool_address:
        return []
    # Guard numeric parsing so a malformed ``tick_spacing`` / ``range_pct`` in
    # config doesn't abort fork startup before the helper's "return [] on any
    # error" contract can fire. CodeRabbit P_major.
    try:
        tick_spacing = int(strategy_config.get("tick_spacing", 64))
        range_pct = float(strategy_config.get("range_pct", 20))
    except (TypeError, ValueError) as exc:
        click.echo(f"  Warning: invalid Orca pre-clone config: {exc}", err=True)
        return []

    accounts: list[str] = []
    try:
        import math

        import requests as _req
        from solders.pubkey import Pubkey
    except ImportError:
        return []

    try:
        orca_api = connectors_config_from_env().orca_api_base_url
        resp = _req.get(f"{orca_api}/pools/{pool_address}", timeout=10)
        if resp.status_code != 200:
            return []
        raw = resp.json()
        data = raw.get("data", raw) if isinstance(raw, dict) else raw
        tick_current = int(data.get("tickCurrentIndex", 0))
        tick_spacing = int(data.get("tickSpacing", tick_spacing))

        vault_a = data.get("tokenVaultA")
        vault_b = data.get("tokenVaultB")
        if vault_a:
            accounts.append(vault_a)
        if vault_b:
            accounts.append(vault_b)
    except Exception as _e:
        click.echo(f"  Warning: could not fetch Orca pool state for pre-clone: {_e}")
        return []

    try:
        tick_delta = int(math.log(1 + range_pct / 100) / math.log(1.0001))
        tick_lower = tick_current - tick_delta
        tick_upper = tick_current + tick_delta
        array_size = 88 * tick_spacing

        def _start(tick: int) -> int:
            if tick >= 0:
                return (tick // array_size) * array_size
            return -(((-tick - 1) // array_size + 1) * array_size)

        starts: set[int] = set()
        for t in [tick_lower, tick_current, tick_upper]:
            s = _start(t)
            starts.update([s - array_size, s, s + array_size])

        program_id = Pubkey.from_string("whirLbMiicVdio4qvUfM5KAg6Ct8VwpYzGff3uctyCc")
        pool_pk = Pubkey.from_string(pool_address)
        for s in sorted(starts):
            pda, _ = Pubkey.find_program_address(
                [b"tick_array", bytes(pool_pk), str(s).encode()],
                program_id,
            )
            accounts.append(str(pda))
    except Exception as _e:
        click.echo(f"  Warning: could not derive Orca tick array PDAs: {_e}")

    return accounts
