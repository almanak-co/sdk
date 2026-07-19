"""BENQI teardown on-chain closure verifier (VIB-5795).

TD-14 post-condition for BENQI lending positions (Avalanche). BENQI is a
Compound V2 fork — qiTokens, NOT ERC-4626 — so the ERC-4626 read shape does not
apply. One pinned ``getAccountSnapshot(wallet)`` call on the asset's qiToken
answers both legs:

  * SUPPLY — ``qiTokenBalance × exchangeRateMantissa // 1e18`` (underlying wei
    still supplied; the mantissa already folds in accrued interest).
  * BORROW — ``borrowBalance`` (underlying wei outstanding; exact at the
    pinned post-repay block).

The qiToken address is resolved from the connector's own ``BENQI_QI_TOKENS``
catalogue by (case-insensitive) asset symbol, with a WAVAX→AVAX alias: the
catalogue keys native AVAX as ``"AVAX"`` while accounting-derived surfaces may
report the wrapped symbol. An uncatalogued asset → unmeasured, never a guess.

The snapshot decode reuses ``_decode_account_snapshot`` from
``benqi.lending_read`` (package-internal import — the SAME decode the gateway
account-state read uses, incl. the non-zero-error-word → fault rule), so the
two lanes cannot drift.
"""

from __future__ import annotations

from typing import Any

from almanak.connectors._strategy_base.lending_post_condition import (
    verify_lending_closure,
)
from almanak.connectors._strategy_base.teardown_post_condition import ClosureCheckResult
from almanak.connectors._strategy_base.vault_post_condition import _is_evm_address, _read_with_retry
from almanak.connectors.benqi.adapter import BENQI_QI_TOKENS
from almanak.connectors.benqi.lending_read import (
    _GET_ACCOUNT_SNAPSHOT_SELECTOR,
    _decode_account_snapshot,
)

# The qiToken catalogue is Avalanche-only; any other chain is uncatalogued by
# construction (→ unmeasured).
_BENQI_CHAIN = "avalanche"


def _resolve_qi_token(asset: str) -> str | None:
    """Case-insensitive ``BENQI_QI_TOKENS`` lookup with the WAVAX→AVAX alias."""
    wanted = asset.strip()
    if wanted.upper() == "WAVAX":
        wanted = "AVAX"
    entry = BENQI_QI_TOKENS.get(wanted)
    if entry is None:
        for key, candidate in BENQI_QI_TOKENS.items():
            if key.lower() == wanted.lower():
                entry = candidate
                break
    if entry is None:
        return None
    return str(entry.get("qi_token") or "") or None


def _account_snapshot(
    gateway_client: Any, chain: str, asset: str, wallet_address: str, block: int | str | None
) -> tuple[int, int, int] | None:
    """Pinned ``getAccountSnapshot`` → ``(qiTokenBalance, borrowBalance, exchangeRate)``."""
    if chain != _BENQI_CHAIN:
        return None
    qi_token = _resolve_qi_token(asset)
    if qi_token is None or not _is_evm_address(qi_token) or not _is_evm_address(wallet_address):
        return None
    data = _GET_ACCOUNT_SNAPSHOT_SELECTOR + f"{int(wallet_address, 16):064x}"
    raw = _read_with_retry(lambda: gateway_client.eth_call(chain=chain, to=qi_token, data=data, block=block))
    return _decode_account_snapshot(raw)


def _supply_residual(
    gateway_client: Any, chain: str, asset: str, wallet_address: str, block: int | str | None
) -> int | None:
    snapshot = _account_snapshot(gateway_client, chain, asset, wallet_address, block)
    if snapshot is None:
        return None
    qi_token_balance, _borrow_balance, exchange_rate = snapshot
    return qi_token_balance * exchange_rate // 10**18


def _debt_residual(
    gateway_client: Any, chain: str, asset: str, wallet_address: str, block: int | str | None
) -> int | None:
    snapshot = _account_snapshot(gateway_client, chain, asset, wallet_address, block)
    if snapshot is None:
        return None
    _qi_token_balance, borrow_balance, _exchange_rate = snapshot
    return borrow_balance


def benqi_teardown_post_condition(
    position: Any,
    wallet_address: str,
    gateway_client: Any | None = None,
    rpc_url: str | None = None,  # noqa: ARG001 — protocol signature; gateway boundary: never consumed
    block: int | str | None = None,
) -> ClosureCheckResult:
    """Verify a BENQI position is flat on-chain (supplied value / debt ≤ dust)."""
    return verify_lending_closure(
        position,
        wallet_address,
        gateway_client,
        block,
        read_supply=_supply_residual,
        read_debt=_debt_residual,
    )


__all__ = ["benqi_teardown_post_condition"]
