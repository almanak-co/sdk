"""Aave V3 teardown on-chain closure verifier.

The connector owns Aave's reserve catalogue and ``getUserReserveData`` ABI.
TD-14 therefore reads the exact reserve represented by each SUPPLY/BORROW
position through the gateway at the pinned post-teardown block. Empty is never
treated as zero: an unknown market, malformed response, or RPC fault remains
unmeasured and cannot certify closure.
"""

from __future__ import annotations

from typing import Any

from almanak.connectors._strategy_base.address_registry import AddressRegistry
from almanak.connectors._strategy_base.lending_post_condition import verify_lending_closure
from almanak.connectors._strategy_base.teardown_post_condition import ClosureCheckResult
from almanak.connectors._strategy_base.vault_post_condition import _is_evm_address, _read_with_retry
from almanak.connectors.aave_v3.addresses import AAVE_V3_TOKENS
from almanak.connectors.aave_v3.lending_read import LENDING_READ_SPEC
from almanak.core.constants import canonical_chain_name


def _resolve_reserve(chain: str, asset: str) -> tuple[str, str] | None:
    canonical_chain = canonical_chain_name(chain).lower()
    tokens = AAVE_V3_TOKENS.get(canonical_chain)
    if tokens is None:
        return None
    wanted = asset.strip().lower()
    token = next((address for symbol, address in tokens.items() if symbol.lower() == wanted), None)
    provider = AddressRegistry.resolve_contract_address("aave_v3", chain, LENDING_READ_SPEC.contract_kinds)
    if token is None or not isinstance(provider, str) or not _is_evm_address(token) or not _is_evm_address(provider):
        return None
    return token, provider


def _reserve_position(
    gateway_client: Any,
    chain: str,
    asset: str,
    wallet_address: str,
    block: int | str | None,
) -> Any | None:
    resolved = _resolve_reserve(chain, asset)
    if resolved is None or not _is_evm_address(wallet_address):
        return None
    token, provider = resolved
    data = LENDING_READ_SPEC.build_calldata(token, wallet_address)
    raw = _read_with_retry(lambda: gateway_client.eth_call(chain=chain, to=provider, data=data, block=block))
    if not isinstance(raw, str):
        return None
    return LENDING_READ_SPEC.parse_result(raw, token)


def _supply_residual(
    gateway_client: Any, chain: str, asset: str, wallet_address: str, block: int | str | None
) -> int | None:
    position = _reserve_position(gateway_client, chain, asset, wallet_address, block)
    return None if position is None else position.current_atoken_balance


def _debt_residual(
    gateway_client: Any, chain: str, asset: str, wallet_address: str, block: int | str | None
) -> int | None:
    position = _reserve_position(gateway_client, chain, asset, wallet_address, block)
    return None if position is None else position.total_debt


def aave_v3_teardown_post_condition(
    position: Any,
    wallet_address: str,
    gateway_client: Any | None = None,
    rpc_url: str | None = None,  # noqa: ARG001 -- gateway boundary: never consumed
    block: int | str | None = None,
) -> ClosureCheckResult:
    """Verify one Aave reserve leg is flat at the pinned chain block."""
    return verify_lending_closure(
        position,
        wallet_address,
        gateway_client,
        block,
        read_supply=_supply_residual,
        read_debt=_debt_residual,
    )


__all__ = ["aave_v3_teardown_post_condition"]
