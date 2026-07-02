"""HyperEVM system-contract and read-precompile addresses for Hyperliquid.

Hyperliquid perps live on HyperCore (the native order-book engine). EVM
contracts on HyperEVM (chain id 999) reach HyperCore through two system
surfaces, both verified live against ``https://rpc.hyperliquid.xyz/evm`` on
2026-07-01:

* **CoreWriter** (``0x3333…3333``) — a write bridge. ``sendRawAction(bytes)``
  emits a ``RawAction`` log and queues a versioned action (limit order,
  cancel, USD-class transfer, …) for HyperCore to settle *asynchronously*.
  The EVM tx never carries the fill and never reverts on a Core-side reject.

* **Read precompiles** (``0x0800``–``0x0810``) — synchronous ``staticcall``
  reads of live HyperCore state (positions, oracle/mark prices, asset info).
  Input is raw ABI-encoded arguments with **no 4-byte selector** (a precompile
  is not a Solidity function). This was verified live: ``oraclePx(0)`` →
  ``598970`` for BTC (szDecimals 5 → human ``598970 / 10**(6-5) = 59897``).

Read vs write scaling is asymmetric and easy to conflate — keep them apart:

* **Write** (CoreWriter limitPx / sz): ``round(human * 1e8)`` after tick /
  size rounding (see ``sdk.py``). Flat ``1e8`` per the official Hyperliquid
  Python SDK (``float_to_int_for_hashing`` = ``float_to_int(x, 8)``).
* **Read** (precompile perp px): ``raw / 10**(6 - szDecimals)`` for perps,
  ``raw / 10**(8 - szDecimals)`` for spot.
"""

from __future__ import annotations

# =============================================================================
# CoreWriter — the HyperCore write bridge.
# =============================================================================

CORE_WRITER_ADDRESS: str = "0x3333333333333333333333333333333333333333"

# CoreWriter action-encoding version (only value defined today).
CORE_WRITER_ENCODING_VERSION: int = 1

# Action IDs (big-endian 3-byte suffix in the action header). Verified against
# the Hyperliquid "Interacting with HyperCore" docs. Only the perp-relevant
# subset is wired here; the rest are listed for completeness / future use.
ACTION_LIMIT_ORDER: int = 1
ACTION_VAULT_TRANSFER: int = 2
ACTION_TOKEN_DELEGATE: int = 3
ACTION_STAKING_DEPOSIT: int = 4
ACTION_STAKING_WITHDRAW: int = 5
ACTION_SPOT_SEND: int = 6
ACTION_USD_CLASS_TRANSFER: int = 7
ACTION_FINALIZE_EVM_CONTRACT: int = 8
ACTION_ADD_API_WALLET: int = 9
ACTION_CANCEL_ORDER_BY_OID: int = 10
ACTION_CANCEL_ORDER_BY_CLOID: int = 11
ACTION_APPROVE_BUILDER_FEE: int = 12
ACTION_SEND_ASSET: int = 13
ACTION_BORROW_LEND: int = 15
ACTION_SET_ABSTRACTION: int = 16


# =============================================================================
# Read precompiles (staticcall; raw ABI input, no selector).
# =============================================================================
#
# Verified live: 0x0806 markPx, 0x0807 oraclePx, 0x0809 l1BlockNumber,
# 0x080a perpAssetInfo all returned sane values for BTC (perp index 0).

PRECOMPILE_POSITION: str = "0x0000000000000000000000000000000000000800"
PRECOMPILE_SPOT_BALANCE: str = "0x0000000000000000000000000000000000000801"
PRECOMPILE_VAULT_EQUITY: str = "0x0000000000000000000000000000000000000802"
PRECOMPILE_WITHDRAWABLE: str = "0x0000000000000000000000000000000000000803"
PRECOMPILE_DELEGATIONS: str = "0x0000000000000000000000000000000000000804"
PRECOMPILE_DELEGATOR_SUMMARY: str = "0x0000000000000000000000000000000000000805"
PRECOMPILE_MARK_PX: str = "0x0000000000000000000000000000000000000806"
PRECOMPILE_ORACLE_PX: str = "0x0000000000000000000000000000000000000807"
PRECOMPILE_SPOT_PX: str = "0x0000000000000000000000000000000000000808"
PRECOMPILE_L1_BLOCK_NUMBER: str = "0x0000000000000000000000000000000000000809"
PRECOMPILE_PERP_ASSET_INFO: str = "0x000000000000000000000000000000000000080a"
PRECOMPILE_SPOT_INFO: str = "0x000000000000000000000000000000000000080b"
PRECOMPILE_TOKEN_INFO: str = "0x000000000000000000000000000000000000080C"
PRECOMPILE_BBO: str = "0x000000000000000000000000000000000000080e"
PRECOMPILE_ACCOUNT_MARGIN_SUMMARY: str = "0x000000000000000000000000000000000000080F"
PRECOMPILE_CORE_USER_EXISTS: str = "0x0000000000000000000000000000000000000810"

# RawAction event topic — keccak256("RawAction(address,bytes)").
# CoreWriter emits this as the sole proof-of-submission log.
RAW_ACTION_EVENT_TOPIC: str = "0x8c7f585fb295f7eb1e6aeb8fba61b23a4fe60beda405f0045073b185c74412e3"

# Perp price fixed-point exponent used by HyperCore read precompiles:
# human = raw / 10**(PERP_PX_MAX_DECIMALS - szDecimals). 8 for spot.
PERP_PX_MAX_DECIMALS: int = 6
SPOT_PX_MAX_DECIMALS: int = 8

# CoreWriter wire fixed-point: limitPx and sz are round(human * 10**WIRE_DECIMALS).
WIRE_DECIMALS: int = 8

# The chain this connector executes on.
HYPEREVM_CHAIN: str = "hyperevm"

__all__ = [
    "ACTION_CANCEL_ORDER_BY_CLOID",
    "ACTION_CANCEL_ORDER_BY_OID",
    "ACTION_LIMIT_ORDER",
    "ACTION_USD_CLASS_TRANSFER",
    "ACTION_VAULT_TRANSFER",
    "CORE_WRITER_ADDRESS",
    "CORE_WRITER_ENCODING_VERSION",
    "HYPEREVM_CHAIN",
    "PERP_PX_MAX_DECIMALS",
    "PRECOMPILE_ACCOUNT_MARGIN_SUMMARY",
    "PRECOMPILE_MARK_PX",
    "PRECOMPILE_ORACLE_PX",
    "PRECOMPILE_PERP_ASSET_INFO",
    "PRECOMPILE_POSITION",
    "RAW_ACTION_EVENT_TOPIC",
    "SPOT_PX_MAX_DECIMALS",
    "WIRE_DECIMALS",
]
