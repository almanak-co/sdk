"""Polymarket V2 permission discovery hints.

Polymarket trading is hybrid:
- Off-chain: orders match via the V2 CLOB API. Polymarket's operator settles
  matched orders on-chain — that path is NOT initiated by the strategy.
- On-chain: the strategy itself only ever calls approvals, the collateral
  ramp (wrap/unwrap), and CTF redemption / split / merge.

The IntentCompiler can't discover most of those selectors via synthetic
intent compilation because trade intents (PREDICTION_BUY / PREDICTION_SELL)
return ActionBundles with empty `transactions=[]` (off-chain). We declare
the on-chain entry points statically so the manifest matches reality.
"""

from almanak.framework.connectors.polymarket.models import (
    COLLATERAL_OFFRAMP,
    COLLATERAL_ONRAMP,
    CONDITIONAL_TOKENS,
    CTF_EXCHANGE_V2,
    NEG_RISK_ADAPTER,
    NEG_RISK_EXCHANGE_V2,
    PUSD,
    USDC_NATIVE_POLYGON,
    USDCE_POLYGON,
)
from almanak.framework.permissions.hints import PermissionHints, StaticPermissionEntry

# Selectors — keccak256(signature)[:4]
_APPROVE = "0x095ea7b3"  # approve(address,uint256)
_SET_APPROVAL_FOR_ALL = "0xa22cb465"  # setApprovalForAll(address,bool)
_WRAP = "0x62355638"  # wrap(address,address,uint256)
_UNWRAP = "0x8cc7104f"  # unwrap(address,address,uint256)
_REDEEM_POSITIONS = "0x01b7037c"  # redeemPositions(address,bytes32,bytes32,uint256[])
_SPLIT_POSITION = "0x72ce4275"  # splitPosition(address,bytes32,bytes32,uint256[],uint256)
_MERGE_POSITIONS = "0x9e7212ad"  # mergePositions(address,bytes32,bytes32,uint256[],uint256)


_POLYGON_STATIC_PERMISSIONS = [
    # Source assets — approve to CollateralOnramp (so user can wrap to pUSD)
    StaticPermissionEntry(
        target=USDCE_POLYGON,
        label="USDC.e (bridged) — Onramp source",
        selectors={_APPROVE: "approve(address,uint256)"},
    ),
    StaticPermissionEntry(
        target=USDC_NATIVE_POLYGON,
        label="USDC native (Circle) — future Onramp source",
        selectors={_APPROVE: "approve(address,uint256)"},
    ),
    # pUSD — approve to both V2 exchanges
    StaticPermissionEntry(
        target=PUSD,
        label="pUSD (V2 spendable collateral)",
        selectors={_APPROVE: "approve(address,uint256)"},
    ),
    # CollateralOnramp.wrap — wrap source asset → pUSD
    StaticPermissionEntry(
        target=COLLATERAL_ONRAMP,
        label="Polymarket CollateralOnramp",
        selectors={_WRAP: "wrap(address,address,uint256)"},
    ),
    # CollateralOfframp.unwrap — pUSD → source asset
    StaticPermissionEntry(
        target=COLLATERAL_OFFRAMP,
        label="Polymarket CollateralOfframp",
        selectors={_UNWRAP: "unwrap(address,address,uint256)"},
    ),
    # ConditionalTokens — operator approval + redeem/split/merge
    StaticPermissionEntry(
        target=CONDITIONAL_TOKENS,
        label="Conditional Tokens (CTF)",
        selectors={
            _SET_APPROVAL_FOR_ALL: "setApprovalForAll(address,bool)",
            _REDEEM_POSITIONS: "redeemPositions(address,bytes32,bytes32,uint256[])",
            _SPLIT_POSITION: "splitPosition(address,bytes32,bytes32,uint256[],uint256)",
            _MERGE_POSITIONS: "mergePositions(address,bytes32,bytes32,uint256[],uint256)",
        },
    ),
    # CTF Exchange V2 — only setApprovalForAll target reaches here from the
    # strategy side. Trade fills are operator-submitted off-chain → not the
    # strategy's role member.
    StaticPermissionEntry(
        target=CTF_EXCHANGE_V2,
        label="CTF Exchange V2 (operator approval target)",
        selectors={},
    ),
    # NegRisk Exchange V2 — same: receives setApprovalForAll(CTF, true).
    StaticPermissionEntry(
        target=NEG_RISK_EXCHANGE_V2,
        label="NegRisk CTF Exchange V2 (operator approval target)",
        selectors={},
    ),
    # NegRisk Adapter — handles split/merge for neg-risk markets; receives
    # setApprovalForAll(CTF, true) from the strategy.
    StaticPermissionEntry(
        target=NEG_RISK_ADAPTER,
        label="NegRisk Adapter (split/merge)",
        selectors={},
    ),
]


PERMISSION_HINTS = PermissionHints(
    static_permissions={"polygon": _POLYGON_STATIC_PERMISSIONS},
    selector_labels={
        _APPROVE: "approve(address,uint256)",
        _SET_APPROVAL_FOR_ALL: "setApprovalForAll(address,bool)",
        _WRAP: "wrap(address,address,uint256)",
        _UNWRAP: "unwrap(address,address,uint256)",
        _REDEEM_POSITIONS: "redeemPositions(address,bytes32,bytes32,uint256[])",
        _SPLIT_POSITION: "splitPosition(address,bytes32,bytes32,uint256[],uint256)",
        _MERGE_POSITIONS: "mergePositions(address,bytes32,bytes32,uint256[],uint256)",
    },
)
