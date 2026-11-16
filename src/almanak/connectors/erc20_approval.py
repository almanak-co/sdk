"""ERC-20 approve results follow OpenZeppelin SafeERC20 return semantics."""

from eth_abi import encode

from almanak.caip import AssetType

ABI_TRUE = encode(("bool",), (True,))

# Tokens that revert a nonzero-to-nonzero approve, so a residual allowance is cleared
# to zero before a new one. Reviewed per token: other tokens keep approving directly,
# which leaves their planned steps, and retained executions, unchanged.
ZERO_FIRST_APPROVALS = frozenset(
    {AssetType("eip155:1/erc20:0xdac17f958d2ee523a2206206994597c13d831ec7")}  # Ethereum USDT
)


def approval_succeeded(raw: bytes) -> bool:
    """Accept an empty return (legacy tokens such as Ethereum USDT) or exactly ABI true.

    Only ERC-20 ``approve`` calls, including zero-amount allowance cleanup, may use this:
    ABI false, any other length and noncanonical encodings stay refused.
    """
    return raw == b"" or raw == ABI_TRUE


def requires_zero_first(asset: AssetType) -> bool:
    """Whether a residual allowance of `asset` must be cleared before a new approve."""
    return asset in ZERO_FIRST_APPROVALS
