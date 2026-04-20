"""Curvance low-level SDK — calldata builders and event helpers.

The SDK is stateless: it produces encoded calldata and transaction targets for
Curvance operations. Higher-level state (wallet, gateway, configuration) lives
in ``CurvanceAdapter``.

All calldata is ABI-encoded using ``eth_abi`` to keep a clean boundary between
bytes-level encoding and dataclass-based domain models.
"""

from __future__ import annotations

from eth_abi import encode as abi_encode
from eth_utils import to_checksum_address

# -----------------------------------------------------------------------------
# Function selectors (keccak256(sig)[:4])
# -----------------------------------------------------------------------------

# cToken — collateral side (ERC-4626-extended)
SELECTOR_DEPOSIT_AS_COLLATERAL = bytes.fromhex("2f4a61d9")  # depositAsCollateral(uint256,address)
SELECTOR_DEPOSIT_AS_COLLATERAL_FOR = bytes.fromhex("b3bffb45")  # depositAsCollateralFor(uint256,address)
SELECTOR_DEPOSIT = bytes.fromhex("6e553f65")  # deposit(uint256,address)  — ERC-4626
SELECTOR_WITHDRAW = bytes.fromhex(
    "b460af94"
)  # withdraw(uint256,address,address) — ERC-4626 (non-collateral redemption only)
SELECTOR_WITHDRAW_COLLATERAL = bytes.fromhex(
    "72d46ac2"
)  # withdrawCollateral(uint256,address,address) — forces collateral unwind
SELECTOR_REDEEM = bytes.fromhex("ba087652")  # redeem(uint256,address,address) — ERC-4626
SELECTOR_REDEEM_COLLATERAL = bytes.fromhex("cd88c072")  # redeemCollateral(uint256,address,address)

# BorrowableCToken — debt side
# NOTE: Curvance's borrow() takes TWO args (assets, receiver) — it is NOT the
# Compound-style single-arg function. Verified against
# github.com/curvance/curvance-contracts contracts/market/token/BorrowableCToken.sol.
SELECTOR_BORROW = bytes.fromhex("4b3fd148")  # borrow(uint256,address)
SELECTOR_REPAY = bytes.fromhex("371fd8e6")  # repay(uint256)
SELECTOR_REPAY_FOR = bytes.fromhex("9e591a44")  # repayFor(uint256,address)
SELECTOR_DEBT_BALANCE = bytes.fromhex("11005b07")  # debtBalance(address)

# ERC20 helpers
SELECTOR_APPROVE = bytes.fromhex("095ea7b3")  # approve(address,uint256)
SELECTOR_BALANCE_OF = bytes.fromhex("70a08231")  # balanceOf(address)

# MarketManager view helpers
SELECTOR_ACTIONS_PAUSED = bytes.fromhex("699ba8b3")  # actionsPaused(address)

# Sentinels
MAX_UINT256 = 2**256 - 1

# -----------------------------------------------------------------------------
# Event topics (keccak256 of canonical event sig)
# -----------------------------------------------------------------------------

# Deposit(address indexed from, address indexed to, uint256 assets, uint256 shares)
EVENT_TOPIC_DEPOSIT = "0xdcbc1c05240f31ff3ad067ef1ee35ce4997762752e3a095284754544f4c709d7"

# Withdraw(address indexed sender, address indexed receiver, address indexed owner,
#          uint256 assets, uint256 shares)
EVENT_TOPIC_WITHDRAW = "0xfbde797d201c681b91056529119e0b02407c7bb96a4a2c75c01fc9667232c8db"

# Borrow(uint256 assets, uint256 debtAssetsOwed, address account) — non-indexed params
EVENT_TOPIC_BORROW = "0xbec1750eb40c00e8dc2e1c84babbddd5779eaa06c951ab2c66416d05910e7a73"

# Repay(uint256 assets, uint256 debtAssetsOwed, address payer, address account) — non-indexed params
EVENT_TOPIC_REPAY = "0x21afd5f303208e3668ecf03cf3aa4036a5f71fbdd357478968cfd1502c25953d"


# -----------------------------------------------------------------------------
# Calldata builders
# -----------------------------------------------------------------------------


def encode_approve(spender: str, amount: int) -> bytes:
    """Encode ERC20 ``approve(spender, amount)`` calldata."""
    return SELECTOR_APPROVE + abi_encode(["address", "uint256"], [to_checksum_address(spender), amount])


def encode_deposit_as_collateral(assets: int, receiver: str) -> bytes:
    """Encode ``cToken.depositAsCollateral(assets, receiver)`` calldata.

    This is the primary supply path — atomically deposits and posts the
    resulting shares as collateral against the market. No separate
    ``enterMarkets`` call is required.
    """
    return SELECTOR_DEPOSIT_AS_COLLATERAL + abi_encode(["uint256", "address"], [assets, to_checksum_address(receiver)])


def encode_deposit(assets: int, receiver: str) -> bytes:
    """Encode ERC-4626 ``deposit(assets, receiver)`` — lend without posting collateral."""
    return SELECTOR_DEPOSIT + abi_encode(["uint256", "address"], [assets, to_checksum_address(receiver)])


def encode_redeem_collateral(shares: int, receiver: str, owner: str) -> bytes:
    """Encode ``cToken.redeemCollateral(shares, receiver, owner)`` calldata.

    Used for withdrawing collateral from Curvance. ``shares`` is the cToken
    share amount (NOT the underlying asset amount); ``owner`` is the account
    whose shares are being redeemed (normally the caller).

    MAX_UINT256 is NOT accepted here — the contract does not clamp; it would
    attempt to redeem the sentinel value. Pass the exact share balance for a
    full exit.

    Note: Curvance enforces a 20-minute ``MIN_HOLD_PERIOD`` on collateral
    before it can be redeemed. Attempting redemption earlier reverts with
    ``MarketManager__MinimumHoldPeriod()``.
    """
    return SELECTOR_REDEEM_COLLATERAL + abi_encode(
        ["uint256", "address", "address"],
        [shares, to_checksum_address(receiver), to_checksum_address(owner)],
    )


def encode_withdraw_assets(assets: int, receiver: str, owner: str) -> bytes:
    """Encode ERC-4626 ``withdraw(assets, receiver, owner)``.

    WARNING: plain ERC-4626 ``withdraw`` does NOT unwind collateral that was
    posted via ``depositAsCollateral``. It operates on the caller's non-
    collateral (lending-only) share balance. For collateral unwind, use
    ``encode_withdraw_collateral`` or ``encode_redeem_collateral``.
    """
    return SELECTOR_WITHDRAW + abi_encode(
        ["uint256", "address", "address"],
        [assets, to_checksum_address(receiver), to_checksum_address(owner)],
    )


def encode_withdraw_collateral(assets: int, receiver: str, owner: str) -> bytes:
    """Encode ``cToken.withdrawCollateral(assets, receiver, owner)`` calldata.

    Asset-amount variant of ``redeemCollateral``. Forces collateral to be
    unwound from ``owner``'s posted position and converted back to the
    underlying asset. ``owner`` is the account whose collateral is being
    withdrawn; ``receiver`` is who gets the underlying. For a call-by-caller
    flow, pass ``owner == msg.sender``.

    Note: Curvance's 20-minute ``MIN_HOLD_PERIOD`` applies to this function
    as well — attempting withdrawal earlier reverts with
    ``MarketManager__MinimumHoldPeriod()``.
    """
    return SELECTOR_WITHDRAW_COLLATERAL + abi_encode(
        ["uint256", "address", "address"],
        [assets, to_checksum_address(receiver), to_checksum_address(owner)],
    )


def encode_redeem_shares(shares: int, receiver: str, owner: str) -> bytes:
    """Encode ERC-4626 ``redeem(shares, receiver, owner)``."""
    return SELECTOR_REDEEM + abi_encode(
        ["uint256", "address", "address"],
        [shares, to_checksum_address(receiver), to_checksum_address(owner)],
    )


def encode_borrow(amount: int, receiver: str) -> bytes:
    """Encode ``BorrowableCToken.borrow(assets, receiver)`` calldata.

    Curvance's borrow takes two args — the amount plus the receiver of the
    borrowed assets. The caller (msg.sender) is always the debt owner.
    """
    return SELECTOR_BORROW + abi_encode(["uint256", "address"], [amount, to_checksum_address(receiver)])


def encode_repay(amount: int) -> bytes:
    """Encode ``BorrowableCToken.repay(amount)`` calldata.

    Pass ``amount=0`` for a full-debt repay — Curvance treats 0 as the "repay
    everything outstanding" sentinel (per the contract's NatSpec, verified in
    ``contracts/market/token/BorrowableCToken.sol``). ``MAX_UINT256`` is NOT a
    valid full-repay sentinel here; that convention is Morpho/Aave-specific.
    """
    return SELECTOR_REPAY + abi_encode(["uint256"], [amount])


def encode_repay_for(amount: int, owner: str) -> bytes:
    """Encode ``BorrowableCToken.repayFor(assets, owner)`` calldata.

    Arg order is (assets, owner) — verified against the Curvance source. Other
    Compound-fork layouts put the account first; Curvance does not.
    """
    return SELECTOR_REPAY_FOR + abi_encode(["uint256", "address"], [amount, to_checksum_address(owner)])


# -----------------------------------------------------------------------------
# View-call encoders (for pre-flight checks via RPC)
# -----------------------------------------------------------------------------


def encode_debt_balance(account: str) -> bytes:
    """Encode ``BorrowableCToken.debtBalance(account)`` calldata for eth_call."""
    return SELECTOR_DEBT_BALANCE + abi_encode(["address"], [to_checksum_address(account)])


def encode_balance_of(account: str) -> bytes:
    """Encode ERC20 ``balanceOf(account)`` calldata for eth_call."""
    return SELECTOR_BALANCE_OF + abi_encode(["address"], [to_checksum_address(account)])


def encode_actions_paused(c_token: str) -> bytes:
    """Encode ``MarketManager.actionsPaused(cToken)`` calldata for eth_call."""
    return SELECTOR_ACTIONS_PAUSED + abi_encode(["address"], [to_checksum_address(c_token)])


# -----------------------------------------------------------------------------
# Default gas estimates (conservative upper bounds)
# -----------------------------------------------------------------------------

DEFAULT_GAS_ESTIMATES: dict[str, int] = {
    # Curvance cTokens perform significant work per call: interest accrual on
    # both sides of the market, oracle price pull via OracleManager, MarketManager
    # risk checks, and position-tracking updates. Generous ceilings avoid false
    # OOG reverts — surplus gas is refunded by the EVM. Tuned 2026-04-18 against
    # Monad Anvil forks; actual usage tends to be 55-75% of these ceilings.
    "approve": 80_000,
    "supply_collateral": 600_000,  # depositAsCollateral + collateral posting + risk checks
    "supply_loan": 400_000,  # plain deposit (lend only) — lighter, no risk checks
    "withdraw_collateral": 550_000,  # redeemCollateral + debt-health re-check
    "withdraw_assets": 550_000,  # ERC-4626 withdraw
    "borrow": 700_000,  # interest accrual + health check + disburse
    "repay": 400_000,
}


class CurvanceSDK:
    """Thin stateless wrapper exposing calldata/encoder helpers.

    The SDK is stateless by design — higher-level state (wallet address,
    gateway client, chain-scoped market lookup) belongs in the adapter. A
    class form is kept for parity with other connectors (e.g. MorphoBlueSDK)
    and to make future extension easier without changing import sites.
    """

    def __init__(self, chain: str) -> None:
        self.chain = chain.lower()

    # Each method below is a thin pass-through to the module-level encoder.
    # Adapter code is free to call either the method or the module function.

    @staticmethod
    def approve(spender: str, amount: int) -> bytes:
        return encode_approve(spender, amount)

    @staticmethod
    def deposit_as_collateral(assets: int, receiver: str) -> bytes:
        return encode_deposit_as_collateral(assets, receiver)

    @staticmethod
    def deposit(assets: int, receiver: str) -> bytes:
        return encode_deposit(assets, receiver)

    @staticmethod
    def redeem_collateral(shares: int, receiver: str, owner: str) -> bytes:
        return encode_redeem_collateral(shares, receiver, owner)

    @staticmethod
    def withdraw_assets(assets: int, receiver: str, owner: str) -> bytes:
        return encode_withdraw_assets(assets, receiver, owner)

    @staticmethod
    def redeem_shares(shares: int, receiver: str, owner: str) -> bytes:
        return encode_redeem_shares(shares, receiver, owner)

    @staticmethod
    def borrow(amount: int, receiver: str) -> bytes:
        return encode_borrow(amount, receiver)

    @staticmethod
    def repay(amount: int) -> bytes:
        return encode_repay(amount)

    @staticmethod
    def debt_balance_call(account: str) -> bytes:
        return encode_debt_balance(account)
