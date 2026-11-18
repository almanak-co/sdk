"""Liquidity actions with a native PoolKey currency, wrapped and unwrapped by the PositionManager.

The fund holds only the wrapped native and every call carries no value:

- Mint: SETTLE pulls the native leg's maximum as wrapped native into the PoolManager, TAKE
  hands that credit to the PositionManager, UNWRAP turns all of it into native, MINT_POSITION
  owes both legs, SETTLE pays exactly the native debt from the PositionManager and the token
  debt from the fund, then WRAP and SWEEP return any unused native to the fund as wrapped.
- Collect and close: DECREASE_LIQUIDITY or BURN_POSITION credits both legs, TAKE pays the
  token to the fund and the native to the PositionManager, which wraps all of it and sweeps
  the wrapped native to the fund.

The PositionManager ends every call holding nothing it did not hold before. The three
templates share one padded word length, so Roles 2.1 combines them as OR branches.
"""

from eth_abi import encode

from almanak.connectors.uniswap_v4.abi import MODIFY, POOL_KEY, contract_call, pool_key

DECREASE, MINT, BURN = 0x01, 0x02, 0x03
SETTLE, TAKE, SWEEP, WRAP, UNWRAP = 0x0B, 0x0E, 0x14, 0x15, 0x16
NATIVE = "0x" + "00" * 20
ADDRESS_THIS = "0x" + "00" * 19 + "02"
CONTRACT_BALANCE = 1 << 255
OPEN_DELTA = 0
# The mint template's length; collect and close are padded to it with zero words, which
# periphery ignores. Kept tight: Roles 2.1 indexes a grant's condition nodes with a uint8.
NATIVE_LP_WORDS = 50


def legs(target):
    """(native index, wrapped native, token index, token) of a native-currency target."""
    native = target.native_index
    return native, target.assets[native].reference, 1 - native, target.assets[1 - native].reference


def native_mint(target, liquidity, maximum0, maximum1, deadline, bounds):
    native, wrapped, _, token = legs(target)
    pulled = (maximum0, maximum1)[native]
    mint = encode(
        (POOL_KEY, "int24", "int24", "uint256", "uint128", "uint128", "address", "bytes"),
        (
            pool_key(target.pool),
            bounds.lower,
            bounds.upper,
            liquidity,
            maximum0,
            maximum1,
            target.fund.address,
            b"",
        ),
    )
    return modify(
        target,
        (
            (SETTLE, settle(wrapped, pulled, True)),
            (TAKE, take(wrapped, ADDRESS_THIS)),
            (UNWRAP, encode(("uint256",), (CONTRACT_BALANCE,))),
            (MINT, mint),
            (SETTLE, settle(NATIVE, OPEN_DELTA, False)),
            (SETTLE, settle(token, OPEN_DELTA, True)),
            *returned(target, wrapped),
        ),
        deadline,
    )


def native_collect(target, token_id, deadline):
    decrease = encode(
        ("uint256", "uint256", "uint128", "uint128", "bytes"), (token_id, 0, 0, 0, b"")
    )
    return modify(target, ((DECREASE, decrease), *paid_out(target)), deadline)


def native_close(target, token_id, minimum0, minimum1, deadline):
    burn = encode(("uint256", "uint128", "uint128", "bytes"), (token_id, minimum0, minimum1, b""))
    return modify(target, ((BURN, burn), *paid_out(target)), deadline)


def paid_out(target):
    _, wrapped, _, token = legs(target)
    return (
        (TAKE, take(NATIVE, ADDRESS_THIS)),
        (TAKE, take(token, target.fund.address)),
        *returned(target, wrapped),
    )


def returned(target, wrapped):
    """Wrap the PositionManager's whole native balance and sweep the wrapped native to the fund."""
    return (
        (WRAP, encode(("uint256",), (CONTRACT_BALANCE,))),
        (SWEEP, encode(("address", "address"), (wrapped, target.fund.address))),
    )


def settle(currency, amount, payer_is_user):
    return encode(("address", "uint256", "bool"), (currency, amount, payer_is_user))


def take(currency, recipient):
    return encode(("address", "address", "uint256"), (currency, recipient, OPEN_DELTA))


def modify(target, steps, deadline):
    actions = bytes(action for action, _ in steps)
    unlock = encode(("bytes", "bytes[]"), (actions, [params for _, params in steps]))
    if len(unlock) > NATIVE_LP_WORDS * 32:
        raise ValueError("native liquidity template exceeds its common word envelope")
    return contract_call(
        target,
        target.contracts.position_manager,
        MODIFY,
        ("bytes", "uint256"),
        (unlock.ljust(NATIVE_LP_WORDS * 32, b"\0"), deadline),
    )
