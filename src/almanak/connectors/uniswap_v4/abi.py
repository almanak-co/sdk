"""Canonical deployed periphery action templates, shared by all environments."""

from eth_abi import encode

from almanak.gateway.accounts import ContractCall
from almanak.gateway.evm import currency_address
from almanak.gateway.safe_fixture import calldata

POOL_KEY = "(address,address,uint24,int24,address)"
SWAP_PARAMS = f"({POOL_KEY},bool,uint128,uint128,bytes)"
HOP_PRICE_SWAP_PARAMS = f"({POOL_KEY},bool,uint128,uint128,uint256,bytes)"
QUOTE_PARAMS = f"({POOL_KEY},bool,uint128,bytes)"
ROUTER_EXECUTE = "execute(bytes,bytes[],uint256)"
MODIFY = "modifyLiquidities(bytes,uint256)"
SWAP_ACTIONS = bytes((0x06, 0x0C, 0x0F))
MINT_ACTIONS = bytes((0x02, 0x0D))
COLLECT_ACTIONS = bytes((0x01, 0x11))
CLOSE_ACTIONS = bytes((0x03, 0x11))


def pool_key(pool):
    return (
        currency_address(pool.currency0, pool.currency0.chain),
        currency_address(pool.currency1, pool.currency1.chain),
        pool.fee,
        pool.tick_spacing,
        pool.hooks,
    )


def contract_call(target, to, signature, types, values):
    return ContractCall(fund=target.fund, to=to, value=0, data=calldata(signature, types, values))


def approve(target, asset, amount):
    return contract_call(
        target,
        asset.reference,
        "approve(address,uint256)",
        ("address", "uint256"),
        (target.contracts.permit2, amount),
    )


def permit(target, asset, spender, amount, expiration):
    return contract_call(
        target,
        target.contracts.permit2,
        "approve(address,address,uint160,uint48)",
        ("address", "address", "uint160", "uint48"),
        (asset.reference, spender, amount, expiration),
    )


def swap(target, amount, minimum, deadline):
    zero_for_one = amount.asset == target.assets[0]
    if target.native_index is not None:
        from almanak.connectors.uniswap_v4.native_router import native_swap

        return native_swap(target, zero_for_one, amount.amount, minimum, deadline)
    output = target.assets[1 if zero_for_one else 0]
    params = (
        swap_parameters(target, zero_for_one, amount.amount, minimum),
        encode(("address", "uint256"), (amount.asset.reference, amount.amount)),
        encode(("address", "uint256"), (output.reference, minimum)),
    )
    actions = encode(("bytes", "bytes[]"), (SWAP_ACTIONS, params))
    return contract_call(
        target,
        target.contracts.router,
        ROUTER_EXECUTE,
        ("bytes", "bytes[]", "uint256"),
        (b"\x10", (actions,), deadline),
    )


def swap_parameters(target, zero_for_one, amount, minimum):
    values = (pool_key(target.pool), zero_for_one, amount, minimum)
    if str(target.pool.chain.chain_id) == "eip155:4663":
        if target.contracts.router != "0x8876789976decbfcbbbe364623c63652db8c0904":
            raise ValueError("Robinhood V4 swap requires the source-bound router revision")
        # This revision adds a per-hop price field before hookData. Zero retains
        # the existing amountOutMinimum contract; Roles fixes this added word.
        return encode((HOP_PRICE_SWAP_PARAMS,), ((*values, 0, b""),))
    return encode((SWAP_PARAMS,), ((*values, b""),))


def modify(target, actions, params, deadline):
    unlock = encode(("bytes", "bytes[]"), (actions, params))
    # Roles alternatives require one type tree. The three exact LP templates use
    # a common 24-word envelope; periphery ignores zero-only trailing padding.
    # Permission compilation checks every meaningful offset, length and word.
    unlock = unlock.ljust(24 * 32, b"\0")
    return contract_call(
        target,
        target.contracts.position_manager,
        MODIFY,
        ("bytes", "uint256"),
        (unlock, deadline),
    )


def mint(target, liquidity, maximum0, maximum1, deadline, selected=None):
    bounds = selected if selected is not None else target.range
    if bounds is None:
        raise ValueError("mint requires an explicitly configured or selected range")
    if target.native_index is not None:
        from almanak.connectors.uniswap_v4.native_liquidity import native_mint

        return native_mint(target, liquidity, maximum0, maximum1, deadline, bounds)
    params = (
        encode(
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
        ),
        encode(
            ("address", "address"),
            (target.pool.currency0.reference, target.pool.currency1.reference),
        ),
    )
    return modify(target, MINT_ACTIONS, params, deadline)


def take_pair(target):
    return encode(
        ("address", "address", "address"),
        (target.pool.currency0.reference, target.pool.currency1.reference, target.fund.address),
    )


def collect(target, token_id, deadline):
    if target.native_index is not None:
        from almanak.connectors.uniswap_v4.native_liquidity import native_collect

        return native_collect(target, token_id, deadline)
    params = (
        encode(("uint256", "uint256", "uint128", "uint128", "bytes"), (token_id, 0, 0, 0, b"")),
        take_pair(target),
    )
    return modify(target, COLLECT_ACTIONS, params, deadline)


def close(target, token_id, minimum0, minimum1, deadline):
    if target.native_index is not None:
        from almanak.connectors.uniswap_v4.native_liquidity import native_close

        return native_close(target, token_id, minimum0, minimum1, deadline)
    params = (
        encode(("uint256", "uint128", "uint128", "bytes"), (token_id, minimum0, minimum1, b"")),
        take_pair(target),
    )
    return modify(target, CLOSE_ACTIONS, params, deadline)
