// SPDX-License-Identifier: MIT
pragma solidity 0.8.26;

struct PoolKey {
    address currency0;
    address currency1;
    uint24 fee;
    int24 tickSpacing;
    address hooks;
}

struct SwapParams {
    bool zeroForOne;
    int256 amountSpecified;
    uint160 sqrtPriceLimitX96;
}

interface IPoolManager {
    function updateDynamicLPFee(PoolKey calldata key, uint24 newDynamicLPFee) external;
}

/// @dev Test-only family: no proxies, dependencies or token transfers in callbacks.
contract DynamicFeeFixture {
    address public immutable poolManager;
    address public immutable owner;
    bool public immutable useOverride;
    uint24 public currentFee = 1000;

    constructor(address manager, address authorizedOwner, bool overrideMode) {
        poolManager = manager;
        owner = authorizedOwner;
        useOverride = overrideMode;
    }

    function afterInitialize(address, PoolKey calldata key, uint160, int24) external returns (bytes4) {
        require(msg.sender == poolManager);
        IPoolManager(poolManager).updateDynamicLPFee(key, currentFee);
        return this.afterInitialize.selector;
    }

    function beforeSwap(address, PoolKey calldata, SwapParams calldata, bytes calldata hookData)
        external view returns (bytes4, int256, uint24)
    {
        require(msg.sender == poolManager && useOverride);
        require(keccak256(hookData) == keccak256(hex"1234"));
        return (this.beforeSwap.selector, 0, currentFee | 0x400000);
    }

    function setFee(PoolKey calldata key, uint24 nextFee) external {
        require(msg.sender == owner && nextFee <= 1000000);
        currentFee = nextFee;
        if (!useOverride) IPoolManager(poolManager).updateDynamicLPFee(key, nextFee);
    }
}
