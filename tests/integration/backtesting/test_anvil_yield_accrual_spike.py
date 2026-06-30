"""VIB-2630: Spike — Verify Anvil interest accrual with evm_increaseTime + poke.

This test validates the foundational assumption of yield-aware paper trading:
that advancing time on an Anvil fork correctly surfaces on-chain interest
accrual for lending protocols, and measures which protocols need an explicit
"poke" transaction versus which project interest lazily in their view reads.

Tests:
    1. Aave V3 on Arbitrum: supply USDC, advance 24h, measure aUSDC balance
       WITHOUT a poke, then measure the supply(0) poke (receipt status + delta),
       and compare actual interest against getReserveData.currentLiquidityRate.
    2. Compound V3 on Arbitrum (native USDC Comet): supply USDC, advance 24h,
       measure balanceOf WITHOUT a poke, then accrueAccount(wallet) poke, and
       compare actual interest against getSupplyRate(getUtilization()).
    3. Aave V3 poke comparison: eth_call (view) vs eth_sendTransaction.
    4. Morpho Blue on Ethereum: supply USDC into the wstETH/USDC market,
       advance 24h, verify market() storage does NOT move without a poke, then
       accrueInterest(marketParams) and verify position value increases.

Measured findings (2026-06-13, see
docs/internal/archive/reports/spike-vib-2630-anvil-interest-accrual.md):
    - Aave V3 and Compound V3 accrue WITHOUT any poke: their balanceOf
      implementations project the interest index lazily from block.timestamp,
      so evm_increaseTime + evm_mine alone moves the balance.
    - Aave V3 supply(0) REVERTS (ValidationLogic INVALID_AMOUNT, error '26');
      it never worked as a poke. The balance movement previously attributed to
      it came from the time advance alone.
    - Compound V3 accrueAccount(address) is selector 0xbfe69c8d. The previous
      constant 0xf51e181a is scale() — a no-op when sent as a transaction.
    - Morpho Blue is the only protocol of the three that needs a poke:
      position()/market() return raw storage that only updates on
      accrueInterest(marketParams).

Run:
    uv run pytest tests/integration/backtesting/test_anvil_yield_accrual_spike.py \
        -v -s --log-cli-level=INFO --import-mode=importlib -p no:xdist -o addopts=""

Requires:
    - ALCHEMY_API_KEY env var
    - `anvil` binary on PATH
"""

import logging
import os
from decimal import Decimal
from typing import Any

import aiohttp
import pytest
import pytest_asyncio

from almanak.connectors.morpho_blue.addresses import MORPHO_BLUE, MORPHO_MARKETS
from almanak.framework.anvil.fork_manager import RollingForkManager

logger = logging.getLogger(__name__)

# Skip if no Alchemy key
pytestmark = pytest.mark.skipif(
    not os.environ.get("ALCHEMY_API_KEY"),
    reason="ALCHEMY_API_KEY not set",
)

# ---------------------------------------------------------------------------
# Contract addresses (Arbitrum)
# ---------------------------------------------------------------------------
AAVE_V3_POOL_ARBITRUM = "0x794a61358D6845594F94dc1DB02A252b5b4814aD"
USDC_ARBITRUM = "0xaf88d065e77c8cC2239327C5EDb3A432268e5831"
AUSDC_ARBITRUM = "0x724dc807b04555b71ed48a6896b6F41593b8C637"

# Compound V3 native-USDC Comet on Arbitrum. Verified on-chain 2026-06-13:
# baseToken() == 0xaf88...5831 (native USDC). The address previously used here
# (0xA5EDBDD9646f8dFF606d7448e414884C7d905dCA) is the bridged USDC.e Comet
# (baseToken() == 0xFF97...5CC8) — supplying native USDC to it reverts, which
# is why this test used to skip.
COMPOUND_V3_COMET_ARBITRUM = "0x9c4ec768c28520B50860ea7a15bd7213a9fF58bf"

# ---------------------------------------------------------------------------
# Contract addresses (Ethereum — Morpho Blue)
# ---------------------------------------------------------------------------
MORPHO_BLUE_ETHEREUM = MORPHO_BLUE["ethereum"]["morpho"]
USDC_ETHEREUM = "0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48"
# wstETH/USDC (86% LLTV) — top-TVL market in the connector catalogue; the same
# market the production poke_morpho_blue targets.
MORPHO_MARKET_ID = "0xb323495f7e4148be5643a4ea4a8221eef163e4bccfdedc2a6f4696baacbc86cc"

# ---------------------------------------------------------------------------
# Function selectors (all verified with `cast sig` — do not hand-compute)
# ---------------------------------------------------------------------------
BALANCE_OF_SIG = "0x70a08231"  # balanceOf(address)
APPROVE_SIG = "0x095ea7b3"  # approve(address,uint256)
# Aave V3
SUPPLY_SIG = "0x617ba037"  # supply(address,uint256,address,uint16)
GET_RESERVE_DATA_SIG = "0x35ea6a75"  # getReserveData(address)
# Compound V3 (Comet)
ACCRUE_ACCOUNT_SIG = "0xbfe69c8d"  # accrueAccount(address) — NOT 0xf51e181a (=scale())
SUPPLY_COMPOUND_SIG = "0xf2b9fdb8"  # supply(address,uint256)
GET_UTILIZATION_SIG = "0x7eb71131"  # getUtilization()
GET_SUPPLY_RATE_SIG = "0xd955759d"  # getSupplyRate(uint256)
# Morpho Blue
MORPHO_SUPPLY_SIG = "0xa99aad89"  # supply((address,address,address,address,uint256),uint256,uint256,address,bytes)
MORPHO_ACCRUE_SIG = "0x151c1ade"  # accrueInterest((address,address,address,address,uint256))
MORPHO_POSITION_SIG = "0x93c52062"  # position(bytes32,address)
MORPHO_MARKET_SIG = "0x5c60e39a"  # market(bytes32)

ONE_DAY_SECONDS = 86400
SECONDS_PER_YEAR = 31_536_000
RAY = 10**27
SUPPLY_AMOUNT = 1000 * 10**6  # 1000 USDC (6 decimals)

# Tolerance for expected-vs-actual interest. The expected value is a linear
# projection of the rate read right after the supply; the fork is quiet, so
# the only drift sources are second-granularity timestamp bumps from the few
# blocks we mine. 5% relative tolerance is generous.
RATE_TOLERANCE = 0.05


def _pad_address(addr: str) -> str:
    """Left-pad an address to 32 bytes for ABI encoding."""
    return addr.lower().replace("0x", "").zfill(64)


def _pad_uint256(value: int) -> str:
    """Encode a uint256 as 32-byte hex."""
    return hex(value)[2:].zfill(64)


def _encode_call(sig: str, *args: str) -> str:
    """Encode a function call with args."""
    return sig + "".join(args)


def _words(hex_result: str) -> list[int]:
    """Split an ABI-encoded return blob into 32-byte words as ints."""
    body = hex_result[2:] if hex_result.startswith("0x") else hex_result
    return [int(body[i : i + 64], 16) for i in range(0, len(body), 64)]


async def _rpc_full(fork: RollingForkManager, method: str, params: list[Any]) -> dict[str, Any]:
    """Raw JSON-RPC call returning the full response (incl. error objects).

    The fork manager's _rpc_call_raw discards error messages; this helper keeps
    them so revert reasons (e.g. Aave error codes) can be captured as evidence.
    """
    payload = {"jsonrpc": "2.0", "method": method, "params": params, "id": 1}
    async with aiohttp.ClientSession() as session:
        async with session.post(fork.get_rpc_url(), json=payload, timeout=aiohttp.ClientTimeout(total=30)) as resp:
            return await resp.json()


async def _eth_call(fork: RollingForkManager, to: str, data: str) -> str | None:
    """Execute eth_call and return result hex string."""
    return await fork._rpc_call(
        "eth_call",
        [{"to": to, "data": data}, "latest"],
    )


async def _get_balance(fork: RollingForkManager, token: str, wallet: str) -> int:
    """Get ERC-20 balance of wallet."""
    call_data = _encode_call(BALANCE_OF_SIG, _pad_address(wallet))
    result = await _eth_call(fork, token, call_data)
    if result and result != "0x":
        return int(result, 16)
    return 0


async def _send_tx(fork: RollingForkManager, from_addr: str, to: str, data: str, value: str = "0x0") -> str | None:
    """Send a transaction via eth_sendTransaction (auto-impersonate on Anvil).

    Calls evm_mine after sending to ensure the transaction is included in a block
    before returning, preventing race conditions with subsequent balance checks.

    NOTE: a returned tx hash does NOT mean the transaction succeeded — Anvil
    mines reverting transactions too (status 0x0). Use _tx_status() to verify.
    """
    tx_hash = await fork._rpc_call(
        "eth_sendTransaction",
        [{"from": from_addr, "to": to, "data": data, "value": value, "gas": "0x500000"}],
    )
    if tx_hash:
        await fork._rpc_call("evm_mine", [])
    return tx_hash


async def _tx_status(fork: RollingForkManager, tx_hash: str) -> int:
    """Return the receipt status (1 = success, 0 = reverted) for a mined tx."""
    receipt = await fork._rpc_call("eth_getTransactionReceipt", [tx_hash])
    assert receipt is not None, f"No receipt for {tx_hash}"
    return int(receipt["status"], 16)


async def _advance_time(fork: RollingForkManager, seconds: int) -> None:
    """evm_increaseTime + evm_mine."""
    ok, _ = await fork._rpc_call_raw("evm_increaseTime", [seconds])
    assert ok, "evm_increaseTime failed"
    ok, _ = await fork._rpc_call_raw("evm_mine", [])
    assert ok, "evm_mine failed"


async def _aave_current_liquidity_rate(fork: RollingForkManager) -> int:
    """Read currentLiquidityRate (ray, annual) from getReserveData(USDC).

    Return-word layout (verified on-chain 2026-06-13 against the Arbitrum pool):
    word0 = configuration bitmap, word1 = liquidityIndex (ray),
    word2 = currentLiquidityRate (ray), word3 = variableBorrowIndex, ...
    """
    data = _encode_call(GET_RESERVE_DATA_SIG, _pad_address(USDC_ARBITRUM))
    result = await _eth_call(fork, AAVE_V3_POOL_ARBITRUM, data)
    assert result and result != "0x", "getReserveData call failed"
    return _words(result)[2]


def _morpho_supply_calldata(market: dict[str, Any], assets: int, on_behalf: str) -> str:
    """ABI-encode supply(MarketParams, assets, 0 shares, onBehalf, empty bytes).

    MarketParams is a static struct (5 words) so it is inlined in the head.
    Head = 5 struct words + assets + shares + onBehalf + bytes offset = 9 words,
    so the bytes payload starts at offset 9*32 = 0x120; empty bytes = length 0.
    """
    return (
        MORPHO_SUPPLY_SIG
        + _pad_address(market["loan_token_address"])
        + _pad_address(market["collateral_token_address"])
        + _pad_address(market["oracle"])
        + _pad_address(market["irm"])
        + _pad_uint256(market["lltv"])
        + _pad_uint256(assets)
        + _pad_uint256(0)  # shares (exactly one of assets/shares must be zero)
        + _pad_address(on_behalf)
        + _pad_uint256(0x120)  # offset of bytes data
        + _pad_uint256(0)  # bytes length 0 (no callback)
    )


def _morpho_accrue_calldata(market: dict[str, Any]) -> str:
    """ABI-encode accrueInterest(MarketParams)."""
    return (
        MORPHO_ACCRUE_SIG
        + _pad_address(market["loan_token_address"])
        + _pad_address(market["collateral_token_address"])
        + _pad_address(market["oracle"])
        + _pad_address(market["irm"])
        + _pad_uint256(market["lltv"])
    )


async def _morpho_market_state(fork: RollingForkManager, market_id: str) -> dict[str, int]:
    """Read market(id) storage: totals, lastUpdate, fee."""
    data = _encode_call(MORPHO_MARKET_SIG, market_id[2:].zfill(64))
    result = await _eth_call(fork, MORPHO_BLUE_ETHEREUM, data)
    assert result and result != "0x", "market(id) call failed"
    w = _words(result)
    return {
        "total_supply_assets": w[0],
        "total_supply_shares": w[1],
        "total_borrow_assets": w[2],
        "total_borrow_shares": w[3],
        "last_update": w[4],
        "fee": w[5],
    }


async def _morpho_supply_shares(fork: RollingForkManager, market_id: str, wallet: str) -> int:
    """Read position(id, user).supplyShares."""
    data = _encode_call(MORPHO_POSITION_SIG, market_id[2:].zfill(64), _pad_address(wallet))
    result = await _eth_call(fork, MORPHO_BLUE_ETHEREUM, data)
    assert result and result != "0x", "position(id, user) call failed"
    return _words(result)[0]


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest_asyncio.fixture
async def arbitrum_fork():
    """Create a fresh Anvil fork of Arbitrum mainnet."""
    alchemy_key = os.environ["ALCHEMY_API_KEY"]
    rpc_url = f"https://arb-mainnet.g.alchemy.com/v2/{alchemy_key}"

    fork = RollingForkManager(
        rpc_url=rpc_url,
        chain="arbitrum",
        anvil_port=18545,  # Use non-standard port to avoid conflicts
        auto_impersonate=True,
    )

    success = await fork.start()
    assert success, "Failed to start Anvil fork"

    yield fork

    await fork.stop()


@pytest_asyncio.fixture
async def ethereum_fork():
    """Create a fresh Anvil fork of Ethereum mainnet (for Morpho Blue)."""
    alchemy_key = os.environ["ALCHEMY_API_KEY"]
    rpc_url = f"https://eth-mainnet.g.alchemy.com/v2/{alchemy_key}"

    fork = RollingForkManager(
        rpc_url=rpc_url,
        chain="ethereum",
        anvil_port=18546,  # Distinct from the Arbitrum fixture port
        auto_impersonate=True,
    )

    success = await fork.start()
    assert success, "Failed to start Anvil fork"

    yield fork

    await fork.stop()


# ---------------------------------------------------------------------------
# Test: Aave V3 interest accrual
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_aave_v3_interest_accrues_with_time_advance(arbitrum_fork: RollingForkManager):
    """Verify Aave V3 aToken balance increases after time advancement.

    Measures three things the VIB-2630 spike needs:
        1. Does aUSDC balanceOf move after evm_increaseTime + evm_mine ALONE
           (no poke)? Expected yes: AToken.balanceOf scales the stored balance
           by POOL.getReserveNormalizedIncome(), which projects the liquidity
           index to block.timestamp lazily inside the view call.
        2. Does the supply(0) "poke" transaction actually succeed? (It does
           not — Aave V3 ValidationLogic rejects zero amounts.)
        3. Does the measured interest match getReserveData.currentLiquidityRate?
    """
    wallet = "0xf39Fd6e51aad88F6F4ce6aB8827279cffFb92266"  # Anvil default account

    # Step 1: Fund wallet with USDC
    await arbitrum_fork.fund_tokens(wallet, {"USDC": Decimal("2000")})
    usdc_balance = await _get_balance(arbitrum_fork, USDC_ARBITRUM, wallet)
    assert usdc_balance >= SUPPLY_AMOUNT, f"USDC funding failed: balance={usdc_balance}"
    logger.info(f"Wallet funded with {usdc_balance / 1e6:.2f} USDC")

    # Step 2: Approve USDC for Aave V3 Pool
    approve_data = _encode_call(
        APPROVE_SIG,
        _pad_address(AAVE_V3_POOL_ARBITRUM),
        _pad_uint256(SUPPLY_AMOUNT),
    )
    tx = await _send_tx(arbitrum_fork, wallet, USDC_ARBITRUM, approve_data)
    assert tx and await _tx_status(arbitrum_fork, tx) == 1, "Approve TX failed"

    # Step 3: Supply USDC to Aave V3
    # supply(address asset, uint256 amount, address onBehalfOf, uint16 referralCode)
    supply_data = _encode_call(
        SUPPLY_SIG,
        _pad_address(USDC_ARBITRUM),
        _pad_uint256(SUPPLY_AMOUNT),
        _pad_address(wallet),
        _pad_uint256(0),  # referralCode
    )
    tx = await _send_tx(arbitrum_fork, wallet, AAVE_V3_POOL_ARBITRUM, supply_data)
    assert tx and await _tx_status(arbitrum_fork, tx) == 1, "Supply TX failed"

    # Step 4: Record initial aUSDC balance and the current supply rate
    balance_before = await _get_balance(arbitrum_fork, AUSDC_ARBITRUM, wallet)
    assert balance_before > 0, "aUSDC balance is 0 after supply"
    liquidity_rate = await _aave_current_liquidity_rate(arbitrum_fork)
    expected_interest = balance_before * liquidity_rate * ONE_DAY_SECONDS // (SECONDS_PER_YEAR * RAY)
    logger.info(f"aUSDC balance after supply: {balance_before / 1e6:.6f}")
    logger.info(
        f"currentLiquidityRate: {liquidity_rate} ray ({liquidity_rate / RAY * 100:.4f}% APR) "
        f"-> expected 24h interest: {expected_interest / 1e6:.6f} USDC"
    )

    # Step 5: Advance time by 24 hours
    await _advance_time(arbitrum_fork, ONE_DAY_SECONDS)
    logger.info(f"Advanced time by {ONE_DAY_SECONDS}s (24 hours)")

    # Step 6: Measure WITHOUT any poke. AToken.balanceOf projects the liquidity
    # index lazily, so the time advance alone should move the balance.
    balance_no_poke = await _get_balance(arbitrum_fork, AUSDC_ARBITRUM, wallet)
    interest_no_poke = balance_no_poke - balance_before
    logger.info(
        f"aUSDC balance after 24h, NO poke: {balance_no_poke / 1e6:.6f} (interest {interest_no_poke / 1e6:.6f} USDC)"
    )
    assert balance_no_poke > balance_before, (
        f"aUSDC balance did NOT increase after 24h time advance (no poke). "
        f"Before: {balance_before}, After: {balance_no_poke}. "
        f"This means the persistent fork approach for yield paper trading will NOT work."
    )

    # Step 7: Measure the historical supply(0) "poke" — and its receipt status.
    # Aave V3 ValidationLogic.validateSupply requires amount != 0, so this
    # transaction is expected to REVERT (error '26' INVALID_AMOUNT).
    poke_data = _encode_call(
        SUPPLY_SIG,
        _pad_address(USDC_ARBITRUM),
        _pad_uint256(0),
        _pad_address(wallet),
        _pad_uint256(0),
    )
    poke_tx = await _send_tx(arbitrum_fork, wallet, AAVE_V3_POOL_ARBITRUM, poke_data)
    assert poke_tx, "Poke transaction (supply(0)) was not accepted by Anvil"
    poke_status = await _tx_status(arbitrum_fork, poke_tx)
    # The spike's conclusion ("supply(0) is NOT a valid Aave poke") is only true
    # while this reverts. Make it load-bearing so the suite fails — rather than
    # silently passing — if Aave V3 ever starts accepting zero-amount supplies.
    assert poke_status == 0, "supply(0) unexpectedly succeeded — Aave V3 no longer rejects zero amounts"
    # Capture the revert reason via eth_call for the spike evidence.
    sim = await _rpc_full(arbitrum_fork, "eth_call", [{"from": wallet, "to": AAVE_V3_POOL_ARBITRUM, "data": poke_data}, "latest"])
    assert "error" in sim, "supply(0) eth_call no longer reports a revert"
    logger.info(f"supply(0) poke receipt status: {poke_status} (1=success, 0=reverted)")
    logger.info(f"supply(0) eth_call simulation: {sim.get('error', sim.get('result'))}")

    # Step 8: Final balance and expected-vs-actual comparison
    balance_after = await _get_balance(arbitrum_fork, AUSDC_ARBITRUM, wallet)
    interest_earned = balance_after - balance_before
    interest_pct = (interest_earned / balance_before) * 100 if balance_before > 0 else 0
    logger.info(f"aUSDC balance after 24h + poke attempt: {balance_after / 1e6:.6f}")
    logger.info(
        f"Interest earned: {interest_earned / 1e6:.6f} USDC ({interest_pct:.4f}%) "
        f"vs expected {expected_interest / 1e6:.6f} USDC"
    )

    assert balance_after >= balance_no_poke > balance_before
    # Expected-vs-actual: linear projection of the supply-time rate.
    assert expected_interest > 0, "Zero supply rate — pick a different reserve for the spike"
    rel_err = abs(interest_earned - expected_interest) / expected_interest
    assert rel_err < RATE_TOLERANCE, (
        f"Measured interest {interest_earned} deviates {rel_err * 100:.2f}% from "
        f"expected {expected_interest} (currentLiquidityRate projection)"
    )

    # Sanity check: interest should be plausible (0.001% to 1% per day for USDC)
    assert interest_pct < 1.0, f"Interest rate suspiciously high: {interest_pct:.4f}% per day"
    assert interest_pct > 0.0001, f"Interest rate suspiciously low: {interest_pct:.6f}% per day"

    logger.info("SPIKE PASSED: Aave V3 accrues on an Anvil fork from the time advance alone.")
    logger.info(f"  Supply: {SUPPLY_AMOUNT / 1e6:.2f} USDC, duration 24h (simulated)")
    logger.info(f"  Interest: {interest_earned / 1e6:.6f} USDC ({interest_pct:.4f}%/day, ~{interest_pct * 365:.2f}% APY)")
    logger.info(f"  supply(0) poke status: {poke_status} — {'works' if poke_status == 1 else 'REVERTS; not a valid poke'}")


# ---------------------------------------------------------------------------
# Test: Compound V3 interest accrual
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_compound_v3_interest_accrues_with_time_advance(arbitrum_fork: RollingForkManager):
    """Verify Compound V3 balance increases after time advancement.

    Measures:
        1. Does Comet.balanceOf move after evm_increaseTime + evm_mine ALONE
           (no poke)? Expected yes: balanceOf calls accruedInterestIndices()
           which projects the supply index to the current timestamp in-view.
        2. Does accrueAccount(wallet) (selector 0xbfe69c8d) succeed as a poke?
        3. Does the measured interest match getSupplyRate(getUtilization())?

    Uses the native-USDC Comet (0x9c4e...) — the bridged USDC.e Comet
    (0xA5ED...) rejects native USDC and made this test skip historically.
    """
    wallet = "0xf39Fd6e51aad88F6F4ce6aB8827279cffFb92266"

    # Step 1: Fund USDC
    await arbitrum_fork.fund_tokens(wallet, {"USDC": Decimal("2000")})

    # Step 2: Approve USDC for Compound V3
    approve_data = _encode_call(
        APPROVE_SIG,
        _pad_address(COMPOUND_V3_COMET_ARBITRUM),
        _pad_uint256(SUPPLY_AMOUNT),
    )
    tx = await _send_tx(arbitrum_fork, wallet, USDC_ARBITRUM, approve_data)
    assert tx and await _tx_status(arbitrum_fork, tx) == 1, "Approve TX failed"

    # Step 3: Supply USDC to Compound V3 — supply(address asset, uint256 amount)
    supply_data = _encode_call(
        SUPPLY_COMPOUND_SIG,
        _pad_address(USDC_ARBITRUM),
        _pad_uint256(SUPPLY_AMOUNT),
    )
    tx = await _send_tx(arbitrum_fork, wallet, COMPOUND_V3_COMET_ARBITRUM, supply_data)
    assert tx and await _tx_status(arbitrum_fork, tx) == 1, "Compound V3 supply TX failed"

    # Step 4: Record initial balance and current supply rate
    balance_before = await _get_balance(arbitrum_fork, COMPOUND_V3_COMET_ARBITRUM, wallet)
    assert balance_before > 0, (
        "Compound V3 balanceOf is 0 after a successful supply — wrong Comet address?"
    )
    util_result = await _eth_call(arbitrum_fork, COMPOUND_V3_COMET_ARBITRUM, GET_UTILIZATION_SIG)
    assert util_result and util_result != "0x"
    utilization = int(util_result, 16)
    rate_result = await _eth_call(
        arbitrum_fork,
        COMPOUND_V3_COMET_ARBITRUM,
        _encode_call(GET_SUPPLY_RATE_SIG, _pad_uint256(utilization)),
    )
    assert rate_result and rate_result != "0x"
    supply_rate = int(rate_result, 16)  # per-second, 1e18 scale
    expected_interest = balance_before * supply_rate * ONE_DAY_SECONDS // 10**18
    logger.info(f"Compound V3 balance after supply: {balance_before / 1e6:.6f}")
    logger.info(
        f"utilization={utilization / 1e18:.4f}, supplyRate={supply_rate}/s "
        f"({supply_rate * SECONDS_PER_YEAR / 1e18 * 100:.4f}% APR) "
        f"-> expected 24h interest: {expected_interest / 1e6:.6f} USDC"
    )

    # Step 5: Advance time
    await _advance_time(arbitrum_fork, ONE_DAY_SECONDS)

    # Step 6: Measure WITHOUT any poke — Comet.balanceOf projects indices lazily.
    balance_no_poke = await _get_balance(arbitrum_fork, COMPOUND_V3_COMET_ARBITRUM, wallet)
    logger.info(
        f"Compound V3 balance after 24h, NO poke: {balance_no_poke / 1e6:.6f} "
        f"(interest {(balance_no_poke - balance_before) / 1e6:.6f} USDC)"
    )
    assert balance_no_poke > balance_before, (
        f"Compound V3 balanceOf did NOT increase after 24h (no poke). "
        f"Before: {balance_before}, After: {balance_no_poke}"
    )

    # Step 7: Poke — accrueAccount(wallet), the real state-write accrual entrypoint.
    poke_data = _encode_call(ACCRUE_ACCOUNT_SIG, _pad_address(wallet))
    poke_tx = await _send_tx(arbitrum_fork, wallet, COMPOUND_V3_COMET_ARBITRUM, poke_data)
    assert poke_tx, "Compound V3 accrueAccount poke transaction was not accepted"
    poke_status = await _tx_status(arbitrum_fork, poke_tx)
    assert poke_status == 1, "accrueAccount(wallet) reverted — selector or Comet address wrong"
    logger.info("Poke executed: accrueAccount(wallet), status=1")

    # Step 8: Final balance and expected-vs-actual
    balance_after = await _get_balance(arbitrum_fork, COMPOUND_V3_COMET_ARBITRUM, wallet)
    interest_earned = balance_after - balance_before
    interest_pct = (interest_earned / balance_before) * 100 if balance_before > 0 else 0
    logger.info(f"Compound V3 balance after 24h + poke: {balance_after / 1e6:.6f}")
    logger.info(
        f"Interest earned: {interest_earned / 1e6:.6f} USDC ({interest_pct:.4f}%) "
        f"vs expected {expected_interest / 1e6:.6f} USDC"
    )

    assert balance_after >= balance_no_poke > balance_before
    assert expected_interest > 0, "Zero supply rate — pick a different Comet for the spike"
    rel_err = abs(interest_earned - expected_interest) / expected_interest
    assert rel_err < RATE_TOLERANCE, (
        f"Measured interest {interest_earned} deviates {rel_err * 100:.2f}% from "
        f"expected {expected_interest} (getSupplyRate projection)"
    )

    logger.info("SPIKE PASSED: Compound V3 accrues on an Anvil fork from the time advance alone.")
    logger.info(f"  Interest: {interest_earned / 1e6:.6f} USDC (~{interest_pct * 365:.2f}% APY)")


# ---------------------------------------------------------------------------
# Test: Verify whether a poke is needed at all for Aave V3 (eth_call vs TX)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_aave_v3_eth_call_poke_vs_tx_poke(arbitrum_fork: RollingForkManager):
    """Measure whether any poke is needed for Aave V3 balance reads.

    An eth_call cannot mutate state, so if the balance moves after the view
    "poke", the movement came from evm_increaseTime + evm_mine alone — i.e.
    AToken.balanceOf projects the liquidity index lazily and NO poke is needed
    for balance reads. The supply(0) TX is also measured (receipt status) to
    settle whether it ever functioned as a state-writing poke.
    """
    wallet = "0xf39Fd6e51aad88F6F4ce6aB8827279cffFb92266"

    # Fund and supply
    await arbitrum_fork.fund_tokens(wallet, {"USDC": Decimal("2000")})

    # Approve max
    approve_data = _encode_call(
        APPROVE_SIG,
        _pad_address(AAVE_V3_POOL_ARBITRUM),
        "ff" * 32,  # max uint256
    )
    tx = await _send_tx(arbitrum_fork, wallet, USDC_ARBITRUM, approve_data)
    assert tx and await _tx_status(arbitrum_fork, tx) == 1, "Approve TX failed"

    # Supply 1000 USDC
    supply_data = _encode_call(
        SUPPLY_SIG,
        _pad_address(USDC_ARBITRUM),
        _pad_uint256(SUPPLY_AMOUNT),
        _pad_address(wallet),
        _pad_uint256(0),
    )
    tx = await _send_tx(arbitrum_fork, wallet, AAVE_V3_POOL_ARBITRUM, supply_data)
    assert tx and await _tx_status(arbitrum_fork, tx) == 1, "Supply TX failed"

    balance_after_supply = await _get_balance(arbitrum_fork, AUSDC_ARBITRUM, wallet)
    assert balance_after_supply > 0

    # Advance time
    await _advance_time(arbitrum_fork, ONE_DAY_SECONDS)

    # Try eth_call poke first (view-only — CANNOT mutate state)
    poke_data = _encode_call(GET_RESERVE_DATA_SIG, _pad_address(USDC_ARBITRUM))
    await _eth_call(arbitrum_fork, AAVE_V3_POOL_ARBITRUM, poke_data)

    balance_after_view_poke = await _get_balance(arbitrum_fork, AUSDC_ARBITRUM, wallet)

    # Now try supply(0) TX poke and capture its receipt status
    supply_zero_data = _encode_call(
        SUPPLY_SIG,
        _pad_address(USDC_ARBITRUM),
        _pad_uint256(0),  # zero amount — rejected by Aave V3 ValidationLogic
        _pad_address(wallet),
        _pad_uint256(0),
    )
    tx = await _send_tx(arbitrum_fork, wallet, AAVE_V3_POOL_ARBITRUM, supply_zero_data)
    assert tx, "supply(0) transaction was not accepted by Anvil"
    supply_zero_status = await _tx_status(arbitrum_fork, tx)
    assert supply_zero_status == 0, "supply(0) unexpectedly succeeded — Aave V3 no longer rejects zero amounts"

    balance_after_tx_poke = await _get_balance(arbitrum_fork, AUSDC_ARBITRUM, wallet)

    logger.info(f"After supply: {balance_after_supply / 1e6:.6f}")
    logger.info(f"After eth_call (view, cannot mutate): {balance_after_view_poke / 1e6:.6f}")
    logger.info(f"After supply(0) TX (status={supply_zero_status}): {balance_after_tx_poke / 1e6:.6f}")

    # The balance must already have moved BEFORE any transaction: the view call
    # cannot mutate state, so this isolates the effect of the time advance.
    assert balance_after_view_poke > balance_after_supply, (
        "aUSDC balance did not move after evm_increaseTime + evm_mine alone — "
        "lazy index projection assumption broken"
    )

    view_only_accrual = balance_after_view_poke - balance_after_supply
    tx_poke_extra = balance_after_tx_poke - balance_after_view_poke
    logger.info(
        f"Accrual from time advance alone: {view_only_accrual / 1e6:.6f} USDC; "
        f"extra from supply(0) TX: {tx_poke_extra / 1e6:.6f} USDC "
        f"(expected ~0: the TX only adds ~1s of block timestamp)"
    )
    logger.info(
        "Conclusion: Aave V3 needs NO poke for balanceOf reads on a persistent "
        f"fork; supply(0) is {'a working poke' if supply_zero_status == 1 else 'NOT a valid poke (reverts)'}"
    )


# ---------------------------------------------------------------------------
# Test: Morpho Blue interest accrual (Ethereum) — the protocol that NEEDS a poke
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_morpho_blue_interest_accrual_ethereum(ethereum_fork: RollingForkManager):
    """Verify Morpho Blue accrual semantics on an Ethereum mainnet fork.

    Morpho Blue stores positions as shares and market totals as raw storage
    (no lazy projection in position()/market() — those are plain storage
    reads). Interest lands in totalSupplyAssets only when accrueInterest()
    runs. So unlike Aave/Compound, Morpho genuinely needs the poke for
    storage-based position valuation — which is exactly what the framework's
    MORPHO_BLUE_ACCOUNT_STATE_READ spec consumes.

    Steps:
        1. Supply 1000 USDC into the wstETH/USDC market (loan token = USDC)
        2. Record position shares + market totals -> position value
        3. Advance 24h; verify market() storage does NOT move (no poke)
        4. accrueInterest(marketParams) poke; verify totals + position value rise
    """
    wallet = "0xf39Fd6e51aad88F6F4ce6aB8827279cffFb92266"
    market = MORPHO_MARKETS["ethereum"][MORPHO_MARKET_ID]

    # Step 0: Market must exist and have outstanding borrows (no borrows = no interest)
    state0 = await _morpho_market_state(ethereum_fork, MORPHO_MARKET_ID)
    assert state0["last_update"] > 0, f"Market {MORPHO_MARKET_ID} does not exist on this fork"
    if state0["total_borrow_assets"] == 0:
        pytest.skip("Market has zero borrows — no interest to accrue; pick a different market")
    logger.info(
        f"Market {market['name']}: totalSupplyAssets={state0['total_supply_assets'] / 1e6:.2f} USDC, "
        f"totalBorrowAssets={state0['total_borrow_assets'] / 1e6:.2f} USDC, "
        f"utilization={state0['total_borrow_assets'] / state0['total_supply_assets'] * 100:.2f}%"
    )

    # Step 1: Fund + approve + supply
    await ethereum_fork.fund_tokens(wallet, {"USDC": Decimal("2000")})
    usdc_balance = await _get_balance(ethereum_fork, USDC_ETHEREUM, wallet)
    assert usdc_balance >= SUPPLY_AMOUNT, f"USDC funding failed: balance={usdc_balance}"

    approve_data = _encode_call(APPROVE_SIG, _pad_address(MORPHO_BLUE_ETHEREUM), _pad_uint256(SUPPLY_AMOUNT))
    tx = await _send_tx(ethereum_fork, wallet, USDC_ETHEREUM, approve_data)
    assert tx and await _tx_status(ethereum_fork, tx) == 1, "Approve TX failed"

    supply_data = _morpho_supply_calldata(market, SUPPLY_AMOUNT, wallet)
    tx = await _send_tx(ethereum_fork, wallet, MORPHO_BLUE_ETHEREUM, supply_data)
    assert tx and await _tx_status(ethereum_fork, tx) == 1, "Morpho Blue supply TX failed"

    # Step 2: Position value = shares * totalSupplyAssets / totalSupplyShares
    shares = await _morpho_supply_shares(ethereum_fork, MORPHO_MARKET_ID, wallet)
    assert shares > 0, "supplyShares is 0 after supply"
    state1 = await _morpho_market_state(ethereum_fork, MORPHO_MARKET_ID)
    assets_before = shares * state1["total_supply_assets"] // state1["total_supply_shares"]
    logger.info(f"Position after supply: {shares} shares = {assets_before / 1e6:.6f} USDC")

    # Step 3: Advance 24h — storage must NOT move without a poke
    await _advance_time(ethereum_fork, ONE_DAY_SECONDS)
    state_no_poke = await _morpho_market_state(ethereum_fork, MORPHO_MARKET_ID)
    shares_no_poke = await _morpho_supply_shares(ethereum_fork, MORPHO_MARKET_ID, wallet)
    logger.info(
        f"After 24h, NO poke: totalSupplyAssets={state_no_poke['total_supply_assets']} "
        f"(was {state1['total_supply_assets']}), lastUpdate={state_no_poke['last_update']}"
    )
    assert state_no_poke["total_supply_assets"] == state1["total_supply_assets"], (
        "market() storage moved without a poke — unexpected for Morpho Blue"
    )
    assert shares_no_poke == shares, "supplyShares changed without any transaction"

    # Step 4: Poke — accrueInterest(marketParams)
    poke_tx = await _send_tx(ethereum_fork, wallet, MORPHO_BLUE_ETHEREUM, _morpho_accrue_calldata(market))
    assert poke_tx, "accrueInterest poke transaction was not accepted"
    poke_status = await _tx_status(ethereum_fork, poke_tx)
    assert poke_status == 1, "accrueInterest(marketParams) reverted — market params mismatch?"
    logger.info("Poke executed: accrueInterest(marketParams), status=1")

    state2 = await _morpho_market_state(ethereum_fork, MORPHO_MARKET_ID)
    assets_after = shares * state2["total_supply_assets"] // state2["total_supply_shares"]
    interest_earned = assets_after - assets_before
    interest_pct = interest_earned / assets_before * 100
    logger.info(
        f"After poke: totalSupplyAssets={state2['total_supply_assets']} "
        f"(+{state2['total_supply_assets'] - state1['total_supply_assets']}), "
        f"lastUpdate={state2['last_update']}"
    )
    logger.info(
        f"Position value: {assets_before / 1e6:.6f} -> {assets_after / 1e6:.6f} USDC "
        f"(interest {interest_earned / 1e6:.6f} USDC, {interest_pct:.4f}%/day, ~{interest_pct * 365:.2f}% APY)"
    )

    assert state2["total_supply_assets"] > state1["total_supply_assets"], (
        "accrueInterest did not increase totalSupplyAssets"
    )
    assert assets_after > assets_before, "Position value did not increase after poke"
    # Sanity: plausible daily interest for a stablecoin loan market
    assert 0.0001 < interest_pct < 1.0, f"Implied daily rate implausible: {interest_pct:.6f}%"

    logger.info("SPIKE PASSED: Morpho Blue requires (and responds to) the accrueInterest poke.")
