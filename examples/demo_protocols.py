#!/usr/bin/env python3
"""
===============================================================================
ALMANAK SDK DEMO: Multi-Protocol DeFi Interactions on Arbitrum
===============================================================================

This showcase demonstrates the power of the Almanak SDK by executing
multiple protocol interactions across DeFi protocols on Arbitrum.

PROTOCOLS DEMONSTRATED (IntentCompiler-supported):
--------------------------------------------------
1. Uniswap V3     - Swap (WETH -> USDC)
2. Aave V3        - Supply (USDC as collateral)
3. Aave V3        - Borrow (WETH against collateral)
4. GMX V2         - [SKIP on Anvil - requires keeper execution]
5. Enso           - Aggregated Swap (USDC -> ARB via best route)
6. Enso           - Aggregated Swap (ARB -> WETH via Camelot/SushiSwap)
7. Uniswap V3     - Stable Swap (USDC -> USDT)
8. Enso           - Swap (USDT -> wstETH, multi-hop route)
9. Pendle         - Yield Tokenization (wstETH -> PT-wstETH)
10. Uniswap V3    - Add LP (WETH/USDC concentrated position)

NOTE: Ethena (USDe/sUSDe staking) is only available on Ethereum mainnet.
While the SDK has connectors for Curve, Compound, TraderJoe, etc.,
they are not yet wired into the IntentCompiler. This demo uses protocols
that work end-to-end via the Intent -> Compile -> Execute pipeline.

TEARDOWN:
---------
After demonstrating all protocols, the script executes a complete teardown
to convert all positions back to WETH.

USAGE:
------
    # Full demo with real on-chain execution (requires Anvil + gateway)
    # Terminal 1: Start Anvil fork
    anvil --fork-url https://arb-mainnet.g.alchemy.com/v2/<KEY> --port 8545

    # Terminal 2: Start gateway
    almanak gateway --network anvil

    # Terminal 3: Run demo
    python examples/demo_10_protocols.py

    # Dry-run mode (shows intents without executing)
    python examples/demo_10_protocols.py --dry-run

    # Quick mode (fewer interactions for faster testing)
    python examples/demo_10_protocols.py --quick

PREREQUISITES:
--------------
1. Anvil running with Arbitrum fork: anvil --fork-url <RPC> --port 8545
2. Gateway running: almanak gateway --network anvil
3. Environment variables:
   - ALCHEMY_API_KEY (for prices)
   - ALMANAK_PRIVATE_KEY (optional, uses Anvil default)

===============================================================================
"""

import argparse
import asyncio
import logging
import os
import subprocess
import sys
import time
from dataclasses import dataclass, field
from decimal import Decimal
from pathlib import Path

# Add project root to path if needed
project_root = Path(__file__).parent.parent
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)


# =============================================================================
# CONFIGURATION
# =============================================================================


# Default Anvil test wallet (account #0)
ANVIL_WALLET = "0xf39Fd6e51aad88F6F4ce6aB8827279cffFb92266"
ANVIL_PRIVATE_KEY = "0xac0974bec39a17e36ba4a6b4d238ff944bacb478cbed5efcae784d7bf4f2ff80"


@dataclass
class DemoConfig:
    """Configuration for the 10 protocols demo."""

    # Chain configuration
    chain: str = "arbitrum"
    rpc_url: str = "http://127.0.0.1:8545"

    # Wallet configuration
    wallet_address: str = ANVIL_WALLET
    private_key: str = ANVIL_PRIVATE_KEY

    # Initial amounts (assuming funded wallet)
    initial_weth: Decimal = field(default_factory=lambda: Decimal("2.0"))

    # Allocation per protocol (approximate)
    swap_amount_usd: Decimal = field(default_factory=lambda: Decimal("500"))
    supply_amount_usd: Decimal = field(default_factory=lambda: Decimal("300"))
    lp_amount_usd: Decimal = field(default_factory=lambda: Decimal("200"))
    perp_collateral_weth: Decimal = field(default_factory=lambda: Decimal("0.1"))

    # Risk parameters
    max_slippage: Decimal = field(default_factory=lambda: Decimal("0.01"))  # 1%
    perp_leverage: Decimal = field(default_factory=lambda: Decimal("2.0"))

    # Aave parameters
    aave_ltv: Decimal = field(default_factory=lambda: Decimal("0.5"))  # 50% LTV

    # Execution modes
    dry_run: bool = False
    quick_mode: bool = False


# Token addresses on Arbitrum
ARBITRUM_TOKENS = {
    "WETH": "0x82aF49447D8a07e3bd95BD0d56f35241523fBab1",
    "USDC": "0xaf88d065e77c8cC2239327C5EDb3A432268e5831",  # Native USDC
    "USDC.e": "0xFF970A61A04b1cA14834A43f5dE4533eBDDB5CC8",  # Bridged USDC
    "USDT": "0xFd086bC7CD5C481DCC9C85ebE478A1C0b69FCbb9",
    "ARB": "0x912CE59144191C1204E64559FE8253a0e49E6548",
    "LINK": "0xf97f4df75117a78c1A5a0DBb814Af92458539FB4",
    "WBTC": "0x2f2a2543B76A4166549F7aaB2e75Bef0aefC5B0f",
}


# =============================================================================
# WALLET FUNDING
# =============================================================================


def fund_wallet(config: DemoConfig) -> bool:
    """Fund the test wallet with required tokens on Anvil."""
    print_header("FUNDING WALLET")
    print(f"Wallet: {config.wallet_address}")
    print(f"RPC: {config.rpc_url}")
    print("")

    rpc = config.rpc_url
    wallet = config.wallet_address

    try:
        # 1. Fund with native ETH for gas (100 ETH)
        print("   [1/3] Funding with 100 ETH for gas...")
        subprocess.run(
            ["cast", "rpc", "anvil_setBalance", wallet, "0x56BC75E2D63100000", "--rpc-url", rpc],
            check=True,
            capture_output=True,
        )

        # Verify ETH balance
        result = subprocess.run(
            ["cast", "balance", wallet, "--rpc-url", rpc, "--ether"],
            check=True,
            capture_output=True,
            text=True,
        )
        eth_balance = result.stdout.strip()
        print(f"        ETH Balance: {eth_balance}")

        # 2. Fund with WETH by wrapping ETH
        print(f"   [2/3] Wrapping {config.initial_weth} ETH to WETH...")
        weth_amount_wei = int(config.initial_weth * Decimal("1e18"))

        # Wrap ETH to WETH (send ETH to WETH contract on Arbitrum)
        subprocess.run(
            [
                "cast", "send", ARBITRUM_TOKENS["WETH"],
                "--value", str(weth_amount_wei),
                "--from", wallet,
                "--private-key", config.private_key,
                "--rpc-url", rpc,
            ],
            check=True,
            capture_output=True,
        )

        # Verify WETH balance
        result = subprocess.run(
            ["cast", "call", ARBITRUM_TOKENS["WETH"], "balanceOf(address)(uint256)", wallet, "--rpc-url", rpc],
            check=True,
            capture_output=True,
            text=True,
        )
        # Parse cast output: "2000000000000000000 [2e18]" -> extract first number
        weth_str = result.stdout.strip().split()[0]
        weth_balance = int(weth_str, 16) if weth_str.startswith("0x") else int(weth_str)
        print(f"        WETH Balance: {weth_balance / 1e18:.4f}")

        # 3. Fund with USDC (100,000 USDC for various operations)
        print("   [3/3] Funding with 100,000 USDC...")
        usdc_amount = 100_000_000_000  # 100,000 USDC (6 decimals)

        # Calculate storage slot for USDC balance
        result = subprocess.run(
            ["cast", "index", "address", wallet, "9"],  # USDC balance slot is 9
            check=True,
            capture_output=True,
            text=True,
        )
        storage_slot = result.stdout.strip()

        # Set USDC balance
        subprocess.run(
            ["cast", "rpc", "anvil_setStorageAt", ARBITRUM_TOKENS["USDC"], storage_slot, f"0x{usdc_amount:064x}", "--rpc-url", rpc],
            check=True,
            capture_output=True,
        )

        # Verify USDC balance
        result = subprocess.run(
            ["cast", "call", ARBITRUM_TOKENS["USDC"], "balanceOf(address)(uint256)", wallet, "--rpc-url", rpc],
            check=True,
            capture_output=True,
            text=True,
        )
        # Parse cast output: "100000000000 [1e11]" -> extract first number
        usdc_str = result.stdout.strip().split()[0]
        usdc_balance = int(usdc_str, 16) if usdc_str.startswith("0x") else int(usdc_str)
        print(f"        USDC Balance: {usdc_balance / 1e6:,.2f}")

        print("")
        print("   Wallet funded successfully!")
        return True

    except subprocess.CalledProcessError as e:
        print(f"   ERROR: Failed to fund wallet: {e}")
        print(f"   stderr: {e.stderr.decode() if e.stderr else 'N/A'}")
        return False
    except FileNotFoundError:
        print("   ERROR: 'cast' command not found. Please install Foundry.")
        print("   Run: curl -L https://foundry.paradigm.xyz | bash && foundryup")
        return False


# =============================================================================
# DEMO EXECUTION CLASS
# =============================================================================


class TenProtocolsDemo:
    """Demonstrates 10 protocol interactions on Arbitrum with real execution."""

    def __init__(self, config: DemoConfig):
        self.config = config
        self.intents_generated: list[dict] = []
        self.execution_results: list[dict] = []
        self.execution_times: dict[str, float] = {}
        self.positions: dict[str, dict] = {}
        self.lp_position_ids: dict[str, str | None] = {}

        # Execution infrastructure (lazy initialized)
        self._orchestrator = None
        self._compiler = None

    async def _init_execution_infrastructure(self) -> bool:
        """Initialize execution infrastructure."""
        try:
            from almanak.framework.execution import (
                ExecutionOrchestrator,
                LocalKeySigner,
                PublicMempoolSubmitter,
                DirectSimulator,
            )
            from almanak.framework.intents.compiler import IntentCompiler, IntentCompilerConfig

            # Create signer
            signer = LocalKeySigner(private_key=self.config.private_key)
            print(f"   Signer initialized: {signer.address}")

            # Create submitter
            submitter = PublicMempoolSubmitter(
                rpc_url=self.config.rpc_url,
                max_retries=3,
                timeout_seconds=120,
            )

            # Create simulator (direct pass-through for Anvil)
            simulator = DirectSimulator()

            # Create orchestrator (uses default risk config which is permissive enough for Anvil)
            self._orchestrator = ExecutionOrchestrator(
                signer=signer,
                submitter=submitter,
                simulator=simulator,
                chain=self.config.chain,
                rpc_url=self.config.rpc_url,
            )

            # Create compiler with price oracle
            # For demo, we use placeholder prices (they get refined during compilation)
            self._compiler = IntentCompiler(
                chain=self.config.chain,
                wallet_address=self.config.wallet_address,
                rpc_url=self.config.rpc_url,  # Required for Pendle and other protocols
                config=IntentCompilerConfig(
                    allow_placeholder_prices=True,  # OK for demo on Anvil
                ),
            )

            print("   Execution infrastructure initialized!")
            return True

        except ImportError as e:
            print(f"   ERROR: Failed to import execution modules: {e}")
            return False
        except Exception as e:
            print(f"   ERROR: Failed to initialize execution: {e}")
            return False

    # =========================================================================
    # MAIN DEMO ORCHESTRATION
    # =========================================================================

    async def run(self) -> None:
        """Execute the full demo."""
        print_header("ALMANAK SDK - 10 PROTOCOL DEMO")
        print(f"Chain: {self.config.chain.upper()}")
        print(f"Mode: {'DRY RUN (intent generation only)' if self.config.dry_run else 'LIVE EXECUTION (on-chain transactions)'}")
        print(f"Wallet: {self.config.wallet_address}")
        print(f"RPC: {self.config.rpc_url}")
        print("")

        total_start = time.time()

        # Initialize execution infrastructure
        if not self.config.dry_run:
            print_header("INITIALIZING EXECUTION", char="-")
            if not await self._init_execution_infrastructure():
                print("ERROR: Failed to initialize execution infrastructure")
                return

        # Phase 1: Build Positions
        print_header("PHASE 1: BUILDING POSITIONS", char="-")
        await self._build_positions()

        # Phase 2: Position Summary
        print_header("PHASE 2: POSITION SUMMARY", char="-")
        self._print_positions()

        # Phase 3: Teardown
        print_header("PHASE 3: TEARDOWN", char="-")
        await self._teardown_positions()

        # Final Summary
        total_time = time.time() - total_start
        print_header("DEMO COMPLETE")
        self._print_summary(total_time)

    # =========================================================================
    # INTENT EXECUTION
    # =========================================================================

    async def _execute_intent(self, name: str, intent, step_start: float) -> bool:
        """Compile and execute an intent, returning success status."""
        from almanak.framework.intents.compiler import CompilationStatus
        from almanak.framework.execution.orchestrator import ExecutionContext

        self._record_intent(name, intent, step_start)

        if self.config.dry_run:
            print(f"   [DRY RUN] Intent recorded: {intent.intent_type.value}")
            return True

        if not self._compiler or not self._orchestrator:
            print("   ERROR: Execution infrastructure not initialized")
            return False

        try:
            # Compile intent to ActionBundle
            compilation_result = self._compiler.compile(intent)

            if compilation_result.status != CompilationStatus.SUCCESS:
                print(f"   COMPILATION FAILED: {compilation_result.error}")
                self.execution_results.append({
                    "name": name,
                    "success": False,
                    "error": f"Compilation: {compilation_result.error}",
                })
                return False

            action_bundle = compilation_result.action_bundle
            print(f"   Compiled: {len(action_bundle.transactions)} transaction(s)")

            # Execute the ActionBundle
            context = ExecutionContext(
                strategy_id="demo_10_protocols",
                intent_id=f"{name}_{int(time.time())}",
                chain=self.config.chain,
                wallet_address=self.config.wallet_address,
                simulation_enabled=False,  # Skip simulation on Anvil
            )

            execution_result = await self._orchestrator.execute(action_bundle, context)

            if execution_result.success:
                tx_hashes = [tr.tx_hash[:16] + "..." for tr in execution_result.transaction_results]
                print(f"   EXECUTED: {', '.join(tx_hashes)}")
                print(f"   Gas used: {execution_result.total_gas_used:,}")

                self.execution_results.append({
                    "name": name,
                    "success": True,
                    "tx_hashes": [tr.tx_hash for tr in execution_result.transaction_results],
                    "gas_used": execution_result.total_gas_used,
                    "position_id": execution_result.position_id,
                })

                # Store position ID if returned (for LP positions)
                if execution_result.position_id:
                    self.lp_position_ids[name] = str(execution_result.position_id)

                return True
            else:
                print(f"   EXECUTION FAILED: {execution_result.error}")
                self.execution_results.append({
                    "name": name,
                    "success": False,
                    "error": execution_result.error,
                })
                return False

        except Exception as e:
            print(f"   EXECUTION ERROR: {e}")
            self.execution_results.append({
                "name": name,
                "success": False,
                "error": str(e),
            })
            return False

    # =========================================================================
    # POSITION BUILDING
    # =========================================================================

    async def _build_positions(self) -> None:
        """Execute all 10 protocol interactions."""
        from almanak.framework.intents import Intent

        # Step 1: Uniswap V3 - Swap WETH -> USDC
        await self._step_1_uniswap_swap()

        # Step 2: Aave V3 - Supply USDC
        await self._step_2_aave_supply()

        # Step 3: Aave V3 - Borrow WETH
        await self._step_3_aave_borrow()

        # Step 4: GMX V2 - Open Perp Long
        await self._step_4_gmx_perp()

        if not self.config.quick_mode:
            # Step 5: Enso - Swap USDC -> ARB
            await self._step_5_enso_swap()

            # Step 6: Camelot/Enso - Swap ARB -> WETH
            await self._step_6_camelot_swap()

            # Step 7: Uniswap V3 - Stable Swap USDC -> USDT
            await self._step_7_stableswap()

            # Step 8: Enso - Swap USDT -> wstETH (get wstETH for Pendle)
            await self._step_8_enso_usdt_wsteth_swap()

            # Step 9: Pendle - Yield tokenization (wstETH -> PT-wstETH)
            await self._step_9_pendle_yield_tokenization()

            # Step 10: Uniswap V3 - Add LP
            await self._step_10_uniswap_lp()
        else:
            print("\n[Quick mode: Skipping steps 5-10]")

    # =========================================================================
    # INDIVIDUAL PROTOCOL STEPS
    # =========================================================================

    async def _step_1_uniswap_swap(self) -> None:
        """Step 1: Swap WETH -> USDC on Uniswap V3."""
        from almanak.framework.intents import Intent

        step_start = time.time()
        print_step(1, "Uniswap V3", "SWAP", "WETH -> USDC")

        # Calculate amount to swap (keep some for later steps)
        swap_amount = self.config.initial_weth * Decimal("0.4")

        intent = Intent.swap(
            from_token="WETH",
            to_token="USDC",
            amount=swap_amount,
            max_slippage=self.config.max_slippage,
            protocol="uniswap_v3",
        )

        print(f"   Amount: {swap_amount} WETH")
        await self._execute_intent("uniswap_swap", intent, step_start)

    async def _step_2_aave_supply(self) -> None:
        """Step 2: Supply USDC to Aave V3."""
        from almanak.framework.intents import Intent

        step_start = time.time()
        print_step(2, "Aave V3", "SUPPLY", "USDC as collateral")

        supply_amount = self.config.supply_amount_usd

        intent = Intent.supply(
            protocol="aave_v3",
            token="USDC",
            amount=supply_amount,
            use_as_collateral=True,
            chain=self.config.chain,
        )

        print(f"   Amount: ${supply_amount} USDC")
        print("   Use as Collateral: True")

        success = await self._execute_intent("aave_supply", intent, step_start)
        if success:
            self.positions["aave_supply"] = {
                "type": "SUPPLY",
                "token": "USDC",
                "amount": supply_amount,
            }

    async def _step_3_aave_borrow(self) -> None:
        """Step 3: Borrow WETH from Aave V3."""
        from almanak.framework.intents import Intent

        step_start = time.time()
        print_step(3, "Aave V3", "BORROW", "WETH against USDC collateral")

        borrow_amount = Decimal("0.05")

        intent = Intent.borrow(
            protocol="aave_v3",
            collateral_token="USDC",
            collateral_amount=Decimal("0"),  # Already supplied
            borrow_token="WETH",
            borrow_amount=borrow_amount,
            interest_rate_mode="variable",
            chain=self.config.chain,
        )

        print(f"   Borrow Amount: {borrow_amount} WETH")
        print("   Interest Rate: Variable")

        success = await self._execute_intent("aave_borrow", intent, step_start)
        if success:
            self.positions["aave_borrow"] = {
                "type": "BORROW",
                "token": "WETH",
                "amount": borrow_amount,
            }

    async def _step_4_gmx_perp(self) -> None:
        """Step 4: Open perpetual long on GMX V2.

        NOTE: GMX V2 uses an asynchronous order system - orders are created
        then executed by keepers. On Anvil forks without keepers, we skip
        this step as orders won't be executed.
        """
        print_step(4, "GMX V2", "PERP_OPEN", "Long ETH/USD")

        # GMX V2 orders require keeper execution which doesn't work on Anvil forks
        # Skip in quick mode or when not running with GMX infrastructure
        print("   [SKIP] GMX V2 requires keeper execution (not available on Anvil fork)")
        print("   In production, this would create a market order for keepers to execute.")

        # Record as skipped for summary
        self.intents_generated.append({
            "name": "gmx_perp",
            "intent_type": "PERP_OPEN",
            "time_ms": 0,
            "intent": None,
            "skipped": True,
            "reason": "GMX V2 requires keeper execution",
        })
        return

        # Original code preserved for reference:
        # collateral = self.config.perp_collateral_weth
        # position_size = collateral * Decimal("3400") * self.config.perp_leverage
        # intent = Intent.perp_open(
        #     market="ETH/USD",
        #     collateral_token="WETH",
        #     collateral_amount=collateral,
        #     size_usd=position_size,
        #     is_long=True,
        #     leverage=self.config.perp_leverage,
        #     max_slippage=self.config.max_slippage,
        #     protocol="gmx_v2",
        # )

    async def _step_5_enso_swap(self) -> None:
        """Step 5: Swap USDC -> ARB via Enso aggregator."""
        from almanak.framework.intents import Intent

        step_start = time.time()
        print_step(5, "Enso", "SWAP", "USDC -> ARB (aggregated)")

        swap_amount = Decimal("100")  # 100 USDC

        intent = Intent.swap(
            from_token="USDC",
            to_token="ARB",
            amount=swap_amount,
            max_slippage=self.config.max_slippage,
            protocol="enso",
        )

        print(f"   Amount: ${swap_amount} USDC")
        print("   Routing: Enso multi-DEX aggregation")

        await self._execute_intent("enso_swap", intent, step_start)

    async def _step_6_camelot_swap(self) -> None:
        """Step 6: Swap ARB -> WETH via Camelot (Arbitrum native DEX)."""
        from almanak.framework.intents import Intent

        step_start = time.time()
        print_step(6, "Camelot", "SWAP", "ARB -> WETH (native Arbitrum DEX)")

        # Swap some ARB back to WETH using Enso aggregator (routes through Camelot)
        swap_amount = Decimal("100")  # ~$100 worth of ARB

        intent = Intent.swap(
            from_token="ARB",
            to_token="WETH",
            amount=swap_amount,
            max_slippage=Decimal("0.02"),  # 2% slippage
            protocol="enso",  # Enso routes through Camelot on Arbitrum
        )

        print(f"   Amount: {swap_amount} ARB -> WETH")
        print("   Router: Enso aggregation (routes via Camelot)")

        await self._execute_intent("camelot_swap", intent, step_start)

    async def _step_7_stableswap(self) -> None:
        """Step 7: Swap USDC -> USDT via Uniswap V3 stable pool."""
        from almanak.framework.intents import Intent

        step_start = time.time()
        print_step(7, "Uniswap V3", "SWAP", "USDC -> USDT (stable swap)")

        swap_amount = Decimal("100")

        # Use Uniswap V3's low-fee stable pool for stablecoin swaps
        intent = Intent.swap(
            from_token="USDC",
            to_token="USDT",
            amount=swap_amount,
            max_slippage=Decimal("0.005"),  # 0.5% for stables
            protocol="uniswap_v3",
        )

        print(f"   Amount: ${swap_amount} USDC -> USDT")
        print("   Pool: Uniswap V3 stable pool (0.01% fee tier)")

        await self._execute_intent("stable_swap", intent, step_start)

    async def _step_8_enso_usdt_wsteth_swap(self) -> None:
        """Step 8: Swap USDT -> wstETH via Enso (multi-hop aggregated route)."""
        from almanak.framework.intents import Intent

        step_start = time.time()
        print_step(8, "Enso", "SWAP", "USDT -> wstETH (for Pendle)")

        # Swap USDT to wstETH via Enso - this gets us the token needed for Pendle
        # wstETH is Lido's wrapped staked ETH (liquid staking token)
        # Step 7 produces ~99 USDT, we use 100 to get more wstETH for Pendle (needs ~0.05)
        swap_amount = Decimal("100")

        # Use Enso aggregator for multi-hop USDT -> wstETH route
        intent = Intent.swap(
            from_token="USDT",
            to_token="WSTETH",
            amount=swap_amount,
            max_slippage=Decimal("0.05"),  # 5% slippage for multi-hop
            protocol="enso",
        )

        print(f"   Amount: ${swap_amount} USDT -> wstETH")
        print("   Router: Enso (aggregated multi-hop route)")

        await self._execute_intent("usdt_wsteth_swap", intent, step_start)

    async def _step_9_pendle_yield_tokenization(self) -> None:
        """Step 9: Swap wstETH -> PT-wstETH via Pendle (yield tokenization)."""
        from almanak.framework.intents import Intent

        step_start = time.time()
        print_step(9, "Pendle", "SWAP", "wstETH -> PT-wstETH (yield tokenization)")

        # Use current wstETH market on Arbitrum
        # Available PT tokens: PT-WSTETH, PT-wstETH
        # Note: Using larger amount (0.05) to avoid minimum amount issues on Anvil forks
        swap_amount = Decimal("0.05")

        intent = Intent.swap(
            from_token="WSTETH",
            to_token="PT-wstETH",
            amount=swap_amount,
            max_slippage=Decimal("0.10"),  # 10% slippage for PT pricing
            protocol="pendle",
        )

        print(f"   Amount: {swap_amount} wstETH -> PT-wstETH")
        print("   Market: Pendle wstETH (current active market)")
        print("   Strategy: Fixed yield via Principal Token discount")

        await self._execute_intent("pendle_pt_swap", intent, step_start)

    async def _step_10_uniswap_lp(self) -> None:
        """Step 10: Add concentrated liquidity on Uniswap V3."""
        from almanak.framework.intents import Intent

        step_start = time.time()
        print_step(10, "Uniswap V3", "LP_OPEN", "WETH/USDC concentrated LP")

        # Use smaller amounts with wide range to ensure LP succeeds
        # Current ETH price is ~$3400, so we use a range that includes this
        amount_weth = Decimal("0.01")
        amount_usdc = Decimal("34")  # ~$34 to match WETH value at $3400

        # Use a very wide range around current price to maximize success
        intent = Intent.lp_open(
            pool="WETH/USDC/500",  # 0.05% fee tier
            amount0=amount_weth,
            amount1=amount_usdc,
            range_lower=Decimal("2000"),  # Wide price range lower
            range_upper=Decimal("5000"),  # Wide price range upper
            protocol="uniswap_v3",
        )

        print("   Pool: WETH/USDC (0.05% fee tier)")
        print(f"   Amount: {amount_weth} WETH + ${amount_usdc} USDC")
        print("   Range: $2,000 - $5,000 (wide for demo)")

        success = await self._execute_intent("uniswap_lp", intent, step_start)
        if success:
            # Only store position_id if actually returned - None means teardown will be skipped
            position_id = self.execution_results[-1].get("position_id")
            if position_id:
                self.lp_position_ids["uniswap_v3"] = position_id
            self.positions["uniswap_lp"] = {
                "type": "LP",
                "pool": "WETH/USDC",
                "amount0": amount_weth,
                "amount1": amount_usdc,
                "range": "$2,000 - $5,000",
                "position_id": position_id,
            }

    # =========================================================================
    # TEARDOWN
    # =========================================================================

    async def _teardown_positions(self) -> None:
        """Reverse all positions back to WETH."""
        from almanak.framework.intents import Intent

        print("\nExecuting teardown sequence (reverse order)...\n")

        teardown_steps = [
            ("Uniswap LP", "LP_CLOSE", self._teardown_uniswap_lp),
            ("Pendle PT", "Hold PT-wstETH (fixed yield)", self._teardown_pendle),  # PT token held
            ("Enso wstETH", "wstETH converted to PT", self._teardown_step8),  # wstETH from Enso
            ("Camelot", "Already swapped to WETH", self._teardown_camelot),
            ("Enso", "Already swapped ARB", self._teardown_enso),
            ("GMX V2", "PERP_CLOSE", self._teardown_gmx),
            ("Aave V3", "REPAY + WITHDRAW", self._teardown_aave),
            ("Final Swap", "USDC -> WETH", self._teardown_final_swap),
        ]

        if self.config.quick_mode:
            teardown_steps = teardown_steps[-3:]  # Just Aave, GMX, final swap

        for i, (name, action, func) in enumerate(teardown_steps, 1):
            print(f"   [{i}/{len(teardown_steps)}] {name}: {action}")
            await func()

    async def _teardown_uniswap_lp(self) -> None:
        """Close Uniswap V3 LP position."""
        from almanak.framework.intents import Intent

        position_id = self.lp_position_ids.get("uniswap_v3")
        if not position_id:
            logger.warning("No Uniswap LP position ID found, skipping teardown")
            print("        [SKIP] No position ID")
            return

        intent = Intent.lp_close(
            position_id=position_id,
            pool="WETH/USDC/500",
            collect_fees=True,
            protocol="uniswap_v3",
        )
        success = await self._execute_intent("teardown_uniswap_lp", intent, time.time())
        print(f"        {'[OK]' if success else '[FAIL]'}")

    async def _teardown_pendle(self) -> None:
        """Pendle PT-wstETH is held for fixed yield - redeem at maturity."""
        print("        [HOLD] PT-wstETH held for fixed yield until maturity")

    async def _teardown_step8(self) -> None:
        """Step 8 swapped USDT -> wstETH via Enso (used for Pendle step)."""
        print("        [SKIP] wstETH converted to PT-wstETH in step 9")

    async def _teardown_camelot(self) -> None:
        """Camelot swap already converted ARB to WETH - nothing to teardown."""
        print("        [SKIP] Already swapped to WETH")

    async def _teardown_enso(self) -> None:
        """Enso swap already converted ARB - nothing to teardown."""
        print("        [SKIP] ARB already converted in step 6")

    async def _teardown_gmx(self) -> None:
        """Close GMX perpetual position."""
        # GMX V2 was skipped (requires keeper execution on Anvil forks)
        if "gmx_perp" not in self.positions:
            print("        [SKIP] No GMX position to close")
            return
        # Original code for production:
        # intent = Intent.perp_close(...)
        # success = await self._execute_intent("teardown_gmx", intent, time.time())

    async def _teardown_aave(self) -> None:
        """Repay Aave debt and withdraw collateral."""
        from almanak.framework.intents import Intent

        # Check if we have Aave positions
        has_borrow = "aave_borrow" in self.positions
        has_supply = "aave_supply" in self.positions

        if not has_borrow and not has_supply:
            print("        [SKIP] No Aave positions to teardown")
            return

        # Repay the debt first (if we borrowed)
        if has_borrow:
            borrow_amount = self.positions["aave_borrow"].get("amount", Decimal("0.05"))
            intent_repay = Intent.repay(
                protocol="aave_v3",
                token="WETH",
                amount=borrow_amount,  # Use explicit amount instead of repay_full
                chain=self.config.chain,
            )
            success1 = await self._execute_intent("teardown_aave_repay", intent_repay, time.time())
            print(f"        Repay: {'[OK]' if success1 else '[FAIL]'}")
        else:
            print("        Repay: [SKIP] No borrow")

        # Withdraw all collateral
        if has_supply:
            supply_amount = self.positions["aave_supply"].get("amount", Decimal("300"))
            intent_withdraw = Intent.withdraw(
                protocol="aave_v3",
                token="USDC",
                amount=supply_amount,  # Use explicit amount
                chain=self.config.chain,
            )
            success2 = await self._execute_intent("teardown_aave_withdraw", intent_withdraw, time.time())
            print(f"        Withdraw: {'[OK]' if success2 else '[FAIL]'}")
        else:
            print("        Withdraw: [SKIP] No supply")

    async def _teardown_final_swap(self) -> None:
        """Swap remaining USDC back to WETH."""
        from almanak.framework.intents import Intent

        # For simplicity, swap a fixed amount instead of trying to query balance
        # In production, you'd query the actual USDC balance
        swap_amount = Decimal("200")  # Swap back some USDC

        # Use higher slippage for demo - placeholder prices ($2000/ETH) differ
        # from real prices (~$3400/ETH), so normal slippage would fail
        demo_slippage = Decimal("0.50")  # 50% to account for price difference

        intent = Intent.swap(
            from_token="USDC",
            to_token="WETH",
            amount=swap_amount,
            max_slippage=demo_slippage,
            protocol="uniswap_v3",
        )
        print(f"   Swapping {swap_amount} USDC -> WETH (50% slippage for placeholder price demo)")
        success = await self._execute_intent("teardown_final", intent, time.time())
        print(f"        {'[OK]' if success else '[FAIL]'}")

    # =========================================================================
    # HELPERS
    # =========================================================================

    def _record_intent(self, name: str, intent, start_time: float) -> None:
        """Record an intent for later summary."""
        elapsed = time.time() - start_time
        self.execution_times[name] = elapsed
        self.intents_generated.append({
            "name": name,
            "intent_type": intent.intent_type.value,
            "time_ms": elapsed * 1000,
            "intent": intent,
        })

    def _print_positions(self) -> None:
        """Print summary of all positions."""
        print("\n" + "=" * 60)
        print("OPEN POSITIONS")
        print("=" * 60 + "\n")

        total_value = Decimal("0")

        for name, pos in self.positions.items():
            print(f"   {name}:")
            for k, v in pos.items():
                print(f"      {k}: {v}")

            # Estimate value
            if pos.get("type") == "SUPPLY":
                value = pos.get("amount", Decimal("0"))
                total_value += value
            elif pos.get("type") == "LP":
                value = pos.get("amount1", Decimal("0")) * 2  # Rough estimate
                total_value += value
            elif pos.get("type") == "PERP_LONG":
                value = pos.get("size_usd", Decimal("0"))
                total_value += value

            print("")

        print(f"   Estimated Total Value: ~${total_value:,.2f}")

    def _print_summary(self, total_time: float) -> None:
        """Print final summary."""
        print(f"\n{'=' * 60}")
        print("EXECUTION SUMMARY")
        print(f"{'=' * 60}\n")

        print(f"Total Intents Generated: {len(self.intents_generated)}")
        print(f"Total Execution Time: {total_time:.2f}s")
        print(f"Mode: {'DRY RUN' if self.config.dry_run else 'LIVE EXECUTION'}")
        print("")

        # Execution results
        if self.execution_results:
            successful = sum(1 for r in self.execution_results if r.get("success"))
            failed = len(self.execution_results) - successful
            print(f"Executions: {successful} successful, {failed} failed")

            total_gas = sum(r.get("gas_used", 0) for r in self.execution_results if r.get("success"))
            if total_gas > 0:
                print(f"Total Gas Used: {total_gas:,}")
            print("")

        print("Intents by Type:")
        intent_types: dict[str, int] = {}
        for item in self.intents_generated:
            t = item["intent_type"]
            intent_types[t] = intent_types.get(t, 0) + 1

        for t, count in sorted(intent_types.items()):
            print(f"   {t}: {count}")

        print("")
        print("Protocols Used:")
        protocols = set()
        for item in self.intents_generated:
            intent = item["intent"]
            if intent is not None and hasattr(intent, "protocol"):
                protocols.add(intent.protocol)

        for p in sorted(protocols):
            print(f"   - {p}")

        print("")
        print("=" * 60)
        if self.config.dry_run:
            print("DRY RUN COMPLETE - No transactions executed")
        else:
            # Count successful executions
            successful = sum(1 for r in self.execution_results if r.get("success"))
            total = len(self.execution_results)
            print(f"DEMO COMPLETE - {successful}/{total} intents executed successfully")
        print("=" * 60)


# =============================================================================
# UTILITY FUNCTIONS
# =============================================================================


def print_header(title: str, char: str = "=") -> None:
    """Print a formatted header."""
    print("")
    print(char * 60)
    print(title)
    print(char * 60)


def print_step(num: int, protocol: str, action: str, description: str) -> None:
    """Print a step header."""
    print(f"\n[Step {num}/10] {protocol} - {action}")
    print(f"   {description}")


# =============================================================================
# MAIN
# =============================================================================


async def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Almanak SDK Demo: 10 Protocol Interactions with Real Execution"
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Generate intents without executing (no Anvil required)",
    )
    parser.add_argument(
        "--quick",
        action="store_true",
        help="Quick mode - fewer interactions",
    )
    parser.add_argument(
        "--rpc-url",
        type=str,
        default="http://127.0.0.1:8545",
        help="RPC URL for Anvil (default: http://127.0.0.1:8545)",
    )
    parser.add_argument(
        "--wallet",
        type=str,
        default=ANVIL_WALLET,
        help=f"Wallet address (default: Anvil account #0: {ANVIL_WALLET})",
    )
    parser.add_argument(
        "--private-key",
        type=str,
        default=None,
        help="Private key (default: Anvil account #0 key)",
    )
    parser.add_argument(
        "--skip-funding",
        action="store_true",
        help="Skip wallet funding (if already funded)",
    )
    args = parser.parse_args()

    # Get private key from args or environment or use default
    private_key = args.private_key or os.environ.get("ALMANAK_PRIVATE_KEY", ANVIL_PRIVATE_KEY)

    config = DemoConfig(
        dry_run=args.dry_run,
        quick_mode=args.quick,
        rpc_url=args.rpc_url,
        wallet_address=args.wallet,
        private_key=private_key,
    )

    # Fund wallet if not in dry-run mode and not skipped
    if not config.dry_run and not args.skip_funding:
        if not fund_wallet(config):
            print("\nERROR: Failed to fund wallet. Ensure Anvil is running.")
            print("Start Anvil with: anvil --fork-url <YOUR_RPC_URL> --port 8545")
            sys.exit(1)

    demo = TenProtocolsDemo(config)
    await demo.run()


if __name__ == "__main__":
    asyncio.run(main())
