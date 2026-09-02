"""Four-layer exact-address Aerodrome Slipstream LP_OPEN + address-bound LP_CLOSE on Base Anvil (ALM-3462).

The symbolic ``TOKEN0/TOKEN1/tick_spacing`` lifecycle lives in
``test_aerodrome_slipstream_lp.py``. This cell proves the OTHER admission lane:
a strategy names the pool by its bare address, the compiler reverses the
address into the pool's own ``token0/token1/tickSpacing/factory`` through the
gateway-shaped adapter, authenticates it against the reviewed current factory,
and the block-anchored verifier certifies THAT address before the mint. The
close phase then proves the address-bound cross-check: a close naming a
different (real, current-generation) pool is refused, and the close naming the
NFT's own pool executes.
"""

import os
from decimal import Decimal

import pytest
from web3 import Web3

from almanak.connectors.aerodrome.addresses import slipstream_lp_deployments
from almanak.connectors._strategy_base.teardown_post_condition import get_teardown_post_condition
from almanak.connectors.aerodrome.pool_validation import encode_aerodrome_cl_get_pool
from almanak.connectors.aerodrome.receipt_parser import AerodromeSlipstreamReceiptParser
from almanak.framework.execution.orchestrator import ExecutionOrchestrator
from almanak.framework.intents import Intent, IntentCompiler, LPCloseIntent, LPOpenIntent
from almanak.framework.intents.vocabulary import IntentType, PriceBand
from almanak.framework.teardown.models import PositionInfo, PositionType
from almanak.framework.venues import correlate_verified_venue_receipts
from tests.intents._lp_setup_helpers import query_position_liquidity
from tests.intents.conftest import CHAIN_CONFIGS, get_token_balance, get_token_decimals

CHAIN_NAME = "base"
CURRENT_DEPLOYMENT = slipstream_lp_deployments(CHAIN_NAME)[0]
POSITION_MANAGER = CURRENT_DEPLOYMENT.position_manager
# WETH/USDC tick_spacing=50 on the CURRENT reviewed Slipstream factory — the
# same pool the symbolic sibling opens as "WETH/USDC/50", named here by address
# so the two cells certify the same venue through both lanes.
EXACT_POOL = "0x3FE04A59Ebd38cF06080a6F60a98D124eb59392A"
EXPECTED_TICK_SPACING = 50
# A different real pool on the same (current) factory: WETH/VVV ts=100. Closing
# the WETH/USDC NFT while naming this pool must be refused.
OTHER_CURRENT_POOL = "0xa135b59fe221c0c8d441294f97f96fbc37bc9fbe"
# Discriminating refusal controls for LP_OPEN: real, well-formed addresses the
# exact lane must refuse before any approval is built.
LEGACY_GENERATION_POOL = "0xb2cc224c1c9feE385f8ad6a55b4d94E92359DC59"  # WETH/USDC ts=100 on the legacy factory
UNISWAP_V3_LOOKALIKE_POOL = "0xd0b53D9277642d899DF5C87A3966A349A798F224"  # Uniswap V3 WETH/USDC on Base
_TOKEN0_SELECTOR = "0x0dfe1681"
_TOKEN1_SELECTOR = "0xd21220a7"
_TICK_SPACING_SELECTOR = "0xd0c93a7c"
_FACTORY_SELECTOR = "0xc45a0155"
_NPM_POSITIONS_SELECTOR = "0x99fbab88"
_NPM_MINT_SELECTOR = "0xb5007d1f"
LP_AMOUNT_WETH = Decimal("0.1")  # amount0 (WETH is token0 on Base: 0x4200… < 0x8335…)
LP_AMOUNT_USDC = Decimal("250")  # amount1
PRICE_BAND_LOWER = Decimal("1000")  # USDC per WETH
PRICE_BAND_UPPER = Decimal("12000")


def _call(web3: Web3, to: str, data: str) -> bytes:
    return bytes(web3.eth.call({"to": Web3.to_checksum_address(to), "data": data}))


def _word_address(word: bytes) -> str:
    return "0x" + word[-20:].hex()


def _read_pool_tuple(web3: Web3, pool: str) -> tuple[str, str, int, str]:
    """Independently read (token0, token1, tickSpacing, factory) straight from the pool contract."""
    token0 = _word_address(_call(web3, pool, _TOKEN0_SELECTOR))
    token1 = _word_address(_call(web3, pool, _TOKEN1_SELECTOR))
    tick_spacing = int.from_bytes(_call(web3, pool, _TICK_SPACING_SELECTOR)[:32], "big", signed=True)
    factory = _word_address(_call(web3, pool, _FACTORY_SELECTOR))
    return token0, token1, tick_spacing, factory


def _read_npm_position(web3: Web3, token_id: int) -> tuple[str, str, int]:
    """Read (token0, token1, tickSpacing) of a minted position from the reviewed NPM's positions()."""
    raw = _call(web3, POSITION_MANAGER, _NPM_POSITIONS_SELECTOR + token_id.to_bytes(32, "big").hex())
    words = [raw[i : i + 32] for i in range(0, len(raw), 32)]
    return _word_address(words[2]), _word_address(words[3]), int.from_bytes(words[4], "big", signed=True)


def _teardown_position(token_id: int) -> PositionInfo:
    """The position as the teardown lane would enumerate it: NFT id + reviewed manager identity."""
    return PositionInfo(
        position_type=PositionType.LP,
        position_id=str(token_id),
        chain=CHAIN_NAME,
        protocol="aerodrome_slipstream",
        value_usd=Decimal("0"),
        details={"nft_manager": POSITION_MANAGER, "pool_address": EXACT_POOL.lower()},
    )


def _decode_mint_tuple(calldata: str) -> tuple[str, str, int]:
    """Decode (token0, token1, tickSpacing) from Slipstream NPM mint calldata (static-tuple head)."""
    assert calldata.lower().startswith(_NPM_MINT_SELECTOR)
    body = bytes.fromhex(calldata[10:])
    words = [body[i : i + 32] for i in range(0, len(body), 32)]
    return _word_address(words[0]), _word_address(words[1]), int.from_bytes(words[2], "big", signed=True)


@pytest.mark.base
@pytest.mark.lp
class TestAerodromeSlipstreamExactPoolLP:
    @pytest.mark.intent(IntentType.LP_OPEN, IntentType.LP_CLOSE)
    @pytest.mark.asyncio
    async def test_exact_pool_address_open_close_roundtrip(
        self,
        web3: Web3,
        funded_wallet: str,
        orchestrator: ExecutionOrchestrator,
        anvil_rpc_url: str,
        anvil_eth_call_adapter,
    ):
        """Compile, execute, parse, and balance-check an address-bound Slipstream open and close."""
        tokens = CHAIN_CONFIGS[CHAIN_NAME]["tokens"]
        usdc_addr = tokens["USDC"]
        weth_addr = tokens["WETH"]
        usdc_decimals = get_token_decimals(web3, usdc_addr)
        weth_decimals = get_token_decimals(web3, weth_addr)

        usdc_before = get_token_balance(web3, usdc_addr, funded_wallet)
        weth_before = get_token_balance(web3, weth_addr, funded_wallet)
        assert usdc_before > 0 and weth_before > 0, "funded_wallet seeding failed"

        # Intent preservation: the public factory and a serialize/deserialize
        # round trip must carry the exact address unchanged.
        intent = Intent.lp_open(
            pool=EXACT_POOL,
            amount0=LP_AMOUNT_WETH,
            amount1=LP_AMOUNT_USDC,
            range_spec=PriceBand(lower=PRICE_BAND_LOWER, upper=PRICE_BAND_UPPER),
            protocol="aerodrome_slipstream",
            chain=CHAIN_NAME,
        )
        assert isinstance(intent, LPOpenIntent)
        assert intent.pool == EXACT_POOL
        assert LPOpenIntent.deserialize(intent.serialize()).pool == EXACT_POOL

        # Authoritative resolution, independent of the compiler: the pool's own
        # tuple, and the reviewed factory round-tripping it to EXACT_POOL.
        pool_token0, pool_token1, pool_tick_spacing, pool_factory = _read_pool_tuple(web3, EXACT_POOL)
        assert (pool_token0, pool_token1, pool_tick_spacing) == (
            weth_addr.lower(),
            usdc_addr.lower(),
            EXPECTED_TICK_SPACING,
        )
        assert pool_factory == CURRENT_DEPLOYMENT.factory.lower()
        factory_answer = _word_address(
            _call(web3, pool_factory, encode_aerodrome_cl_get_pool(pool_token0, pool_token1, pool_tick_spacing))
        )
        assert factory_answer == EXACT_POOL.lower()

        compiler = IntentCompiler(
            chain=CHAIN_NAME,
            wallet_address=funded_wallet,
            price_oracle={"USDC": Decimal("1"), "WETH": Decimal("3000")},
            rpc_url=anvil_rpc_url,
            gateway_client=anvil_eth_call_adapter,
            venue_verification_gateway_factory=lambda: anvil_eth_call_adapter,
        )

        # Layer 1 — the bare address clears the format gate and is bound exactly.
        compilation = compiler.compile(intent)
        assert compilation.status.value == "SUCCESS", f"Exact-pool LP_OPEN compilation failed: {compilation.error}"
        assert compilation.action_bundle is not None
        metadata = compilation.action_bundle.metadata
        assert metadata["pool"].lower() == EXACT_POOL.lower()
        assert metadata["tick_spacing"] == EXPECTED_TICK_SPACING, "tick spacing must come from the pool contract"
        assert metadata["token0"]["address"].lower() == weth_addr.lower()
        assert metadata["token1"]["address"].lower() == usdc_addr.lower()
        assert metadata["nft_manager"].lower() == POSITION_MANAGER.lower()
        assert metadata["slipstream_deployment"] == "current"
        binding_hash = metadata["venue_binding_hash"]
        assert isinstance(binding_hash, str) and binding_hash
        assert metadata["venue_verification"]["blockNumber"] > 0
        operational_targets = {ref["reference"].lower() for ref in metadata["venue_operational_refs"]}
        assert POSITION_MANAGER.lower() in operational_targets
        mint_tx = compilation.action_bundle.transactions[-1]
        mint_target = mint_tx["to"] if isinstance(mint_tx, dict) else mint_tx.to
        assert mint_target.lower() == POSITION_MANAGER.lower()
        # Compile binding: the emitted mint calldata carries the pool's canonical tuple.
        mint_data = mint_tx["data"] if isinstance(mint_tx, dict) else mint_tx.data
        assert _decode_mint_tuple(mint_data) == (pool_token0, pool_token1, pool_tick_spacing)

        # Mismatch refusal, before any approval: two real, well-formed addresses
        # that are NOT admissible exact pools must fail with balances untouched.
        for wrong_pool, needle in (
            (LEGACY_GENERATION_POOL, "legacy factory generation"),
            (UNISWAP_V3_LOOKALIKE_POOL, "reports unreviewed factory"),
        ):
            refused = compiler.compile(
                Intent.lp_open(
                    pool=wrong_pool,
                    amount0=LP_AMOUNT_WETH,
                    amount1=LP_AMOUNT_USDC,
                    range_spec=PriceBand(lower=PRICE_BAND_LOWER, upper=PRICE_BAND_UPPER),
                    protocol="aerodrome_slipstream",
                    chain=CHAIN_NAME,
                )
            )
            assert refused.status.value == "FAILED", f"{wrong_pool} must be refused"
            assert refused.action_bundle is None
            assert needle in (refused.error or ""), refused.error
        assert get_token_balance(web3, usdc_addr, funded_wallet) == usdc_before
        assert get_token_balance(web3, weth_addr, funded_wallet) == weth_before

        # Layer 2 — execute on the managed fork.
        execution = await orchestrator.execute(compilation.action_bundle)
        assert execution.success, f"Exact-pool LP_OPEN execution failed: {execution.error}"
        # CI pins the fork block; a local managed fork may leave it unset.
        fork_block = int(os.environ.get("ANVIL_FORK_BLOCK_BASE") or 1)
        assert execution.transaction_results
        for tx_result in execution.transaction_results:
            assert tx_result.tx_hash.startswith("0x") and len(tx_result.tx_hash) == 66
            assert tx_result.receipt is not None
            assert tx_result.receipt.status == 1
            assert tx_result.receipt.block_number >= fork_block
        assert (
            correlate_verified_venue_receipts(
                bundle_metadata=metadata,
                expected_binding_hash=binding_hash,
                receipts=tuple(tx.receipt.to_dict() for tx in execution.transaction_results if tx.receipt),
            )
            == binding_hash
        )

        # Layer 3 — the production parser recovers the minted position.
        parser = AerodromeSlipstreamReceiptParser(chain=CHAIN_NAME)
        position_id = None
        for tx_result in execution.transaction_results:
            if tx_result.receipt is None:
                continue
            parsed = parser.parse_receipt(tx_result.receipt.to_dict())
            assert parsed.success, f"LP_OPEN receipt parsing failed for {tx_result.tx_hash}: {parsed.error}"
            position_id = parser.extract_position_id(tx_result.receipt.to_dict()) or position_id
        assert position_id is not None, "Exact-pool LP_OPEN receipt must contain the minted position id"
        # Outcome binding: the terminal position state on the reviewed NPM is
        # the exact pool's canonical tuple, not merely "some liquidity".
        assert _read_npm_position(web3, int(position_id)) == (pool_token0, pool_token1, pool_tick_spacing)
        assert query_position_liquidity(web3, POSITION_MANAGER, int(position_id)) > 0

        # Layer 4 — bilateral deltas: the price band straddles spot, so BOTH
        # assets are deposited, and neither exceeds the requested amount.
        usdc_spent = usdc_before - get_token_balance(web3, usdc_addr, funded_wallet)
        weth_spent = weth_before - get_token_balance(web3, weth_addr, funded_wallet)
        assert usdc_spent > 0 and weth_spent > 0
        assert usdc_spent <= int(LP_AMOUNT_USDC * Decimal(10**usdc_decimals))
        assert weth_spent <= int(LP_AMOUNT_WETH * Decimal(10**weth_decimals))
        usdc_after_open = get_token_balance(web3, usdc_addr, funded_wallet)
        weth_after_open = get_token_balance(web3, weth_addr, funded_wallet)

        # Teardown post-condition (TD-14), through the gateway-shaped adapter,
        # while the position is OPEN: a MEASURED residual, never "closed".
        teardown_hook = get_teardown_post_condition("aerodrome_slipstream")
        assert teardown_hook is not None, "aerodrome_slipstream must have a registered teardown post-condition"
        open_check = teardown_hook(
            _teardown_position(int(position_id)), funded_wallet, gateway_client=anvil_eth_call_adapter
        )
        assert open_check.closed is False and not open_check.unmeasured, open_check
        assert open_check.residual and open_check.residual["liquidity"] > 0
        assert open_check.residual["position_manager"].lower() == POSITION_MANAGER.lower()

        # Close phase: the supplied bare pool must be the NFT's own pool.
        wrong_close = compiler.compile(
            LPCloseIntent(
                position_id=str(position_id),
                pool=OTHER_CURRENT_POOL,
                collect_fees=True,
                protocol="aerodrome_slipstream",
                chain=CHAIN_NAME,
            )
        )
        assert wrong_close.status.value == "FAILED"
        assert wrong_close.action_bundle is None
        assert f"tokenId={position_id} belongs to pool {EXACT_POOL.lower()}" in (wrong_close.error or "")
        assert OTHER_CURRENT_POOL in (wrong_close.error or "")
        assert get_token_balance(web3, usdc_addr, funded_wallet) == usdc_after_open
        assert get_token_balance(web3, weth_addr, funded_wallet) == weth_after_open

        close_compilation = compiler.compile(
            LPCloseIntent(
                position_id=str(position_id),
                pool=EXACT_POOL,
                collect_fees=True,
                protocol="aerodrome_slipstream",
                chain=CHAIN_NAME,
            )
        )
        assert close_compilation.status.value == "SUCCESS", (
            f"Exact-pool LP_CLOSE compilation failed: {close_compilation.error}"
        )
        assert close_compilation.action_bundle is not None
        assert close_compilation.action_bundle.metadata["venue_binding_hash"] == binding_hash
        close_execution = await orchestrator.execute(close_compilation.action_bundle)
        assert close_execution.success, f"Exact-pool LP_CLOSE execution failed: {close_execution.error}"
        for tx_result in close_execution.transaction_results:
            assert tx_result.receipt is not None and tx_result.receipt.status == 1
            parsed = parser.parse_receipt(tx_result.receipt.to_dict())
            assert parsed.success, f"LP_CLOSE receipt parsing failed for {tx_result.tx_hash}: {parsed.error}"
        assert query_position_liquidity(web3, POSITION_MANAGER, int(position_id)) == 0
        assert get_token_balance(web3, usdc_addr, funded_wallet) > usdc_after_open
        assert get_token_balance(web3, weth_addr, funded_wallet) > weth_after_open

        # Teardown post-condition AFTER the close, pinned to the close receipt
        # block: a MEASURED closure on the exact reviewed manager — the
        # certification the ticket said could not be produced.
        close_block = max(tx.receipt.block_number for tx in close_execution.transaction_results if tx.receipt)
        closed_check = teardown_hook(
            _teardown_position(int(position_id)),
            funded_wallet,
            gateway_client=anvil_eth_call_adapter,
            block=close_block,
        )
        assert closed_check.closed is True and not closed_check.unmeasured, closed_check
