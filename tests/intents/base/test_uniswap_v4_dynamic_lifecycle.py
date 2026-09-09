"""Real V4 PoolManager proofs for stored fees and exact-key LP closure."""

import hashlib
import json
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest
from eth_abi import decode, encode
from eth_utils import keccak
from web3 import Web3

from almanak.connectors._strategy_base.v4_pool_abi import encode_get_slot0
from almanak.connectors.uniswap_v4.addresses import UNISWAP_V4
from almanak.connectors.uniswap_v4.pool_key import PoolKey
from almanak.connectors.uniswap_v4.position import observe_position
from almanak.connectors.uniswap_v4.receipt_parser import UniswapV4ReceiptParser
from almanak.framework.intents.compiler import IntentCompiler
from almanak.framework.intents.vocabulary import IntentType, LPCloseIntent, LPOpenIntent, SwapIntent
from tests.intents._lp_setup_helpers import _send_via_orchestrator
from tests.intents.conftest import CHAIN_CONFIGS, get_token_balance

pytestmark = [
    pytest.mark.base,
    pytest.mark.lp,
    pytest.mark.no_zodiac(reason="EOA dynamic hook qualification; Safe admission tested separately"),
]
FACTORY = "0x4e59b44847b379578588920cA78FbF26c0B4956C"
KEY_ABI = "(address,address,uint24,int24,address)"


def key_tuple(key):
    return key.currency0, key.currency1, key.fee, key.tick_spacing, key.hooks


async def deploy_hook(web3, orchestrator, wallet, override=False):
    source = Path(__file__).parents[1] / "fixtures/uniswap_v4/DynamicFeeFixture.sol"
    artifact = json.loads(source.with_suffix(".json").read_text())
    assert hashlib.sha256(source.read_bytes()).hexdigest() == artifact["source_sha256"]
    assert (
        keccak(web3.eth.get_code(FACTORY)).hex() == "2fa86add0aed31f33a762c9d88e807c475bd51d0f52bd0955754b2608f7e4989"
    )
    code = bytes.fromhex(artifact["bytecode"][2:]) + encode(
        ["address", "address", "bool"],
        [UNISWAP_V4["base"]["pool_manager"], wallet, override],
    )
    prefix = b"\xff" + bytes.fromhex(FACTORY[2:])
    code_hash = keccak(code)
    flags = 0x1080 if override else 0x1000
    for number in range(1_000_000):
        salt = number.to_bytes(32, "big")
        address = keccak(prefix + salt + code_hash)[12:]
        if int.from_bytes(address, "big") & 0x3FFF == flags:
            break
    else:
        pytest.fail("CREATE2 hook salt mining exhausted")
    await _send_via_orchestrator(orchestrator, FACTORY, salt + code, intent_type="SETUP")
    deployed = Web3.to_checksum_address(address)
    assert web3.eth.get_code(deployed)
    return deployed


@pytest.mark.parametrize("mode", ["static", "stored", "override"])
@pytest.mark.intent(IntentType.LP_OPEN)
@pytest.mark.intent(IntentType.LP_CLOSE)
@pytest.mark.asyncio
async def test_full_key_lp_lifecycle(
    web3, funded_wallet, orchestrator, anvil_eth_call_adapter, monkeypatch, mode, tmp_path, anvil_rpc_url
):
    dynamic = mode != "static"
    tokens = CHAIN_CONFIGS["base"]["tokens"]
    weth, usdc = tokens["WETH"], tokens["USDC"]
    gateway = anvil_eth_call_adapter
    orchestrator.operation_observer_factory = lambda: gateway
    compiler = IntentCompiler(
        chain="base",
        wallet_address=funded_wallet,
        gateway_client=gateway,
        venue_verification_gateway_factory=lambda: gateway,
        price_oracle={weth: Decimal(2500), usdc: Decimal(1)},
    )
    parser = UniswapV4ReceiptParser(chain="base")
    reference = PoolKey(weth, usdc, 500, 10)
    slot = web3.eth.call({"to": UNISWAP_V4["base"]["state_view"], "data": encode_get_slot0(reference.pool_id)})
    sqrt_price = decode(["uint160", "int24", "uint24", "uint24"], slot)[0]
    price = Decimal(sqrt_price**2) / Decimal(2**192) * Decimal(10**12)
    key = reference
    if dynamic:
        hook = await deploy_hook(web3, orchestrator, funded_wallet, override=mode == "override")
        key = PoolKey(weth, usdc, 0x800000, 60, hook)
        await _send_via_orchestrator(
            orchestrator,
            UNISWAP_V4["base"]["pool_manager"],
            keccak(text=f"initialize({KEY_ABI},uint160)")[:4]
            + encode([KEY_ABI, "uint160"], [key_tuple(key), sqrt_price]),
            intent_type="SETUP",
        )
    params = {"pool_key": key.to_wire(), "hook_data": "0x"}
    swap_params = dict(params)
    if mode == "override":
        from almanak.connectors.uniswap_v4 import behavior
        from almanak.connectors.uniswap_v4.behavior import HookEvidence

        runtime = bytes(web3.eth.get_code(Web3.to_checksum_address(key.hooks)))

        class FixtureProfile:
            name = "test_fixture_override"
            version = "1"

            def verify(self, *, chain, key, operation, route, hook_data, gateway, block_number):
                if key.hooks.lower() != hook.lower() or operation != "swap_exact_in" or route != "universal_router_eoa":
                    return None
                from almanak.connectors.uniswap_v4.venue_verifier import address_ref
                from almanak.framework.venues import VenueTargetRole

                observed = gateway.code(
                    chain=chain,
                    target=address_ref(VenueTargetRole.PERMISSION_TARGET, key.hooks),
                    block_number=block_number,
                )
                if observed != runtime or hook_data != bytes.fromhex("1234"):
                    raise ValueError("Fixture runtime or hook data differs from the reviewed deployment")
                return HookEvidence(
                    self.name,
                    self.version,
                    operation,
                    route,
                    chain,
                    key.hooks,
                    block_number,
                    gateway.block_hash(chain=chain, block_number=block_number),
                    "0x" + keccak(runtime).hex(),
                    True,
                )

        # Admission is test-local and authenticates only this deterministic deployment.
        monkeypatch.setattr(behavior, "REVIEWED_PROFILES", (*behavior.REVIEWED_PROFILES, FixtureProfile()))
        swap_params["hook_data"] = "0x1234"
    before = [get_token_balance(web3, token, funded_wallet) for token in (weth, usdc)]
    amount0, amount1 = Decimal("0.001"), Decimal(3)
    opened = compiler.compile(
        LPOpenIntent(
            pool=key.pool_id,
            amount0=amount0,
            amount1=amount1,
            range_lower=price * Decimal("0.8"),
            range_upper=price * Decimal("1.2"),
            protocol="uniswap_v4",
            protocol_params=params,
            max_slippage=Decimal("0.005"),
        )
    )
    assert opened.status.value == "SUCCESS", opened.error
    result = await orchestrator.execute(opened.action_bundle)
    assert result.success, result.error
    receipt = result.transaction_results[-1].receipt.to_dict()
    token_id = parser.extract_position_id(receipt)
    assert token_id is not None
    position = observe_position(gateway, chain="base", token_id=token_id, wallet=funded_wallet)
    assert position.key == key and position.liquidity > 0
    after = [get_token_balance(web3, token, funded_wallet) for token in (weth, usdc)]
    assert 0 < before[0] - after[0] <= int(amount0 * 10**18)
    assert 0 < before[1] - after[1] <= int(amount1 * 10**6)

    if dynamic:
        # The pool identity stays constant while the stored LP fee changes.
        for fee in (777, 31100, 100000):
            await _send_via_orchestrator(
                orchestrator,
                key.hooks,
                keccak(text=f"setFee({KEY_ABI},uint24)")[:4] + encode([KEY_ABI, "uint24"], [key_tuple(key), fee]),
                intent_type="SETUP",
            )
            intent = SwapIntent(
                from_token=usdc,
                to_token=weth,
                amount=Decimal("0.01"),
                protocol="uniswap_v4",
                swap_params=swap_params,
                max_slippage=Decimal("0.005"),
            )
            swap = compiler.compile(intent)
            assert swap.status.value == "SUCCESS", swap.error
            # Primitive identity differs for LP versus swap; PoolKey never does.
            assert swap.action_bundle.metadata["pool_id"] == key.pool_id
            balances = [get_token_balance(web3, token, funded_wallet) for token in (weth, usdc)]
            executed = await orchestrator.execute(swap.action_bundle)
            assert executed.success, executed.error
            parsed = parser.parse_receipt(
                executed.transaction_results[-1].receipt.to_dict(),
                swap_token_meta=swap.action_bundle.metadata["swap_token_meta"],
            )
            assert parsed.swap_events[0].fee == fee
            assert parsed.swap_result.amount_out == get_token_balance(web3, weth, funded_wallet) - balances[0]
            assert balances[1] - get_token_balance(web3, usdc, funded_wallet) == 10000
        stale = compiler.compile(intent)
        assert stale.status.value == "SUCCESS", stale.error

        await _send_via_orchestrator(
            orchestrator,
            key.hooks,
            keccak(text=f"setFee({KEY_ABI},uint24)")[:4] + encode([KEY_ABI, "uint24"], [key_tuple(key), 500000]),
            intent_type="SETUP",
        )
        balances = [get_token_balance(web3, token, funded_wallet) for token in (weth, usdc)]
        rejected = await orchestrator.execute(stale.action_bundle)
        assert not rejected.success, "Fee movement must not bypass the quote's minimum output"
        assert balances == [get_token_balance(web3, token, funded_wallet) for token in (weth, usdc)]
        slot = web3.eth.call({"to": UNISWAP_V4["base"]["state_view"], "data": encode_get_slot0(key.pool_id)})
        stored = decode(["uint160", "int24", "uint24", "uint24"], slot)[3]
        assert stored == (1000 if mode == "override" else 500000)
        assert observe_position(gateway, chain="base", token_id=token_id, wallet=funded_wallet).key == key

    close_intent = LPCloseIntent(
        position_id=str(token_id), protocol="uniswap_v4", protocol_params=params, max_slippage=Decimal("0.005")
    )
    closed = compiler.compile(close_intent)
    assert closed.status.value == "SUCCESS", closed.error
    assert closed.action_bundle.metadata["withdrawal_bounds_source"] == "measured_principal"
    if mode == "stored":
        from almanak.framework.execution.orchestrator import ExecutionContext
        from almanak.framework.execution.simulator.local import LocalSimulator

        context = ExecutionContext(chain="base", wallet_address=funded_wallet, simulation_enabled=True)
        original_simulate = orchestrator._phase_simulate
        bracket = {}
        price_move = compiler.compile(
            SwapIntent(
                from_token=usdc,
                to_token=weth,
                amount=Decimal("0.2"),
                protocol="uniswap_v4",
                swap_params=swap_params,
                max_slippage=Decimal("0.005"),
            )
        )
        assert price_move.status.value == "SUCCESS", price_move.error

        async def move_price_after_simulation(pipeline):
            outcome = await original_simulate(pipeline)
            if pipeline.context is context:
                assert outcome is None
                assert pipeline.result.simulation_result.simulated
                assert pipeline.result.simulation_result.success
                # Model another transaction changing price after the close simulated.
                moved = await orchestrator.execute(price_move.action_bundle)
                assert moved.success, moved.error
                bracket["nonce"] = web3.eth.get_transaction_count(funded_wallet)
                bracket["native"] = web3.eth.get_balance(funded_wallet)
                bracket["tokens"] = [get_token_balance(web3, token, funded_wallet) for token in (weth, usdc)]
            return outcome

        assert len(closed.action_bundle.transactions) == 1
        with monkeypatch.context() as scoped:
            scoped.setattr(orchestrator, "simulator", LocalSimulator(rpc_url=anvil_rpc_url))
            scoped.setattr(orchestrator, "_phase_simulate", move_price_after_simulation)
            failed_close = await orchestrator.execute(closed.action_bundle, context=context)
        assert not failed_close.success
        assert len(failed_close.transaction_results) == 1
        failed_receipt = failed_close.transaction_results[0].receipt
        assert failed_receipt is not None and failed_receipt.status == 0
        assert web3.eth.get_transaction_count(funded_wallet) == bracket["nonce"] + 1
        assert bracket["native"] - web3.eth.get_balance(funded_wallet) == failed_close.total_gas_cost_wei
        assert bracket["tokens"] == [get_token_balance(web3, token, funded_wallet) for token in (weth, usdc)]
        assert (
            observe_position(gateway, chain="base", token_id=token_id, wallet=funded_wallet).liquidity
            == position.liquidity
        )
        retry = compiler.compile(close_intent)
        assert retry.status.value == "SUCCESS", retry.error
        before_close = [get_token_balance(web3, token, funded_wallet) for token in (weth, usdc)]
        native_before_retry = web3.eth.get_balance(funded_wallet)

        async def execute_retry():
            with monkeypatch.context() as scoped:
                scoped.setattr(orchestrator, "simulator", LocalSimulator(rpc_url=anvil_rpc_url))
                return await orchestrator.execute(retry.action_bundle, context=context)

        result = await assert_retry_receipts_persisted(
            tmp_path,
            gateway,
            funded_wallet,
            close_intent,
            context,
            closed.action_bundle.metadata,
            failed_close,
            retry.action_bundle.metadata,
            execute_retry,
        )
        assert result.simulation_result.simulated and result.simulation_result.success
        assert native_before_retry - web3.eth.get_balance(funded_wallet) == result.total_gas_cost_wei
        closed = retry
    else:
        before_close = [get_token_balance(web3, token, funded_wallet) for token in (weth, usdc)]
        result = await orchestrator.execute(closed.action_bundle)
    assert result.success, result.error
    assert observe_position(gateway, chain="base", token_id=token_id, wallet=funded_wallet).liquidity == 0
    for index, token in enumerate((weth, usdc)):
        delta = get_token_balance(web3, token, funded_wallet) - before_close[index]
        assert delta >= int(closed.action_bundle.metadata[f"amount{index}_min"]) > 0
    parsed = parser.parse_receipt(result.transaction_results[-1].receipt.to_dict())
    assert parsed.modify_liquidity_events[0].liquidity_delta == -position.liquidity


async def assert_retry_receipts_persisted(
    tmp_path, gateway, wallet, intent, context, failed_meta, failed, retry_meta, execute_retry
):
    """Real receipts through production runner commit and SQLite; hosted transport is a separate test surface."""
    from almanak.framework.accounting.accountant_test import _cell_g10_multi_tx_atomicity
    from almanak.framework.observability.context import clear_cycle_id, get_cycle_id, new_cycle_id, set_cycle_id
    from almanak.framework.runner.strategy_runner import SingleChainExecutionState, StrategyRunner
    from almanak.framework.state.backends.sqlite import SQLiteConfig, SQLiteStore

    store = SQLiteStore(SQLiteConfig(db_path=str(tmp_path / "failed-retry.db")))
    await store.initialize()
    runner = StrategyRunner.__new__(StrategyRunner)
    runner.state_manager = store
    runner.config = SimpleNamespace(chain="base", dry_run=False, paper_mode=False)
    runner._maybe_enrich_result_with_runner_hooks = Mock()
    runner._maybe_save_ledger_with_registry = AsyncMock(return_value=False)
    runner._emit_position_event_for_intent = AsyncMock()
    runner._get_gateway_client = lambda: pool_lookup_gateway(gateway, wallet, int(intent.position_id))
    runner._build_curve_pool_meta_lookup = lambda: None
    oracle = {"ETH": Decimal(2500), "WETH": Decimal(2500), "USDC": Decimal(1)}
    runner._merge_oracle_for_ledger = lambda *args, **kwargs: oracle
    strategy = SimpleNamespace(deployment_id="deployment:v4-failed-retry", chain="base", wallet_address=wallet)
    state = SingleChainExecutionState(
        strategy=strategy,
        intent=intent,
        start_time=datetime.now(UTC),
        deployment_id=strategy.deployment_id,
        last_execution_result=failed,
        last_execution_context=context,
        last_bundle_metadata=failed_meta,
    )
    previous_cycle = get_cycle_id()
    set_cycle_id(new_cycle_id())
    try:
        await runner._single_chain_persist_failed_attempt(state, failed)
        first_id = state.failed_attempt_ledger_id
        assert first_id
        await runner._single_chain_persist_failed_attempt(state, failed)
        rows = await store.get_ledger_entries(strategy.deployment_id)
        assert len(rows) == 1 and rows[0].id == first_id and not rows[0].success
        runner._emit_position_event_for_intent.assert_not_awaited()
        evidence = json.loads(rows[0].extracted_data_json)
        assert evidence["compiler_evidence"]["v4_operation"] == json.loads(json.dumps(failed_meta["v4_operation"]))
        assert any(receipt["status"] == 0 for receipt in evidence["failed_attempt"]["receipts"])
        retried = await execute_retry()
        assert retried.success, retried.error
        state.last_execution_result = retried
        state.last_bundle_metadata = retry_meta
        runner._single_chain_enrich_execution_result(state)
        await runner._write_ledger_entry(strategy, intent, result=retried, success=True, price_oracle=oracle)
        rows = await store.get_ledger_entries(strategy.deployment_id)
        assert len(rows) == 2 and sum(row.success for row in rows) == 1
        assert rows[0].cycle_id == rows[1].cycle_id and rows[0].cycle_id
        assert _cell_g10_multi_tx_atomicity([row.to_dict() for row in rows], [], []).status == "PASS"
        assert sum(row.gas_used for row in rows) == failed.total_gas_used + retried.total_gas_used
        assert sum(Decimal(row.gas_usd) for row in rows) == (
            Decimal(failed.total_gas_cost_wei + retried.total_gas_cost_wei) * Decimal(2500) / Decimal(10**18)
        )
        persisted = [
            item["tx_hash"] for row in rows for item in json.loads(row.extracted_data_json)["sub_transactions"]
        ]
        expected = [transaction.tx_hash for result in (failed, retried) for transaction in result.transaction_results]
        assert sorted(persisted) == sorted(expected) and len(persisted) == len(set(persisted))
        assert await store.get_accounting_events(strategy.deployment_id) == []
        return retried
    finally:
        if previous_cycle is None:
            clear_cycle_id()
        else:
            set_cycle_id(previous_cycle)
        await store.close()


def pool_lookup_gateway(gateway, wallet, token_id):
    """In-process production service/client bridge over an observed key; does not certify gRPC transport."""
    import asyncio

    from almanak.connectors.uniswap_v4.gateway.pool_key_cache import CachedPoolKey, V4PoolKeyCache
    from almanak.gateway.services.market_service import MarketServiceServicer

    observed = observe_position(gateway, chain="base", token_id=token_id, wallet=wallet)
    cache = V4PoolKeyCache()
    cache.register("base", observed.key.pool_id, CachedPoolKey(**observed.key.to_wire()))
    service = MarketServiceServicer.__new__(MarketServiceServicer)
    service._get_pool_key_cache = AsyncMock(return_value=cache)

    def lookup(request, timeout):
        del timeout
        # The real client bridge invokes the blocking stub via asyncio.to_thread.
        with pytest.raises(RuntimeError):
            asyncio.get_running_loop()
        grpc_context = Mock()
        response = asyncio.run(service.LookupV4PoolKey(request, grpc_context))
        grpc_context.set_code.assert_not_called()
        assert response.chain == "base"
        assert bytes(request.pool_id) == bytes.fromhex(observed.key.pool_id.removeprefix("0x"))
        return response

    return SimpleNamespace(market=SimpleNamespace(LookupV4PoolKey=lookup))
