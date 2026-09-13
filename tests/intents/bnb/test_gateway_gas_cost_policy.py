"""EOA gas-policy acceptance through the real multichain gateway route on BSC.

The USD quote is a controlled, freshly timestamped test observation. This proves
policy transport and signing enforcement, not production oracle availability.
Compilation uses the existing real-price fixture; execution and receipts use Anvil.
"""

from dataclasses import replace
from datetime import UTC, datetime
from decimal import Decimal
from types import SimpleNamespace

import pytest

from almanak.connectors.uniswap_v3.receipt_parser import UniswapV3ReceiptParser
from almanak.framework.execution.gateway_orchestrator import GatewayExecutionOrchestrator
from almanak.framework.execution.multichain import MultiChainOrchestrator
from almanak.framework.execution.signer.local import LocalKeySigner
from almanak.framework.execution.submitter.public import PublicMempoolSubmitter
from almanak.framework.gateway_client import GatewayClient, GatewayClientConfig
from almanak.framework.intents import SwapIntent
from almanak.framework.intents.compiler import IntentCompiler
from almanak.framework.intents.vocabulary import IntentType
from almanak.gateway.core.settings import GatewaySettings
from almanak.gateway.services.execution_service import ExecutionServiceServicer
from tests.conftest_gateway import GatewayServerThread, find_free_port
from tests.intents.conftest import CHAIN_CONFIGS, SWAP_MAX_SLIPPAGE, get_token_balance
from tests.intents.pool_helpers import fail_if_v3_pool_missing

CHAIN = "bsc"
USD_QUOTE = Decimal("600")
pytestmark = [
    pytest.mark.bsc,
    pytest.mark.swap,
    pytest.mark.no_zodiac(reason="EOA gateway signing boundary: assert no owner signature or broadcast on cap refusal"),
]


@pytest.fixture
def gas_policy_gateway(anvil_bsc, test_private_key, monkeypatch, tmp_path):
    """Reuse the gateway test server with one fork and observation-only probes."""
    observed = SimpleNamespace(compilations=[], policies=[], signed=[], submitted=[])
    original_compile = IntentCompiler.compile
    original_execute = ExecutionServiceServicer.ExecuteWithGasPolicy
    original_sign = LocalKeySigner.sign
    original_submit = PublicMempoolSubmitter.submit
    original_sequential = PublicMempoolSubmitter.submit_sequential

    def compile_observed(self, *args, **kwargs):
        result = original_compile(self, *args, **kwargs)
        observed.compilations.append(result)
        return result

    async def execute_observed(self, request, context):
        observed.policies.append(request.gas_cost_policy)
        return await original_execute(self, request, context)

    async def sign_observed(self, tx, chain):
        observed.signed.append(replace(tx))
        return await original_sign(self, tx, chain)

    async def submit_observed(self, txs, *args, **kwargs):
        observed.submitted.extend(txs)
        return await original_submit(self, txs, *args, **kwargs)

    async def sequential_observed(self, txs, *args, **kwargs):
        observed.submitted.extend(txs)
        return await original_sequential(self, txs, *args, **kwargs)

    async def fixture_native_quote(self, chain):
        assert chain == CHAIN
        return float(USD_QUOTE), datetime.now(UTC)

    monkeypatch.setattr(IntentCompiler, "compile", compile_observed)
    monkeypatch.setattr(ExecutionServiceServicer, "ExecuteWithGasPolicy", execute_observed)
    monkeypatch.setattr(ExecutionServiceServicer, "_gas_policy_native_price", fixture_native_quote)
    monkeypatch.setattr(LocalKeySigner, "sign", sign_observed)
    monkeypatch.setattr(PublicMempoolSubmitter, "submit", submit_observed)
    monkeypatch.setattr(PublicMempoolSubmitter, "submit_sequential", sequential_observed)
    monkeypatch.setenv("ALMANAK_STATE_DB", str(tmp_path / "almanak_state.db"))
    port = find_free_port()
    settings = GatewaySettings(
        grpc_port=port,
        grpc_host="127.0.0.1",
        network="anvil",
        chains=[CHAIN],
        private_key=test_private_key,
        metrics_enabled=False,
        audit_enabled=False,
        allow_insecure=True,
    )
    server = GatewayServerThread(settings, anvil_ports={CHAIN: anvil_bsc.port, "bnb": anvil_bsc.port})
    server.start()
    client = GatewayClient(GatewayClientConfig(host="127.0.0.1", port=port))
    try:
        client.connect()
        yield client, observed
    finally:
        client.disconnect()
        server.stop()


@pytest.mark.intent(IntentType.SWAP)
@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("native_cap", "usd_cap", "refusal"),
    [(0.01, 10.0, None), (1e-18, 10.0, "native"), (0.01, 1e-12, "USD")],
    ids=["allowed", "native-cap-refuses", "usd-cap-refuses"],
)
async def test_multichain_gateway_gas_policy(
    web3, funded_wallet, price_oracle, gas_policy_gateway, native_cap, usd_cap, refusal
):  # noqa: layers - CompileIntent performs compilation inside the real gateway; results are observed below.
    client, observed = gas_policy_gateway
    token_in = CHAIN_CONFIGS[CHAIN]["tokens"]["USDT"]
    token_out = CHAIN_CONFIGS[CHAIN]["tokens"]["WBNB"]
    fail_if_v3_pool_missing(web3, CHAIN, "uniswap_v3", token_in, token_out, 500)
    amount = Decimal("1")
    intent = SwapIntent(
        from_token="USDT",
        to_token="WBNB",
        amount=amount,
        max_slippage=SWAP_MAX_SLIPPAGE,
        protocol="uniswap_v3",
        chain=CHAIN,
    )
    orchestrator = MultiChainOrchestrator.from_gateway(
        gateway_client=client,
        chains=["arbitrum", CHAIN],
        primary_chain="arbitrum",
        wallet_address=funded_wallet,
        max_gas_cost_native=native_cap,
        max_gas_cost_usd=usd_cap,
    )
    input_before = get_token_balance(web3, token_in, funded_wallet)
    output_before = get_token_balance(web3, token_out, funded_wallet)
    native_before = web3.eth.get_balance(funded_wallet)
    nonce_before = web3.eth.get_transaction_count(funded_wallet)
    execution_result = await orchestrator.execute(intent, price_map={k: str(v) for k, v in price_oracle.items()})
    assert observed.compilations
    assert all(compilation.status.value == "SUCCESS" for compilation in observed.compilations)
    assert all(compilation.action_bundle is not None for compilation in observed.compilations)
    assert set(orchestrator._gateway_orchestrators) == {CHAIN}
    assert isinstance(orchestrator._gateway_orchestrators[CHAIN], GatewayExecutionOrchestrator)
    assert len(observed.policies) == 1
    policy = observed.policies[0]
    assert policy.HasField("max_gas_cost_native") and policy.max_gas_cost_native == native_cap
    assert policy.HasField("max_gas_cost_usd") and policy.max_gas_cost_usd == usd_cap
    input_after = get_token_balance(web3, token_in, funded_wallet)
    output_after = get_token_balance(web3, token_out, funded_wallet)
    native_after = web3.eth.get_balance(funded_wallet)
    if refusal:
        assert not execution_result.success
        assert refusal.lower() in execution_result.error.lower(), execution_result.error
        assert "exceeds" in execution_result.error.lower(), execution_result.error
        assert observed.signed == []
        assert observed.submitted == []
        assert input_after == input_before
        assert output_after == output_before
        assert native_after == native_before
        assert web3.eth.get_transaction_count(funded_wallet) == nonce_before
        return

    assert execution_result.success, execution_result.error
    assert observed.signed and observed.submitted
    assert input_before - input_after == int(amount * 10**18)
    amount_received = output_after - output_before
    assert amount_received > 0
    gateway_result = execution_result.tx_result
    assert gateway_result is not None and gateway_result.receipts
    parser = UniswapV3ReceiptParser(chain=CHAIN)
    swaps = []
    gas_paid = 0
    for transaction in gateway_result.transaction_results:
        assert transaction.receipt is not None
        receipt = transaction.receipt
        parsed = parser.parse_receipt(receipt.to_dict())
        assert parsed.success
        if parsed.swap_result is not None:
            swaps.append(parsed.swap_result)
        mined = web3.eth.get_transaction_receipt(receipt.tx_hash)
        assert mined["status"] == 1
        gas_paid += mined["gasUsed"] * mined["effectiveGasPrice"]
    assert len(swaps) == 1
    assert swaps[0].amount_in == input_before - input_after
    assert swaps[0].amount_out == amount_received
    assert native_before - native_after == gas_paid > 0
    assert web3.eth.get_transaction_count(funded_wallet) - nonce_before == len(gateway_result.receipts)
    for tx in observed.signed:
        fee = tx.max_fee_per_gas if tx.max_fee_per_gas is not None else tx.gas_price
        assert fee is not None and fee > 0
        liability = Decimal(tx.gas_limit * fee) / Decimal(10**18)
        assert liability <= Decimal(str(native_cap))
        assert liability * USD_QUOTE <= Decimal(str(usd_cap))
