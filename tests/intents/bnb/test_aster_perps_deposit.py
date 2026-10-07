"""PERP_DEPOSIT into Aster Pro's BSC vault on a fork: compile, execute, decode, balances.

The vault is on-chain, so the deposit is fully observable on a fork; the
off-chain credit that follows is not, and is proven by the mainnet run instead.
"""

from decimal import Decimal

import pytest
from web3 import Web3

from almanak.connectors.aster_perps.addresses import ASTER_PRO, FUTURES_BROKER_ID
from almanak.connectors.aster_perps.vault_events import decode_deposits
from almanak.framework.execution.orchestrator import ExecutionContext, ExecutionOrchestrator
from almanak.framework.intents.compiler import IntentCompiler
from almanak.framework.intents.perp_intents import PerpDepositIntent
from almanak.framework.intents.vocabulary import IntentType
from tests.intents.conftest import CHAIN_CONFIGS, get_token_balance, get_token_decimals

CHAIN_NAME = "bsc"
VAULT = Web3.to_checksum_address(ASTER_PRO[CHAIN_NAME]["vault"])


@pytest.mark.bsc
@pytest.mark.no_zodiac(reason="Aster Pro authorizes EOA accounts only (connector declares safe_supported=False)")
@pytest.mark.intent(IntentType.PERP_DEPOSIT)
@pytest.mark.asyncio
async def test_aster_perps_deposit_moves_usdt_into_the_vault(
    web3: Web3,
    funded_wallet: str,
    orchestrator: ExecutionOrchestrator,
    price_oracle: dict[str, Decimal],
) -> None:
    usdt = CHAIN_CONFIGS[CHAIN_NAME]["tokens"]["USDT"]
    decimals = get_token_decimals(web3, usdt)
    amount = Decimal("25")
    amount_wei = int(amount * 10**decimals)
    wallet_before = get_token_balance(web3, usdt, funded_wallet)
    vault_before = get_token_balance(web3, usdt, VAULT)

    intent = PerpDepositIntent(amount=amount, asset="USDT", protocol="aster_perps", chain=CHAIN_NAME)
    compiled = IntentCompiler(chain=CHAIN_NAME, wallet_address=funded_wallet, price_oracle=price_oracle).compile(intent)
    assert compiled.status.value == "SUCCESS", compiled.error
    assert compiled.action_bundle.transactions[-1]["to"].lower() == VAULT.lower()

    result = await orchestrator.execute(
        compiled.action_bundle,
        ExecutionContext(chain=CHAIN_NAME, wallet_address=funded_wallet, simulation_enabled=True),
    )
    assert result.success, result.error

    deposits = [
        d for tx in result.transaction_results if tx.receipt for d in decode_deposits(tx.receipt.to_dict(), vault=VAULT)
    ]
    assert len(deposits) == 1
    [deposit] = deposits
    assert deposit.account.lower() == funded_wallet.lower()
    assert deposit.currency.lower() == usdt.lower()
    assert (deposit.amount, deposit.broker, deposit.is_native) == (amount_wei, FUTURES_BROKER_ID, False)

    assert wallet_before - get_token_balance(web3, usdt, funded_wallet) == amount_wei
    assert get_token_balance(web3, usdt, VAULT) - vault_before == amount_wei
