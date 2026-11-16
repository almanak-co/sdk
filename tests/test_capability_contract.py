"""Shared capabilities retain economic scope without lending or EVM identities."""

import json
from dataclasses import replace

import pytest

from almanak.caip import AssetType, ChainId
from almanak.effects import Balance, Portfolio, WalletAsset
from almanak.models import Chain, Model
from tests.capability_support import (
    NonEvmAuthorization,
    TransferCapability,
    TransferConfig,
    Transfers,
    transfer_codec,
)


def transfer(chain):
    asset = AssetType(f"{chain.chain_id}/token:So11111111111111111111111111111111111111112")
    return TransferCapability(
        source=WalletAsset(account="fund", chain=chain, asset=asset),
        destination=WalletAsset(account="recipient", chain=chain, asset=asset),
        maximum=12,
    )


def test_named_declarations_require_a_binding_and_retain_authority_validation():
    chain = Chain(
        chain_id=ChainId("solana:5eykt4UsFv8P8NJdTREpY1vzqKqZKvdp"), environment="fixture"
    )
    config = TransferConfig(authority=Transfers(requirements=(transfer(chain),)))
    codec = transfer_codec()
    with pytest.raises(ValueError, match="no named capability declaration binding"):
        codec.declared(config)
    bound = replace(codec, declare=lambda config: config.authority)
    assert bound.declared(config) == config.authority
    forged = replace(codec, declare=lambda config: Transfers(requirements=()))
    with pytest.raises(ValueError, match="explicit economic parameters"):
        forged.declared(config)


def test_non_evm_capability_hook_authorization_and_accounting():
    chain = Chain(
        chain_id=ChainId("solana:5eykt4UsFv8P8NJdTREpY1vzqKqZKvdp"), environment="fixture"
    )
    intent = transfer(chain)
    config = TransferConfig(authority=Transfers(requirements=(intent,)))
    codec = transfer_codec()
    retained = codec.execute_hook(
        json.dumps(
            {
                "source": "def capabilities(config):\n    return config.authority\n",
                "filename": "strategy.py",
                "config": config.model_dump(mode="json"),
            }
        )
    )
    assert codec.capabilities.validate_json(retained.model_dump_json()) == retained
    codec.admit(retained, intent)
    inventory = Portfolio[Model](
        balances=(Balance(key=intent.source, amount=20), Balance(key=intent.destination, amount=0))
    )
    result = inventory.apply(NonEvmAuthorization(chain).authorize(intent))
    assert (result.balance(intent.source), result.balance(intent.destination)) == (8, 12)
    wrong = transfer(chain.model_copy(update={"environment": "another-deployment"}))
    with pytest.raises(ValueError, match="authority"):
        codec.admit(retained, wrong)
    with pytest.raises(ValueError, match="adapter"):
        NonEvmAuthorization(chain).authorize(wrong)
    with pytest.raises(ValueError, match="authority"):
        codec.admit(retained, intent.model_copy(update={"maximum": 13}))


@pytest.mark.parametrize("source", ["pass", "def capabilities(config): return {}"])
def test_missing_or_untyped_capabilities_fail_before_authorization(source):
    chain = Chain(
        chain_id=ChainId("solana:5eykt4UsFv8P8NJdTREpY1vzqKqZKvdp"), environment="fixture"
    )
    config = TransferConfig(authority=Transfers(requirements=(transfer(chain),)))
    with pytest.raises((ValueError, TypeError)):
        transfer_codec().execute_hook(
            json.dumps(
                {
                    "source": source,
                    "filename": "strategy.py",
                    "config": config.model_dump(mode="json"),
                }
            )
        )
