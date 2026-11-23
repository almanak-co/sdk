"""Shared values for bridge intent and transfer tests. No connector is implied."""

from datetime import UTC, datetime

from almanak.caip import AssetType, ChainId
from almanak.connectors.identifiers import ConnectorId
from almanak.effects import WalletAsset
from almanak.models import Account, AssetAmount, Chain
from almanak.transfers import Transfer

ORIGIN = Chain(chain_id=ChainId("eip155:42161"), environment="fork")
DESTINATION = Chain(chain_id=ChainId("eip155:8453"), environment="fork")
THIRD = Chain(chain_id=ChainId("eip155:10"), environment="fork")
SENT = AssetType("eip155:42161/erc20:0xaf88d065e77c8cc2239327c5edb3a432268e5831")
RECEIVED = AssetType("eip155:8453/erc20:0x833589fcd6edb6e08f4c7c32d4f71b54bda02913")
THIRD_ASSET = AssetType("eip155:10/erc20:0x0b2c639c533813f4aa9d7837caf62653d097ff85")
SOURCE_FUND = Account(chain=ORIGIN, address="0x" + "11" * 20)
DESTINATION_FUND = Account(chain=DESTINATION, address="0x" + "22" * 20)
DEADLINE = datetime(2026, 11, 23, 12, tzinfo=UTC)
ASSETS = {ORIGIN: SENT, DESTINATION: RECEIVED, THIRD: THIRD_ASSET}


def amount(asset, value):
    return AssetAmount(asset=asset, amount=value)


def transfer(reference="deposit-1", leaves=ORIGIN, reaches=DESTINATION, **changes):
    values = {
        "connector": ConnectorId("test_bridge"),
        "allocation": "move",
        "source": WalletAsset(account="here", chain=leaves, asset=ASSETS[leaves]),
        "sent": amount(ASSETS[leaves], 1_000),
        "destination": WalletAsset(account="there", chain=reaches, asset=ASSETS[reaches]),
        "offered": amount(ASSETS[reaches], 990),
        "reference": reference,
        "fill_deadline": DEADLINE,
        **changes,
    }
    return Transfer(**values)
