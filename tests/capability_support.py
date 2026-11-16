"""Non-lending connector and non-EVM authorization conformance fixtures."""

import json
from typing import Literal

from pydantic import TypeAdapter, model_validator

from almanak.capabilities import CapabilityCodec, CapabilitySet
from almanak.effects import AssetDelta, Effects, WalletAsset
from almanak.gateway.evm import erc20_address
from almanak.gateway.permission_models import PermissionManifest
from almanak.gateway.roles_conditions import FunctionGrant, bounded, equal, matches
from almanak.models import Address, Model, StrategyAccounts, UInt


class TransferCapability(Model):
    kind: Literal["transfer"] = "transfer"
    source: WalletAsset
    destination: WalletAsset
    maximum: UInt

    @model_validator(mode="after")
    def units(self):
        if (self.source.chain, self.source.asset) != (
            self.destination.chain,
            self.destination.asset,
        ):
            raise ValueError("transfer cannot create a free bridge or conversion")
        return self


Transfers = CapabilitySet[TransferCapability]


class TransferConfig(Model):
    authority: Transfers


def validate_transfer(config, retained):
    if retained != config.authority:
        raise ValueError("capabilities differ from explicit economic parameters")


def admit_transfer(retained, intent):
    permitted = [
        r
        for r in retained.requirements
        if (r.source, r.destination) == (intent.source, intent.destination)
    ]
    if len(permitted) != 1 or not 0 < intent.maximum <= permitted[0].maximum:
        raise ValueError("transfer is outside retained authority")


def transfer_codec():
    return CapabilityCodec(
        TypeAdapter(TransferConfig),
        TypeAdapter(Transfers),
        validate_transfer,
        admit_transfer,
        "tests.capability_support:transfer_codec",
    )


class NonEvmAuthorization:
    def __init__(self, configured):
        self.configured = configured

    def authorize(self, intent):
        if (
            intent.source.chain != self.configured
            or intent.source.chain.chain_id.namespace != "solana"
        ):
            raise ValueError("account adapter does not authorize this chain/environment")
        return Effects[Model](
            assets=(
                AssetDelta(key=intent.source, amount=-intent.maximum),
                AssetDelta(key=intent.destination, amount=intent.maximum),
            )
        )


class SwapCapability(Model):
    kind: Literal["swap"] = "swap"
    source: WalletAsset
    output: WalletAsset
    maximum: UInt
    minimum_output: UInt
    venue: Address
    beneficiary: Address


Swaps = CapabilitySet[SwapCapability]


class SwapConfig(Model):
    accounts: StrategyAccounts
    authority: Swaps


def validate_swap(config, retained):
    if retained != config.authority:
        raise ValueError("swap authority differs from economic parameters")
    for requirement in retained.requirements:
        profile = config.accounts.binding(requirement.source.account).profile
        if (requirement.source.chain, requirement.beneficiary) != (
            profile.fund.chain,
            profile.fund.address,
        ):
            raise ValueError("swap capability has a different fund scope")
        if requirement.output.chain != requirement.source.chain:
            raise ValueError("swap output has a different chain scope")


def admit_swap(retained, intent):
    if intent not in retained.requirements:
        raise ValueError("swap intent is outside retained authority")


class InProcessCodec(CapabilityCodec):
    # This conformance fixture tests the shared codec independently of OS isolation.
    # The discovered Aave suite exercises evaluate() in the actual sandbox process.
    def evaluate(self, strategy, config):
        return self.execute_hook(
            json.dumps(
                {
                    "source": strategy.source,
                    "filename": strategy.filename,
                    "config": config.model_dump(mode="json"),
                }
            )
        )


class SwapPermissions:
    runtime_checks = (
        "Quote freshness and economically acceptable execution remain runtime checks.",
    )
    manifest_type = PermissionManifest[Swaps]
    codec = InProcessCodec(
        TypeAdapter(SwapConfig), TypeAdapter(Swaps), validate_swap, admit_swap, "fixture"
    )

    def __init__(self, contracts):
        self.contracts = contracts

    def identities(self, config, account, reader):
        observed = tuple(reader.identity(c.address) for c in self.contracts)
        if observed != self.contracts:
            raise ValueError("swap target code changed")
        return observed

    @staticmethod
    def derive(profile, capabilities):
        grants = []
        for requirement in capabilities.requirements:
            if requirement.source.chain != profile.fund.chain:
                raise ValueError("swap grants cannot mix chain/environment scopes")
            inputs = matches(
                equal(
                    "address",
                    erc20_address(requirement.source.asset, requirement.source.chain.chain_id),
                ),
                equal(
                    "address",
                    erc20_address(requirement.output.asset, requirement.output.chain.chain_id),
                ),
                bounded(requirement.maximum),
                parameter=3,
            )
            receivers = matches(
                matches(
                    equal("address", requirement.beneficiary),
                    equal("uint256", requirement.minimum_output),
                    parameter=3,
                ),
                parameter=4,
            )
            grants.append(
                FunctionGrant(
                    target=requirement.venue,
                    signature="swap((address,address,uint256),(address,uint256)[])",
                    condition=matches(inputs, receivers),
                    explanation=(
                        "Bounded input and exactly one Safe recipient with its minimum output.",
                    ),
                )
            )
        return tuple(grants), ()
