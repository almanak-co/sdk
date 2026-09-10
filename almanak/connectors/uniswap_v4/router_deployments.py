"""Deployment-bound UniversalRouter calldata layouts."""

from dataclasses import dataclass
from enum import StrEnum


class RouterABI(StrEnum):
    V4 = "v4"
    V4_HOP_PRICE = "v4_hop_price"


@dataclass(frozen=True)
class RouterDeployment:
    address: str
    abi: RouterABI
    runtime_hash: str | None = None


# Addresses qualify the ABI independently of pool fee, hook, and token selection.
ROUTER_DEPLOYMENTS = {
    "ethereum": RouterDeployment("0x66a9893cC07D91D95644AEDD05D03f95e1dBA8Af".lower(), RouterABI.V4),
    "base": RouterDeployment("0x6fF5693b99212Da76ad316178A184AB56D299b43".lower(), RouterABI.V4),
    "arbitrum": RouterDeployment("0xA51afAFe0263b40EdaEf0Df8781eA9aa03E381a3".lower(), RouterABI.V4),
    "optimism": RouterDeployment("0x851116D9223fabED8E56C0E6b8Ad0c31d98B3507".lower(), RouterABI.V4),
    "polygon": RouterDeployment("0x1095692A6237d83C6a72F3F5eFEdb9A670C49223".lower(), RouterABI.V4),
    "avalanche": RouterDeployment("0x94b75331AE8d42C1b61065089B7d48FE14aA73b7".lower(), RouterABI.V4),
    "bsc": RouterDeployment("0x1906c1d672b88cD1B9aC7593301cA990F94Eae07".lower(), RouterABI.V4),
    "robinhood": RouterDeployment(
        "0x8876789976dEcBfCbBbe364623C63652db8C0904".lower(),
        RouterABI.V4_HOP_PRICE,
        "0x2ce6aaaf9f4151f5e1cbf774668772f17f532ae11b15e9284fd0a072a8b0fbde",
    ),
}


def router_deployment(chain: str, address: str) -> RouterDeployment:
    deployment = ROUTER_DEPLOYMENTS.get(chain)
    if deployment is None or deployment.address != address.lower():
        raise ValueError("V4 router deployment has no qualified calldata layout")
    return deployment
