"""Concrete candidate parameters from the reusable Pancake author configuration."""

from almanak.configuration.yaml import load
from almanak.connectors.pancakeswap_v3.models import Parameters
from almanak.models import Account, AccountBinding, Eoa, StrategyAccounts
from scripts.legacy_configuration.author import normalize
from tests.legacy_references import harness_directory


def parameters(*, lp=False, profile=None, network="bnb"):
    name = "lp" if lp else "trading"
    frozen = harness_directory(f"pancakeswap_v3_{network}_{name}")
    raw = load(frozen / "config.yaml").value
    raw.pop("execution")
    author = normalize(raw)
    origin = author.allocations[0].chain
    if profile is None:
        profile = Eoa(kind="eoa", fund=Account(chain=origin, address="0x" + "12" * 20))
    allocations = tuple(
        a.model_copy(update={"pool": a.pool.model_copy(update={"chain": profile.fund.chain})})
        for a in author.allocations
    )
    return Parameters(
        connector="pancakeswap_v3",
        decision_trigger=author.decision_trigger,
        asset_metadata=author.asset_metadata,
        accounts=StrategyAccounts(bindings=(AccountBinding(name="fund", profile=profile),)),
        allocations=allocations,
    )
