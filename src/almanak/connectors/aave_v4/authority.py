"""Economic capability validation shared by admission and permission compilation."""

from almanak.connectors.aave_v4.models import (
    AaveCapabilities,
    BorrowCapability,
    DebtAllocation,
    LendingParameters,
    SupplyCapability,
)
from almanak.lending_intents import (
    BorrowIntent,
    RepayIntent,
    SupplyIntent,
    WithdrawIntent,
)


def supply_capability(config: LendingParameters, name: str) -> SupplyCapability:
    return SupplyCapability(target=config.target(name))


def borrow_capability(config: LendingParameters, name: str) -> BorrowCapability:
    debt = config.allocation(name)
    if not isinstance(debt, DebtAllocation):
        raise ValueError("borrowing requires a configured debt allocation")
    return BorrowCapability(
        target=config.target(name),
        collateral=tuple(config.target(item) for item in debt.collateral),
    )


def declared(config: LendingParameters) -> AaveCapabilities:
    return AaveCapabilities(
        requirements=(
            *(supply_capability(config, a.name) for a in config.allocations),
            *(borrow_capability(config, a.name) for a in config.debts),
        )
    )


def validate_capabilities(config: LendingParameters, capabilities: AaveCapabilities) -> None:
    expected = tuple(config.target(a.name) for a in config.positions)
    actual = tuple(item.target for item in capabilities.requirements)
    if len(set(actual)) != len(actual) or set(actual) != set(expected):
        raise ValueError("capabilities must cover each configured position exactly once")
    for requirement in capabilities.requirements:
        validate_requirement(config, requirement)


def validate_requirement(config, requirement):
    allocation = config.allocation(requirement.target.allocation)
    expected = (
        borrow_capability(config, allocation.name)
        if isinstance(allocation, DebtAllocation)
        else supply_capability(config, allocation.name)
    )
    if requirement != expected:
        raise ValueError("capability differs from its validated economic allocation")


def admit_intent(capabilities: AaveCapabilities, intent) -> None:
    requirements = [item for item in capabilities.requirements if item.target == intent.target]
    if len(requirements) != 1:
        raise ValueError("intent has no retained economic capability")
    requirement = requirements[0]
    allowed = (
        (SupplyIntent, WithdrawIntent)
        if isinstance(requirement, SupplyCapability)
        else (BorrowIntent, RepayIntent)
    )
    if not isinstance(intent, allowed):
        raise ValueError("intent action is outside its asset-specific capability")


def capability_codec():
    from pydantic import TypeAdapter

    from almanak.capabilities import CapabilityCodec
    from almanak.connectors.aave_v4.models import EffectiveLendingConfig

    return CapabilityCodec(
        parameters=TypeAdapter(EffectiveLendingConfig),
        capabilities=TypeAdapter(AaveCapabilities),
        validate=validate_capabilities,
        admit=admit_intent,
        worker_factory="almanak.connectors.aave_v4.authority:capability_codec",
        declare=declared,
    )
