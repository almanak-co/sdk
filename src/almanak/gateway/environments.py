"""Execution environment bindings keep origin identity separate from transport authority."""

from pydantic import model_validator

from almanak.caip import ChainId
from almanak.models import Block, Chain, Hash32, Model

FORK_ENVIRONMENT = "anvil:"


def is_fork(chain: Chain) -> bool:
    """Whether the chain is one identified fork node: the prefix alone names none."""
    environment = chain.environment
    return environment.startswith(FORK_ENVIRONMENT) and len(environment) > len(FORK_ENVIRONMENT)


def same_environment(first: Chain, second: Chain) -> bool:
    """Each fork node names its own environment, so two identified forks are one
    environment. A fork never pairs with anything else."""
    chains = (first, second)
    if any(chain.environment.startswith(FORK_ENVIRONMENT) for chain in chains):
        return all(is_fork(chain) for chain in chains)
    return first.environment == second.environment


class ForkBinding(Model):
    chain: Chain
    origin: Block
    instance: Hash32
    signing_chain: ChainId = ChainId("eip155:31337")

    @model_validator(mode="after")
    def scope(self):
        if self.chain.environment != FORK_ENVIRONMENT + self.instance:
            raise ValueError("fork scope must identify its exact Anvil instance")
        if self.chain.chain_id not in (self.origin.chain.chain_id, ChainId("eip155:31337")):
            raise ValueError("fork logical chain differs from its origin")
        if self.signing_chain != ChainId("eip155:31337"):
            raise ValueError("identified Anvil fixtures use the isolated signing chain")
        return self


def fork_bindings(config) -> tuple[ForkBinding, ...]:
    bindings = getattr(config, "fork_bindings", ())
    if bindings:
        return bindings
    if getattr(config, "fork_origin", None) is None:
        return ()
    return (
        ForkBinding(
            chain=config.accounts.bindings[0].profile.fund.chain,
            origin=config.fork_origin,
            instance=config.fork_instance,
        ),
    )


def fork_binding(config, chain: Chain) -> ForkBinding:
    for binding in fork_bindings(config):
        if binding.chain == chain:
            return binding
    raise ValueError("chain has no identified fork binding")


def origin_chain(config, chain: Chain) -> Chain:
    if config.production is not None:
        return chain
    return fork_binding(config, chain).origin.chain


def validate_environments(config):
    from almanak.connectors.support import RunMode
    from almanak.connectors.target_routing import connector_for_target

    bindings = fork_bindings(config)
    if config.production is not None:
        if (
            bindings
            or getattr(config, "fork_instance", None) is not None
            or getattr(config, "fork_origin", None) is not None
        ):
            raise ValueError("production authority cannot contain fork identity")
        config.production.validate_accounts(config.accounts, config.allocations)
        mode = RunMode.PRODUCTION
    else:
        if not bindings:
            raise ValueError("execution requires explicit fork or production authority")
        validate_fork_bindings(config, bindings)
        mode = RunMode.FORK
    for allocation in config.positions:
        account = config.accounts.binding(allocation.account)
        target = config.target(allocation.name)
        connector = connector_for_target(target)
        chain = origin_chain(config, account.profile.fund.chain)
        connector.require_target(chain.chain_id, target, mode, account_profile=account.profile.kind)


def validate_fork_bindings(config, bindings):
    if getattr(config, "fork_bindings", ()) and (
        getattr(config, "fork_instance", None) is not None
        or getattr(config, "fork_origin", None) is not None
    ):
        raise ValueError("choose collection bindings or a retained legacy fork identity")
    if getattr(config, "fork_bindings", ()) and any(
        binding.chain.chain_id != binding.origin.chain.chain_id for binding in bindings
    ):
        raise ValueError("new fork bindings must preserve origin CAIP identity")
    chains = [binding.chain for binding in bindings]
    if len(set(chains)) != len(chains):
        raise ValueError("fork bindings must be unique")
    if set(chains) != {b.profile.fund.chain for b in config.accounts.bindings}:
        raise ValueError("fork bindings must cover exactly the configured account chains")
