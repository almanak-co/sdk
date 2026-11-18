"""One Uniswap protocol adapter for identified forks and explicit mainnet profiles."""

from almanak.connectors.erc20_approval import approval_succeeded
from almanak.connectors.funding import (
    capital_requirements,
    exchange_entry_transactions,
    funding_rows,
)
from almanak.connectors.gas_lifecycle import UNISWAP_V4_LIFECYCLE
from almanak.connectors.uniswap_v4.deployments import identities
from almanak.connectors.uniswap_v4.hooks import require_entry_code
from almanak.connectors.uniswap_v4.observations import snapshot
from almanak.connectors.uniswap_v4.planning import call, operation
from almanak.connectors.uniswap_v4.reconciliation import reconcile
from almanak.connectors.uniswap_v4.state import Approval, Burn, Collect, Mint, Swap
from almanak.connectors.uniswap_v4.validation import (
    expired,
    increases_exposure,
    validate_current,
    validate_operation,
)
from almanak.gateway.ethereum import DataError
from almanak.gateway.roles_storage import AuthorityReader
from almanak.models import Eoa, SafeRoles


class UniswapExecution:
    @staticmethod
    def validate_replacement(config, operation):
        # Verified deployments and direct EOA / Roles CALL paths have no gas-price-based
        # economic parameters. Current simulation still enforces the original operation.
        profile = config.accounts.binding(operation.account).profile
        if not isinstance(profile, (Eoa, SafeRoles)) or not isinstance(
            operation, (Approval, Swap, Mint, Collect, Burn)
        ):
            raise DataError(
                "replacement_unsupported", "operation/account does not support fee-only replacement"
            )
        from almanak.gateway.replacement_assets import require_network

        require_network(operation.target.pool.chain.chain_id)

    snapshot = staticmethod(snapshot)
    operation = staticmethod(operation)
    call = staticmethod(call)
    validate_operation = staticmethod(validate_operation)
    validate_current = staticmethod(validate_current)
    expired = staticmethod(expired)
    reconcile = staticmethod(reconcile)
    increases_exposure = staticmethod(increases_exposure)

    requirements = staticmethod(capital_requirements)
    # An entry slice approves Permit2 and the router's Permit2 allowance for the funding
    # currency, then swaps. A liquidity allocation, after its slices, approves Permit2
    # and the position manager's Permit2 allowance for both currencies (the swap spent
    # the funding ones) and mints: 8 with a single slice.
    gas_lifecycle = UNISWAP_V4_LIFECYCLE
    entry_transactions = staticmethod(exchange_entry_transactions(gas_lifecycle))

    @staticmethod
    def preflight(config, account, observations):
        # Composed callers scope observations by connector only, so bind them to the account here.
        observed = [o for o in observations if o.target.account == account]
        balances = {v.asset: v.amount for o in observed for v in (o.wallet0, o.wallet1)}
        tokens = list(funding_rows(capital_requirements(config, account), balances))
        return tokens, bool(observed) and all(
            not o.needs_cleanup and o.position_liquidity == 0 for o in observed
        )

    @staticmethod
    def validate_config(config):
        from almanak.connectors.uniswap_v4.deployments import infrastructure

        for allocation in config.allocations:
            infrastructure(config.target(allocation.name))

    @staticmethod
    def validate_simulation(operation, result, before):
        raw = bytes.fromhex(result[2:])
        expected = isinstance(operation, Approval) and operation.expiration is None
        if expected:
            if not approval_succeeded(raw):
                raise DataError("simulation", "token refused the bounded approval")
        elif raw:
            raise DataError("simulation", "periphery returned unexpected simulation bytes")

    @staticmethod
    def plan_evidence(backend, operation, call, before):
        backend.reader.pin_block(before.block.number)
        authority = AuthorityReader(backend.reader, before.block)
        identities(backend.config, operation.account, authority)
        require_hook_code(backend.config, operation, authority)
        return {}

    @staticmethod
    def current_checks(backend, plan, block=None):
        from almanak.gateway.replacement_assets import verify_asset
        from almanak.transaction_family import replacement_policy

        block = block or backend.reader.pin_block("latest")
        authority = AuthorityReader(backend.reader, block)
        identities(backend.config, plan.operation.account, authority)
        require_hook_code(backend.config, plan.operation, authority)
        if replacement_policy(backend.config, plan) is not None:
            for asset in plan.operation.target.assets:
                verify_asset(backend.reader, asset, block)


def require_hook_code(config, operation, authority):
    """Changed hook code refuses new exposure; exits and cleanup keep executing."""
    require_entry_code(
        authority, operation.target.pool, entering=increases_exposure(config, operation)
    )
