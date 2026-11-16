"""Reserve resolution shared by the compatible Aave V3 and SparkLend Pool ABIs.

Both connectors verify the same Pool, addresses provider and reserve token roles
at one anchor. Each keeps its own selection tag, reviewed Pool table and resolver
identity; only these verification steps are shared.
"""

from typing import Annotated

from pydantic import Field

from almanak.caip import AssetType, ChainId
from almanak.gateway.ethereum import DataError
from almanak.gateway.evm import erc20_address
from almanak.models import ZERO_ADDRESS, Asset, Block, Model

TOKEN_ROLES = ("a_token", "variable_debt_token")


class ReserveAssertions(Model):
    a_token: AssetType | None = None
    variable_debt_token: AssetType | None = None
    decimals: Annotated[int, Field(ge=0, le=255)] | None = None

    def foreign(self, chain: ChainId) -> bool:
        tokens = (self.a_token, self.variable_debt_token)
        return any(token is not None and token.chain != chain for token in tokens)

    def verify(self, tokens: tuple[AssetType, AssetType], asset: Asset):
        observed = {
            "a_token": tokens[0],
            "variable_debt_token": tokens[1],
            "decimals": asset.decimals,
        }
        for field, expected in self.model_dump(exclude_none=True).items():
            if expected != observed[field]:
                raise ValueError(f"reserve assertion {field} differs from verified metadata")


def require_selection_scope(selection):
    if selection.chain.namespace != "eip155" or selection.pool == ZERO_ADDRESS:
        raise ValueError("reserve selection requires an EVM chain and a nonzero Pool")
    erc20_address(selection.asset, selection.chain)
    if selection.assertions is not None and selection.assertions.foreign(selection.chain):
        raise ValueError("reserve token assertion belongs to another chain")


def require_scope(reader, selection, block: Block, name: str):
    if selection.chain != block.chain.chain_id or reader.chain != block.chain:
        raise DataError(
            "chain_mismatch", f"{name} resolution requires its selected chain and anchor"
        )


def require_anchor(reader, block: Block, name: str):
    if reader.pin_block(block.number) != block:
        raise DataError("block_mismatch", f"{name} resolution anchor is no longer canonical")


def reviewed_pool(selection, pool: str) -> bool:
    """An explicit Pool serves future chains with more than one reviewed Pool."""
    return selection.pool is None or selection.pool == pool


def require_pool(selection, pool: str, name: str) -> str:
    if not reviewed_pool(selection, pool):
        raise DataError(
            "deployment_mismatch",
            f"{name} selection names a Pool outside the reviewed table; "
            f"name {pool} on {selection.chain} or omit the field",
        )
    return pool


def verified_reserve(reader, protocol, source, block: Block):
    """The Pool, its addresses provider and the reserve's own tokens, at one anchor."""
    identities = [protocol.verify_revision(reader, source.pool, block, "pool")]
    protocol.pool_provider(reader, source, block)
    observed = protocol.read_reserve(reader, source, block)
    tokens = (observed.a_token, observed.variable_debt_token)
    for token, role in zip(tokens, TOKEN_ROLES, strict=True):
        identities.append(protocol.verify_revision(reader, token.reference, block, role))
    return tokens, tuple(identities)


def require_resolved_scope(resolved):
    block = resolved.block
    scopes = (resolved.reserve.chain, resolved.asset.chain)
    if resolved.selection.chain != block.chain.chain_id or scopes != (block.chain, block.chain):
        raise ValueError("resolved reserve has mixed chain or anchor scope")


def require_resolved_target(resolved, pool: str):
    selection, reserve = resolved.selection, resolved.reserve
    named = (reserve.pool, reserve.asset, resolved.asset.identifier)
    if named != (pool, selection.asset, selection.asset):
        raise ValueError("resolved reserve differs from its authored target")
    if not reviewed_pool(selection, pool):
        raise ValueError("resolved reserve names a Pool outside the reviewed table")


def require_resolved_identities(resolved):
    """A reloaded result must carry the verified roles, not only the reserve."""
    expected = (
        resolved.reserve.pool,
        resolved.a_token.reference,
        resolved.variable_debt_token.reference,
    )
    if tuple(identity.address for identity in resolved.identities) != expected:
        raise ValueError("resolved reserve identities differ from the verified roles")
    if resolved.selection.assertions is not None:
        resolved.selection.assertions.verify(resolved.tokens, resolved.asset)


def reserve_summary(resolved) -> dict:
    """Resolved identities for previews; evidence bodies never appear."""
    return {
        "selection": resolved.selection.model_dump(mode="json"),
        "pool": resolved.reserve.pool,
        "asset": str(resolved.asset.identifier),
        "decimals": resolved.asset.decimals,
        "a_token": str(resolved.a_token),
        "variable_debt_token": str(resolved.variable_debt_token),
        "block": resolved.block.number,
    }
