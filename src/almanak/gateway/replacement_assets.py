"""Verified ERC-20 fee independence, without selecting an author's token addresses."""

from almanak.caip import ChainId
from almanak.gateway.ethereum import DataError
from almanak.gateway.evm import erc20_address
from almanak.gateway.roles_storage import AuthorityReader
from almanak.models import Hash32, Model, UInt


class TokenRevision(Model):
    code_hash: Hash32
    implementation_hash: Hash32 | None = None
    implementation_slot: UInt | None = None


# These WETH and USDC bytecode revisions were exercised by the Ethereum
# replacement proofs. Deployment addresses do not change their fee independence.
REVISIONS = (
    TokenRevision(
        code_hash="0xd0a06b12ac47863b5c7be4185c2deaad1c61557033f56c7d4ea74429cbb25e23",
    ),
    TokenRevision(
        code_hash="0xd80d4b7c890cb9d6a4893e6b52bc34b56b25335cb13716e0d1d31383e6b41505",
        implementation_hash="0xcdfb7d322961af3acae7a8f7ee8b69c205b36f576cc5b077f170c7eb8ecbe3ea",
        implementation_slot=50801780122331352337026042894847907698553222651959119521779622085092237899971,
    ),
)


def require_network(chain):
    if chain != ChainId("eip155:1"):
        raise DataError(
            "replacement_unsupported", "network has no verified fee-only execution proof"
        )


def verify_asset(reader, asset, block):
    require_network(asset.chain)
    observed = AuthorityReader(reader, block).identity(erc20_address(asset, asset.chain))
    revision = TokenRevision.model_validate(
        observed.model_dump(include=set(TokenRevision.model_fields))
    )
    if revision not in REVISIONS:
        raise DataError(
            "deployment_changed", "fee-only asset code differs from the verified revisions"
        )
