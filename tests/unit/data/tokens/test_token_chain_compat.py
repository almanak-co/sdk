"""Token-model legacy-chain deserialization contract (Chain-enum removal, Rung 3).

``TokenRef.to_dict`` / ``ResolvedToken.to_dict`` historically wrote UPPERCASE
Chain-enum values (``"ETHEREUM"``) as their stable wire shapes, and disk-cache
rows persist them. The read-path contract is case-insensitive resolution,
forever; the write path now emits canonical lowercase names.
"""

from almanak.framework.data.tokens.models import ResolvedToken, TokenRef


def test_token_ref_legacy_uppercase_wire_record_loads() -> None:
    ref = TokenRef.from_dict(
        {
            "chain": "ETHEREUM",
            "address": "0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48",
            "decimals": 6,
            "symbol": "USDC",
        }
    )
    assert ref.chain == "ethereum"
    # New wire shape is canonical lowercase.
    assert ref.to_dict()["chain"] == "ethereum"


def test_token_ref_roundtrips_canonical_lowercase() -> None:
    ref = TokenRef(chain="arbitrum", address="0xFF970A61A04b1cA14834A43f5dE4533eBDDB5CC8", decimals=6)
    assert TokenRef.from_dict(ref.to_dict()) == ref


def test_resolved_token_legacy_uppercase_disk_cache_row_loads() -> None:
    token = ResolvedToken.from_dict(
        {
            "symbol": "USDC",
            "address": "0xaf88d065e77c8cc2239327c5edb3a432268e5831",
            "decimals": 6,
            "chain": "ARBITRUM",
            "chain_id": 42161,
        }
    )
    assert token.chain == "arbitrum"
    assert token.to_dict()["chain"] == "arbitrum"


def test_resolved_token_accepts_aliases_and_validates_chain_id() -> None:
    token = ResolvedToken(
        symbol="WBNB",
        address="0xbb4cdb9cbd36b01bd1cbaebf2de08d9173bc095c",
        decimals=18,
        chain="bnb",  # alias -> canonical "bsc"
        chain_id=56,
    )
    assert token.chain == "bsc"
