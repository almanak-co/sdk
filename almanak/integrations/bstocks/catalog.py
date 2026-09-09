"""Reviewed token identities for issuer-adjusted reference composition."""

from dataclasses import dataclass

MAX_CONTRACT_AGE_SECONDS = 30
MAX_CLOCK_SKEW_SECONDS = 5


@dataclass(frozen=True)
class TokenReferenceProfile:
    chain: str
    chain_id: int
    symbol: str
    address: str
    underlying: str
    decimals: int
    beacon: str
    implementation: str


# Chainlink's BSC GOOGL/USD product identifies GOOGL_EQ / USD_FX (share price),
# not a certificate NAV or an already multiplier-adjusted token price:
# https://data.chain.link/feeds/bsc/mainnet/googl-usd
# BNB's raw/share unit contract: https://docs.bnbchain.org/developer-kit/scaled-ui-amount/
GOOGLB = TokenReferenceProfile(
    chain="bsc",
    chain_id=56,
    symbol="GOOGLB",
    address="0x3F53De71c126BdaBAe20f9cD64848d317f6C3238".lower(),
    underlying="GOOGL",
    decimals=18,
    beacon="0x156D6dce9a4f6139a3406F1f021F1A4880De93a3".lower(),
    implementation="0xCFEd6c4679297ea4889F8183bC057B4A86C64e46".lower(),
)
PROFILES = (GOOGLB,)


def reference_profile(chain: str, instrument: str, token_address: str = "") -> TokenReferenceProfile | None:
    """Resolve only curated identity; explicit addresses must agree with symbols."""
    candidates = [p for p in PROFILES if p.chain == chain.lower() and p.symbol == instrument.strip().upper()]
    if token_address:
        candidates = [p for p in candidates if p.address == token_address.lower()]
        if len(candidates) != 1:
            raise ValueError("reference_token_identity_mismatch")
    if not candidates and any(p.symbol == instrument.strip().upper() for p in PROFILES):
        raise ValueError("reference_token_chain_unsupported")
    return candidates[0] if len(candidates) == 1 else None
