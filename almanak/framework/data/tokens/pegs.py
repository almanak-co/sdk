"""Address-keyed synthetic peg prices.

The token registry is the sole authoring surface for peg eligibility.  Every
lookup requires a strict :class:`TokenRef`, so a symbol collision can never
grant a synthetic price to an unrelated contract.
"""

from decimal import Decimal
from types import MappingProxyType

from almanak.core.chains import ChainRegistry

from .defaults import DEFAULT_TOKENS
from .models import PegClass, TokenRef, normalize_token_address_for_chain

_PEG_CLASS_VALUES: MappingProxyType[PegClass, Decimal] = MappingProxyType({PegClass.USD: Decimal("1")})
PEG_DEVIATION_THRESHOLD_BPS = Decimal("100")


def _build_peg_registry() -> MappingProxyType[tuple[str, str], PegClass]:
    registry: dict[tuple[str, str], PegClass] = {}
    for token in DEFAULT_TOKENS:
        if token.peg_class is None:
            continue
        for chain in token.chains:
            address = token.get_address(chain)
            if not address:
                continue
            identity = TokenRef(
                chain=chain,
                address=address,
                decimals=token.get_decimals(chain),
                symbol=token.symbol,
                provenance="static_registry",
            ).identity_key
            previous = registry.setdefault(identity, token.peg_class)
            if previous is not token.peg_class:
                raise RuntimeError(
                    f"Conflicting peg classes for token identity {identity}: "
                    f"{previous.value} vs {token.peg_class.value}"
                )
    return MappingProxyType(registry)


PEG_REGISTRY: MappingProxyType[tuple[str, str], PegClass] = _build_peg_registry()


def peg_for_identity(chain: str, address: str) -> Decimal | None:
    """Return the configured peg for a normalized chain/address identity."""
    canonical_chain = ChainRegistry.resolve(str(chain)).name
    identity = (canonical_chain, normalize_token_address_for_chain(address, canonical_chain))
    peg_class = PEG_REGISTRY.get(identity)
    return _PEG_CLASS_VALUES.get(peg_class) if peg_class is not None else None


def peg_class_for(token_ref: TokenRef) -> PegClass | None:
    """Return the registry peg class for an exact token identity, if any."""
    return PEG_REGISTRY.get(token_ref.identity_key)


def is_pegged(token_ref: TokenRef) -> Decimal | None:
    """Return the configured peg value for an exact token identity, if any."""
    peg_class = peg_class_for(token_ref)
    return _PEG_CLASS_VALUES.get(peg_class) if peg_class is not None else None


def peg_deviation_bps(price: Decimal, peg: Decimal) -> Decimal:
    """Return the absolute deviation between a measured price and its peg."""
    if peg <= 0:
        raise ValueError("Peg value must be positive")
    return abs(price - peg) / peg * Decimal("10000")


def is_within_peg(price: Decimal, peg: Decimal) -> bool:
    """Whether a measured price is within the SDK-wide peg tolerance."""
    return peg_deviation_bps(price, peg) <= PEG_DEVIATION_THRESHOLD_BPS


__all__ = [
    "PEG_DEVIATION_THRESHOLD_BPS",
    "PEG_REGISTRY",
    "is_pegged",
    "is_within_peg",
    "peg_for_identity",
    "peg_class_for",
    "peg_deviation_bps",
]
