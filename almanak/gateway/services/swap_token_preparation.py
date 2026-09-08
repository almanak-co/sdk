"""Gateway-local metadata preparation for synchronous EVM swap compilation."""

import warnings
from collections.abc import Awaitable, Callable
from dataclasses import replace

from almanak.core.asset_identity import AssetIdentity, AssetNamespace
from almanak.core.chains import ChainRegistry
from almanak.framework.data.tokens import ResolvedToken, SymbolTokenResolutionWarning, TokenResolver
from almanak.framework.data.tokens.address_resolution import looks_like_evm_address
from almanak.framework.data.tokens.defaults import NATIVE_SENTINEL
from almanak.framework.data.tokens.exceptions import TokenNotFoundError, TokenResolutionError
from almanak.framework.data.tokens.resolver import fold_native_address_alias
from almanak.framework.intents.vocabulary import SwapIntent


class SwapTokenPreparationError(ValueError):
    """A swap lacks exact contract metadata or an exact contract price."""

    def __init__(self, message: str, code: str = "TOKEN_METADATA_UNAVAILABLE") -> None:
        super().__init__(message)
        self.code = code


def swap_token_inputs(intent: SwapIntent, chain: str) -> list[str]:
    """Keep contract identities intact; never reduce a contract to its ticker."""
    if intent.chain and ChainRegistry.resolve(intent.chain).name != chain:
        raise SwapTokenPreparationError("Swap intent chain does not match the compilation chain", "INVALID_CHAIN")
    tokens = []
    for token in (intent.from_token, intent.to_token):
        if "/" in token:
            identity = AssetIdentity.from_caip19(token)
            if identity.chain != chain:
                raise SwapTokenPreparationError(
                    "Swap token chain does not match the compilation chain", "INVALID_CHAIN"
                )
            token = NATIVE_SENTINEL if identity.asset_namespace is AssetNamespace.NATIVE else identity.asset_reference
        if looks_like_evm_address(token):
            token = token.lower()
            if fold_native_address_alias(token, chain).lower() == NATIVE_SENTINEL.lower():
                # Native assets have a registry-defined ticker, not an ERC-20
                # contract endpoint. Keep that authorized price-source route.
                token = ChainRegistry.resolve(chain).native.symbol
        tokens.append(token)
    return list(dict.fromkeys(tokens))


async def discover_swap_tokens(
    inputs: list[str],
    chain: str,
    resolver: TokenResolver,
    resolve_for_pricing: Callable[[str, str], Awaitable[ResolvedToken | None]] | None,
) -> tuple[ResolvedToken, ...]:
    """Resolve only offline misses through the existing in-process gateway seam."""
    discovered = []
    for token in inputs:
        try:
            with warnings.catch_warnings():
                warnings.simplefilter("ignore", SymbolTokenResolutionWarning)
                cached = resolver.resolve(token, chain, skip_gateway=True, log_errors=False)
            if not cached.is_verified:
                discovered.append(cached)
            continue
        except TokenNotFoundError:
            pass
        except TokenResolutionError as exc:
            raise SwapTokenPreparationError(str(exc), "INVALID_TOKEN") from exc
        if not looks_like_evm_address(token) or resolve_for_pricing is None:
            raise SwapTokenPreparationError(
                f"Cannot resolve swap token {token} on {chain}. Use its contract address and enable gateway metadata lookup."
            )
        resolved = await resolve_for_pricing(token, chain)
        if (
            not isinstance(resolved, ResolvedToken)
            or resolved.chain != chain
            or resolved.address.lower() != token.lower()
            or resolved.chain_id != ChainRegistry.resolve(chain).chain_id
            or resolved.is_native
        ):
            raise SwapTokenPreparationError(f"No matching on-chain token metadata for {chain}:{token}.")
        # ERC-20 metadata is display/amount information, not authority to use a
        # symbol-derived price or synthetic peg for an unregistered contract.
        discovered.append(replace(resolved, is_verified=False, coingecko_id=None))
    return tuple(discovered)
