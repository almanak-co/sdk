"""QuoteAsset — the asset a strategy's performance is measured in.

Introduced as **definition only** (PR-1): strategy authors declare a quote
asset, the framework resolves and exposes it as metadata, and the hosted
platform consumes it for performance reporting. The SDK does **not** yet change
any valuation, accounting, or CLI behaviour based on this value — every strategy
is still measured in USD on-disk. See
``docs/internal/blueprints/04-strategy-layer.md`` (§Quote asset).

Two kinds:

* ``fiat_usd`` — the synthesized USD numeraire the platform already uses.
* ``token``    — a specific ERC-20 / SPL token identified by ``(chain_id, address)``.

Chains are identified by **numeric chain_id only** (never a chain-name string),
matching the canonical on-chain ``(chain_id, address)`` token identity and
side-stepping chain-name normalisation (and the duplicate-``Chain``-enum
tech-debt). Native gas / L1 tokens (ETH, AVAX, MNT, 0G) are represented by their
wrapped ERC-20 (WETH, WAVAX, WMNT, W0G) because the model requires an address.

This is a ``almanak.core`` value type: dependency-free, no framework imports, so
it can be consumed by both the strategy decorator and the config schema without
import cycles.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Any, Literal

QuoteAssetKind = Literal["fiat_usd", "token"]

# EVM addresses are 20-byte hex (``0x`` / ``0X`` + 40 hex chars). EVM chains
# (``chain_id != 0``) must match this — it catches a truncated / typo'd address
# cheaply, without a network round-trip — and are canonicalised to lowercase. The
# non-EVM sentinel ``chain_id == 0`` (Solana) accepts a base58 address as-is.
_EVM_ADDRESS_RE = re.compile(r"^0[xX][0-9a-fA-F]{40}$")


@dataclass(frozen=True)
class QuoteAsset:
    """The asset a strategy's performance is measured in (USD or a token).

    Construct via :meth:`usd`, :meth:`token`, or :meth:`parse`. Instances are
    frozen and structurally validated at construction. EVM-style addresses are
    lower-cased so ``(chain_id, address)`` is a stable canonical key.

    Attributes:
        kind: ``"fiat_usd"`` or ``"token"``.
        chain_id: Numeric chain id for token quote assets (``None`` for fiat).
            ``0`` is the non-EVM sentinel (Solana), mirroring the chain registry.
        address: Token contract address for token quote assets (``None`` for fiat).
    """

    kind: QuoteAssetKind
    chain_id: int | None = None
    address: str | None = None

    def __post_init__(self) -> None:
        if self.kind == "fiat_usd":
            if self.chain_id is not None or self.address is not None:
                raise ValueError(
                    "fiat_usd quote asset must not set chain_id/address; "
                    f"got chain_id={self.chain_id!r}, address={self.address!r}"
                )
            return
        if self.kind == "token":
            # ``bool`` is a subclass of ``int`` — reject it explicitly.
            if not isinstance(self.chain_id, int) or isinstance(self.chain_id, bool):
                raise ValueError(f"token quote asset requires an integer chain_id, got {self.chain_id!r}")
            if self.chain_id < 0:
                raise ValueError(f"token quote asset chain_id must be non-negative, got {self.chain_id}")
            if not isinstance(self.address, str) or not self.address.strip():
                raise ValueError("token quote asset requires a non-empty address")
            address = self.address.strip()
            # EVM chains (chain_id != 0) must carry an EVM-shaped address; only the
            # chain_id == 0 sentinel (Solana) is relaxed. Non-EVM L1s with a non-zero
            # id (none modelled today) would need an is_evm check from the chain
            # registry. Shape is enforced, but EIP-55 checksum is NOT — a mistyped
            # but well-shaped address keys to a different / nonexistent token.
            if self.chain_id == 0:
                # Non-EVM sentinel (Solana): accept the case-sensitive base58
                # address verbatim — there is no EVM shape to enforce.
                pass
            elif _EVM_ADDRESS_RE.match(address):
                address = address.lower()  # canonical key
            else:
                raise ValueError(
                    f"token quote asset on an EVM chain (chain_id={self.chain_id}) requires a "
                    f"'0x' + 40 hex-char address, got {self.address!r}"
                )
            # frozen dataclass: normalise the stored value via object.__setattr__.
            object.__setattr__(self, "address", address)
            return
        raise ValueError(f"unknown quote asset kind {self.kind!r}; expected 'fiat_usd' or 'token'")

    # -- constructors -------------------------------------------------------

    @classmethod
    def usd(cls) -> QuoteAsset:
        """USD fiat numeraire — the default quote asset for every strategy."""
        return cls(kind="fiat_usd")

    @classmethod
    def token(cls, chain_id: int, address: str) -> QuoteAsset:
        """A token quote asset identified by ``(chain_id, address)``."""
        return cls(kind="token", chain_id=chain_id, address=address)

    @classmethod
    def parse(cls, raw: Any) -> QuoteAsset:
        """Normalise a decorator / config value into a :class:`QuoteAsset`.

        Accepts:

        * ``None`` -> USD (the default).
        * an existing :class:`QuoteAsset` -> returned unchanged.
        * the string ``"USD"`` (case-insensitive) -> USD.
        * a mapping ``{"type"|"kind": "fiat_usd"}`` -> USD.
        * a mapping ``{"type"|"kind": "token", "chain_id": <int>, "address": "0x..."}``.

        Chain-*name* strings are intentionally rejected — only numeric
        ``chain_id`` is accepted. Raises ``ValueError`` / ``TypeError`` otherwise.
        """
        if raw is None:
            return cls.usd()
        if isinstance(raw, QuoteAsset):
            return raw
        if isinstance(raw, str):
            if raw.strip().upper() == "USD":
                return cls.usd()
            raise ValueError(
                f"unrecognised quote_asset string {raw!r}; use 'USD' or an object form "
                '{"type": "token", "chain_id": <int>, "address": "0x..."}'
            )
        if isinstance(raw, dict):
            return cls._parse_mapping(raw)
        raise TypeError(f"quote_asset must be None, str, dict, or QuoteAsset; got {type(raw).__name__}")

    @classmethod
    def _parse_mapping(cls, raw: dict[str, Any]) -> QuoteAsset:
        """Parse the mapping form accepted by :meth:`parse` (``{"type": ...}``)."""
        if "chain" in raw:
            raise ValueError(
                'quote_asset identifies chains by numeric "chain_id" only, not a '
                f'"chain" name string (got chain={raw["chain"]!r})'
            )
        kind = raw.get("type") or raw.get("kind")
        if kind is None:
            # Infer: token if it carries token fields, otherwise fiat USD.
            kind = "token" if (raw.get("chain_id") is not None or raw.get("address") is not None) else "fiat_usd"
        kind = str(kind).lower()
        if kind in ("fiat_usd", "usd", "fiat"):
            return cls.usd()
        if kind == "token":
            chain_id = raw.get("chain_id")
            address = raw.get("address")
            if chain_id is None or not address:
                raise ValueError(
                    'token quote_asset requires "chain_id" (int) and "address"; '
                    f"got chain_id={chain_id!r}, address={address!r}"
                )
            return cls.token(chain_id=chain_id, address=address)
        raise ValueError(f"unknown quote_asset type {kind!r}; expected 'fiat_usd' or 'token'")

    # -- serialisation / accessors -----------------------------------------

    def to_dict(self) -> dict[str, Any]:
        """JSON-serialisable canonical form (round-trips through :meth:`parse`)."""
        if self.kind == "fiat_usd":
            return {"type": "fiat_usd"}
        return {"type": "token", "chain_id": self.chain_id, "address": self.address}

    @property
    def is_usd(self) -> bool:
        """True for the fiat-USD numeraire."""
        return self.kind == "fiat_usd"

    def __str__(self) -> str:
        if self.kind == "fiat_usd":
            return "USD"
        return f"token:{self.chain_id}:{self.address}"


__all__ = ["QuoteAsset", "QuoteAssetKind"]
