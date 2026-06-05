"""Connector-owned contract monitoring declarations."""

from __future__ import annotations

from collections.abc import Iterator, Mapping
from dataclasses import dataclass, field


@dataclass(frozen=True)
class ContractMonitoringSpec:
    """One contract-address-table selector plus receipt-parser binding.

    ``contract_key`` selects one exact address-table entry. ``contract_key_prefix``
    selects every entry whose key starts with the prefix, which lets a connector
    publish dynamic contract families such as per-market addresses without central
    framework branches.
    """

    protocol: str
    parser_module: str
    parser_class_name: str
    supported_actions: tuple[str, ...] = field(default_factory=tuple)
    contract_key: str | None = None
    contract_key_prefix: str | None = None

    def __post_init__(self) -> None:
        """Validate the selector shape without importing parser targets."""
        for field_name in ("protocol", "parser_module", "parser_class_name"):
            value = getattr(self, field_name)
            if not isinstance(value, str) or not value.strip():
                raise ValueError(f"ContractMonitoringSpec.{field_name} must be a non-empty string, got {value!r}")

        has_key = isinstance(self.contract_key, str) and bool(self.contract_key.strip())
        has_prefix = isinstance(self.contract_key_prefix, str) and bool(self.contract_key_prefix.strip())
        if has_key == has_prefix:
            raise ValueError("ContractMonitoringSpec requires exactly one of contract_key or contract_key_prefix")

        if self.contract_key is not None and not has_key:
            raise ValueError(
                f"ContractMonitoringSpec.contract_key must be non-empty when set, got {self.contract_key!r}"
            )
        if self.contract_key_prefix is not None and not has_prefix:
            raise ValueError(
                "ContractMonitoringSpec.contract_key_prefix must be non-empty when set, "
                f"got {self.contract_key_prefix!r}"
            )
        if not isinstance(self.supported_actions, tuple):
            raise ValueError(
                f"ContractMonitoringSpec.supported_actions must be a tuple[str, ...], got {self.supported_actions!r}"
            )
        bad_actions = [action for action in self.supported_actions if not isinstance(action, str) or not action.strip()]
        if bad_actions:
            raise ValueError(
                f"ContractMonitoringSpec.supported_actions must contain only non-empty strings, got {bad_actions!r}"
            )

    def matching_contracts(self, contracts: Mapping[str, str]) -> Iterator[tuple[str, str]]:
        """Yield ``(contract_type, address)`` pairs selected from an address table."""
        if self.contract_key is not None:
            address = contracts.get(self.contract_key)
            if address:
                yield self.contract_key, address
            return

        assert self.contract_key_prefix is not None
        for key, address in contracts.items():
            if key.startswith(self.contract_key_prefix) and address:
                yield key, address


__all__ = ["ContractMonitoringSpec"]
