"""Gas estimation defaults.

Per-chain gas knobs are owned by :class:`almanak.core.chains.ChainRegistry`
since VIB-4801. This package only re-exports the framework-wide defaults
(``DEFAULT_GAS_BUFFER``, ``DEFAULT_SIMULATION_BUFFER``); read per-chain
values via ``ChainRegistry.get(chain).gas.<field>`` for :class:`Chain`
enums, or ``ChainRegistry.try_resolve("chain-name").gas.<field>`` for
name / alias strings.
"""

from almanak.framework.execution.gas.constants import (
    DEFAULT_GAS_BUFFER,
    DEFAULT_SIMULATION_BUFFER,
)

__all__ = [
    "DEFAULT_GAS_BUFFER",
    "DEFAULT_SIMULATION_BUFFER",
]
