"""Gas estimation defaults.

Per-chain gas knobs are owned by :class:`almanak.core.chains.ChainRegistry`
since VIB-4801. This package only re-exports the framework-wide defaults
(``DEFAULT_GAS_BUFFER``, ``DEFAULT_SIMULATION_BUFFER``); read per-chain
values via ``ChainRegistry.try_resolve(chain).gas.<field>``.
"""

from almanak.framework.execution.gas.constants import (
    DEFAULT_GAS_BUFFER,
    DEFAULT_SIMULATION_BUFFER,
)

__all__ = [
    "DEFAULT_GAS_BUFFER",
    "DEFAULT_SIMULATION_BUFFER",
]
