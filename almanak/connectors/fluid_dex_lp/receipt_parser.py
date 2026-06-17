"""Back-compat shim: ``fluid_dex_lp.receipt_parser`` re-exports the fungible-LP
receipt parser from ``almanak.connectors._fluid_core.dex_lp_receipt_parser``.
"""

from almanak.connectors._fluid_core import dex_lp_receipt_parser as _impl
from almanak.connectors._fluid_core.dex_lp_receipt_parser import *  # noqa: F401,F403


def __getattr__(name: str):  # noqa: ANN202
    return getattr(_impl, name)
