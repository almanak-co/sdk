"""Prepared dataset identity and decoding use concrete connector types."""

from almanak.connectors import v3_economic_book as family
from almanak.connectors.dex_economic_dataset import targets as targets
from almanak.connectors.uniswap_v3 import economic_data
from almanak.connectors.uniswap_v3.history import BoundaryEvidence


class PoolBook(family.PoolBook):
    data = economic_data
    boundary_type = BoundaryEvidence


identity = PoolBook.identity
