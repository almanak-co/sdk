"""Connector manifest contract for fixed teardown tolerances."""

import pytest

from almanak.connectors._base.types import ProtocolKind
from almanak.connectors._connector import Connector, ImportRef


def test_fixed_teardown_slippage_defaults_to_none() -> None:
    connector = Connector(name="example", kind=ProtocolKind.SWAP)

    assert connector.fixed_teardown_slippage is None


def test_fixed_teardown_slippage_accepts_import_ref() -> None:
    ref = ImportRef(module="example.module", attribute="resolve_tolerance")

    connector = Connector(name="example", kind=ProtocolKind.SWAP, fixed_teardown_slippage=ref)

    assert connector.fixed_teardown_slippage == ref


def test_fixed_teardown_slippage_rejects_non_import_ref() -> None:
    with pytest.raises(ValueError, match="fixed_teardown_slippage"):
        Connector(name="example", kind=ProtocolKind.SWAP, fixed_teardown_slippage="resolver")  # type: ignore[arg-type]
