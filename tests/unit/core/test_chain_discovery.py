"""Descriptor-module auto-discovery guards (Chain-enum removal ladder, Rung 1).

``almanak/core/chains/__init__.py`` discovers descriptor modules with
``pkgutil`` — there is no hand-maintained import list. These tests pin the
two failure modes discovery could otherwise hide:

* a descriptor file that never registers (dropped chain), and
* a registration without a backing file (stale registry state).
"""

import importlib
from pathlib import Path

import almanak.core.chains as chains_pkg
from almanak.core.chains import ChainDescriptor, ChainRegistry


def _descriptor_stems() -> set[str]:
    pkg_dir = Path(next(iter(chains_pkg.__path__)))
    return {
        path.stem
        for path in pkg_dir.glob("*.py")
        # Reuse the package's own exclusion rule so the test cannot drift
        # from the discovery implementation.
        if chains_pkg._is_descriptor_module(path.stem)
    }


def test_descriptor_files_biject_registry_names() -> None:
    assert _descriptor_stems() == set(ChainRegistry.names())


def test_every_descriptor_module_exports_its_registration() -> None:
    for stem in sorted(_descriptor_stems()):
        module = importlib.import_module(f"almanak.core.chains.{stem}")
        descriptor = getattr(module, "DESCRIPTOR", None)
        assert isinstance(descriptor, ChainDescriptor), (
            f"almanak.core.chains.{stem} must define a module-level DESCRIPTOR"
        )
        assert descriptor.name == stem
        assert ChainRegistry.resolve(stem) is descriptor
