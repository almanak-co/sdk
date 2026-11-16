"""Bundled LLM-friendly documentation for the Almanak SDK.

The llms-full.txt file is generated from the MkDocs documentation site during
publish builds and bundled into the wheel as package data.

Usage:
    from almanak.llms import get_path, get_text

    # Get the file path (for agents to grep/read directly)
    path = get_path()

    # Or load the full text into memory
    text = get_text()
"""

from atexit import register
from importlib.resources import as_file, files

_resource_context = None
_resource_path = None


def get_path() -> str:
    """Return the filesystem path to the bundled llms-full.txt.

    This is the preferred API for CLI agents that want to search the file
    repeatedly using grep/read tools without loading it into memory. The
    as_file context is kept alive for the process lifetime because zip
    installs extract the resource to a temporary location that must not be
    deleted between calls; atexit releases it on shutdown.

    Raises:
        FileNotFoundError: If llms-full.txt was not bundled into this install.
    """
    global _resource_context, _resource_path
    if _resource_path is not None:
        return str(_resource_path)
    ref = files("almanak.llms").joinpath("llms-full.txt")
    _resource_context = as_file(ref)
    _resource_path = _resource_context.__enter__()
    if not _resource_path.exists():
        _resource_context.__exit__(None, None, None)
        _resource_context = None
        _resource_path = None
        raise FileNotFoundError(
            "llms-full.txt not found in this almanak install."
            " It is generated during publish builds - see docs for details."
        )
    register(_resource_context.__exit__, None, None, None)
    return str(_resource_path)


def get_text() -> str:
    """Load and return the full llms-full.txt content as a string.

    For one-off reads. If you need to search repeatedly, use get_path()
    and work with the file directly.
    """
    ref = files("almanak.llms").joinpath("llms-full.txt")
    return ref.read_text(encoding="utf-8")
