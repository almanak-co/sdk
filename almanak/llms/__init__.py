"""Bundled LLM-friendly documentation for the Almanak SDK.

The llms-full.txt file is generated from the MkDocs documentation site during
CI builds and bundled into the package as package data.

Usage:
    from almanak.llms import get_path, get_text

    # Get the file path (for agents to grep/read directly)
    path = get_path()

    # Or load the full text into memory
    text = get_text()
"""

from importlib.resources import as_file, files


def get_path() -> str:
    """Return the filesystem path to the bundled llms-full.txt.

    This is the preferred API for CLI agents that want to search the file
    repeatedly using grep/read tools without loading it into memory.

    Raises:
        FileNotFoundError: If llms-full.txt was not bundled into this install.
    """
    ref = files("almanak.llms").joinpath("llms-full.txt")
    # as_file() extracts to a temp dir for zipped packages, but for normal
    # installs it returns the real path. We keep the context manager open
    # by not using `with` - the file persists for the process lifetime.
    ctx = as_file(ref)
    path = ctx.__enter__()
    if not path.exists():
        ctx.__exit__(None, None, None)
        raise FileNotFoundError(
            "llms-full.txt not found in this almanak install. It is generated during CI builds - see docs for details."
        )
    return str(path)


def get_text() -> str:
    """Load and return the full llms-full.txt content as a string.

    For one-off reads. If you need to search repeatedly, use get_path()
    and work with the file directly.
    """
    ref = files("almanak.llms").joinpath("llms-full.txt")
    return ref.read_text(encoding="utf-8")
