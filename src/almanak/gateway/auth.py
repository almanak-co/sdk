"""Local credentials for the dedicated trusted-runtime connection."""

import re
from pathlib import Path
from urllib.parse import urlsplit


def read_token(path: Path) -> str:
    if path.stat().st_mode & 0o077:
        raise ValueError("gateway authentication file must have private permissions (0600)")
    token = path.read_text().strip()
    if re.fullmatch(r"[0-9a-f]{64}", token) is None:
        raise ValueError("gateway authentication file must contain a 32-byte hex token")
    return token


def loopback_endpoint(endpoint: str) -> str:
    url = urlsplit(endpoint)
    if (
        url.scheme != "http"
        or url.hostname != "127.0.0.1"
        or not url.port
        or url.path not in ("", "/")
        or url.query
        or url.fragment
        or url.username is not None
    ):
        raise ValueError("gateway client requires an HTTP loopback endpoint with explicit port")
    return endpoint.rstrip("/")
