"""Offline, self-contained browser artifacts using the shared renderer."""

import base64
import hashlib
import html
import json
import os
from pathlib import Path

from almanak.dashboard.models import Response

ASSETS = Path(__file__).parent


def schema() -> dict:
    return {
        "$schema": "https://json-schema.org/draft/2020-12/schema",
        **Response.model_json_schema(),
    }


def script_json(value) -> str:
    return json.dumps(value, ensure_ascii=True).replace("<", "\\u003c").replace("&", "\\u0026")


def csp_hash(value: str) -> str:
    digest = base64.b64encode(hashlib.sha256(value.encode()).digest()).decode()
    return f"'sha256-{digest}'"


def render_html(response: Response, *, theme: dict[str, str] | None = None) -> str:
    """A self-contained page. `theme` maps renderer tokens to colours or to the embedding
    host's CSS variables; a host-themed page loosens only its CSP style-src (to
    'unsafe-inline') so the host's styles apply, and scripts stay pinned."""
    validated = Response.model_validate_json(response.model_dump_json())
    css = (ASSETS / "renderer.css").read_text()
    javascript = (ASSETS / "validate.js").read_text() + "\n" + (ASSETS / "renderer.js").read_text()
    javascript += "\nAlmanakDashboard.mount(document.getElementById('dashboard'), "
    javascript += "JSON.parse(document.getElementById('payload').textContent), "
    javascript += "JSON.parse(document.getElementById('schema').textContent), "
    javascript += f"{{theme: {script_json(theme)}}});\n"
    # A host theme reads the embedding page's CSS variables, which arrive as the host's own
    # styles: only the style rule loosens; scripts stay pinned and nothing may be fetched.
    styles = "'unsafe-inline'" if theme else csp_hash(css)
    policy = (
        "default-src 'none'; base-uri 'none'; form-action 'none'; connect-src 'none'; "
        f"script-src {csp_hash(javascript)}; style-src {styles}"
    )
    content = html.escape(policy, quote=True)
    meta = f'<meta http-equiv="Content-Security-Policy" content="{content}">'
    return (
        '<!doctype html><html lang="en"><head><meta charset="utf-8">'
        f"{meta}"
        '<meta name="viewport" content="width=device-width,initial-scale=1">'
        f"<title>Strategy dashboard | Almanak</title><style>{css}</style></head><body>"
        '<main id="dashboard"></main><noscript>This dashboard requires JavaScript.</noscript>'
        '<script type="application/json" id="payload">'
        f"{script_json(validated.model_dump(mode='json'))}</script>"
        f'<script type="application/json" id="schema">{script_json(schema())}</script>'
        f"<script>{javascript}</script></body></html>\n"
    )


def write_private(path: Path, content: str):
    # Preview artifacts can contain private strategy state. Never overwrite an
    # existing file or follow a symlink to an unrelated artifact.
    descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    with os.fdopen(descriptor, "w") as stream:
        stream.write(content)
