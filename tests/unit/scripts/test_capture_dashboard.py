"""Contracts for the canonical quant-test dashboard capture helper."""

from __future__ import annotations

import base64
import importlib.util
import json
from pathlib import Path

SCRIPT = Path(__file__).parents[3] / "qa_lab" / "capture_dashboard.py"


def _load_module():
    spec = importlib.util.spec_from_file_location("capture_dashboard", SCRIPT)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_capture_creates_output_parent_before_browser_writes(monkeypatch, tmp_path: Path) -> None:
    module = _load_module()
    output = tmp_path / "dashboard" / "h00-full.png"
    browser_calls: list[tuple[str, ...]] = []

    def fake_ab(_session: str, *args: str, timeout: int = 60) -> str:
        del timeout
        browser_calls.append(args)
        if args[0] == "eval":
            if "btoa" in args[1]:
                payload = {
                    "schema_version": 1,
                    "title": "Strategy dashboard",
                    "url": "http://dashboard",
                    "visible_text": "PnL\nTrade Tape",
                    "elements": [{"kind": "heading", "tag": "h1", "text": "Strategy dashboard"}],
                }
                return base64.b64encode(json.dumps(payload).encode()).decode()
            return "Y"
        if args[0] == "screenshot":
            assert output.parent.is_dir()
            Path(args[-1]).write_bytes(b"raw")
        return ""

    def fake_trim(raw: str, out: str) -> tuple[int, int]:
        assert Path(raw).parent == output.parent
        Path(out).write_bytes(b"png")
        return (1600, 3000)

    monkeypatch.setattr(module, "_ab", fake_ab)
    monkeypatch.setattr(module, "_trim", fake_trim)
    monkeypatch.setattr(module.time, "sleep", lambda _seconds: None)

    assert module.capture("http://dashboard", str(output), timeout=1) == (1600, 3000, True)
    assert output.read_bytes() == b"png"
    assert any(call[0] == "screenshot" for call in browser_calls)
    evidence = json.loads(output.with_suffix(".render.json").read_text())
    assert evidence["artifact_kind"] == "almanak.dashboard_render_observation"
    assert evidence["measurement_status"] == "MEASURED"
    assert evidence["marker"] == {"rendered": True, "text": "Trade Tape"}
    assert evidence["render"]["visible_text"] == "PnL\nTrade Tape"
    assert evidence["screenshot"]["path"] == "h00-full.png"
    assert len(evidence["screenshot"]["sha256"]) == 64


def test_capture_without_dom_snapshot_is_explicitly_unmeasured(monkeypatch, tmp_path: Path) -> None:
    module = _load_module()
    output = tmp_path / "dashboard" / "h00-full.png"

    def fake_ab(_session: str, *args: str, timeout: int = 60) -> str:
        del timeout
        if args[0] == "eval":
            return "Y" if "btoa" not in args[1] else ""
        if args[0] == "screenshot":
            Path(args[-1]).write_bytes(b"raw")
        return ""

    def fake_trim(_raw: str, out: str) -> tuple[int, int]:
        Path(out).write_bytes(b"png")
        return (1600, 3000)

    monkeypatch.setattr(module, "_ab", fake_ab)
    monkeypatch.setattr(module, "_trim", fake_trim)
    monkeypatch.setattr(module.time, "sleep", lambda _seconds: None)

    assert module.capture("http://dashboard", str(output), timeout=1) == (1600, 3000, True)
    evidence = json.loads(output.with_suffix(".render.json").read_text())
    assert evidence["measurement_status"] == "UNMEASURED"
    assert evidence["render"] is None
