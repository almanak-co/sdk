"""Unit tests for ``scripts/run_demo.py`` env preparation.

Pins the env-isolation contract: ambient developer-shell state that would
break the smoke run (``AGENT_ID``, ``ALMANAK_GATEWAY_AUTH_TOKEN``,
``ALMANAK_CHAIN``, ``ALMANAK_CHAINS``) MUST NOT leak into the subprocess
spawned by the demo runner.

Regression history:

* iter-177 regress (2026-05-08) — ``make test-demo-quick`` flipped red on
  ``morpho_looping@ethereum`` because the developer's shell exported
  ``ALMANAK_CHAIN=arbitrum``. The runtime resolver
  (``almanak/framework/cli/run.py:398``) correctly raised ``ClickException``
  for the conflict, but the failure was an environment artefact, not a code
  regression. VIB-4177 added a bare ``env.pop`` for chain overrides.
* iter-178 / iter-179 regress (2026-05-09) — VIB-4177's ``env.pop`` was
  defeated by the subprocess's own ``python-dotenv`` reload. ``override=False``
  only protects keys already present in ``os.environ``; after ``pop()`` there
  is nothing to protect, so a ``.env`` line on disk re-injected the stale
  chain. VIB-4178 pins ``ALMANAK_CHAIN`` / ``ALMANAK_CHAINS`` to the target
  chain explicitly, so the dotenv reload is a no-op for those keys.
"""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parents[3]
_SCRIPT_PATH = _REPO_ROOT / "scripts" / "run_demo.py"
_spec = importlib.util.spec_from_file_location("run_demo", _SCRIPT_PATH)
run_demo = importlib.util.module_from_spec(_spec)
sys.modules["run_demo"] = run_demo
_spec.loader.exec_module(run_demo)


def test_prepare_subprocess_env_pins_chain_to_target() -> None:
    """ALMANAK_CHAIN / ALMANAK_CHAINS must be set to target_chain regardless of parent."""
    parent = {
        "ALMANAK_CHAIN": "arbitrum",
        "ALMANAK_CHAINS": "arbitrum,base",
        "PATH": "/usr/bin",
    }
    env = run_demo._prepare_subprocess_env("ethereum", parent)
    assert env["ALMANAK_CHAIN"] == "ethereum"
    assert env["ALMANAK_CHAINS"] == "ethereum"
    assert env["PATH"] == "/usr/bin"


def test_prepare_subprocess_env_dotenv_reload_simulation() -> None:
    """VIB-4178: simulate the subprocess's python-dotenv override=False reload.

    The subprocess calls ``load_dotenv(override=False)`` which copies ``.env``
    entries into ``os.environ`` ONLY when the key is absent. We assert that
    after ``_prepare_subprocess_env`` runs, the chain keys are present, so the
    simulated reload cannot mutate them.
    """
    parent = {
        # Simulates the developer's shell having ALMANAK_CHAIN=arbitrum, which
        # ``.env`` on disk would re-inject if the key were absent.
        "ALMANAK_CHAIN": "arbitrum",
    }
    env = run_demo._prepare_subprocess_env("ethereum", parent)

    # Simulate python-dotenv's override=False semantics against an .env line
    # `ALMANAK_CHAIN=arbitrum` on disk.
    dotenv_line = ("ALMANAK_CHAIN", "arbitrum")
    if dotenv_line[0] not in env:
        env[dotenv_line[0]] = dotenv_line[1]
    assert env["ALMANAK_CHAIN"] == "ethereum", (
        "VIB-4178: dotenv override=False must not be able to reintroduce a "
        "stale chain — _prepare_subprocess_env must leave the key set."
    )


def test_prepare_subprocess_env_strips_gateway_conflict_vars() -> None:
    """AGENT_ID / ALMANAK_GATEWAY_AUTH_TOKEN must not propagate (insecure-mode conflict)."""
    parent = {
        "AGENT_ID": "agent-12345",
        "ALMANAK_GATEWAY_AUTH_TOKEN": "secret-token",
    }
    env = run_demo._prepare_subprocess_env("ethereum", parent)
    assert "AGENT_ID" not in env
    assert "ALMANAK_GATEWAY_AUTH_TOKEN" not in env


def test_prepare_subprocess_env_strips_execution_mode() -> None:
    """ALMANAK_EXECUTION_MODE from the parent shell must not propagate.

    The smoke harness signs with the Anvil default EOA key, so an inherited
    Safe / Safe+Zodiac execution mode would trigger Safe-specific preflight
    and signing paths with no live Safe to talk to.
    """
    parent = {"ALMANAK_EXECUTION_MODE": "safe_zodiac"}
    env = run_demo._prepare_subprocess_env("ethereum", parent)
    assert "ALMANAK_EXECUTION_MODE" not in env


def test_prepare_subprocess_env_forces_allow_insecure() -> None:
    """ALMANAK_GATEWAY_ALLOW_INSECURE is always forced to 'true' for the smoke run."""
    parent: dict[str, str] = {}
    env = run_demo._prepare_subprocess_env("ethereum", parent)
    assert env["ALMANAK_GATEWAY_ALLOW_INSECURE"] == "true"


def test_prepare_subprocess_env_overrides_pre_existing_allow_insecure() -> None:
    """A parent ``ALMANAK_GATEWAY_ALLOW_INSECURE=false`` must be force-set to ``true``."""
    parent = {"ALMANAK_GATEWAY_ALLOW_INSECURE": "false"}
    env = run_demo._prepare_subprocess_env("ethereum", parent)
    assert env["ALMANAK_GATEWAY_ALLOW_INSECURE"] == "true"


def test_prepare_subprocess_env_sets_default_anvil_key() -> None:
    """Anvil default account #0 is set when no key is in the parent env."""
    env = run_demo._prepare_subprocess_env("ethereum", {})
    assert env["ALMANAK_PRIVATE_KEY"].startswith("0xac0974bec39a17e36ba4a6b4d238ff944")


def test_prepare_subprocess_env_preserves_existing_private_key() -> None:
    """A parent-supplied ALMANAK_PRIVATE_KEY is preserved (setdefault, not overwrite)."""
    parent = {"ALMANAK_PRIVATE_KEY": "0xdeadbeef"}
    env = run_demo._prepare_subprocess_env("ethereum", parent)
    assert env["ALMANAK_PRIVATE_KEY"] == "0xdeadbeef"


def test_prepare_subprocess_env_does_not_mutate_parent() -> None:
    """The parent dict supplied by the caller must not be mutated."""
    parent = {
        "ALMANAK_CHAIN": "arbitrum",
        "AGENT_ID": "agent-9",
    }
    snapshot = dict(parent)
    run_demo._prepare_subprocess_env("ethereum", parent)
    assert parent == snapshot
