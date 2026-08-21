"""Unit tests for ``scripts/ci/resolve-fork-block.sh``.

The script decides which Anvil fork block every intent-test lane runs against.
Its cached pin normally survives a whole ISO week, which is what keeps the RPC
proxy cache warm — but it also means the fork can lag live chain state by days.
VIB-6763: LiFi allow-listed a newly deployed fee forwarder in its Diamond and
its API immediately began routing every quote through it, so every fork pinned
before that allow-list transaction reverted with ``ContractCallNotAllowed()``.
No repo change can fix that; only moving the pin can. ``FORK_PIN_EPOCH`` is the
lever that moves it, and these tests are what keep the lever connected.

The epoch lives in the pin's FILENAME. ``/tmp/anvil-fork-pins`` is restored
twice by ``template_intent_test.yml`` from two independently prefix-matched
archives, and the second restore overlays the first — same-named files are
overwritten, files present only in the earlier archive survive. A pin plus a
separate epoch marker could therefore be sourced from different archives and
disagree; one filename cannot. ``test_divergent_archives_cannot_revive_a_stale_pin``
is the regression test for exactly that.

The script talks to an RPC over ``curl``. Rather than refactor it to accept an
injectable endpoint, the tests put a ``curl`` shim first on ``PATH`` that
answers ``eth_blockNumber`` and ``eth_call`` from a fixture, so the whole
script runs end to end without network access.
"""

from __future__ import annotations

import os
import subprocess
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[3]
SCRIPT = REPO_ROOT / "scripts" / "ci" / "resolve-fork-block.sh"

# Arbitrary head the curl shim reports for eth_blockNumber.
FAKE_HEAD = 0x1D6F0A1  # 30_797_985
FAKE_HEAD_DEC = str(FAKE_HEAD)

# A servable, syntactically valid, but STALE pin — the VIB-6763 shape.
STALE_PIN = "111111"

CURL_SHIM = """#!/usr/bin/env bash
# Minimal stand-in for curl covering exactly the two JSON-RPC calls
# resolve-fork-block.sh makes. Everything is answered from the args; no
# network, no state. eth_call always succeeds, so any cached pin looks
# perfectly servable and the servability self-heal can never fire — only the
# epoch mechanism can discard a pin in these tests.
payload=""
for ((i = 1; i <= $#; i++)); do
  if [[ "${!i}" == "-d" ]]; then
    j=$((i + 1))
    payload="${!j}"
  fi
done
if [[ "$payload" == *eth_blockNumber* ]]; then
  echo '{"jsonrpc":"2.0","id":1,"result":"__HEAD_HEX__"}'
elif [[ "$payload" == *eth_call* ]]; then
  echo '{"jsonrpc":"2.0","id":1,"result":"0x0000000000000000000000000000000000000000000000000000000000000001"}'
else
  echo '{"jsonrpc":"2.0","id":1,"error":{"code":-32601,"message":"unexpected"}}'
fi
"""


@pytest.fixture
def fake_curl(tmp_path: Path) -> Path:
    """A ``curl`` that answers the script's two RPC calls, first on PATH."""
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    shim = bin_dir / "curl"
    shim.write_text(CURL_SHIM.replace("__HEAD_HEX__", hex(FAKE_HEAD)))
    shim.chmod(0o755)
    return bin_dir


def run_resolver(
    chain: str,
    cache_dir: Path,
    fake_curl: Path,
    github_env: Path | None = None,
) -> subprocess.CompletedProcess[str]:
    env = dict(os.environ)
    env["PATH"] = f"{fake_curl}{os.pathsep}{env['PATH']}"
    if github_env is None:
        # Unset -> the script prints the export instead of writing it.
        env.pop("GITHUB_ENV", None)
    else:
        env["GITHUB_ENV"] = str(github_env)
    return subprocess.run(
        ["bash", str(SCRIPT), chain, "fake-alchemy-key", str(cache_dir)],
        capture_output=True,
        text=True,
        env=env,
        check=False,
    )


def script_epoch() -> str:
    """The FORK_PIN_EPOCH constant declared in the script under test."""
    for line in SCRIPT.read_text().splitlines():
        if line.startswith("FORK_PIN_EPOCH="):
            return line.split("=", 1)[1].strip()
    raise AssertionError("FORK_PIN_EPOCH is not declared in resolve-fork-block.sh")


def pin_path(cache: Path, chain: str, epoch: str | None = None) -> Path:
    return cache / f"block-pin-{chain}-e{epoch or script_epoch()}.txt"


# --------------------------------------------------------------------------- #
# Pin selection                                                               #
# --------------------------------------------------------------------------- #
def test_bootstraps_pin_on_empty_cache(tmp_path: Path, fake_curl: Path) -> None:
    cache = tmp_path / "cache"
    result = run_resolver("arbitrum", cache, fake_curl)

    assert result.returncode == 0, result.stderr
    assert "Bootstrapped fork block pin" in result.stdout
    assert pin_path(cache, "arbitrum").read_text().strip() == FAKE_HEAD_DEC


def test_reuses_a_current_epoch_pin(tmp_path: Path, fake_curl: Path) -> None:
    """Don't repin every run — that would keep the RPC cache permanently cold."""
    cache = tmp_path / "cache"
    cache.mkdir()
    pin_path(cache, "arbitrum").write_text(f"{STALE_PIN}\n")

    result = run_resolver("arbitrum", cache, fake_curl)

    assert result.returncode == 0, result.stderr
    assert f"Using pinned fork block: {STALE_PIN}" in result.stdout
    assert "Bootstrapped" not in result.stdout
    assert pin_path(cache, "arbitrum").read_text().strip() == STALE_PIN


def test_stale_epoch_forces_a_repin(tmp_path: Path, fake_curl: Path) -> None:
    """VIB-6763: the whole point of the lever.

    The cached pin is perfectly servable — the curl shim answers every probe
    successfully — so the pre-existing servability self-heal cannot fire. Only
    the epoch can discard it.
    """
    cache = tmp_path / "cache"
    cache.mkdir()
    pin_path(cache, "arbitrum", epoch="1").write_text(f"{STALE_PIN}\n")

    result = run_resolver("arbitrum", cache, fake_curl)

    assert result.returncode == 0, result.stderr
    assert "Bootstrapped fork block pin" in result.stdout
    assert pin_path(cache, "arbitrum").read_text().strip() == FAKE_HEAD_DEC


def test_pin_written_before_the_epoch_mechanism_is_ignored(tmp_path: Path, fake_curl: Path) -> None:
    """A cache archive from before this change carries an un-suffixed pin.

    That is the state every lane is in on the first run after the bump, so it
    has to bootstrap rather than silently reuse the pre-epoch pin.
    """
    cache = tmp_path / "cache"
    cache.mkdir()
    (cache / "block-pin-arbitrum.txt").write_text(f"{STALE_PIN}\n")

    result = run_resolver("arbitrum", cache, fake_curl)

    assert result.returncode == 0, result.stderr
    assert "Bootstrapped fork block pin" in result.stdout
    assert pin_path(cache, "arbitrum").read_text().strip() == FAKE_HEAD_DEC


def test_divergent_archives_cannot_revive_a_stale_pin(tmp_path: Path, fake_curl: Path) -> None:
    """The reason the epoch is in the filename rather than in a sidecar file.

    ``/tmp/anvil-fork-pins`` is restored twice from two independently
    prefix-matched cache archives, and the later restore only overwrites files
    it actually contains. So the directory can legitimately hold artifacts from
    two different runs at once. This reproduces that: a stale pre-epoch pin
    from one archive, a stale previous-epoch pin from another, and — the shape
    that broke the sidecar design — a leftover ``pin-epoch-<chain>.txt`` marker
    claiming the CURRENT epoch, which under the old design would have paired
    with the stale pin and suppressed the repin.

    None of them may be read. Only the epoch-suffixed pin counts, and there
    isn't one.
    """
    cache = tmp_path / "cache"
    cache.mkdir()
    (cache / "block-pin-arbitrum.txt").write_text("495316173\n")
    pin_path(cache, "arbitrum", epoch="1").write_text(f"{STALE_PIN}\n")
    (cache / "pin-epoch-arbitrum.txt").write_text(f"{script_epoch()}\n")

    result = run_resolver("arbitrum", cache, fake_curl)

    assert result.returncode == 0, result.stderr
    assert "Bootstrapped fork block pin" in result.stdout, (
        "a stale pin plus a current-looking epoch marker must not suppress the repin"
    )
    assert pin_path(cache, "arbitrum").read_text().strip() == FAKE_HEAD_DEC
    # The stale artifacts are ignored, not consumed, and not resurrected.
    assert (cache / "block-pin-arbitrum.txt").read_text().strip() == "495316173"
    assert pin_path(cache, "arbitrum", epoch="1").read_text().strip() == STALE_PIN


def test_epoch_never_moves_a_fixed_pin_chain(tmp_path: Path, fake_curl: Path) -> None:
    """Fixed-pin chains are calibrated to a block their fixture hard-pins.

    A repin lever that dragged them to head would silently decouple the CI pin
    from ``AnvilFixture``'s pin, which is the failure the FIXED_PIN branch
    exists to prevent.

    Also asserts the cache-save gate, because writing a pin file that did not
    exist is a repin here too. After an epoch bump this is a brand-new
    ``-e<epoch>`` filename while the cache archive still matches on the old
    one, so ``ANVIL_FORK_PIN_REPINNED`` is the only thing that can open the
    workflow's save gate — without it the chain re-bootstraps every run,
    silently green.
    """
    cache = tmp_path / "cache"
    cache.mkdir()
    github_env = tmp_path / "github_env"
    github_env.touch()

    result = run_resolver("robinhood", cache, fake_curl, github_env=github_env)

    assert result.returncode == 0, result.stderr
    assert "Using fixed fork block pin: 5610000" in result.stdout
    assert pin_path(cache, "robinhood").read_text().strip() == "5610000"
    written = _github_env_vars(github_env)
    assert written.get("ANVIL_FORK_BLOCK_ROBINHOOD") == "5610000"
    assert written.get("ANVIL_FORK_PIN_REPINNED") == "true", (
        "a fixed-pin chain writing a pin file that did not exist must signal the "
        "cache-save gate, or the file is never archived"
    )


def test_fixed_pin_reuse_does_not_write_the_cache_save_gate_variable(tmp_path: Path, fake_curl: Path) -> None:
    """The other direction: an already-present fixed pin must not re-signal."""
    cache = tmp_path / "cache"
    cache.mkdir()
    pin_path(cache, "robinhood").write_text("5610000\n")
    github_env = tmp_path / "github_env"
    github_env.touch()

    result = run_resolver("robinhood", cache, fake_curl, github_env=github_env)

    assert result.returncode == 0, result.stderr
    assert "Using fixed fork block pin: 5610000" in result.stdout
    assert "ANVIL_FORK_PIN_REPINNED" not in _github_env_vars(github_env)


# --------------------------------------------------------------------------- #
# REPINNED propagation                                                        #
#                                                                             #
# Once the epoch lives in the filename, an epoch bump leaves the cache archive #
# MATCHING (it still holds the previous epoch's pin), so the workflow's        #
# `pin-cache-restore.cache-matched-key == ''` arm no longer fires.             #
# ANVIL_FORK_PIN_REPINNED becomes the only thing that can open the "Save fork  #
# block pin" gate. If it stopped being written, every test above would stay    #
# green while CI silently never archived the new pin — repinning on every run, #
# cold RPC cache forever.                                                      #
# --------------------------------------------------------------------------- #
def _github_env_vars(github_env: Path) -> dict[str, str]:
    out: dict[str, str] = {}
    for line in github_env.read_text().splitlines():
        if "=" in line:
            key, _, value = line.partition("=")
            out[key] = value
    return out


def test_repin_writes_the_cache_save_gate_variable(tmp_path: Path, fake_curl: Path) -> None:
    cache = tmp_path / "cache"
    cache.mkdir()
    pin_path(cache, "arbitrum", epoch="1").write_text(f"{STALE_PIN}\n")
    github_env = tmp_path / "github_env"
    github_env.touch()

    result = run_resolver("arbitrum", cache, fake_curl, github_env=github_env)

    assert result.returncode == 0, result.stderr
    written = _github_env_vars(github_env)
    assert written.get("ANVIL_FORK_PIN_REPINNED") == "true", (
        "a bootstrap after an epoch bump must signal the workflow's cache-save gate, "
        "otherwise the fresh pin is never archived"
    )
    assert written.get("ANVIL_FORK_BLOCK_ARBITRUM") == FAKE_HEAD_DEC


def test_reuse_does_not_write_the_cache_save_gate_variable(tmp_path: Path, fake_curl: Path) -> None:
    """The gate must stay shut on a reuse, or every run burns cache quota."""
    cache = tmp_path / "cache"
    cache.mkdir()
    pin_path(cache, "arbitrum").write_text(f"{STALE_PIN}\n")
    github_env = tmp_path / "github_env"
    github_env.touch()

    result = run_resolver("arbitrum", cache, fake_curl, github_env=github_env)

    assert result.returncode == 0, result.stderr
    written = _github_env_vars(github_env)
    assert "ANVIL_FORK_PIN_REPINNED" not in written
    assert written.get("ANVIL_FORK_BLOCK_ARBITRUM") == STALE_PIN


# --------------------------------------------------------------------------- #
# The constant itself                                                         #
# --------------------------------------------------------------------------- #
def test_epoch_is_a_positive_integer_and_never_regresses() -> None:
    """The constant is only ever bumped upward, and only with a rationale."""
    epoch = script_epoch()
    assert epoch.isdigit(), f"FORK_PIN_EPOCH must be an integer, got {epoch!r}"
    assert int(epoch) >= 2, "FORK_PIN_EPOCH must not be lowered below the VIB-6763 bump"


def test_pin_filename_carries_the_epoch() -> None:
    """The single-artifact property this whole design rests on.

    If the epoch ever moves back out of the filename into a companion file,
    the pin and the epoch become two artifacts sourced from two independently
    selected cache archives, and they can disagree.
    """
    source = SCRIPT.read_text()
    assert 'PIN_FILE="$CACHE_DIR/block-pin-${CHAIN}-e${FORK_PIN_EPOCH}.txt"' in source, (
        "the epoch must be part of the pin filename, not a sidecar marker file"
    )
    assert "EPOCH_FILE" not in source, "a separate epoch marker file reintroduces the divergence bug"


def test_no_other_consumer_hardcodes_the_pin_filename() -> None:
    """The pin filename is private to the resolver — everything else reads its output.

    ``nightly-test-builds/entrypoint.sh`` used to rebuild ``block-pin-<chain>.txt``
    itself. Once the epoch entered the filename that ``[ -f ]`` test went false on
    every chain, exporting nothing, and both the ``|| true`` and the ``if`` guard
    made it silent: every strategy forked at ``latest`` with a cold cache and no
    log line saying so. The lane's own CI could not catch it, because nightly is
    a different lane from this PR's.

    So the name may appear only where it is defined. Consumers parse the
    resolver's printed ``ANVIL_FORK_BLOCK_<CHAIN>=<block>`` contract instead,
    which also survives an epoch bump and carries the bnb -> BSC alias.
    """
    offenders: list[str] = []
    for path in REPO_ROOT.rglob("*"):
        if not path.is_file() or path.suffix not in {".sh", ".py", ".yml", ".yaml"}:
            continue
        if any(part in {".git", ".venv", "node_modules"} for part in path.parts):
            continue
        if path == SCRIPT or path == Path(__file__):
            continue
        try:
            text = path.read_text(encoding="utf-8", errors="ignore")
        except OSError:  # pragma: no cover - unreadable file
            continue
        if "block-pin-" in text:
            offenders.append(str(path.relative_to(REPO_ROOT)))

    assert not offenders, (
        "these files hard-code the versioned pin filename and will silently stop "
        f"matching at the next FORK_PIN_EPOCH bump: {offenders}. "
        "Parse resolve-fork-block.sh's printed ANVIL_FORK_BLOCK_<CHAIN>=<block> "
        "lines instead."
    )
