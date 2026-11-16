"""Pinning copies exact bytes and refuses provider or credential text; fetching verifies them."""

import gzip
import hashlib
import json

import pytest

from scripts import evidence_fixtures as tool


@pytest.fixture
def tree(tmp_path, monkeypatch):
    evidence = tmp_path / "evidence"
    fixtures = tmp_path / "fixtures"
    monkeypatch.setattr(tool, "EVIDENCE", evidence)
    monkeypatch.setattr(tool, "FIXTURES", fixtures)
    env = tmp_path / ".env"
    env.write_text(
        "# comment=ignored-value\nALCHEMY_API_KEY='operator-key-123'\nSHORT=abc\n"
        "API_TOKEN=inline-comment-value # operator note\n"
    )
    return evidence, fixtures, env


def write(path, content):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(content)


def options(tree):
    _, fixtures, _ = tree
    return ["--manifest", str(fixtures / "m.json"), "--cache", str(fixtures.parent / "cache")]


def run(tree, *names):
    _, _, env = tree
    return tool.main(["pin", *names, "--env-file", str(env), *options(tree)])


def test_clean_files_are_copied_byte_for_byte_and_pinned_in_sorted_order(tree):
    evidence, fixtures, _ = tree
    compressed = gzip.compress(b'{"api_key_env": "ALCHEMY_API_KEY", "x": 1}')
    write(evidence / "b/capture.json.gz", compressed)
    write(evidence / "a/manifest.json", b'{"note": "no production authorization"}')
    assert run(tree, "b/capture.json.gz") == 0
    assert run(tree, "a/manifest.json") == 0
    assert (fixtures / "evidence/b/capture.json.gz").read_bytes() == compressed
    digest = hashlib.sha256(compressed).hexdigest()
    assert (fixtures.parent / "cache" / digest).read_bytes() == compressed
    assert json.loads((fixtures / "m.json").read_text()) == {
        "a/manifest.json": hashlib.sha256(b'{"note": "no production authorization"}').hexdigest(),
        "b/capture.json.gz": hashlib.sha256(compressed).hexdigest(),
    }


@pytest.mark.parametrize(
    "name,content,reason",
    [
        ("leak.json.gz", gzip.compress(b'{"rpc": "https://eth.example/v2/x"}'), "'https://'"),
        ("leak.json", b'{"api_key": "x"}', "'api_key\":'"),
        ("leak.json", b'{"header": "Authorization: Bearer x"}', "'Authorization: Bearer'"),
        ("leak.json", b"provider operator-key-123 kept", "operator env value"),
        ("leak.json", b'{"access_token": "not-in-env"}', "'access_token\":'"),
        ("leak.json", b"value inline-comment-value kept", "operator env value"),
        ("leak.json", b'{"rpc": "https:\\/\\/rpc.example/v2/x"}', "'https:\\\\/\\\\/'"),
        ("leak.json", b'{"mirror": "ftp://private.example/file"}', "'ftp://'"),
    ],
    ids=[
        "url",
        "api-key",
        "bearer",
        "env-value",
        "access-token",
        "env-inline-comment",
        "json-escaped-url",
        "ftp-url",
    ],
)
def test_any_refusal_writes_nothing(tree, capsys, name, content, reason):
    evidence, fixtures, _ = tree
    write(evidence / "clean.json", b"{}")
    write(evidence / name, content)
    assert run(tree, "clean.json", name) == 1
    assert reason in capsys.readouterr().err
    assert not fixtures.exists()
    assert not (fixtures.parent / "cache").exists()


@pytest.mark.parametrize("name", ["/etc/passwd", "../README.md", "a/../../README.md"])
def test_names_outside_the_evidence_tree_are_refused(tree, capsys, name):
    assert run(tree, name) == 1
    assert "not a path inside the evidence tree" in capsys.readouterr().err


def test_symlinks_leaving_the_evidence_tree_are_refused(tree, capsys):
    evidence, fixtures, _ = tree
    (evidence.parent / "outside.json").write_bytes(b"{}")
    evidence.mkdir()
    (evidence / "link.json").symlink_to(evidence.parent / "outside.json")
    assert run(tree, "link.json") == 1
    assert "resolves outside the evidence tree" in capsys.readouterr().err
    assert not fixtures.exists()
    assert not (fixtures.parent / "cache").exists()


@pytest.mark.parametrize("link", ["evidence", "evidence/a", "evidence/a/capture.json"])
def test_symlinked_copy_destinations_are_refused(tree, tmp_path, capsys, link):
    evidence, fixtures, _ = tree
    write(evidence / "a/capture.json", b"{}")
    elsewhere = tmp_path / "elsewhere"
    elsewhere.mkdir()
    (fixtures / link).parent.mkdir(parents=True, exist_ok=True)
    (fixtures / link).symlink_to(elsewhere)
    assert run(tree, "a/capture.json") == 1
    assert "copy destination passes through a symlink" in capsys.readouterr().err
    assert list(elsewhere.iterdir()) == []


def test_missing_env_file_stops_before_any_check(tree, tmp_path, capsys):
    evidence, fixtures, _ = tree
    write(evidence / "clean.json", b"{}")
    with pytest.raises(SystemExit) as stopped:
        tool.main(["pin", "clean.json", "--env-file", str(tmp_path / "absent.env")])
    assert stopped.value.code == 2
    assert "does not exist" in capsys.readouterr().err
    assert not fixtures.exists()
    assert not (fixtures.parent / "cache").exists()


def test_missing_original_is_refused(tree, capsys):
    assert run(tree, "absent.json") == 1
    assert "not a retained file" in capsys.readouterr().err


def test_committed_fixtures_name_github_sources_only():
    assert tool.FORBIDDEN.search(b"https://github.com/curvefi/twocrypto-ng") is None
    assert tool.FORBIDDEN.search(b'"https:\\/\\/github.com\\/curvefi"') is None


def pinned(tree, name, content):
    evidence, fixtures, _ = tree
    write(evidence / name, content)
    assert run(tree, name) == 0
    return fixtures / "evidence" / name


def test_fetch_places_copies_from_the_cache_without_an_evidence_tree(tree):
    evidence, _, _ = tree
    copy = pinned(tree, "a/capture.json", b'{"x": 1}')
    copy.unlink()
    (evidence / "a/capture.json").unlink()
    assert tool.main(["fetch", *options(tree)]) == 0
    assert copy.read_bytes() == b'{"x": 1}'


def test_fetch_replaces_a_changed_copy_from_the_evidence_tree(tree):
    _, fixtures, _ = tree
    copy = pinned(tree, "a/capture.json", b'{"x": 1}')
    for cached in (fixtures.parent / "cache").iterdir():
        cached.write_bytes(b"tampered")
    copy.write_bytes(b"changed")
    assert tool.main(["fetch", *options(tree)]) == 0
    assert copy.read_bytes() == b'{"x": 1}'


def test_fetch_fails_when_no_source_has_the_pinned_digest(tree, capsys):
    evidence, fixtures, _ = tree
    copy = pinned(tree, "a/capture.json", b'{"x": 1}')
    copy.unlink()
    for cached in (fixtures.parent / "cache").iterdir():
        cached.unlink()
    (evidence / "a/capture.json").write_bytes(b'{"x": 2}')
    assert tool.main(["fetch", *options(tree)]) == 1
    assert "missing a/capture.json: no copy with digest" in capsys.readouterr().err
    assert not copy.exists()


def test_fetch_refuses_a_symlinked_destination(tree, tmp_path, capsys):
    _, fixtures, _ = tree
    copy = pinned(tree, "a/capture.json", b"{}")
    copy.unlink()
    copy.parent.rmdir()
    elsewhere = tmp_path / "elsewhere"
    elsewhere.mkdir()
    copy.parent.symlink_to(elsewhere)
    assert tool.main(["fetch", *options(tree)]) == 1
    assert "copy destination passes through a symlink" in capsys.readouterr().err
    assert list(elsewhere.iterdir()) == []


@pytest.mark.parametrize("name", ["../escape.json", "/tmp/escape.json"])
def test_fetch_refuses_manifest_names_outside_the_copy_tree(tree, tmp_path, capsys, name):
    _, fixtures, _ = tree
    fixtures.mkdir()
    (fixtures / "m.json").write_text(json.dumps({name: hashlib.sha256(b"{}").hexdigest()}))
    assert tool.main(["fetch", *options(tree)]) == 1
    assert "not a path inside the evidence tree" in capsys.readouterr().err
    assert not (fixtures / "escape.json").exists()


def test_fetching_without_a_manifest_fails(tree, capsys):
    assert tool.main(["fetch", *options(tree)]) == 1
    assert "the manifest does not exist" in capsys.readouterr().err
