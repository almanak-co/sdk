"""The public hygiene gate names private material without printing it."""

import gzip
import lzma

import pytest

from scripts.ci import public_hygiene as hygiene

PATTERN = hygiene.compiled(hygiene.RULES)
# Joined here so the evidence guard, which skips modules naming the retained evidence
# tree in one literal, still collects this module under --skip-missing-evidence.
EVIDENCE_PATH = "/".join(
    ("docs", "internal", "reports", "evidence", "2026-09-07-rollout", "manifest.json")
)


def rules(text: str) -> list[str]:
    return [finding.rule for finding in hygiene.scan("file", text.encode(), PATTERN)]


@pytest.mark.parametrize(
    ("text", "rule"),
    [
        ("AKIA" + "A" * 16, "aws-access-key"),
        ("ghp_" + "a" * 36, "github-token"),
        ("-----BEGIN EC PRIVATE KEY-----", "private-key-block"),
        ("https://eth-mainnet.g.alchemy.com/v2/abcdefgh12345678", "provider-url-key"),
        ("project starry-agency-339312", "gcp-project"),
        ("ci@example.iam.gserviceaccount.com", "gcp-identity"),
        ("gs://bucket/path", "gcs-bucket"),
        ("/home/someone/checkout/.env", "workstation-path"),
        ("/Users/Someone/checkout", "workstation-path"),
        ("fixed in ALM-10271", "tracker-reference"),
        ("https://linear.app/team/issue", "tracker-reference"),
        ("see almanak-sdk-private", "private-repository"),
        ("[plan](docs/internal/blueprints/00-sdk-specification.md)", "private-document-link"),
        ("(internal/reports/2026-09-06-report.md)", "private-document-link"),
    ],
)
def test_rule_matches(text, rule):
    assert rules(text) == [rule]


@pytest.mark.parametrize(
    "text",
    [
        "https://eth-mainnet.g.alchemy.com/v2/test-key",
        EVIDENCE_PATH,
        "the /home directory and ~/.cache/almanak-sdk",
        "ALMANAK_AX_GATEWAY_URL and eip155:1/erc20:0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48",
    ],
)
def test_public_text_passes(text):
    assert rules(text) == []


def test_finding_reports_line_and_never_the_match():
    secret = "ghp_" + "b" * 36
    (finding,) = hygiene.scan("a.txt", f"one\ntwo {secret}\n".encode(), PATTERN)
    assert finding == hygiene.Finding("a.txt", 2, "github-token")
    assert secret not in repr(finding)


def test_denied_address_matches_in_any_case():
    address = "0x" + "aB" * 20
    denied = frozenset({address.lower().encode()})
    data = f"first\nowner: {address}\nother: 0x{'11' * 20}\n".encode()
    assert list(hygiene.scan("c.yaml", data, PATTERN, denied)) == [
        hygiene.Finding("c.yaml", 2, "denied-address")
    ]
    assert list(hygiene.scan("c.yaml", data, PATTERN)) == []


def test_local_denylist_is_optional_and_applied(tmp_path):
    address = "0x" + "cd" * 20
    (tmp_path / "config.yaml").write_text(f"fund: {address.upper().replace('0X', '0x')}\n")
    assert hygiene.findings(tmp_path, ["config.yaml"]) == []
    (tmp_path / ".local").mkdir()
    (tmp_path / hygiene.DENYLIST).write_text(f"# owned accounts\n\n{address}  # safe\n")
    assert hygiene.findings(tmp_path, ["config.yaml"]) == [
        hygiene.Finding("config.yaml", 1, "denied-address")
    ]


def test_compressed_files_are_read_decompressed(tmp_path):
    text = b"/home/someone/run\n"
    (tmp_path / "a.json.gz").write_bytes(gzip.compress(text))
    (tmp_path / "b.sqlite.xz").write_bytes(lzma.compress(text))
    (tmp_path / "c.txt").write_bytes(b"clean\n")
    found = hygiene.findings(tmp_path, ["a.json.gz", "b.sqlite.xz", "c.txt", "missing.txt"])
    assert [(item.path, item.rule) for item in found] == [
        ("a.json.gz", "workstation-path"),
        ("b.sqlite.xz", "workstation-path"),
    ]


def test_private_and_exempt_paths_are_not_published():
    names = ["docs/internal/reports/a.md", "docs/index.md", ".gitignore", "src/almanak/cli.py"]
    assert hygiene.published(names) == ["docs/index.md", "src/almanak/cli.py"]


def test_main_exit_status_and_output(tmp_path, monkeypatch, capsys):
    (tmp_path / "bad.md").write_text("see gs://bucket\n")
    (tmp_path / "good.md").write_text("fine\n")
    monkeypatch.setattr(hygiene, "ROOT", tmp_path)
    assert hygiene.main(["good.md"]) == 0
    assert hygiene.main(["bad.md", "good.md"]) == 1
    captured = capsys.readouterr()
    assert captured.out == "bad.md:1: gcs-bucket\n"
    assert "1 finding(s) in 1 file(s)" in captured.err
