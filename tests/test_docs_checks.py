import shutil
from pathlib import Path

from scripts.docs import check_i18n_structure as i18n
from scripts.docs import check_mkdocs_nav as nav
from scripts.docs import check_mkdocstrings_paths as paths
from scripts.docs import generate_cli_reference as cli_docs
from scripts.docs import generate_support_matrix as support_docs

ROOT = Path(__file__).parents[1]


def write_site(root, nav_text, pages):
    (root / "mkdocs.yml").write_text(nav_text)
    docs = root / "docs"
    for name, text in pages.items():
        (docs / name).parent.mkdir(parents=True, exist_ok=True)
        (docs / name).write_text(text)
    return docs


def test_nav_check_reports_missing_and_orphan_pages(tmp_path, capsys):
    write_site(
        tmp_path,
        "site_name: x\nnav:\n  - Home: index.md\n  - Missing: gone.md#top\n  - https://example.invalid\n",
        {
            "index.md": "# Home\n",
            "orphan.md": "# Orphan\n",
            "index.fr.md": "# Accueil\n",
            "cli/a.md": "x",
        },
    )
    assert nav.main([], repo_root=tmp_path) == 1
    out = capsys.readouterr().out
    assert "missing file: docs/gone.md" in out
    assert "orphan page (not in nav): docs/orphan.md" in out
    assert "index.fr.md" not in out and "cli/a.md" not in out


def test_nav_check_passes_on_repository_and_missing_config(tmp_path):
    assert nav.main(["--quiet"], repo_root=tmp_path) == 2
    # The generated pages are gitignored, so the nav is checked against a copy
    # of the checkout that the generators populate.
    site = tmp_path / "site"
    shutil.copytree(
        ROOT / "docs", site / "docs", ignore=shutil.ignore_patterns("internal", "cli", "support.md")
    )
    shutil.copy(ROOT / "mkdocs.yml", site / "mkdocs.yml")
    assert cli_docs.main(["--output", str(site / "docs/cli")]) == 0
    assert support_docs.main(["--output", str(site / "docs/support.md")]) == 0
    assert nav.main(["--quiet"], repo_root=site) == 0


def test_mkdocstrings_check_resolves_modules_and_attributes(tmp_path, capsys):
    docs = write_site(tmp_path, "", {"a.md": "::: almanak.reports\n::: almanak.reports.support\n"})
    assert paths.main([], repo_root=tmp_path) == 0
    (docs / "b.md").write_text("::: almanak.reports.missing_name\n::: no_such_module_anywhere\n")
    assert paths.main([], repo_root=tmp_path) == 1
    out = capsys.readouterr().out
    assert "has no attribute 'missing_name'" in out
    assert "could not import any prefix" in out
    assert paths.main(["--quiet"]) == 0


def test_i18n_check_accepts_partial_translations_and_flags_reordering(tmp_path, capsys):
    docs = write_site(
        tmp_path,
        "",
        {"page.md": "# T\n\n## A\n\n### A1\n\n## B\n", "page.fr.md": "# T\n\n## A\n\n## B\n"},
    )
    assert i18n.main([], repo_root=tmp_path) == 0
    (docs / "page.zh.md").write_text("# T\n\n### X\n\n## A\n\n## B\n\n## C\n")
    assert i18n.main([], repo_root=tmp_path) == 1
    assert "page.zh.md" in capsys.readouterr().out
    assert i18n.main(["--quiet"]) == 0
    assert i18n.is_subsequence((2, 2), (2, 3, 2)) and not i18n.is_subsequence((3, 2, 2), (2, 3, 2))
