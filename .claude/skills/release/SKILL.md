---
name: release
description: "Create a new SDK release: compile changes since the last tag, determine the semver bump, update CHANGELOG.md and pyproject.toml, tag, push, and create the GitHub release. Use when the user says 'release', 'cut a release', 'new version', or 'publish a release'."
metadata:
  internal: true
---

# Release - SDK Version Release Pipeline

## Purpose

Compile all changes between the latest stable tag and HEAD on main, determine the
semver bump, update `CHANGELOG.md` and `pyproject.toml`, then tag, push and create a
GitHub release. Publishing the release triggers `.github/workflows/release_tag.yml`.

## Prerequisites

- On `main` with a clean working tree, pulled from origin.
- `gh` CLI authenticated.
- `uv` installed; the version in `pyproject.toml` is the single source of truth and
  the release workflow refuses a tag that disagrees with it.

## Workflow

### Phase 1: Gather changes

1. Confirm the branch is `main`; abort otherwise.
2. `git pull origin main`.
3. Latest stable tag (RC, alpha and beta tags are ignored):
   ```bash
   LATEST_TAG=$(git tag --sort=-v:refname | grep -E '^v[0-9]+\.[0-9]+\.[0-9]+$' | head -1)
   RANGE="${LATEST_TAG:+${LATEST_TAG}..}HEAD"
   ```
   When no stable tag exists (the first 3.0.0 release), the range starts at the
   root commit and the version is the one already in `pyproject.toml`.
4. Current version: `uv version --short`.
5. Commits: `git log ${RANGE} --pretty=format:"%H|%s" --first-parent`.
6. Merged PRs since the tag date (every merged PR when no stable tag exists):
   ```bash
   if [ -n "${LATEST_TAG}" ]; then
     PR_SEARCH="merged:>=$(git log -1 --format=%aI "${LATEST_TAG}")"
   else
     PR_SEARCH="is:merged"
   fi
   gh pr list --state merged --search "${PR_SEARCH}" --json number,title,body,labels,mergedAt --limit 200
   ```
7. Read PR bodies for the full scope of user-facing change.

### Phase 2: Categorize

Keep a Changelog groups: Added, Changed, Deprecated, Removed, Fixed, Security. One
line per entry with the PR number in parentheses. Exclude PRs that only touch
`.claude/`, `.github/` or CI tooling unless they change the
published package, the CLI, or the public docs site.

### Phase 3: Semver bump

| Condition | Bump |
|---|---|
| Removed or renamed public API, CLI command or option, incompatible config schema | major |
| New capability, connector, chain, intent, CLI command or option | minor |
| Fix, performance, docs, internal refactor | patch |

Highest wins; prefer minor over patch when unsure. Compute the next version.

### Phase 4: Present for approval

Use `AskUserQuestion` to show the version bump, the changelog text and the release
notes. Do not proceed without explicit approval.

### Phase 5: Apply

1. `CHANGELOG.md`: insert `## [X.Y.Z] - YYYY-MM-DD` below `## [Unreleased]`, keeping
   only the category headers that have entries. For the first release, replace the
   `## [3.0.0] - unreleased` placeholder with the dated header.
2. `uv version X.Y.Z` (edits `pyproject.toml`; run `uv lock` afterwards so the lock
   records the new version).
3. Commit `release: vX.Y.Z` with `CHANGELOG.md`, `pyproject.toml` and `uv.lock`.
4. `git tag -a vX.Y.Z -m "Release vX.Y.Z"`.
5. `git push origin main --follow-tags`.
6. `gh release create vX.Y.Z --title "vX.Y.Z" --notes-file <notes>` with sections
   `## Features`, `## Fixes`, `## Breaking Changes` (major only), `## Deprecations`.

### Phase 6: Confirm

Report the tag, the release URL, the commit URL and the `release_tag.yml` run URL.
The workflow runs lint, types, the hosted test scope, the CLI smoke jobs and
the docs build, then builds and verifies the
distributions once (`build_dist`) and uploads those files to PyPI
(`publish_pypi`). Nothing else is published, and no step uses Google Cloud. Say which jobs were red when reporting.

Print a Slack message (at most 10 lines):

```text
:rocket: *Almanak SDK vX.Y.Z*

<3-7 bullets: features, breaking changes, important fixes; no PR numbers>

Release: <GitHub release URL>
```

## Error handling

- Push rejected by branch protection: tell the user and suggest a release PR.
- `gh release create` failure: print the command for a manual retry.
- Dirty tree at start: list the files and abort. Never force-push main.
