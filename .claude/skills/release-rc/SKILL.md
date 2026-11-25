---
name: release-rc
description: "Create a release candidate for the SDK. Runs the gates and builds and verifies the distributions without publishing them anywhere. Use when the user says 'rc release', 'internal release', 'candidate release', or 'release rc'."
metadata:
  internal: true
---

# Release RC - Release Candidate

## Purpose

Tag a release candidate so `.github/workflows/release_candidate.yml` runs the gates
and builds and verifies the `X.Y.ZrcN` distributions. RCs publish nowhere, and
they never go to PyPI. The verified files are kept as
the run's `sdk-dist` artifact (7 days).

## Prerequisites

- In the `sdk-v3` repository on `main`, pulled, clean tree, `gh` authenticated.

## What gets produced

| Artifact | Where | Version |
|---|---|---|
| `almanak` sdist and five platform wheels | `sdk-dist` artifact of the workflow run | `X.Y.ZrcN` |
| `release-sha256.txt`, `release-manifest.json` | `release-sha256` artifact of the workflow run | `X.Y.ZrcN` |

Not published: PyPI, any package index, any image.

## Workflow

### Step 1: Validate state

```bash
git checkout main && git pull origin main && git status
```

### Step 2: Determine the RC version

1. Latest tag overall; with no tags at all, the base version is `uv version --short`.
   ```bash
   LATEST_TAG=$(git tag --sort=-v:refname | head -1)
   ```
2. If `LATEST_TAG` is an RC (`vX.Y.Z-rcN`), continue that series with base
   `X.Y.Z`. If it is stable, PEP 440 orders `X.Y.ZrcN` before `X.Y.Z`, so ask the
   user which next version the RC targets; do not guess.
3. Existing RCs for the base: `git tag -l "v${BASE}-rc*" | sort -V`; the new number
   is one above the highest.
4. Tag `v${BASE}-rc${N}`; built version `${BASE}rc${N}`.

### Step 3: Present for approval

Show the tag, the PEP 440 version, the commits since the last tag and the artifact
table above. Confirm before tagging.

### Step 4: Tag and push

```bash
git tag -a vX.Y.Z-rcN -m "Release candidate vX.Y.Z-rcN"
git push origin vX.Y.Z-rcN
```

The RC workflow checks that the tag's base version equals `pyproject.toml`, runs
lint, types, the hosted test scope, the CLI smoke jobs and the docs build, then builds and verifies the distributions.

### Step 5: Monitor and report

```bash
gh run list --workflow release_candidate.yml --commit "$(git rev-parse "vX.Y.Z-rcN^{commit}")" --limit 1 --json databaseId,status,url
```

Report the tag, the run URL and the version string `X.Y.ZrcN`. To install the RC,
download the `sdk-dist` artifact (`gh run download <run-id> -n sdk-dist`) and
install the wheel for your operating system.

## Error handling

- Tag push failure: check whether the tag already exists remotely. Never force-push
  tags.
- Workflow failure: link the run and the failing job.
