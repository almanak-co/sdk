"""Capture the author-hook contracts of every unconverted reference in one batch.

This is the "before" side of scripts/compare_reference_conversion.py. Each legacy
author file is captured with placeholder account bindings derived from the file's
own named accounts, so nothing is signed and no provider is contacted. A reference
the installed support matrix refuses fails inside the capture; that failure is
recorded in the index instead of ending the batch.
"""

import sys
from pathlib import Path

if __package__ in (None, ""):
    # Running this file by path puts scripts/ on sys.path, not the repository root.
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import argparse
import json

from almanak.models import StrategyAccounts
from scripts.capture_author_hooks import EVIDENCE, ROOT, capture
from scripts.capture_config_baseline import digest, source_revision, write_capture
from scripts.ci.check_strategies import CONVERTED
from scripts.legacy_configuration.author import read_author

# Capture name suffix and author file name; the production variant is optional.
VARIANTS = (("", "config.yaml"), (".production", "config.production.yaml"))


def unconverted_directories(root):
    directories = sorted(path.parent for path in (root / "strategies").glob("*/strategy.py"))
    return [path for path in directories if path.name not in CONVERTED]


def reference_directories(root, only):
    directories = unconverted_directories(root)
    return [path for path in directories if path.name in only] if only else directories


def reference_files(root, only):
    """One capture per author file, named for its reference and variant."""
    files = []
    for directory in reference_directories(root, only):
        for suffix, name in VARIANTS:
            if (directory / name).is_file():
                files.append((directory.name + suffix, directory / name))
    return files


def placeholder_binding(name, account, index):
    """Positional placeholder address; the capture binds accounts without signing them."""
    chain = account.chain.model_dump(mode="json")
    return {
        "name": name,
        "profile": {
            "kind": "eoa",
            "fund": {
                "chain": {"chain_id": chain["chain_id"], "environment": "mainnet"},
                "address": "0x" + f"{index + 1:02x}" * 20,
            },
        },
    }


def placeholder_accounts(config_path):
    """One non-signing EOA binding per author account, in sorted account name order."""
    accounts = read_author(config_path).author_accounts
    bindings = [
        placeholder_binding(name, accounts[name], index)
        for index, name in enumerate(sorted(accounts))
    ]
    return StrategyAccounts.model_validate_json(json.dumps({"bindings": bindings}))


def capture_one(name, config_path, output_dir):
    """Capture one author file; its placeholder accounts are retained beside the capture."""
    accounts_path = output_dir / f"{name}.accounts.json"
    accounts_path.write_text(placeholder_accounts(config_path).model_dump_json())
    output = output_dir / f"{name}.json"
    result = capture(config_path, config_path.parent / "strategy.py", accounts_path)
    write_capture(output, result)
    return {"sha256": digest(output.read_bytes()), "content_sha256": result["content_sha256"]}


def capture_batch(root, output_dir, only):
    captured: dict[str, dict] = {}
    failed: dict[str, dict] = {}
    for name, config_path in reference_files(root, only):
        try:
            captured[f"{name}.json"] = capture_one(name, config_path, output_dir)
        except Exception as error:  # one refused reference must not end the batch
            failed[f"{name}.json"] = {"error": type(error).__name__, "message": str(error)}
    return captured, failed


def write_index(output_dir, revision, captured, failed):
    index = {
        "schema": "almanak.reference-hook-batch.v1",
        "source_revision": revision,
        "captured": captured,
        "failed": failed,
        "counts": {"captured": len(captured), "failed": len(failed)},
    }
    write_capture(output_dir / "index.json", index)
    return index["counts"]


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output-dir", type=Path, required=True, help="new evidence directory")
    parser.add_argument("--only", action="append", help="repeatable reference directory name")
    parser.add_argument("--root", type=Path, default=ROOT)
    args = parser.parse_args(argv)
    output_dir = args.output_dir.resolve()
    if not output_dir.parent.is_relative_to(EVIDENCE.resolve()):
        raise ValueError("hook capture must remain under the ignored local evidence directory")
    if output_dir.exists():
        raise ValueError("hook capture output directory already exists")
    revision = source_revision(args.root)
    output_dir.mkdir(parents=True)
    captured, failed = capture_batch(args.root, output_dir, set(args.only or ()))
    print(json.dumps(write_index(output_dir, revision, captured, failed)))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
