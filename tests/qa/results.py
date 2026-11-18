"""Complete denominators and an explicit boundary for publishable evidence."""

import hashlib
import html
import json
import re
import shutil
import xml.etree.ElementTree as ET
from dataclasses import asdict
from urllib.parse import quote, urlsplit

from dotenv import dotenv_values

from tests.qa.inputs import write_json

PRIVATE_NAMES = {
    "gateway-auth.token",
    "keystores.json",
    "password",
    "signing-keys.json",
    "credentials.json",
}


def assert_junit(path):
    root = ET.parse(path).getroot()
    cases = root.findall(".//testcase")
    if not cases:
        raise ValueError("pytest produced no executed cases")
    bad = [c.attrib.get("name") for c in cases if not passed(c)]
    if bad:
        raise ValueError(f"required pytest cases did not pass: {bad}")


def passed(case):
    return all(case.find(tag) is None for tag in ("skipped", "failure", "error"))


def secrets_for(env_file, directory):
    values: set[str] = set()
    if env_file is not None and env_file.is_file():
        values.update(v for v in dotenv_values(env_file, interpolate=False).values() if v)
    for path in directory.rglob("*"):
        if path.is_file() and (path.suffix == ".token" or path.name == "password"):
            values.add(path.read_text().strip())
    expanded = set(values)
    for value in values:
        expanded.add(quote(value, safe=""))
        if value.startswith(("http://", "https://")):
            parsed = urlsplit(value)
            expanded.update(part for part in parsed.path.split("/") if len(part) >= 12)
            expanded.update(part.split("=", 1)[-1] for part in parsed.query.split("&") if part)
    return tuple(sorted((v for v in expanded if len(v) >= 6), key=len, reverse=True))


def redact(value, secrets):
    for secret in secrets:
        value = value.replace(secret, "[REDACTED]")
    return re.sub(r'https?://[^\s"<>]+', lambda m: safe_url(m.group()), value)


def safe_url(value):
    try:
        parsed = urlsplit(value)
        if parsed.hostname in ("localhost", "127.0.0.1", "::1"):
            return value
        return f"{parsed.scheme}://{parsed.hostname}/[REDACTED]"
    except ValueError:
        return "[REDACTED_URL]"


def export_evidence(private, public, secrets):
    errors = []
    for path in sorted(private.rglob("*")):
        if not path.is_file() or path.is_symlink() or path.name in PRIVATE_NAMES:
            continue
        if path.suffix not in (".json", ".log", ".xml", ".sqlite", ".jsonl"):
            continue
        if path.name.endswith(".token") or path.name == "worker.json":
            continue
        relative = path.relative_to(private)
        # Workspace duplicates contain credentials and live databases. The fork
        # harness already takes a consistent, reviewed evidence backup.
        if "workspace" in relative.parts:
            continue
        data = path.read_bytes()
        destination = public / relative
        destination.parent.mkdir(parents=True, exist_ok=True)
        if path.suffix in (".log", ".xml"):
            destination.write_text(redact(data.decode(errors="replace"), secrets))
        elif any(secret.encode() in data for secret in secrets):
            errors.append(str(relative))
        else:
            shutil.copyfile(path, destination)
    return errors


def row(case):
    return {**asdict(case), "status": "blocked", "category": "not_started", "wall_seconds": None}


def save_results(directory, metadata, rows, secrets=()):
    payload = {**metadata, "schema_version": 1, "cases": rows}
    ledger = proof_ledger(metadata.get("proof_mapping", ()), rows)
    payload["status"] = (
        "passed"
        if rows
        and all(r["status"] == "passed" for r in rows)
        and all(item["status"] == "passed" for item in ledger)
        else "incomplete_or_failed"
    )
    payload = json.loads(redact(json.dumps(payload, default=str), secrets))
    write_json(directory / "results.json", payload)
    write_json(
        directory / "proof-ledger.json",
        {
            "schema_version": 1,
            "identity": payload.get("identity"),
            "execution": payload.get("execution"),
            "status": payload["status"],
            "obligations": ledger,
        },
    )
    write_junit(directory, payload)
    lines = [
        "# Daily SDK QA",
        "",
        f"Result: {payload['status']}",
        "",
        "[Per-obligation proof ledger](proof-ledger.json)",
        "",
        "| Case | Status | Category | Seconds |",
        "| --- | --- | --- | ---: |",
    ]
    lines.extend(
        f"| [{r['id']}]({r['artifact']}/) | {r['status']} | "
        f"{r.get('category', '')} | {r['wall_seconds']} |"
        for r in payload["cases"]
    )
    (directory / "summary.md").write_text("\n".join(lines) + "\n")
    write_html(directory, payload)
    return payload


def ledger_entries(mapping, is_passed):
    # An obligation is satisfied only when every mapped case passes; an empty
    # mapping never passes. Callers label the outcome in their own vocabulary.
    return [
        {**entry, "outstanding_case_ids": [i for i in entry["case_ids"] if not is_passed(i)]}
        for entry in mapping
    ]


def proof_ledger(mapping, rows):
    results = {row["id"]: row for row in rows}
    ledger = ledger_entries(mapping, lambda i: results.get(i, {}).get("status") == "passed")
    for item in ledger:
        complete = item["case_ids"] and not item["outstanding_case_ids"]
        item["status"] = "passed" if complete else "incomplete_or_failed"
    return ledger


def write_html(directory, payload):
    rows = "".join(html_row(row) for row in payload["cases"])
    page = (
        '<!doctype html><meta charset="utf-8"><title>SDK daily QA</title>'
        "<style>body{font:16px system-ui;margin:3rem;max-width:1100px}"
        "table{border-collapse:collapse;width:100%}td,th{padding:.65rem;"
        "border-bottom:1px solid #ddd;text-align:left}a{color:#2457ab}"
        ".failed,.blocked{color:#a12b20}.passed{color:#167047}</style>"
        "<h1>SDK daily QA</h1><p>" + html.escape(payload["status"]) + "</p>"
        '<p><a href="results.json">Complete JSON</a> · '
        '<a href="proof-ledger.json">Proof ledger</a> · '
        '<a href="junit.xml">JUnit</a> · <a href="changes.json">Changes</a></p>'
        "<table><thead><tr><th>Case</th><th>Status</th><th>Category</th>"
        "<th>Seconds</th></tr></thead><tbody>" + rows + "</tbody></table>"
    )
    (directory / "index.html").write_text(page)


def html_row(row):
    name = html.escape(row["id"])
    if row["wall_seconds"] is not None:
        name = f'<a href="{html.escape(row["artifact"], quote=True)}/worker.log">{name}</a>'
    status = html.escape(row["status"], quote=True)
    category = html.escape(row.get("category", ""))
    return (
        f'<tr><td>{name}</td><td class="{status}">{status}</td>'
        f"<td>{category}</td><td>{row['wall_seconds']}</td></tr>"
    )


def write_junit(directory, payload):
    suite = ET.Element("testsuite", name="sdk-daily-qa", tests=str(len(payload["cases"])))
    for row in payload["cases"]:
        case = ET.SubElement(
            suite,
            "testcase",
            name=row["id"],
            classname=row["lane"],
            time=str(row["wall_seconds"] or 0),
        )
        if row["status"] != "passed":
            tag = "skipped" if row["status"] == "skipped" else "failure"
            ET.SubElement(case, tag, message=row.get("category", row["status"]))
    ET.ElementTree(suite).write(directory / "junit.xml", encoding="unicode")


def digest(path):
    with path.open("rb") as source:
        return hashlib.file_digest(source, "sha256").hexdigest()
