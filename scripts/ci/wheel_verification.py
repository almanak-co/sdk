"""Archive integrity checks for SDK release wheels."""

import base64
import configparser
import csv
import hashlib
import io
import stat
import zipfile
from email.message import Message
from email.parser import BytesParser
from pathlib import PurePosixPath


def digest(data: bytes) -> str:
    value = base64.urlsafe_b64encode(hashlib.sha256(data).digest()).rstrip(b"=").decode()
    return "sha256=" + value


def verify_path(name: str) -> None:
    path = PurePosixPath(name)
    if path.is_absolute() or ".." in path.parts or "\\" in name:
        raise ValueError(f"distribution contains an unsafe member path: {name}")
    if path.name.lower().startswith("gha-creds-") or path.name.lower() in {
        ".env",
        "credentials.json",
    }:
        raise ValueError(f"distribution contains a credential file: {name}")


def verify_members(wheel: zipfile.ZipFile) -> set[str]:
    names = [item.filename for item in wheel.infolist()]
    if len(names) != len(set(names)):
        raise ValueError("wheel contains duplicate member names")
    for item in wheel.infolist():
        verify_path(item.filename)
        if stat.S_ISLNK(item.external_attr >> 16) or item.is_dir():
            raise ValueError(f"wheel member is not a regular file: {item.filename}")
    return set(names)


def verify_record(wheel: zipfile.ZipFile, prefix: str, names: set[str]) -> None:
    rows = list(csv.reader(io.StringIO(wheel.read(prefix + "RECORD").decode())))
    if any(len(row) != 3 for row in rows):
        raise ValueError("RECORD rows must contain name, digest and size")
    if len(rows) != len(names) or {row[0] for row in rows} != names:
        raise ValueError("RECORD does not name every wheel member exactly once")
    for name, expected_digest, expected_size in rows:
        if name == prefix + "RECORD":
            if expected_digest or expected_size:
                raise ValueError("RECORD must leave its own digest and size empty")
            continue
        data = wheel.read(name)
        if expected_digest != digest(data) or expected_size != str(len(data)):
            raise ValueError(f"RECORD does not certify {name}")


def verify_metadata_identity(
    wheel: zipfile.ZipFile, prefix: str, name: str, version: str
) -> Message:
    metadata = BytesParser().parsebytes(wheel.read(prefix + "METADATA"))
    if metadata.get_all("Name") != [name] or metadata.get_all("Version") != [version]:
        raise ValueError("wheel name or version does not match the requested release")
    return metadata


class EntryPoints(configparser.ConfigParser):
    def optionxform(self, optionstr: str) -> str:
        return optionstr


def read_entry_points(wheel: zipfile.ZipFile, prefix: str) -> configparser.ConfigParser:
    parser = EntryPoints(interpolation=None)
    parser.read_string(wheel.read(prefix + "entry_points.txt").decode())
    return parser
