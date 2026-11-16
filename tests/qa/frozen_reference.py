"""Historical captures are checked against their sealed executed source archive."""

import gzip
import hashlib
import io
import json
import tarfile

from almanak.strategy import StrategyDefinition


def archived_reference(package, archive, reference):
    manifest = json.loads((package / "manifest.json").read_text())
    entry = next(row for row in manifest["files"] if row["path"] == archive)
    raw = (package / archive).read_bytes()
    assert (len(raw), hashlib.sha256(raw).hexdigest()) == (entry["bytes"], entry["sha256"])
    with tarfile.open(fileobj=io.BytesIO(raw), mode="r:gz") as sources:
        member = sources.extractfile("strategies/" + reference + "/strategy.py")
        assert member is not None
        return StrategyDefinition(filename="strategy.py", source=member.read().decode())


def compressed_reference(package, name):
    manifest = json.loads((package / "manifest.json").read_text())
    entry = manifest["files"][name]
    source = gzip.decompress((package / (name + ".gz")).read_bytes())
    assert (len(source), hashlib.sha256(source).hexdigest()) == (entry["bytes"], entry["sha256"])
    return StrategyDefinition(filename="strategy.py", source=source.decode())
