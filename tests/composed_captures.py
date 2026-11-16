"""Retained composed datasets bound to today's configuration, after proving the binding.

A composed descriptor freezes `identity(config, settings)`: a digest of the whole composed
configuration, including the author budgets that lending exits (blueprint 25) and
scope-only authority (blueprint 27) later removed. The native component datasets it pins
freeze only markets, data policy, schedule and providers, so their bytes and digests stay
valid. A copied descriptor is rebound only after two checks: its retained policy digest
is the identity of the retained run's exact configuration and settings, and today's
configuration and settings are that run's values read through `current_record`. The
retained files are unchanged.
"""

import hashlib
import json

from almanak.backtest.dataset import file_digest
from almanak.connectors.composed.economic_book import MODEL, identity
from tests.lending_captures import current_record


def retained_identity(raw):
    """The composed policy digest of a retained run manifest, from its raw JSON."""
    value = {"model": MODEL, "config": raw["config"], "settings": raw["settings"]}
    return hashlib.sha256(json.dumps(value, sort_keys=True).encode()).hexdigest()


def rebind(dataset, raw, config, settings, current=None):
    """Bind a copied composed descriptor to `config` and `settings`; return its file digest.

    `raw` is the retained run manifest that used the dataset, as decoded JSON; `current` is
    its value in today's interface, `current_record(raw)` unless a caller maps more.
    """
    path = dataset / "dataset.json"
    descriptor = json.loads(path.read_text())
    # A strategy price series is bound to the composed policy digest too; none of the
    # retained composed captures has one, so it is refused rather than rebound.
    assert "prices" not in descriptor, "retained composed dataset carries a price series"
    assert descriptor["policy_digest"] == retained_identity(raw)
    current = current_record(raw) if current is None else current
    assert type(config).model_validate_json(json.dumps(current["config"])) == config
    assert type(settings).model_validate_json(json.dumps(current["settings"])) == settings
    path.write_text(json.dumps({**descriptor, "policy_digest": identity(config, settings)}))
    return file_digest(path)


def with_dataset(manifest, raw, dataset):
    """A mapped run manifest reading a copied dataset; a composed descriptor is rebound."""
    update = {"dataset": dataset}
    if json.loads((dataset / "dataset.json").read_text()).get("model") == MODEL:
        update["dataset_sha256"] = rebind(dataset, raw, manifest.config, manifest.settings)
    return manifest.model_copy(update=update)
