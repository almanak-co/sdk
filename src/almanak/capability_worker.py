"""The trusted caller selects a connector codec before untrusted strategy evaluation."""

import json
import sys
from importlib import import_module

from almanak.strategy_refusal import refusal_document
from almanak.typed_parameters import ParametersRefused
from almanak.worker_policy import restrict_author


def main():
    # This argument comes from explicit trusted constructor wiring, never strategy JSON, so
    # its import runs before the author's budget starts.
    module, name = sys.argv[1].split(":")
    codec = getattr(import_module(module), name)()
    restrict_author()
    try:
        result = codec.execute_hook(sys.stdin.read())
    except ParametersRefused as exc:
        print(json.dumps(refusal_document(str(exc), field=exc.field)))
        return
    print(result.model_dump_json())


if __name__ == "__main__":
    main()
