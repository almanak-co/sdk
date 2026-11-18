"""Typed economic authority with connector-owned codecs and sandboxed author hooks."""

import hashlib
import json
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

from pydantic import TypeAdapter

from almanak.models import Model


class CapabilitySet[Requirement: Model](Model):
    requirements: tuple[Requirement, ...]

    @property
    def digest(self) -> str:
        return hashlib.sha256(self.model_dump_json().encode()).hexdigest()


@dataclass(frozen=True)
class CapabilityCodec:
    parameters: TypeAdapter
    capabilities: TypeAdapter
    validate: Callable[[Any, Any], None]
    admit: Callable[[Any, Any], None]
    worker_factory: str
    declare: Callable[[Any], Any] | None = None

    def declared(self, config):
        if self.declare is None:
            raise ValueError("connector has no named capability declaration binding")
        retained = self.capabilities.validate_python(self.declare(config))
        self.validate(config, retained)
        return retained

    def evaluate(self, strategy, config):
        from almanak.worker_process import run_worker
        from almanak.worker_session import ACTIVE

        request = json.dumps(
            {
                "source": strategy.source,
                "filename": strategy.filename,
                "config": config.model_dump(mode="json"),
                **({"modules": strategy.modules} if strategy.modules else {}),
            }
        )
        if (session := ACTIVE.get()) is not None:
            output = session.run("almanak.capability_worker", request, (self.worker_factory,))
        else:
            output = run_worker("almanak.capability_worker", request, (self.worker_factory,))
        from almanak.runtime import refused

        refused(output.strip(), strategy)
        retained = self.capabilities.validate_json(output.strip())
        self.validate(config, retained)
        return retained

    def execute_hook(self, request: str):
        raw = json.loads(request)
        if set(raw) - {"modules"} != {"source", "filename", "config"}:
            raise ValueError("capability hook request has unexpected fields")
        config = self.parameters.validate_json(json.dumps(raw["config"]))
        from almanak.strategy import StrategyDefinition, install_modules

        modules = install_modules(
            StrategyDefinition(
                filename=raw["filename"], source=raw["source"], modules=raw.get("modules", {})
            )
        )
        namespace = {
            "__name__": "__strategy__",
            "__file__": raw["filename"],
            "__author_modules__": modules,
        }
        exec(compile(raw["source"], raw["filename"], "exec"), namespace)
        hook = namespace.get("capabilities")
        if not callable(hook):
            raise TypeError("strategy must define capabilities(config)")
        from almanak.author_capabilities import DeclaredCapabilities, bind
        from almanak.typed_parameters import typed

        result = hook(typed(namespace, config))
        if isinstance(result, DeclaredCapabilities):
            result = bind(config, result)
        result = self.capabilities.validate_python(result)
        self.validate(config, result)
        return result
