"""Runtime-owned, atomic progress for the gateway's read-only preparation phase."""

import fcntl
import json
import os
from pathlib import Path

from pydantic import model_validator

from almanak.chains import CheckpointReference
from almanak.configuration.resolution import ReadContext, ResolutionAnchors
from almanak.gateway.bootstrap import ReadBinding, ResolutionResult
from almanak.gateway.client import GatewayClient
from almanak.gateway.operator_config import Digest
from almanak.models import Model
from almanak.workspace import save


class ResolutionProgress(Model):
    context: ReadContext
    binding: ReadBinding | None = None
    checkpoints: tuple[CheckpointReference, ...] = ()
    result: ResolutionResult | None = None
    result_digest: Digest | None = None

    @model_validator(mode="after")
    def scope(self):
        if self.binding is not None:
            self.binding.require(self.context)
        if len(self.checkpoints) > len(self.context.anchors):
            raise ValueError("resolution progress contains excess anchors")
        for selection, checkpoint in zip(self.context.anchors, self.checkpoints, strict=False):
            selection.require(checkpoint)
        if self.checkpoints and self.binding is None:
            raise ValueError("resolution anchors require a retained read binding")
        self.require_result()
        return self

    def require_result(self):
        if self.result is None:
            if self.result_digest is not None:
                raise ValueError("partial resolution cannot have a published digest")
            return
        if self.binding is None or self.result_digest != self.result.digest:
            raise ValueError("published resolution has a missing binding or different digest")
        self.result.require(self.context, self.binding)
        if self.result.anchors.checkpoints != self.checkpoints:
            raise ValueError("published resolution differs from retained anchors")


def read_progress(path: Path) -> ResolutionProgress:
    return ResolutionProgress.model_validate_json(path.read_text())


def publish(path: Path, progress: ResolutionProgress, **changes) -> ResolutionProgress:
    updated = ResolutionProgress.model_validate({**progress.model_dump(), **changes})
    save(path, updated.model_dump(mode="json"))
    descriptor = os.open(path.parent, os.O_RDONLY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)
    return updated


def resolve_configuration(
    path: Path, context: ReadContext, client: GatewayClient
) -> ResolutionResult:
    # The existing workspace owns this private path. A process lock serializes
    # atomic publications and releases on process death, including during I/O.
    with path.with_suffix(".lock").open("a") as lock:
        try:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError:
            raise RuntimeError("configuration resolution already has an active owner") from None
        if path.exists():
            progress = read_progress(path)
            if progress.context != context:
                raise ValueError("retained resolution belongs to another provisional context")
        else:
            progress = publish(path, ResolutionProgress(context=context))
        return acquire(path, progress, client)


def acquire(path: Path, progress: ResolutionProgress, client: GatewayClient) -> ResolutionResult:
    context = progress.context
    binding = ReadBinding.model_validate_json(
        json.dumps(
            client.request(
                "resolution-context",
                context_digest=context.digest,
            )
        )
    )
    binding.require(context)
    if progress.binding is not None and progress.binding != binding:
        raise ValueError("gateway restart changed the retained operator binding")
    if progress.binding is None:
        progress = publish(path, progress, binding=binding)
    for selection in context.anchors[len(progress.checkpoints) :]:
        checkpoint = CheckpointReference.model_validate_json(
            json.dumps(
                client.request(
                    "resolution-anchor",
                    context_digest=context.digest,
                    chain=str(selection.chain),
                )
            )
        )
        selection.require(checkpoint)
        progress = publish(path, progress, checkpoints=(*progress.checkpoints, checkpoint))
    anchors = ResolutionAnchors(context_digest=context.digest, checkpoints=progress.checkpoints)
    anchors.require(context)
    if progress.result is not None:
        checked = ResolutionAnchors.model_validate_json(
            json.dumps(
                client.request(
                    "resolution-audit",
                    context_digest=context.digest,
                    anchors=anchors,
                )
            )
        )
        if checked != anchors:
            raise ValueError("gateway audit returned different resolution anchors")
        return progress.result
    result = ResolutionResult.model_validate_json(
        json.dumps(
            client.request(
                "resolution-markets",
                context_digest=context.digest,
                anchors=anchors,
            )
        )
    )
    result.require(context, binding)
    if result.anchors != anchors:
        raise ValueError("gateway resolution changed the retained anchors")
    publish(path, progress, result=result, result_digest=result.digest)
    return result
