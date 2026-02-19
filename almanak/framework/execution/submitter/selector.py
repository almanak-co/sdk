"""Submitter selection helpers for copy-trading execution policy."""

from __future__ import annotations

from dataclasses import dataclass

from almanak.framework.execution.interfaces import SubmissionError, Submitter


@dataclass(frozen=True)
class SubmitterSelection:
    """Selected submitter and resolved mode."""

    submitter: Submitter
    resolved_mode: str


def select_submitter(
    submission_mode: str,
    public_submitter: Submitter,
    private_submitter: Submitter | None = None,
) -> SubmitterSelection:
    """Select submitter according to copy execution policy.

    Modes:
    - `public`: always use public submitter
    - `private`: require private submitter
    - `auto`: prefer private, fallback to public
    """
    mode = submission_mode.lower()

    if mode == "public":
        return SubmitterSelection(submitter=public_submitter, resolved_mode="public")

    if mode == "private":
        if private_submitter is None:
            raise SubmissionError("submission_mode=private but no private submitter configured")
        return SubmitterSelection(submitter=private_submitter, resolved_mode="private")

    if mode == "auto":
        if private_submitter is not None and getattr(private_submitter, "enabled", True):
            return SubmitterSelection(submitter=private_submitter, resolved_mode="private")
        return SubmitterSelection(submitter=public_submitter, resolved_mode="public")

    raise SubmissionError(f"Unknown submission mode: {submission_mode}")
