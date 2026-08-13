"""Shared historical-cadence classification helpers."""

from almanak.framework.data.timeframes import CANONICAL_OHLCV_TIMEFRAMES, OHLCVTimeframe

# Vendors can emit slightly irregular timestamps (for example, 3601 seconds
# between nominal hourly points). Keep this tolerance shared by discovery,
# indicator refusal, and generated configuration patches so they cannot
# disagree about the same measured series.
_CADENCE_JITTER_CAP_SECONDS = 60


def cadence_is_coarser(cadence_seconds: int, timeframe_seconds: int) -> bool:
    """Return whether measured cadence is genuinely coarser than a timeframe."""
    tolerance = min(_CADENCE_JITTER_CAP_SECONDS, int(timeframe_seconds) * 2 // 100)
    return int(cadence_seconds) - int(timeframe_seconds) > tolerance


def canonical_timeframe_for_cadence(cadence_seconds: int) -> OHLCVTimeframe | None:
    """Return the finest compatible canonical timeframe, if one exists.

    The same jitter tolerance used to classify cadence also governs generated
    patches: a measured 3601-second hourly feed maps to the valid ``1h``
    configuration value, never the invalid literal ``3601s``.
    """
    return next(
        (
            timeframe
            for timeframe in CANONICAL_OHLCV_TIMEFRAMES
            if not cadence_is_coarser(cadence_seconds, timeframe.seconds)
        ),
        None,
    )
