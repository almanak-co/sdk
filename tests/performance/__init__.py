"""Performance benchmark tests for the backtesting module.

These tests verify that backtesting operations complete within acceptable
time bounds. They use mock data providers to isolate benchmark timing
from external API latency.

Tests in this module may be slower than unit tests but should still
complete within reasonable bounds (typically under 60 seconds each).
"""
