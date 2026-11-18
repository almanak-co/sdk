"""Entrypoint only launched behind the strategy OS sandbox."""

from almanak.dashboard.evaluate import worker_main

if __name__ == "__main__":
    worker_main()
