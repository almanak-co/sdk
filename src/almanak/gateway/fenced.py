"""The checkpoint changes one lease-fenced gateway request commits after its unlocked work."""

import time

from almanak.gateway.ethereum import DataError


class Deferred:
    """The result of a submission that runs only inside the fenced commit."""


DEFERRED = Deferred()


class Writes:
    """Changes computed from the checkpoint body a request read without the write lock.

    A replacement checkpoint requires that body to be unchanged when the commit holds the
    lock, as though the request had held it throughout. A submission checks the facts it
    depends on itself before it sends. Other changes, such as a retained fault, are
    conservative and apply to whichever checkpoint is current.
    """

    def __init__(self, body: str, durable=None):
        self.body = body
        # Wall time, as the lease row's expiry is: a suspended machine advances it.
        self.started = time.time()
        self.durable = durable
        self.after = None
        self.changes: list = []
        self.submission = None

    def replace(self, after) -> None:
        self.after = after

    def apply(self, change) -> None:
        self.changes.append(change)

    def write_ahead(self, change) -> None:
        """Commit `change` durably now, before an irreversible effect this request takes.

        It requires the body this request read, and later commits build on the result.
        """
        from almanak.state import decode_checkpoint

        if self.durable is None:
            raise ValueError("a write-ahead commit requires a separate locked transaction")
        with self.durable() as (db, current):
            if current != self.body:
                raise DataError("conflict", "checkpoint changed; reload before recording")
            body = change(decode_checkpoint(current)).model_dump_json()
            db.execute("UPDATE checkpoint SET body=? WHERE id=1", (body,))
        self.body = body

    def require_recent(self, seconds: float) -> None:
        """Refuse, as transient, once this request's checks are older than `seconds`.

        Checks made before the lock (authority, simulation, fees, a protocol deadline)
        are not repeated under it. The holder's lease no longer ends at its expiry, so
        this bounds instead how old those checks may be when signed bytes leave. The
        same request asked again checks afresh.
        """
        age = time.time() - self.started
        if age > seconds:
            raise DataError(
                "stale_checks",
                f"the pre-send checks are {age:.0f}s old, longer than the {seconds:g}s"
                " coordinator lease; nothing was sent, and the next attempt checks again",
                transient=True,
            )

    def submit(self, action) -> Deferred:
        self.submission = action
        return DEFERRED

    def commit(self, db, current: str, result):
        from almanak.state import decode_checkpoint

        if self.after is not None and current != self.body:
            raise DataError("conflict", "checkpoint changed; reload before recording")
        if self.submission is not None:
            result = self.submission(decode_checkpoint(current))
        state = self.after
        for change in self.changes:
            state = change(decode_checkpoint(current) if state is None else state)
        if state is not None:
            db.execute("UPDATE checkpoint SET body=? WHERE id=1", (state.model_dump_json(),))
        return result
