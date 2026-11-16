"""A minimal V3 tick ledger with a known fee path, for bracketed band growth proofs.

The ledger follows the pool's accounting rules: fee growth accrues at the current tick,
a crossed initialized tick flips its outside growth, a newly initialized tick records
the global growth only when the current tick is at or above it, and a cleared tick
forgets its outside growth. It also retains every accrual with its tick, so tests can
state the growth any hypothetical range actually earned. It is synthetic evidence, not
a replay of any historical pool.
"""

from types import SimpleNamespace

MODULUS = 2**256


class Ledger:
    def __init__(self, tick, growth=(0, 0), ticks=(), liquidity=0):
        self.tick = tick
        self.liquidity = liquidity
        self.growth = list(growth)
        # index -> [gross, net, outside0, outside1]
        self.ticks = {t.index: [t.gross, t.net, t.outside0, t.outside1] for t in ticks}
        self.accruals = []
        self.events = []

    def accrue(self, amount0, amount1):
        self.growth = [
            (g + a) % MODULUS for g, a in zip(self.growth, (amount0, amount1), strict=True)
        ]
        self.accruals.append((self.tick, amount0, amount1))

    def move(self, tick):
        for index, state in self.ticks.items():
            if (self.tick >= index) != (tick >= index):
                state[2] = (self.growth[0] - state[2]) % MODULUS
                state[3] = (self.growth[1] - state[3]) % MODULUS
                # Crossing upward activates the tick's net liquidity; downward removes it.
                self.liquidity += state[1] if tick >= index else -state[1]
        self.tick = tick

    def modify(self, lower, upper, change):
        for index, sign in ((lower, 1), (upper, -1)):
            state = self.ticks.get(index)
            if state is None:
                below = self.tick >= index
                state = [0, 0, self.growth[0] if below else 0, self.growth[1] if below else 0]
                self.ticks[index] = state
            state[0] += change
            state[1] += sign * change
            if state[0] < 0:
                raise AssertionError("ledger removes more liquidity than a tick holds")
            if not state[0]:
                del self.ticks[index]
        if lower <= self.tick < upper:
            self.liquidity += change
        self.events.append((lower, upper, change))

    def sample(self):
        return SimpleNamespace(
            pool=SimpleNamespace(tick=self.tick),
            global_growth=tuple(self.growth),
            ticks=tuple(
                SimpleNamespace(index=i, gross=s[0], net=s[1], outside0=s[2], outside1=s[3])
                for i, s in sorted(self.ticks.items())
            ),
        )

    def mark(self):
        return len(self.accruals), len(self.events)

    def earned(self, lower, upper, start, end):
        """Growth accrued while lower <= tick < upper between two accrual marks."""
        inside = [a for a in self.accruals[start:end] if lower <= a[0] < upper]
        return sum(a[1] for a in inside), sum(a[2] for a in inside)
