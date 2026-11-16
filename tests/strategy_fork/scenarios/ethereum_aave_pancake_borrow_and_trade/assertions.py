"""One fund retains borrowed proceeds, exchange fills and lending unwind."""

from tests.strategy_fork.composed_pancake_authority import assert_permissions as assert_permissions
from tests.strategy_fork.scenarios.base_borrow_and_trade.assertions import (
    assert_closed as assert_closed,
)
from tests.strategy_fork.scenarios.base_borrow_and_trade.assertions import (
    assert_holding as assert_holding,
)
from tests.strategy_fork.scenarios.base_borrow_and_trade.assertions import (
    entered as entered,
)

crash_operation = "swap"
# The production EOA rehearsal waits from runtime launch through the whole
# composed entry and teardown. In Fork Qualification run 35944476180 it reached
# the 180 s entry default with the borrow mined but undelivered, 105 s after the
# first decision. The same run's fork cases took 284 to 290 s of fork block time
# from the first approval to the entry swap, and 525 s from the reversal
# decision to the withdraw decision, before that withdrawal mined and closed.
entry_timeout = 600
# With those waits, runs 35947744444 and 35948929880 entered and then stopped in
# the first hold check, which needs two scheduled evaluations inside
# max(30 s, 3 intervals). Their first hold evaluations started at 03:43:20.90
# and 03:35:23.44 UTC, and no next evaluation had started when the failures were
# reported at 03:43:44 and 03:35:48, against the 5 s interval. The other
# Ethereum composed references use the same 180 s hold wait.
hold_timeout = 180
# Run 35952807290 then passed the hold and stopped at the 20 s runtime stop: the
# SIGINT arrived in an evaluation that started at 04:55:54.70 UTC, 18 s after it
# was due, because the previous one had run about 23 s against the 5 s interval.
# The runtime defers SIGINT (`interrupts.deferred_sigint`) past that step. The other
# Ethereum composed references use the same 180 s process wait.
process_timeout = 180
# Run 35957906927 then passed entry, both resumes and the gateway replacement,
# and stopped at the 900 s closure wait with the final withdrawal pending. On the
# production path the teardown decisions came 14 s, 298 s and 763 s after the
# request (about 06:13:32 UTC, 900 s before the failure), so reversal to withdraw
# decision took 749 s against the fork cases' 525 s. The closure wait matches
# ethereum_compound_curve_borrow_and_trade.
teardown_timeout = 1800
