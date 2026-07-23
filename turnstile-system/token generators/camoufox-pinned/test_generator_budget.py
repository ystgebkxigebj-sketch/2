"""Tests for the producer's bandwidth brakes.

generator.py imports aiohttp and camoufox at module scope so that it can stay a
single file the workflow runs directly. Neither is needed to exercise the pacing
maths, so both are stubbed before import.
"""

import sys
import types
import unittest


def _stub(name, **attrs):
    module = types.ModuleType(name)
    for key, value in attrs.items():
        setattr(module, key, value)
    sys.modules.setdefault(name, module)


_stub("aiohttp", ClientSession=object, ClientTimeout=object)
_stub("camoufox")
_stub("camoufox.async_api", AsyncCamoufox=object)

import generator  # noqa: E402
from generator import (  # noqa: E402
    MB_PER_TOKEN,
    MintBudget,
    stats_url_for,
    tokens_per_minute_for,
)


class FakeClock:
    def __init__(self):
        self.now = 1000.0

    def __call__(self):
        return self.now

    def advance(self, seconds):
        self.now += seconds


class MintBudgetTests(unittest.TestCase):
    def setUp(self):
        self.clock = FakeClock()
        self._real = generator.time.monotonic
        generator.time.monotonic = self.clock

    def tearDown(self):
        generator.time.monotonic = self._real

    def test_spends_burst_then_waits_for_refill(self):
        budget = MintBudget(per_minute=2.0, burst=3.0, initial=3.0)
        for _ in range(3):
            self.assertEqual(budget.wait_seconds(), 0.0)
            budget.consume()
        # Bucket empty: the next mint waits a full refill period (30 s at 2/min).
        self.assertAlmostEqual(budget.wait_seconds(), 30.0, places=6)
        self.clock.advance(30.0)
        self.assertEqual(budget.wait_seconds(), 0.0)

    def test_unbudgeted_token_is_repaid_by_the_next_wait(self):
        """A freshly rendered widget solves once before it is ever reset."""
        budget = MintBudget(per_minute=2.0, burst=10.0, initial=0.0)
        budget.consume()  # the free first solve of a browser cycle
        # Credits are now -1, so the next mint waits two refill periods, not one.
        self.assertAlmostEqual(budget.wait_seconds(), 60.0, places=6)

    def test_idle_credit_is_capped_by_burst(self):
        budget = MintBudget(per_minute=2.0, burst=30.0, initial=0.0)
        self.clock.advance(24 * 3600)  # a whole idle day
        budget._refill()
        self.assertEqual(budget.credits, 30.0)

    def test_long_run_average_cannot_exceed_the_refill_rate(self):
        """Burst is a loan against the budget, never extra spend."""
        rate, burst, hours = 2.0, 30.0, 6
        budget = MintBudget(per_minute=rate, burst=burst, initial=burst)
        minted = 0
        for _ in range(hours * 3600):
            if budget.wait_seconds() <= 0.0:
                budget.consume()
                minted += 1
            self.clock.advance(1.0)
        ceiling = burst + rate * 60 * hours
        self.assertLessEqual(minted, ceiling)
        self.assertGreater(minted, ceiling * 0.9)

    def test_zero_rate_blocks_forever(self):
        budget = MintBudget(per_minute=0.0, burst=1.0, initial=0.0)
        budget.consume()
        self.assertEqual(budget.wait_seconds(), float("inf"))


class BudgetProjectionTests(unittest.TestCase):
    """The whole point of the change: the fleet must fit inside its GB slice."""

    def spend_gb(self, budget_gb, slots):
        per_slot = tokens_per_minute_for(budget_gb, slots)
        return slots * per_slot * 60 * 24 * 30 * MB_PER_TOKEN / 1024

    def test_derived_rate_spends_exactly_the_budget(self):
        for budget_gb, slots in ((60, 2), (30, 1), (120, 4)):
            with self.subTest(budget_gb=budget_gb, slots=slots):
                self.assertAlmostEqual(self.spend_gb(budget_gb, slots), budget_gb, places=6)

    def test_default_budget_is_a_minority_of_one_webshare_account(self):
        """60 GB leaves the bulk of the 250 GB/month cap for the bot farm."""
        self.assertLess(self.spend_gb(60, 2), 250.0 * 0.25)

    def test_default_rate_is_two_orders_below_an_unthrottled_producer(self):
        """An unthrottled slot mints ~2.35 tok/s = 141 tok/min."""
        self.assertLess(tokens_per_minute_for(60, 2), 141.0 / 100)

    def test_slots_divide_the_budget_rather_than_multiplying_it(self):
        self.assertAlmostEqual(
            tokens_per_minute_for(60, 4) * 4, tokens_per_minute_for(60, 1), places=6
        )

    def test_slot_count_is_floored_at_one(self):
        self.assertEqual(tokens_per_minute_for(60, 0), tokens_per_minute_for(60, 1))


class ShelfTargetTests(unittest.TestCase):
    """Idle hours must not be paid for at the full stocked-shelf rate."""

    def setUp(self):
        import os

        self._saved = dict(os.environ)
        os.environ["AUTH_SECRET"] = "test"
        os.environ["RELAY_SHELF_TARGET"] = "4"
        os.environ["RELAY_IDLE_SHELF"] = "1"
        os.environ["DEMAND_IDLE_SECONDS"] = "600"
        self.clock = FakeClock()
        self._real = generator.time.monotonic
        generator.time.monotonic = self.clock
        self.generator = generator.Generator()

    def tearDown(self):
        import os

        generator.time.monotonic = self._real
        os.environ.clear()
        os.environ.update(self._saved)

    def test_full_shelf_while_demand_is_recent(self):
        self.assertEqual(self.generator.current_shelf_target(), 4)

    def test_drops_to_the_idle_shelf_once_demand_stops(self):
        self.clock.advance(601)
        self.assertEqual(self.generator.current_shelf_target(), 1)

    def test_idle_shelf_is_never_zero_by_default(self):
        """icebot's /assign takes no `wait`, so an empty shelf hides demand."""
        self.clock.advance(24 * 3600)
        self.assertGreaterEqual(self.generator.current_shelf_target(), 1)

    def test_a_consumed_token_restores_the_full_shelf(self):
        self.clock.advance(601)
        self.assertEqual(self.generator.current_shelf_target(), 1)
        self.generator.last_demand_at = self.clock.now  # observed totalOut climb
        self.assertEqual(self.generator.current_shelf_target(), 4)

    def test_shelf_is_affordable_against_the_token_ttl(self):
        """Holding N tokens costs N/210 tok/s forever; keep that under budget."""
        usable_ttl = 210.0
        fleet_rate = tokens_per_minute_for(60, 2) * 2  # tok/min across the fleet
        hold_cost = self.generator.shelf_target / usable_ttl * 60  # tok/min
        self.assertLessEqual(hold_cost, fleet_rate * 1.25)


class StatsUrlTests(unittest.TestCase):
    def test_derives_stats_from_add(self):
        self.assertEqual(
            stats_url_for("https://relay.example:8443/add"),
            "https://relay.example:8443/stats",
        )

    def test_tolerates_a_trailing_slash(self):
        self.assertEqual(
            stats_url_for("https://relay.example:8443/add/"),
            "https://relay.example:8443/stats",
        )

    def test_explicit_override_wins(self):
        import os

        os.environ["RELAY_STATS_URL"] = "https://elsewhere/stats"
        try:
            self.assertEqual(
                stats_url_for("https://relay.example:8443/add"),
                "https://elsewhere/stats",
            )
        finally:
            del os.environ["RELAY_STATS_URL"]


if __name__ == "__main__":
    unittest.main()
