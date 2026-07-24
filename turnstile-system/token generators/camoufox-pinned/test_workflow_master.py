import unittest

from workflow_master import plan_refill


def run(slot, status="in_progress"):
    return {"display_title": f"Camoufox slot {slot}", "status": status}


class PlanRefillTests(unittest.TestCase):
    def test_fills_missing_slots(self):
        plan = plan_refill([run(0), run(2)], target=4)
        self.assertEqual(plan.active_slots, frozenset({0, 2}))
        self.assertEqual(plan.missing_slots, (1, 3))

    def test_completed_runs_do_not_occupy_capacity(self):
        plan = plan_refill([run(0, "completed")], target=2)
        self.assertEqual(plan.missing_slots, (0, 1))

    def test_duplicate_and_unnamed_runs_reduce_capacity(self):
        runs = [run(0), run(0), {"display_title": "legacy", "status": "queued"}]
        plan = plan_refill(runs, target=4)
        self.assertEqual(plan.missing_slots, (1,))
        self.assertEqual(plan.unnamed_active, 1)

    def test_never_exceeds_target(self):
        runs = [run(0), run(1), run(2), run(3)]
        self.assertEqual(plan_refill(runs, target=4).missing_slots, ())


if __name__ == "__main__":
    unittest.main()


class StaggeredDispatchTests(unittest.TestCase):
    """One slot per cycle, so a cold start cannot commit the whole metered
    proxy budget before the first producer is seen to be healthy."""

    def test_cold_start_dispatches_only_one(self):
        plan = plan_refill([], target=20, max_dispatch=1)
        self.assertEqual(plan.missing_slots, (0,))

    def test_ramps_one_at_a_time(self):
        plan = plan_refill([run(0)], target=20, max_dispatch=1)
        self.assertEqual(plan.missing_slots, (1,))

    def test_zero_means_unlimited(self):
        plan = plan_refill([], target=4, max_dispatch=0)
        self.assertEqual(plan.missing_slots, (0, 1, 2, 3))

    def test_nothing_to_do_when_full(self):
        runs = [run(0), run(1)]
        self.assertEqual(plan_refill(runs, target=2, max_dispatch=1).missing_slots, ())

    def test_cap_never_exceeds_capacity(self):
        # target 2, one already active -> only one slot free even if cap is 5
        plan = plan_refill([run(0)], target=2, max_dispatch=5)
        self.assertEqual(plan.missing_slots, (1,))
