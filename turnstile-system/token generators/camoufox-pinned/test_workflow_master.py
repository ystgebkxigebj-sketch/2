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
