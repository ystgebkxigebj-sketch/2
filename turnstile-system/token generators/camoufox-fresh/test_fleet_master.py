"""Tests for fleet_master's slot accounting.

The bug these pin cost a live fleet: a manually dispatched matrix run ("warp
x18") kept reporting its title size after 10 of its 18 jobs had finished, so the
supervisor saw a full fleet, dispatched nothing, and let production halve while
logging success on every tick.
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

from fleet_master import Run, classify, plan_cycle  # noqa: E402


def make_run(**kw) -> Run:
    base = dict(id=1, status="in_progress", title="warp x18 18000s relay=true via=manual",
                age=100.0, tunnel="warp", producers=18, duration=18000, via="manual")
    base.update(kw)
    return Run(**base)


class TestSlots:
    def test_falls_back_to_title_when_live_unknown(self):
        """An API failure must read as 'still full', never as 'died' — the
        conservative direction, since the alternative dispatches a duplicate
        fleet."""
        assert make_run(live=None).slots == 18

    def test_live_count_wins_over_title(self):
        """The actual regression: 8 jobs alive in a run dispatched with 18."""
        assert make_run(live=8).slots == 8

    def test_live_zero_is_honoured(self):
        """A drained run contributes nothing. `0` must not be confused with the
        unknown case — that confusion is what makes the fleet decay silently."""
        assert make_run(live=0).slots == 0

    def test_single_producer_run_unaffected(self):
        run = make_run(producers=1, title="warp x1 18000s relay=true via=supervisor")
        assert run.slots == 1


class TestPlanCycle:
    def test_decaying_matrix_run_triggers_replacements(self):
        """End-to-end shape of the incident: target 18, one matrix run whose
        live count has fallen to 8, so 10 replacements are owed."""
        runs = [make_run(live=8)]
        plan = plan_cycle(runs, target=18, hard_cap=20, overlap_seconds=1800,
                          max_dispatch=6, max_cancel=0)
        assert plan.dispatch > 0, "a decayed fleet must be refilled"

    def test_full_matrix_run_dispatches_nothing(self):
        runs = [make_run(live=18)]
        plan = plan_cycle(runs, target=18, hard_cap=20, overlap_seconds=1800,
                          max_dispatch=6, max_cancel=0)
        assert plan.dispatch == 0

    def test_stale_title_would_have_masked_the_gap(self):
        """Guards the fix itself: with the old title-only accounting the same
        decayed fleet looks full, which is precisely the silent failure."""
        stale = [make_run(live=None)]   # 18 by title, though only 8 are alive
        plan = plan_cycle(stale, target=18, hard_cap=20, overlap_seconds=1800,
                          max_dispatch=6, max_cancel=0)
        assert plan.dispatch == 0, "documents the old behaviour this fix removes"


class TestClassify:
    def test_live_defaults_to_none(self):
        raw = [{"id": 5, "status": "in_progress", "created_at": "2026-07-25T00:00:00Z",
                "display_title": "warp x18 18000s relay=true via=manual"}]
        runs = classify(raw, "warp", 1e9)
        assert runs and runs[0].live is None, "reconcile must be explicit, not implied"
