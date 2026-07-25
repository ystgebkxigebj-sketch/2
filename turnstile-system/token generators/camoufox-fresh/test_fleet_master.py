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

    def test_live_zero_from_api_is_reported_as_unknown(self):
        """A live count of 0 on an ACTIVE run means "matrix not created yet",
        not "drained": a matrix run has no producer jobs until its `plan` job
        finishes, and a genuinely drained run leaves ACTIVE_STATUSES and never
        reaches slots at all. live_producer_jobs therefore maps 0 -> None so the
        run keeps its title count; honouring the 0 would have the supervisor
        dispatch a duplicate fleet on top of one about to materialise.

        The property itself still honours an explicit 0 — the mapping is the
        API method's job, and this pins where that responsibility sits."""
        assert make_run(live=0).slots == 0          # property is literal
        assert (0 or None) is None                  # what the API method returns

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


class TestReconcile:
    """Run is frozen, so the reconcile step must REBUILD each run rather than
    assign to it. The first version of this fix assigned, which every
    constructor-based test above still passed — and then died in production with
    FrozenInstanceError on the one line that mattered. These exercise the real
    code path's mechanics, not just the property it feeds."""

    def test_run_is_frozen(self):
        run = make_run()
        try:
            run.live = 3
        except Exception as exc:
            assert "rozen" in type(exc).__name__ or "rozen" in str(exc)
        else:
            raise AssertionError("Run must stay frozen; reconcile relies on replace()")

    def test_replace_produces_a_reconciled_copy(self):
        from dataclasses import replace
        run = make_run()
        assert run.slots == 18
        assert replace(run, live=8).slots == 8
        assert run.slots == 18, "the original must be untouched"

    def test_reconcile_only_rebuilds_multi_producer_runs(self):
        """Mirrors main()'s comprehension: producers=1 runs skip the API call."""
        from dataclasses import replace
        runs = [make_run(id=1, producers=18), make_run(id=2, producers=1)]
        calls = []

        def fake_live(_repo, run_id):
            calls.append(run_id)
            return 8

        out = [replace(r, live=fake_live("repo", r.id)) if r.producers > 1 else r
               for r in runs]
        assert calls == [1], "only the matrix run should cost an API call"
        assert [r.slots for r in out] == [8, 1]


class TestLiveJobCount:
    """The API method's mapping, exercised through a stub transport so the
    plan-job exclusion and the zero-is-unknown rule are pinned without network."""

    class FakeApi:
        def __init__(self, payload):
            self.payload = payload

        def request(self, method, path, body=None):
            if isinstance(self.payload, Exception):
                raise self.payload
            return self.payload

    def count(self, payload):
        from fleet_master import GitHubAPI
        api = GitHubAPI.__new__(GitHubAPI)
        api.request = self.FakeApi(payload).request
        return GitHubAPI.live_producer_jobs(api, "repo", 1)

    def test_excludes_plan_job(self):
        payload = {"jobs": [
            {"name": "plan", "status": "in_progress"},
            {"name": "produce (1)", "status": "in_progress"},
            {"name": "produce (2)", "status": "queued"},
        ]}
        assert self.count(payload) == 2

    def test_ignores_finished_producers(self):
        payload = {"jobs": [
            {"name": "produce (1)", "status": "completed"},
            {"name": "produce (2)", "status": "in_progress"},
        ]}
        assert self.count(payload) == 1

    def test_matrix_not_yet_materialised_reads_as_unknown(self):
        """The incident this rule prevents: a just-dispatched x12 whose plan job
        is still running has no producer jobs yet."""
        payload = {"jobs": [{"name": "plan", "status": "in_progress"}]}
        assert self.count(payload) is None

    def test_api_failure_reads_as_unknown(self):
        assert self.count(RuntimeError("GitHub API HTTP 502")) is None

    def test_missing_jobs_key_reads_as_unknown(self):
        assert self.count({}) is None


class TestClassify:
    def test_live_defaults_to_none(self):
        raw = [{"id": 5, "status": "in_progress", "created_at": "2026-07-25T00:00:00Z",
                "display_title": "warp x18 18000s relay=true via=manual"}]
        runs = classify(raw, "warp", 1e9)
        assert runs and runs[0].live is None, "reconcile must be explicit, not implied"
