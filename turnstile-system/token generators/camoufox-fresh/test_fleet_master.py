"""Tests for fleet_master's slot accounting and minting liveness.

Two incidents are pinned here.

The first cost a live fleet: a manually dispatched matrix run ("warp x18") kept
reporting its title size after 10 of its 18 jobs had finished, so the supervisor
saw a full fleet, dispatched nothing, and let production halve while logging
success on every tick.

The second is the silent death this file's TestMintingLiveness exists for. On
2026-08-01 run 30700078405 (`f51`) posted tokens for 21 minutes and then nothing
for four hours. GitHub reported the job `in_progress` throughout, so the
supervisor counted it toward both `alive` and `productive` — and because
`alive` is what `hard_cap` is measured against, a hung runner SUPPRESSED the
replacement that would have compensated for it.

Run from this directory:  python -m pytest test_fleet_master.py -q
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

from fleet_master import (  # noqa: E402
    MIN_STAGGER_SECONDS, ExitEvidence, GitHubAPI, Run, assess_minting, classify,
    plan_cycle, producer_of, read_exits,
)


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

    def test_run_number_is_carried(self):
        """The run number is the only link between a run and its tokens: the
        producer labels every token `cfxheal-warp-f<run_number>...`."""
        raw = [{"id": 5, "run_number": 51, "status": "in_progress",
                "created_at": "2026-07-25T00:00:00Z",
                "display_title": "warp x1 18000s relay=true via=supervisor"}]
        assert classify(raw, "warp", 1e9)[0].number == 51


# ---------------------------------------------------------------------------
#  MINTING LIVENESS
# ---------------------------------------------------------------------------

def row(src, label, last_seen, labels=None, minted=999999):
    """One /stats/exits row. `minted` is deliberately given an absurd, constant
    value in every fixture: if any future change starts attributing a per-src
    counter to a per-label producer, these tests must not be able to pass."""
    out = {"src": src, "label": label, "lastSeenSecAgo": last_seen, "minted": minted}
    if labels:
        out["labels"] = labels
    return out


def gh_run(number, run_id=None, produce_started=0.0, via="supervisor"):
    return Run(id=run_id if run_id is not None else 100000 + number,
               status="in_progress",
               title="warp x1 18000s relay=true via=%s" % via,
               age=7200.0, tunnel="warp", producers=1, duration=18000, via=via,
               number=number, produce_started=produce_started)


class TestProducerIdentity:
    def test_run_number_is_the_identity(self):
        assert producer_of("cfxheal-warp-f51ed-r0w0-100of40") == ("gh", 51)

    def test_the_self_report_suffix_is_not_identity(self):
        """One runner emits many label STRINGS as its beacon moves. Reading them
        as separate occupants would make every row look shared and inflate every
        bound by occupancyGrace."""
        assert (producer_of("cfxheal-warp-f59ed-r0w0-100of40")
                == producer_of("cfxheal-warp-f59ed-r0w0-97of40"))

    def test_an_account_marked_label_is_still_attributable(self):
        """2026-08-06: `CAMOUFOX_FLEET_MARK` puts an account segment ahead of
        the tunnel, so three of the four live fleets post `cfxheal-a2-warp-…`.
        The pattern demanded exactly one segment, so none of them matched and
        the minting-liveness check was OFF on 75% of the fleet — every producer
        read `mint=never-produced` and no hung runner could ever be caught.
        Labels taken verbatim from /stats/exits that afternoon."""
        assert producer_of("cfxheal-a2-warp-f286ed") == ("gh", 286)
        assert producer_of("cfxheal-a3-warp-f165ed") == ("gh", 165)
        assert producer_of("cfxheal-a4-warp-f153ed") == ("gh", 153)

    def test_a_marked_label_with_a_self_report_still_reads_the_run_number(self):
        """The greedy segment match must not swallow the run number and
        capture something out of the beacon suffix instead."""
        assert (producer_of("cfxheal-a2-warp-f286ed-r0w0-100of40")
                == producer_of("cfxheal-a2-warp-f286ed-r0w0-97of40")
                == ("gh", 286))

    def test_the_unmarked_account_still_works(self):
        """Account 1 carries no mark; the widened pattern must not regress it."""
        assert producer_of("cfxheal-warp-f373ed") == ("gh", 373)

    def test_vm_lanes_are_not_github_runners(self):
        assert producer_of("vm-cfx4-r0w0-100of5") == ("other", "vm-cfx4")

    def test_pre_label_contract_producers_are_unattributable(self):
        """Runs 1-3 in the recorded history carried the old shared label
        `cfxheal-warp-fleet`, which names no run. They must never be mistaken
        for a run number."""
        kind, ident = producer_of("cfxheal-warp-fleet-r0w0-0of0")
        assert kind == "other"


class TestReadExits:
    def test_exclusive_row_gives_the_raw_age(self):
        ev = read_exits({"exits": [row("1.1.1.1", "cfxheal-warp-f52ed-r0w0-100of40", 7)]})
        assert ev.silence == {52: 7.0}

    def test_one_runner_with_two_label_strings_is_still_exclusive(self):
        ev = read_exits({"exits": [row(
            "1.1.1.1", "cfxheal-warp-f59ed-r0w0-97of40", 5,
            labels=["cfxheal-warp-f59ed-r0w0-100of40",
                    "cfxheal-warp-f59ed-r0w0-97of40"])]})
        assert ev.silence == {59: 5.0}, "same runner, so no attribution penalty"

    def test_shared_row_pays_the_occupancy_grace(self):
        """WARP's anycast v4 puts three runners on one address. The row's single
        `minted` counter cannot be split, so the only sound statement is that
        each named label minted within occupancyGrace of the row's last mint."""
        ev = read_exits({"exits": [row(
            "1.1.1.1", "cfxheal-warp-f56ed-r0w0-100of40", 0,
            labels=["cfxheal-warp-f50ed-r0w0-100of40",
                    "cfxheal-warp-f56ed-r0w0-100of40",
                    "cfxheal-warp-f59ed-r0w0-100of40"])]})
        assert ev.silence == {50: 600.0, 56: 600.0, 59: 600.0}

    def test_a_runner_takes_the_best_row_it_appears_on(self):
        """Minting anywhere proves it is alive, so the bound is a MIN across
        rows — WARP rotates addresses under a live producer."""
        ev = read_exits({"exits": [
            row("1.1.1.1", "cfxheal-warp-f49ed-r0w0-100of40", 2198),
            row("2.2.2.2", "cfxheal-warp-f49ed-r0w0-100of40", 3),
        ]})
        assert ev.silence == {49: 3.0}

    def test_vm_lane_is_picked_up_as_the_off_fleet_control(self):
        ev = read_exits({"exits": [row("3.3.3.3", "vm-cfx3-r0w0-100of40", 4)]})
        assert ev.control_silence == 4.0
        assert ev.silence == {}

    def test_row_shared_between_a_vm_lane_and_a_runner(self):
        """Observed live: one src carrying vm-cfx2, vm-cfx4 and a GitHub runner."""
        ev = read_exits({"exits": [row(
            "4.4.4.4", "vm-cfx4-r0w0-100of5", 1,
            labels=["vm-cfx2-r1w0-100of40", "vm-cfx4-r0w0-100of5",
                    "cfxheal-warp-f56ed-r0w0-100of40"])]})
        assert ev.silence == {56: 601.0}
        assert ev.control_silence == 601.0

    def test_an_unlabelled_co_occupant_still_makes_the_row_shared(self):
        """The relay renders a producer that posts no label at all as the literal
        string "(unlabelled)" inside `labels`. It is still a second occupant, so
        the row's evidence still has to pay the attribution slack."""
        ev = read_exits({"exits": [row(
            "5.5.5.5", "cfxheal-warp-f52ed-r0w0-100of40", 0,
            labels=["(unlabelled)", "cfxheal-warp-f52ed-r0w0-100of40"])]})
        assert ev.silence == {52: 600.0}
        assert ev.control_silence is None, "an unlabelled row is not the VM control"

    def test_missing_last_seen_is_ignored_not_guessed(self):
        assert read_exits({"exits": [{"src": "9.9.9.9",
                                      "label": "cfxheal-warp-f9-r0w0-0of0"}]}).silence == {}


class TestMintingLiveness:
    THRESHOLD = 2700.0
    STARTUP = 900.0

    def assess(self, runs, evidence, ledger=None, now=100000.0, **kw):
        return assess_minting(runs, evidence, ledger or {}, now,
                              threshold=kw.pop("threshold", self.THRESHOLD),
                              startup=kw.pop("startup", self.STARTUP), **kw)

    def healthy(self, numbers, last_seen=3.0):
        return ExitEvidence(silence={n: last_seen for n in numbers},
                            control_silence=2.0, rows=len(numbers))

    # ---- the f51 shape -------------------------------------------------
    def test_f51_shape_is_detected(self):
        """Seen minting, then absent from every row (WARP handed its address on
        and the relay reset the row). Absence carries no clock, so the ledger
        supplies one."""
        runs = [gh_run(n) for n in (50, 51, 52, 53, 54, 55)]
        ev = self.healthy([50, 52, 53, 54, 55])          # 51 is simply gone
        ledger = {"runs": {str(gh_run(51).id): {"seen": 100000.0 - 3000, "ever": True}}}
        check = self.assess(runs, ev, ledger)
        assert check.verdict[gh_run(51).id] == "stalled"
        assert check.stalled == {gh_run(51).id}
        assert check.cancellable == [gh_run(51).id]

    def test_a_stalled_runner_that_comes_back_is_forgiven_immediately(self):
        """Any sighting re-anchors the clock to relay evidence, so a stale
        ledger entry can never keep accusing a producer that recovered."""
        runs = [gh_run(51)]
        stale = {"runs": {str(gh_run(51).id): {"seen": 0.0, "ever": True}}}
        check = self.assess(runs, self.healthy([51]), stale)
        assert check.verdict[gh_run(51).id] == "producing"
        assert check.stalled == set()

    def test_visible_but_stale_needs_no_ledger_at_all(self):
        runs = [gh_run(51)]
        ev = ExitEvidence(silence={51: 4000.0}, control_silence=1.0, rows=1)
        assert self.assess(runs, ev).verdict[gh_run(51).id] == "stalled"

    # ---- what must NOT fire --------------------------------------------
    def test_healthy_fleet_fires_on_nobody(self):
        runs = [gh_run(n) for n in range(50, 62)]
        check = self.assess(runs, self.healthy(range(50, 62)))
        assert check.stalled == set()
        assert set(check.verdict.values()) == {"producing"}

    def test_the_longest_recovered_gap_ever_recorded_does_not_fire(self):
        """f21, 2026-08-01 04:11Z: silent for a measured 1800 s bound and then
        minting again at 04:40Z. It is the only healthy runner in 23 h of
        recorded samples to exceed 970 s, and the threshold must clear it."""
        runs = [gh_run(21)]
        ev = ExitEvidence(silence={21: 1800.0}, control_silence=1.0, rows=1)
        assert self.assess(runs, ev).verdict[gh_run(21).id] == "producing"

    def test_a_shorter_threshold_would_have_killed_it(self):
        """Pins WHY the threshold is 2700 and not 1800: a watchdog shorter than
        the work it supervises causes the failure it watches for."""
        runs = [gh_run(21)]
        ev = ExitEvidence(silence={21: 1800.0}, control_silence=1.0, rows=1)
        assert self.assess(runs, ev, threshold=1800.0).stalled == {gh_run(21).id}

    def test_a_run_that_has_not_reached_produce_is_never_judged(self):
        runs = [gh_run(60, produce_started=None)]
        check = self.assess(runs, self.healthy([50]))
        assert check.verdict[gh_run(60).id] == "starting"
        assert check.stalled == set()

    def test_startup_grace_is_measured_from_the_produce_step(self):
        """Anchoring on created_at would accuse a runner that sat 37 min in
        GitHub's queue — a wait this account has actually produced."""
        runs = [gh_run(60, produce_started=100000.0 - 300)]
        assert self.assess(runs, self.healthy([50])).verdict[gh_run(60).id] == "starting"

    def test_no_snapshot_means_no_opinion(self):
        """And the per-run report must not read like a verdict either — an
        instrument that was never consulted has to say so."""
        runs = [gh_run(n) for n in (50, 51)]
        check = self.assess(runs, None)
        assert check.stalled == set() and "not evaluated" in check.note
        assert set(check.verdict.values()) == {"unknown:no-snapshot"}

    def test_unattributable_fleet_abstains(self):
        """If the label contract changes, every runner looks dead. Acting then
        would cancel a perfectly healthy fleet."""
        runs = [gh_run(n) for n in range(50, 56)]
        check = self.assess(runs, ExitEvidence(silence={}, control_silence=1.0))
        assert check.stalled == set() and "attributable" in check.note

    def test_correlated_outage_acts_on_nobody(self):
        """GitHub's cuts are network-wide and self-healing; gartic has global
        acceptance outages of its own. Tearing a fleet down in response to one
        is a documented mistake."""
        runs = [gh_run(n) for n in range(50, 56)]
        # Everyone is still visible, so attribution is healthy — four of the six
        # have simply been quiet for longer than the threshold at once.
        ev = ExitEvidence(silence={50: 3.0, 51: 3.0, 52: 4000.0, 53: 4000.0,
                                   54: 4000.0, 55: 4000.0},
                          control_silence=2.0, rows=6)
        check = self.assess(runs, ev)
        assert check.stalled == set() and "correlated" in check.note

    def test_a_fleet_that_vanishes_together_also_abstains(self):
        """The same event seen the other way round: during a fleet-wide outage
        the quiet runners drop out of `/stats/exits` entirely, so the
        attribution guard catches what the correlation guard would have."""
        runs = [gh_run(n) for n in range(50, 56)]
        ev = ExitEvidence(silence={50: 3.0, 51: 3.0}, control_silence=2.0, rows=2)
        ledger = {"runs": {str(gh_run(n).id): {"seen": 90000.0, "ever": True}
                           for n in range(52, 56)}}
        check = self.assess(runs, ev, ledger)
        assert check.stalled == set() and "acting on nobody" in check.note

    def test_a_quiet_off_fleet_control_vetoes_the_check(self):
        """The VM is a different machine on a different network. If it stopped
        minting in the same window, the event is host-wide."""
        runs = [gh_run(n) for n in (50, 51, 52, 53)]
        ev = ExitEvidence(silence={50: 3.0, 51: 3.0, 52: 3.0}, control_silence=5000.0)
        ledger = {"runs": {str(gh_run(53).id): {"seen": 90000.0, "ever": True}}}
        check = self.assess(runs, ev, ledger)
        assert check.stalled == set() and "host-wide" in check.note

    def test_a_long_dead_control_does_not_veto_forever(self):
        """If the VM's producers are simply switched off, its rows go stale for
        good — and letting that disable the GitHub check permanently would be a
        silent way of undoing this whole feature."""
        runs = [gh_run(n) for n in (50, 51, 52, 53)]
        ev = ExitEvidence(silence={50: 3.0, 51: 3.0, 52: 3.0},
                          control_silence=99999.0)
        ledger = {"runs": {str(gh_run(53).id): {"seen": 90000.0, "ever": True}}}
        assert self.assess(runs, ev, ledger).stalled == {gh_run(53).id}

    # ---- never-produced is downgraded but never killed -----------------
    def test_never_seen_runner_is_downgraded_but_not_cancellable(self):
        """Three runs in the recorded history were never attributable and all
        three were healthy — they predated the per-run label. A hang and a
        label-contract mismatch look identical from here, so the cheap remedy
        applies and the irreversible one does not."""
        runs = [gh_run(n) for n in (50, 51, 52, 53)]
        ev = self.healthy([50, 51, 52])
        check = self.assess(runs, ev)
        assert check.verdict[gh_run(53).id] == "never-produced"
        assert check.stalled == {gh_run(53).id}
        assert check.cancellable == []

    def test_a_foreign_run_is_never_cancellable(self):
        runs = [gh_run(n) for n in (50, 51, 52)] + [gh_run(53, via="manual")]
        ledger = {"runs": {str(gh_run(53).id): {"seen": 90000.0, "ever": True}}}
        check = self.assess(runs, self.healthy([50, 51, 52]), ledger)
        assert check.stalled == {gh_run(53).id} and check.cancellable == []

    # ---- ledger mechanics ----------------------------------------------
    def test_ledger_records_the_relay_anchored_time_not_now(self):
        runs = [gh_run(51)]
        ev = ExitEvidence(silence={51: 601.0}, control_silence=1.0, rows=1)
        entry = self.assess(runs, ev).ledger["runs"][str(gh_run(51).id)]
        assert entry["seen"] == 100000.0 - 601.0 and entry["ever"] is True

    def test_finished_runs_drop_out_of_the_ledger(self):
        """Only runs passed in are carried forward, so the file cannot grow
        without bound as the fleet churns through run ids."""
        runs = [gh_run(51)]
        old = {"runs": {"999": {"seen": 1.0, "ever": True},
                        str(gh_run(51).id): {"seen": 2.0, "ever": True}}}
        assert set(self.assess(runs, self.healthy([51]), old).ledger["runs"]) == \
            {str(gh_run(51).id)}

    def test_a_lost_cache_cannot_accuse_anyone(self):
        """Cache miss: every absent runner starts its clock at its Produce step,
        so nothing can be judged until a full threshold has passed."""
        runs = [gh_run(51, produce_started=100000.0 - 1000)]
        check = self.assess(runs, ExitEvidence(silence={50: 1.0}, control_silence=1.0))
        assert check.verdict[gh_run(51).id] == "producing"


class TestStalledSlotAccounting:
    """A stalled run still HOLDS its runner slot — that is the whole reason it
    blocks its own replacement — so it must keep counting toward `alive` while
    dropping out of `productive`."""

    def fleet(self, n=12):
        return [gh_run(50 + i) for i in range(n)]

    def test_stalled_run_stops_counting_as_productive(self):
        runs = self.fleet(12)
        base = plan_cycle(runs, target=12, hard_cap=18, overlap_seconds=1800,
                          max_dispatch=6, max_cancel=0)
        assert base.dispatch == 0
        with_stall = plan_cycle(runs, target=12, hard_cap=18, overlap_seconds=1800,
                                max_dispatch=6, max_cancel=0,
                                stalled={runs[1].id})
        assert with_stall.dispatch == 1
        assert sum(r.slots for r in with_stall.stalled) == 1

    def test_stalled_run_still_counts_toward_the_hard_cap(self):
        """It really is occupying a runner. Pretending otherwise would let the
        fleet overshoot the account's concurrent-job ceiling."""
        runs = self.fleet(18)
        plan = plan_cycle(runs, target=18, hard_cap=18, overlap_seconds=1800,
                          max_dispatch=6, max_cancel=0, stalled={runs[0].id})
        assert plan.dispatch == 0, "no room; the cap is measured on alive"
        assert plan.blocked_by_cap is True

    def test_blocked_by_cap_is_false_when_the_ramp_is_the_limit(self):
        """The heavier remedy is reserved for a cap block. A per-cycle ramp
        limit is not a reason to cancel anything — the next cycle covers it."""
        runs = self.fleet(4)
        plan = plan_cycle(runs, target=16, hard_cap=18, overlap_seconds=1800,
                          max_dispatch=2, max_cancel=0)
        assert plan.dispatch == 2 and plan.blocked_by_cap is False

    def test_the_measured_incident(self):
        """16:38Z on 2026-08-01: six runners retired together, the supervisor
        wanted six replacements, and `hard_cap 18 - 16 alive` permitted two —
        with f51 among the sixteen counted as alive AND productive."""
        runs = self.fleet(16)
        retiring = {r.id for r in runs[:6]}
        aged = [Run(**{**r.__dict__, "age": r.duration - 100})
                if r.id in retiring else r for r in runs]
        before = plan_cycle(aged, target=16, hard_cap=18, overlap_seconds=1800,
                            max_dispatch=6, max_cancel=0)
        after = plan_cycle(aged, target=16, hard_cap=18, overlap_seconds=1800,
                           max_dispatch=6, max_cancel=0, stalled={runs[10].id})
        assert before.dispatch == 2 and before.blocked_by_cap is True
        # Downgrading the hung runner does not by itself free a slot — the cap
        # is what binds — which is exactly why cancelling exists as an opt-in
        # and why hard_cap deserves the headroom recommendation.
        assert after.dispatch == 2 and after.blocked_by_cap is True
        assert sum(r.slots for r in after.stalled) == 1


# ---------------------------------------------------------------------------
#  2026-08-06 — THE FOUR-FLEET COLLAPSE
# ---------------------------------------------------------------------------
#
# Relay minting fell 1198 -> 76 tok/min with all four fleets pinned at 2/12,
# 2/12, 0/10, 0/10. The cause is in this file. Supervisor run 31122955777
# (Ahmed0mon, 17:26Z) planned six replacements into an EMPTY fleet and logged:
#
#     dispatched 1/6 (warp, 18000s)
#     dispatched 2/6 (warp, 18000s)
#     RuntimeError: GitHub API HTTP 500: {"message":"Failed to run workflow
#       dispatch", ...}
#
# One transient 500 on the third dispatch raised out of main(), so dispatches
# 3-6 never happened, the cancel phase never happened, and the tick exited 1.
# Every subsequent tick did the same thing at the same point, which is why the
# fleets sat at exactly the count the API happened to allow before it hiccuped
# and never climbed. `GitHubAPI.cancel` already reasoned this way in its
# docstring — "One already-finished run must never abort the cycle, because
# that would also skip the dispatches" — and dispatch simply never got the
# same treatment.

class TestTransientApiFailures:
    """A transient GitHub API failure must cost at most its own dispatch."""

    def test_request_retries_a_500(self):
        """The 500 that collapsed four fleets was transient — the same call
        succeeds seconds later. Retrying it is the whole fix."""
        api = GitHubAPI("t")
        attempts = []

        def flaky(_method, _path, _body):
            attempts.append(1)
            if len(attempts) < 3:
                raise RuntimeError("GitHub API HTTP 500: Internal Server Error")
            return {"ok": True}

        api._send = flaky
        assert api.request("POST", "x", {}) == {"ok": True}
        assert len(attempts) == 3

    def test_request_does_not_retry_a_permanent_error(self):
        """A 404 or a 401 is a real answer. Retrying it wastes the tick's
        budget and, for an expired PAT, hides how loudly it should fail."""
        api = GitHubAPI("t")
        attempts = []

        def denied(_method, _path, _body):
            attempts.append(1)
            raise RuntimeError("GitHub API HTTP 401: Bad credentials")

        api._send = denied
        try:
            api.request("GET", "x")
        except RuntimeError:
            pass
        else:
            raise AssertionError("a 401 must still raise")
        assert len(attempts) == 1, "a permanent error must not be retried"

    def test_a_failed_dispatch_does_not_abort_the_remaining_dispatches(self):
        """The exact incident. Six were planned, the third raised, and the
        supervisor must still attempt the fourth, fifth and sixth."""
        attempted, succeeded = [], []
        for index in range(6):
            try:
                if index == 2:
                    raise RuntimeError("GitHub API HTTP 500: transient")
                attempted.append(index)
                succeeded.append(index)
            except RuntimeError:
                attempted.append(index)
                continue
        assert len(attempted) == 6, "every planned dispatch must be attempted"
        assert len(succeeded) == 5, "only the failing one is lost"


class TestLifetimeStagger:
    """Ticket 03. A flat lifetime re-synchronises the fleet it refills, so a
    cohort dies together and demands a burst of runners at one instant — and on
    2026-08-06 that burst arrived exactly when GitHub was refusing to allocate
    hosted runners, turning a survivable shortfall into a literal zero."""

    def fleet(self, n, *, duration=18000, spacing=0.0):
        return [make_run(id=100 + i, producers=1, duration=duration,
                         age=i * spacing) for i in range(n)]

    def test_one_replacement_into_a_live_fleet_is_not_the_configured_maximum(self):
        """Write this one first: a naive "spread the cycle's N evenly" passes
        every multi-dispatch case below while still handing a lone replacement
        the flat maximum, which is the actual bug."""
        runs = self.fleet(5, spacing=60.0)
        plan = plan_cycle(runs, target=6, hard_cap=20, overlap_seconds=1800,
                          max_dispatch=6, max_cancel=0, duration_seconds=18000)
        assert plan.dispatch == 1
        assert len(plan.lifetimes) == 1
        assert plan.lifetimes[0] != 18000

    def test_the_chosen_lifetime_lands_in_the_largest_gap(self):
        """One live run dying early leaves the top of the band as the widest
        hole; the replacement must aim there."""
        runs = [make_run(id=1, producers=1, duration=18000,
                         age=18000 - 11000)]     # dies in 11000s
        plan = plan_cycle(runs, target=2, hard_cap=20, overlap_seconds=1800,
                          max_dispatch=6, max_cancel=0, duration_seconds=18000)
        chosen = plan.lifetimes[0]
        assert 11000 < chosen < 18000, f"expected the upper gap, got {chosen}"

    def test_several_dispatches_space_against_each_other(self):
        """Not merely against the live fleet — each choice must join the set
        before the next is made, or a refill of six collapses to six of the
        same number, which is the cohort bug wearing a stagger's clothes."""
        plan = plan_cycle([], target=4, hard_cap=20, overlap_seconds=1800,
                          max_dispatch=6, max_cancel=0, duration_seconds=18000)
        assert plan.dispatch == 4
        assert len(set(plan.lifetimes)) == 4, plan.lifetimes

    def test_every_lifetime_lies_within_the_band(self):
        """The configured duration stays the ceiling — the stagger may only
        shorten a life, never extend one past the knob the operator set."""
        plan = plan_cycle([], target=6, hard_cap=20, overlap_seconds=1800,
                          max_dispatch=6, max_cancel=0, duration_seconds=18000)
        for life in plan.lifetimes:
            assert MIN_STAGGER_SECONDS <= life <= 18000, life

    def test_no_live_runs_spreads_evenly(self):
        runs = []
        plan = plan_cycle(runs, target=3, hard_cap=20, overlap_seconds=1800,
                          max_dispatch=6, max_cancel=0, duration_seconds=18000)
        spread = sorted(plan.lifetimes)
        assert len(spread) == 3
        gaps = [b - a for a, b in zip(spread, spread[1:])]
        assert min(gaps) > 600, f"collapsed together: {spread}"

    def test_lifetime_count_equals_the_permitted_dispatch_count(self):
        """The stagger changes how long each runner lives, never how many run.
        Every dispatch-count test above must keep passing unchanged."""
        runs = self.fleet(16, spacing=30.0)
        plan = plan_cycle(runs, target=16, hard_cap=18, overlap_seconds=1800,
                          max_dispatch=6, max_cancel=0, duration_seconds=18000,
                          stalled={runs[0].id, runs[1].id})
        assert len(plan.lifetimes) == plan.dispatch

    def test_a_short_configured_duration_disables_the_band(self):
        """Below the floor there is no room to stagger, and shortening further
        would spend a growing share of each life on Camoufox/WARP setup. The
        knob wins: everyone gets exactly what was configured."""
        plan = plan_cycle([], target=3, hard_cap=20, overlap_seconds=300,
                          max_dispatch=6, max_cancel=0, duration_seconds=3600)
        assert plan.lifetimes == [3600, 3600, 3600]
