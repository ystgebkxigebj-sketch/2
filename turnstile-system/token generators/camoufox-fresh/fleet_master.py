"""Keep a free-egress Camoufox producer fleet at its target size, 24/7.

This is the supervisor for the WARP / VPN Gate producers. It is deliberately a
separate program from `camoufox-pinned/workflow_master.py`, which drives the
metered **Webshare** fleet: that one derives its fleet size from a bandwidth
budget (`BUDGET_SLOTS` / `BUDGET_GB_PER_30D`) and ramps one slot per cycle so a
bad config cannot commit a month of proxy spend. None of that applies here. The
free fleet consumes **zero metered bytes**, so byte-budget coupling would only
be a confusing constraint pretending to be a safety property.

What this one has to get right instead:

  1. **A kill switch that works.** The documented failure mode in this project
     is a producer chain that respawns faster than it can be cancelled — an
     incident that burned ~15 runs before the trick was found. The trick is:
     **disable the workflow FIRST, then cancel the runs.** So nothing here is
     ever dispatched by a producer; only this supervisor dispatches, and it
     refuses to dispatch unless an explicit off-switch says otherwise. The
     switch is a *repository variable*, so the operator can stop the fleet with
     one API call — no commit, no workflow run, nothing to race.

  2. **No coverage gap.** GitHub caps a job at 6 h. A producer that simply ends
     and waits to be noticed leaves its share of the fleet dark for a cron
     interval plus several minutes of Camoufox setup. So a run stops counting
     toward the target once it is within `--overlap-seconds` of its end, and its
     replacement is dispatched then — the two overlap, and production never
     drops to zero.

  3. **Fail safe, not fail open.** `--enabled` must be exactly "true". An unset,
     empty, misspelled or otherwise unreadable value means *disabled*. A kill
     switch that only works when its input parses is not a kill switch.

  4. **A runner that has stopped MINTING must stop counting.** GitHub reports a
     wedged producer as a perfectly healthy `in_progress` job, and the fleet's
     own acceptance beacon says nothing about a producer that mints nothing at
     all. Measured 2026-08-01: run 30700078405 (`f51`) posted tokens for 21
     minutes and then nothing for four hours while occupying a slot the
     supervisor counted toward both `alive` and `productive` — so it also
     *suppressed the refill that would have compensated for it*. The liveness
     signal is the relay's own record of who minted (see assess_minting), never
     GitHub's job status. This is the same silent-death class that
     `lane-mint-watchdog` covers on the Oracle VM.

The supervisor never dispatches a producer that self-dispatches a successor, and
producers have no cron of their own. That is the whole reason a supervisor
exists rather than an `if: always()` chain.
"""

from __future__ import annotations

import argparse
import calendar
import json
import os
import re
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass, field, replace


# A run in any of these states is holding, or is about to hold, a slot.
ACTIVE_STATUSES = {"queued", "in_progress", "waiting", "pending", "requested"}

# `run-name` of a supervisor-dispatched producer, e.g.
#   "warp x1 20800s relay=true via=supervisor"
# Only runs matching `via=supervisor` are ever cancelled by this program; a
# human's manual dispatch still counts toward the fleet (it really does produce)
# but is never touched.
TITLE_RE = re.compile(
    r"^(?P<tunnel>\w+)\s+x(?P<producers>\d+)\s+(?P<duration>\d+)s\s+"
    r"relay=(?P<relay>\w+)\s+via=(?P<via>[\w-]+)"
)


@dataclass(frozen=True)
class Run:
    id: int
    status: str
    title: str
    age: float
    tunnel: str
    producers: int
    duration: int
    via: str

    @property
    def ours(self) -> bool:
        return self.via == "supervisor"

    # Live producer jobs still queued/running inside this run. None until
    # reconcile_slots() fills it in; see slots below for why it matters.
    live: int | None = None

    # GitHub's `run_number`. It is the ONLY link between a run and the tokens it
    # produced: the producer labels every token `cfxheal-<tunnel>-f<run_number>…`
    # (gartic-camoufox-fleet.yml), so the relay knows runners by this number and
    # nothing else. None means the run cannot be scored for minting at all.
    number: int | None = None

    # Epoch second at which this run's `Produce` step started — the moment it
    # became reasonable to expect tokens. NOT `created_at`: a run can sit queued
    # for 37 minutes waiting for a runner grant, and then spends ~5 more minutes
    # installing Camoufox and bringing up WARP before the first token. Anchoring
    # the liveness clock on `created_at` would therefore accuse a runner of being
    # dead while it was still being born. None = the step has not started, which
    # is judged as "starting", never as stalled.
    produce_started: float | None = None

    @property
    def slots(self) -> int:
        """Producer jobs this run STILL contributes.

        The title only says how many a run was dispatched with ("warp x18"), and
        for a matrix run that number stops being true the moment the first job
        finishes. Counting the title instead of live jobs made the supervisor
        believe a decaying 18-job run was still full: it reported success every
        tick while the fleet drained 18 -> 8 and dispatched no replacements.
        The supervisor's own runs are producers=1, where the two agree — this
        only bites on a manually dispatched matrix run, which is exactly when
        the fleet is largest and the silence is most expensive.
        """
        if self.live is not None:
            return self.live
        return max(1, self.producers)


@dataclass
class Plan:
    productive: list[Run] = field(default_factory=list)
    retiring: list[Run] = field(default_factory=list)
    # Runs that hold a slot but are not minting. They still count toward `alive`
    # — they really are occupying a runner — but never toward `productive`, so a
    # replacement is dispatched for them.
    stalled: list[Run] = field(default_factory=list)
    dispatch: int = 0
    # One lifetime per dispatch, chosen so this refill's runners do not die at
    # the same moment as each other or as the fleet they are joining. Always
    # `len(lifetimes) == dispatch` — the stagger decides how long each runner
    # lives, never how many run.
    lifetimes: list[int] = field(default_factory=list)
    cancel: list[Run] = field(default_factory=list)
    reason: str = ""
    # True when the fleet is short of target and the SHORTFALL WAS CAPPED by
    # hard_cap rather than by the per-cycle ramp. That is the only situation in
    # which a stalled run is worth the heavier remedy of cancelling it: with
    # room to spare, downgrading it from `productive` already gets it replaced.
    blocked_by_cap: bool = False


# ---------------------------------------------------------------------------
#  MINTING LIVENESS
# ---------------------------------------------------------------------------
#
# Every producer tags each token it POSTs to the relay with a label, and the
# GitHub fleet's label carries its run number:
#
#     cfxheal-warp-f51ed-r0w0-100of40
#                  ^^^ github.run_number
#
# so a runner's identity at the relay is its LABEL, never its IP. It has to be:
# WARP's IPv4 pool is anycast and small, and two or three of our own producers
# routinely mint from the same address at the same time.
#
# ⚠️ THE TRAP THIS CODE EXISTS TO AVOID. `/stats/exits` keeps exactly ONE
# `minted` counter per address. On a shared row that counter is the SUM over its
# occupants, and splitting it per label is not approximation, it is invention —
# it manufactured a false result on 2026-07-30 and it is defect #6 in
# HANDOFF-2026-08-01 §5. **Nothing here divides, apportions or fits `minted`.**
# The only two facts read off a row are:
#
#   `lastSeenSecAgo` — how long ago ANY activity touched this address. Because
#       assignments and verdicts bump it as well as mints, it is a LOWER bound on
#       time-since-last-mint. Lower is the safe direction: a row that reads stale
#       is definitely not minting, so the check can be late but never wrong.
#
#   `labels` / `label` — who minted here. The relay prunes a label from a row
#       once it has been quiet for `occupancyGrace` (= tokenRefTTL = 600 s) at
#       the next mint on that row, so membership is itself a freshness statement:
#       a label still present minted within 600 s of that row's last mint.
#
# which give a sound UPPER BOUND on how long a given runner has been silent:
#
#   exclusive row (one producer)  ->  lastSeenSecAgo
#   shared row (two or more)      ->  lastSeenSecAgo + 600
#
# and a runner's bound is the MINIMUM over every row that names it, because
# minting anywhere proves it is alive. A runner named by no row at all yields no
# bound — that is the honest UNKNOWN, and it is handled by the ledger below
# rather than guessed at.

# `cfxheal-<tunnel>-f<run_number><gate><profile>-r<rung>w<wraps>-<pct>of<n>`.
# Only the run number is identity; everything after it is the producer's live
# self-report and changes constantly (which is why one runner legitimately shows
# several distinct label strings on one row).
RUNNER_LABEL_RE = re.compile(r"^cfxheal-[a-z0-9]+-f(\d+)[a-z]*(?:-|$)")

# Must equal the relay's `occupancyGrace` (tunnel system/relay/exits.go), which
# is `tokenRefTTL` = 10 min. If the relay ever changes it, a shared row's bound
# here becomes optimistic and this check could accuse a live runner.
OCCUPANCY_GRACE_SECONDS = 600.0

# Producers that are not GitHub runners. `vm-` is the Oracle VM's own lanes and
# is the off-fleet control: it is a different machine on a different network, so
# if it went quiet at the same moment the fleet did, the event is not N runner
# hangs (see assess_minting).
CONTROL_LABEL_PREFIX = "vm-"


def producer_of(label: str) -> tuple[str, object]:
    """Identity of whoever posted `label`, with its live self-report stripped.

    Two label strings from one producer (`…-100of40` and `…-97of40`) must not
    read as two occupants, or every row would look shared and every bound would
    be inflated by 600 s.
    """
    match = RUNNER_LABEL_RE.match(label)
    if match:
        return ("gh", int(match.group(1)))
    return ("other", label.split("-r")[0])


@dataclass
class ExitEvidence:
    """What one `/stats/exits` snapshot proves about who is still minting."""

    # run_number -> UPPER BOUND, in seconds, on time since that runner minted.
    silence: dict[int, float] = field(default_factory=dict)
    # Same bound for the off-fleet control (the VM's lanes). None = no control
    # row in the snapshot, which means "no opinion", not "the VM is fine".
    control_silence: float | None = None
    rows: int = 0


def read_exits(payload: dict) -> ExitEvidence:
    evidence = ExitEvidence()
    rows = (payload or {}).get("exits") or []
    evidence.rows = len(rows)
    for row in rows:
        last_seen = row.get("lastSeenSecAgo")
        if last_seen is None:
            continue
        labels = {row.get("label") or ""}
        labels.update(row.get("labels") or [])
        labels.discard("")
        occupants = {producer_of(l) for l in labels}
        if not occupants:
            continue
        # One producer on the row means its counter-free evidence is exact; two
        # or more means the best we can say is "within occupancyGrace of the
        # row's last mint".
        bound = float(last_seen)
        if len(occupants) > 1:
            bound += OCCUPANCY_GRACE_SECONDS
        for kind, ident in occupants:
            if kind == "gh":
                prior = evidence.silence.get(ident)
                if prior is None or bound < prior:
                    evidence.silence[ident] = bound
            elif isinstance(ident, str) and ident.startswith(CONTROL_LABEL_PREFIX):
                if evidence.control_silence is None or bound < evidence.control_silence:
                    evidence.control_silence = bound
    return evidence


@dataclass
class MintCheck:
    verdict: dict[int, str] = field(default_factory=dict)     # run id -> word
    silence: dict[int, float] = field(default_factory=dict)   # run id -> seconds
    stalled: set[int] = field(default_factory=set)            # run ids
    cancellable: list[int] = field(default_factory=list)      # run ids, worst first
    ledger: dict = field(default_factory=dict)                # to persist
    note: str = ""


def assess_minting(runs: list[Run], evidence: ExitEvidence | None, ledger: dict,
                   now: float, *, threshold: float, startup: float,
                   min_attributable: float = 0.5,
                   control_stale_after: float | None = None) -> MintCheck:
    """Decide which runs have stopped minting.

    Why a LEDGER is needed at all, and why `/stats/exits` alone cannot answer
    this: a row is wiped outright when WARP hands its address to a different
    producer (`resetOccupancyLocked`), so a runner that stops minting disappears
    from the snapshot within minutes and takes its timestamp with it. `f51` was
    gone 4 minutes after its last token. Absence is therefore the signal — but
    absence carries no clock, so the supervisor keeps its own: one epoch per run,
    re-anchored to relay evidence every time the runner is visible. A stale
    ledger cannot accuse anyone, because any sighting overwrites it.

    Everything here fails toward "no opinion":

      * no snapshot (relay down, no credential)  -> nothing is stalled
      * run has not reached its `Produce` step   -> "starting"
      * too few runners attributable at all      -> abstain, act on nobody
      * the off-fleet control is quiet too       -> abstain (host-wide episode)
      * half the fleet quiet at once             -> abstain (correlated outage)

    The last two are not politeness. GitHub's acceptance cuts are network-wide
    and self-healing, gartic has global acceptance outages of its own, and
    tearing a fleet down in response to one is a mistake this project has already
    made and written down.
    """
    check = MintCheck()
    if control_stale_after is None:
        # Past this, a quiet control is not an episode — the VM is simply not
        # running producers, and letting that veto the check forever would be a
        # silent way of disabling it.
        control_stale_after = 6 * threshold

    prior = (ledger or {}).get("runs") or {}
    fresh: dict[str, dict] = {}
    judged: list[Run] = []
    visible = 0

    for run in runs:
        key = str(run.id)
        was = prior.get(key) or {}
        ever = bool(was.get("ever"))
        if run.number is None:
            check.verdict[run.id] = "unknown:unattributable"
            continue
        if run.produce_started is None or now - run.produce_started < startup:
            # Not yet expected to mint. Hold the clock at "now" so the grace
            # period is not silently consumed while the runner is still starting.
            check.verdict[run.id] = "starting"
            fresh[key] = {"seen": now, "ever": ever}
            continue

        bound = None if evidence is None else evidence.silence.get(run.number)
        if bound is not None:
            visible += 1
            last_ok = now - bound
            ever = True
        else:
            last_ok = was.get("seen")
            if last_ok is None:
                # Never seen and no history: the clock starts when it should
                # have started minting, so a producer that never mints once is
                # still caught — that is the `no click driver` failure mode.
                last_ok = run.produce_started
        silence = max(0.0, now - float(last_ok))
        fresh[key] = {"seen": float(last_ok), "ever": ever}
        check.silence[run.id] = silence
        judged.append(run)
        if silence < threshold:
            check.verdict[run.id] = "producing"
        elif ever:
            check.verdict[run.id] = "stalled"
        else:
            check.verdict[run.id] = "never-produced"

    check.ledger = {"version": 1, "runs": fresh}

    if evidence is None:
        # Do not leave "stalled" sitting in the per-run report when nothing was
        # measured. A log line that reads like a verdict but was produced by an
        # absent instrument is how this project has repeatedly talked itself
        # into a false conclusion.
        for run in judged:
            check.verdict[run.id] = "unknown:no-snapshot"
        check.note = "no /stats/exits snapshot — minting liveness not evaluated"
        return check
    if not judged:
        check.note = "no runner is old enough to score yet"
        return check

    quiet = [r for r in judged if check.verdict[r.id] != "producing"]
    if visible < min_attributable * len(judged):
        # Two very different things look the same from here — a fleet-wide
        # outage (silent runners vanish from the snapshot within minutes) and a
        # changed label contract — and the right response to both is the same.
        check.note = (f"only {visible}/{len(judged)} runners are attributable at "
                      f"the relay — a fleet-wide outage or a changed label "
                      f"contract; acting on nobody")
        return check
    control = evidence.control_silence
    if control is not None and threshold <= control < control_stale_after:
        check.note = (f"off-fleet control (vm-) also quiet for {control/60:.0f}m "
                      f"— host-wide episode, not {len(quiet)} hangs; acting on nobody")
        return check
    if len(quiet) >= 3 and len(quiet) >= 0.5 * len(judged):
        check.note = (f"{len(quiet)}/{len(judged)} runners went quiet together — "
                      f"correlated outage, not individual hangs; acting on nobody")
        return check

    check.stalled = {r.id for r in quiet}
    # Only a runner that WAS minting and stopped may be cancelled. A runner that
    # has never been seen is far more likely to be a label-contract mismatch than
    # a hang — three such runs exist in the recorded history, all healthy, from
    # before the per-run label was introduced — so it is downgraded but never
    # killed.
    check.cancellable = [r.id for r in quiet
                         if check.verdict[r.id] == "stalled" and r.ours]
    check.cancellable.sort(key=lambda i: -check.silence[i])
    if quiet:
        check.note = (f"{len(quiet)} not minting: " +
                      ", ".join(f"{r.id}(f{r.number},{check.silence[r.id]/60:.0f}m,"
                                f"{check.verdict[r.id]})" for r in quiet))
    return check


def age_seconds(created_at: str | None, now_epoch: float) -> float:
    """Seconds since a run was created. Unparseable timestamps read as brand new,
    which is the conservative direction: a run of unknown age is never retired."""
    if not created_at:
        return 0.0
    try:
        stamp = time.strptime(created_at, "%Y-%m-%dT%H:%M:%SZ")
    except (TypeError, ValueError):
        return 0.0
    return max(0.0, now_epoch - calendar.timegm(stamp))


def classify(raw_runs: list[dict], tunnel: str, now_epoch: float) -> list[Run]:
    """Alive producer runs on this tunnel, newest first."""
    out: list[Run] = []
    for raw in raw_runs:
        if raw.get("status") not in ACTIVE_STATUSES:
            continue
        match = TITLE_RE.match(raw.get("display_title") or "")
        if not match:
            # An unrecognisable title is still a live producer run consuming
            # concurrency, so it counts — it just cannot be attributed or
            # cancelled. Reported separately by the caller.
            out.append(Run(int(raw["id"]), raw["status"],
                           raw.get("display_title") or "?",
                           age_seconds(raw.get("created_at"), now_epoch),
                           "unknown", 1, 0, "unknown",
                           number=raw.get("run_number")))
            continue
        if match.group("tunnel") != tunnel:
            continue
        out.append(Run(
            id=int(raw["id"]),
            status=raw["status"],
            title=raw.get("display_title") or "",
            age=age_seconds(raw.get("created_at"), now_epoch),
            tunnel=match.group("tunnel"),
            producers=int(match.group("producers")),
            duration=int(match.group("duration")),
            via=match.group("via"),
            number=raw.get("run_number"),
        ))
    return sorted(out, key=lambda r: r.age)


# Floor of the stagger band. A producer spends roughly five minutes installing
# Camoufox, bringing up WARP and building roomverify before it mints anything.
# At three hours that setup is ~3% of the run; below it the share climbs fast
# and the fleet starts paying for churn instead of tokens. So the stagger may
# shorten a life down to here and no further — it is a scheduling knob, not a
# lifetime knob, and the configured duration always remains the ceiling.
MIN_STAGGER_SECONDS = 10800


def stagger_lifetimes(deaths: list[float], count: int,
                      duration_seconds: int) -> list[int]:
    """Lifetimes for `count` replacements, spaced away from `deaths`.

    `deaths` is when each already-live runner is predicted to stop, in seconds
    from now. Each chosen lifetime is the midpoint of the widest gap between
    consecutive scheduled deaths inside the band, with the band edges acting as
    boundaries — and each choice JOINS the set before the next is made, so a
    six-runner refill spaces against itself and not merely against the fleet.

    Why this exists: the supervisor used to hand every replacement the same
    `--duration-seconds`, so each refill quietly re-synchronised the fleet it
    was refilling. Twelve runners in two cohorts once ended within the same
    hour, every one `success`, leaving the account at zero. A cohort death also
    demands a burst of hosted runners at a single instant, which on 2026-08-06
    collided with GitHub refusing to allocate them at all.
    """
    if count <= 0:
        return []
    ceiling = float(duration_seconds)
    floor = float(MIN_STAGGER_SECONDS)
    if ceiling <= floor:
        # No room to stagger. The operator's knob wins outright rather than the
        # code inventing a band on the wrong side of the setup-cost argument.
        return [duration_seconds] * count

    scheduled = sorted(d for d in deaths if floor < d < ceiling)
    chosen: list[int] = []
    for _ in range(count):
        points = [floor, *scheduled, ceiling]
        widest, best = -1.0, ceiling
        for low, high in zip(points, points[1:]):
            if high - low > widest:
                widest, best = high - low, (low + high) / 2.0
        chosen.append(int(best))
        scheduled = sorted([*scheduled, best])
    return chosen


def plan_cycle(runs: list[Run], *, target: int, hard_cap: int,
               overlap_seconds: float, max_dispatch: int,
               max_cancel: int, stalled: set[int] = frozenset(),
               duration_seconds: int = 0) -> Plan:
    """Decide this cycle's dispatches and cancellations.

    A run counts toward the target until it is within `overlap_seconds` of its
    own end. Age is measured from `created_at`, but a producer spends several
    minutes installing Camoufox before it mints, so `created_at + duration`
    predicts the end EARLIER than it really happens. That error is deliberately
    in the safe direction: the successor is dispatched a little early and the two
    overlap, rather than a little late leaving a hole.

    The window is clamped to a third of the run's own duration. Without that,
    an `overlap >= duration` misconfiguration makes EVERY run permanently
    "retiring" — none ever counts toward the target, so the supervisor dispatches
    replacements every single cycle until it hits the hard cap, and the fleet
    churns instead of producing. That exact pair (duration 900, overlap 1500)
    was live in the repo variables for a while, so the guard belongs in the code
    rather than in a comment telling operators not to do it.
    """
    plan = Plan()
    for run in runs:
        remaining = run.duration - run.age if run.duration else float("inf")
        window = min(overlap_seconds, run.duration / 3.0) if run.duration else overlap_seconds
        if run.id in stalled:
            # Holds a slot, produces nothing. Counted in `alive` below (it is
            # really there) but never in `held`, so the fleet is refilled around
            # it instead of pretending it is working.
            plan.stalled.append(run)
        elif remaining <= window:
            plan.retiring.append(run)
        else:
            plan.productive.append(run)

    held = sum(r.slots for r in plan.productive)
    alive = sum(r.slots for r in runs)

    if held < target:
        want = target - held
        room = max(0, hard_cap - alive)
        plan.dispatch = min(want, max_dispatch, room)
        plan.blocked_by_cap = room < want and room < max_dispatch
        if duration_seconds > 0:
            plan.lifetimes = stagger_lifetimes(
                [r.duration - r.age for r in runs if r.duration],
                plan.dispatch, duration_seconds)
        if plan.dispatch < want:
            plan.reason = (f"want {want} more, dispatching {plan.dispatch} "
                           f"(per-cycle cap {max_dispatch}, hard cap {hard_cap} "
                           f"with {alive} alive)")
    elif held > target and max_cancel > 0:
        # Shrink newest-first: the youngest run has produced the least, and
        # cancelling it costs the least supply. Only ever our own runs.
        excess = held - target
        for run in sorted((r for r in plan.productive if r.ours),
                          key=lambda r: r.age):
            if excess <= 0 or len(plan.cancel) >= max_cancel:
                break
            plan.cancel.append(run)
            excess -= run.slots
        plan.reason = f"over target by {held - target}, cancelling {len(plan.cancel)}"
    return plan


class GitHubAPI:
    # ⚠️ 2026-08-06, THE FOUR-FLEET COLLAPSE. api.github.com answers a
    # perfectly valid workflow-dispatch with `HTTP 500 {"message":"Failed to
    # run workflow dispatch"}` often enough to matter, and the identical call
    # succeeds seconds later. Before this retry existed, one such 500 raised
    # straight out of main() on dispatch 3 of 6 — so dispatches 4-6 and the
    # whole cancel phase never happened, the tick exited 1, and the NEXT tick
    # did exactly the same thing. Four fleets sat at 2/12, 2/12, 0/10 and 0/10
    # for hours while every supervisor tick "worked" up to the same hiccup.
    #
    # A dispatch is retried even though it is a POST: the observed 500 creates
    # NO run (verified against the run list — the failing tick produced exactly
    # the two runs it logged), and if a retry ever did duplicate one, `hard_cap`
    # bounds the damage to a single extra producer. An empty fleet is the far
    # more expensive error.
    RETRY_STATUSES = (429, 500, 502, 503, 504)
    RETRY_BACKOFF_SECONDS = (2.0, 5.0, 12.0)

    def __init__(self, token: str) -> None:
        self.token = token

    def _send(self, method: str, path: str, body: dict | None):
        """One attempt. Split out so the retry policy above is testable without
        a network and without monkeypatching urllib."""
        data = json.dumps(body).encode("utf-8") if body is not None else None
        request = urllib.request.Request(
            f"https://api.github.com/{path}", data=data, method=method,
            headers={
                "Accept": "application/vnd.github+json",
                "Authorization": f"Bearer {self.token}",
                "X-GitHub-Api-Version": "2022-11-28",
                "User-Agent": "camoufox-fleet-master/1",
            },
        )
        try:
            with urllib.request.urlopen(request, timeout=30) as response:
                payload = response.read()
        except urllib.error.HTTPError as error:
            detail = error.read(500).decode("utf-8", errors="replace")
            raise RuntimeError(f"GitHub API HTTP {error.code}: {detail}") from error
        except urllib.error.URLError as error:
            # No HTTP status at all — DNS, TLS or a timeout. Transient by
            # nature, and indistinguishable from a 503 for our purposes.
            raise RuntimeError(f"GitHub API unreachable: {error.reason}") from error
        return json.loads(payload) if payload else {}

    def request(self, method: str, path: str, body: dict | None = None):
        last: RuntimeError | None = None
        for delay in (*self.RETRY_BACKOFF_SECONDS, None):
            try:
                return self._send(method, path, body)
            except RuntimeError as error:
                if not self._is_transient(str(error)) or delay is None:
                    raise
                last = error
                print(f"  api: {str(error)[:120]} — retrying in {delay:.0f}s",
                      flush=True)
                time.sleep(delay)
        raise last                                          # unreachable

    @classmethod
    def _is_transient(cls, message: str) -> bool:
        """A permanent answer (401 on an expired PAT, 404 on a wrong repo) must
        raise on the first attempt: retrying it burns the tick's budget and
        makes the loudest possible failure look like a slow one."""
        if "unreachable" in message:
            return True
        return any(f"HTTP {code}:" in message for code in cls.RETRY_STATUSES)

    def list_runs(self, repo: str, workflow: str) -> list[dict]:
        workflow_id = urllib.parse.quote(workflow, safe="")
        data = self.request(
            "GET", f"repos/{repo}/actions/workflows/{workflow_id}/runs?per_page=100")
        return data.get("workflow_runs", [])

    def live_producer_jobs(self, repo: str, run_id: int) -> int | None:
        """Producer jobs still queued/running in a run, or None if unknowable.

        None (rather than a number) on failure is deliberate: it makes slots
        fall back to the title count, so an API hiccup can never read as "this
        run died", which would dispatch a duplicate fleet.

        Zero is also reported as None, which looks wrong but is the same
        argument. Only ACTIVE runs are ever queried, and a matrix run's producer
        jobs do not exist until its short `plan` job has finished — so a
        freshly dispatched "x12" legitimately has zero producer jobs for its
        first minutes. Honouring that zero would tell the supervisor the run
        contributes nothing and provoke a duplicate fleet on top of one that is
        about to materialise. A genuinely drained run leaves ACTIVE_STATUSES and
        is filtered out by classify() before it ever reaches here, so the only
        cost of this choice is briefly overcounting a run in the seconds between
        its last job ending and the run being marked complete.
        """
        try:
            data = self.request(
                "GET", f"repos/{repo}/actions/runs/{run_id}/jobs?per_page=100")
        except RuntimeError:
            return None
        jobs = data.get("jobs")
        if jobs is None:
            return None
        # The short `plan` job holds a runner but produces no tokens, so it must
        # not count toward the fleet it exists to launch.
        live = sum(1 for job in jobs
                   if job.get("name") != "plan"
                   and job.get("status") in ACTIVE_STATUSES)
        return live or None

    def produce_started_at(self, repo: str, run_id: int,
                           now_epoch: float) -> float | None:
        """Epoch at which this run's `Produce` step began, or None.

        This is the liveness clock's anchor and it is deliberately a STEP time,
        not the run's `created_at` and not the job's `started_at`. `created_at`
        is when the run was queued — a run has sat queued for 37 minutes on this
        account — and `job.started_at` is stamped at QUEUE time too, a trap this
        project has already recorded. The step, by contrast, starts the instant
        the generator is launched, after Camoufox and WARP are up. Measured on
        run 30700078405: Produce started 12:42:39Z and its first token reached
        the relay at 12:43:03Z, 24 s later.

        None means "no opinion" — the caller treats that as `starting` and never
        as stalled, so an API failure cannot manufacture a hang.
        """
        try:
            data = self.request(
                "GET", f"repos/{repo}/actions/runs/{run_id}/jobs?per_page=100")
        except RuntimeError:
            return None
        best: float | None = None
        for job in data.get("jobs") or []:
            for step in job.get("steps") or []:
                name = (step.get("name") or "").strip().lower()
                if not name.startswith("produce") or not step.get("started_at"):
                    continue
                stamp = now_epoch - age_seconds(step.get("started_at"), now_epoch)
                if best is None or stamp < best:
                    best = stamp
        return best

    def dispatch(self, repo: str, workflow: str, branch: str, inputs: dict) -> None:
        workflow_id = urllib.parse.quote(workflow, safe="")
        self.request("POST", f"repos/{repo}/actions/workflows/{workflow_id}/dispatches",
                     {"ref": branch, "inputs": inputs})

    def cancel(self, repo: str, run_id: int) -> str:
        """Cancel a run; benign terminal states are outcomes, not errors.

        A 409 means the run is already terminating — the documented response is
        to let it drain (~45 s), not to retry. One already-finished run must
        never abort the cycle, because that would also skip the dispatches.
        """
        try:
            self.request("POST", f"repos/{repo}/actions/runs/{run_id}/cancel")
        except RuntimeError as error:
            message = str(error)
            if "HTTP 409" in message:
                return "already-terminating"
            if "HTTP 404" in message:
                return "gone"
            return f"failed: {message[:120]}"
        return "cancel-requested"


def fetch_exits(url: str, auth: str) -> dict:
    request = urllib.request.Request(url, headers={
        "X-Auth": auth, "User-Agent": "camoufox-fleet-master/1"})
    with urllib.request.urlopen(request, timeout=25) as response:
        return json.loads(response.read())


def load_state(path: str) -> dict:
    """The liveness ledger, or an empty one.

    A missing, truncated or unreadable file is not an error and must never be:
    every run then re-anchors its clock to `now`, so the worst case of losing the
    cache is that nothing can be judged for one full threshold. Failing loudly
    here would take the whole supervisor down over a cache miss.
    """
    if not path:
        return {}
    try:
        with open(path, encoding="utf-8") as handle:
            data = json.load(handle)
        return data if isinstance(data, dict) else {}
    except (OSError, ValueError):
        return {}


def save_state(path: str, state: dict) -> None:
    if not path:
        return
    try:
        directory = os.path.dirname(path)
        if directory:
            os.makedirs(directory, exist_ok=True)
        with open(path, "w", encoding="utf-8") as handle:
            json.dump(state, handle)
    except OSError as error:
        print(f"  could not persist liveness ledger to {path}: {error}", flush=True)


def truthy(raw: str) -> bool:
    """Only an explicit "true" enables the fleet.

    Unset, empty, "TRUE " with whitespace, "1", "yes", or a typo all read as
    DISABLED. A kill switch that depends on its input parsing correctly is not a
    kill switch, so the failure direction is always "stop".
    """
    return raw.strip().lower() == "true"


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--repo", required=True, help="OWNER/REPO")
    # NOT `gartic-camoufox-producers.yml` any more. That producer has no click
    # driver, so since Cloudflare made every Turnstile challenge interactive on
    # 2026-07-27 it has minted zero — greenly, invisibly, while still consuming a
    # runner slot the supervisor counts as productive. The default therefore
    # points at the producer that is measured to work; the supervisor workflow
    # overrides it from the CAMOUFOX_FLEET_WORKFLOW repo variable.
    parser.add_argument("--workflow", default="gartic-camoufox-fleet.yml")
    parser.add_argument("--branch", default="main")
    parser.add_argument("--tunnel", default="warp", choices=("warp", "vpngate"))
    parser.add_argument("--enabled", default="",
                        help='kill switch; must be exactly "true" to dispatch')
    parser.add_argument("--target", default="0",
                        help="producer jobs to keep alive (string so an unset "
                             "repo variable is a clean 0 rather than a crash)")
    parser.add_argument("--hard-cap", type=int, default=16,
                        help="never let total alive producer jobs exceed this, "
                             "whatever --target says")
    parser.add_argument("--runner-ceiling", type=int, default=20,
                        help="the account's MEASURED simultaneous concurrent-job "
                             "ceiling for producer-LENGTH jobs (~20 on a free "
                             "public repo; a probe of trivial jobs undercounts "
                             "it because they cycle through the runner-grant ramp "
                             "before they can stack). The supervisor competes "
                             "with its own producers for that pool, so the cap is "
                             "held below it: if producers fill every slot, this "
                             "job cannot get a runner, and a 24/7 loop whose "
                             "refill step is crowded out by its workers is not "
                             "24/7.")
    parser.add_argument("--reserve-slots", type=int, default=2,
                        help="runner slots kept free for this supervisor's own "
                             "reconcile job, each producer run's short plan job, "
                             "and successors dispatched during an overlap window")
    parser.add_argument("--duration-seconds", type=int, default=20800,
                        help="what each producer is dispatched with (GitHub caps "
                             "a job at 6 h; 20800 s = 5.78 h)")
    parser.add_argument("--overlap-seconds", type=float, default=900,
                        help="stop counting a run toward the target this long "
                             "before it ends, so its successor is up and minting "
                             "before it exits")
    parser.add_argument("--token-interval", default="0")
    parser.add_argument("--post-to-relay", default="true")
    parser.add_argument("--max-dispatch-per-cycle", type=int, default=4)
    parser.add_argument("--max-cancel-per-cycle", type=int, default=0,
                        help="0 disables shrinking; runs then simply age out")

    # ---- minting liveness (see assess_minting). Off unless a relay URL AND a
    # credential are both supplied, so the supervisor's existing behaviour is
    # exactly preserved wherever it is not configured.
    parser.add_argument("--relay-exits-url", default="",
                        help="GET .../stats/exits; empty disables the check")
    parser.add_argument("--relay-auth-env", default="RELAY_AUTH",
                        help="env var holding the relay's X-Auth secret")
    parser.add_argument("--state-file", default="",
                        help="JSON ledger of when each run was last seen "
                             "minting, carried between cycles by the Actions "
                             "cache. Absent = every run starts its clock now, "
                             "so nothing can be judged for one full threshold")
    parser.add_argument("--stall-seconds", type=float, default=2700,
                        help="silence that counts as stalled. Floor is the "
                             "PRODUCER's own recovery: --browser-lifetime 1800 "
                             "plus a restart's first-token latency (~150 s), "
                             "and a watchdog shorter than the work it watches "
                             "destroys the recovery it exists to wait for. The "
                             "extra 600 s is the anycast attribution slack "
                             "(occupancyGrace), because on a shared exit row "
                             "the measurement is an upper bound that carries it")
    parser.add_argument("--stall-startup-seconds", type=float, default=900,
                        help="grace after the Produce step starts before a run "
                             "can be judged (measured Produce->first token: 24 s)")
    parser.add_argument("--max-stall-cancel-per-cycle", type=int, default=0,
                        help="cancel at most this many stalled runs per cycle, "
                             "and only when hard_cap is what is blocking the "
                             "refill. 0 = never cancel, only stop counting them "
                             "as productive. Cancelling DESTROYS the run's "
                             "end-of-run summary (solve= / cf_errors), which is "
                             "the only post-hoc diagnostic a producer leaves")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    # ---- the kill switch, checked before anything else can have an effect ----
    if not truthy(args.enabled):
        print(f"FLEET DISABLED (enabled={args.enabled!r}) — dispatching nothing.",
              flush=True)
        print("To start it:  PATCH /repos/%s/actions/variables/CAMOUFOX_FLEET_ENABLED "
              '{"name":"CAMOUFOX_FLEET_ENABLED","value":"true"}' % args.repo, flush=True)
        return 0

    try:
        target = int(str(args.target).strip() or "0")
    except ValueError:
        print(f"target {args.target!r} is not a number — treating as 0", flush=True)
        target = 0
    if target < 0:
        target = 0

    # Runner headroom. This supervisor draws from the same hosted-runner pool as
    # the producers it starts, so the cap must leave slots free — measured the
    # hard way: right after a 56-job burst, a single-job supervisor run sat
    # queued for 37 minutes and never got a runner. Had that been a real refill
    # cycle with the fleet at full width, nothing would have replaced the
    # producers as they aged out.
    ceiling = max(1, args.runner_ceiling - args.reserve_slots)
    hard_cap = args.hard_cap
    if hard_cap > ceiling:
        print(f"hard cap {hard_cap} would leave fewer than {args.reserve_slots} "
              f"runner slots free of a {args.runner_ceiling}-job ceiling — "
              f"clamped to {ceiling}", flush=True)
        hard_cap = ceiling
    if target > hard_cap:
        print(f"target {target} exceeds hard cap {hard_cap} — clamped", flush=True)
        target = hard_cap

    token = os.environ.get("GH_TOKEN", "").strip()
    if not token:
        print("GH_TOKEN is empty", file=sys.stderr)
        return 1
    api = GitHubAPI(token)

    now = time.time()
    raw = api.list_runs(args.repo, args.workflow)
    runs = classify(raw, args.tunnel, now)
    # Replace each run's title-derived size with its live job count. Only worth
    # an API call for runs that claim more than one producer: a producers=1 run
    # (everything the supervisor itself dispatches) cannot disagree with itself.
    # Run is frozen, so this rebuilds rather than assigns.
    runs = [replace(run, live=api.live_producer_jobs(args.repo, run.id))
            if run.producers > 1 else run
            for run in runs]

    evidence = None
    relay_auth = os.environ.get(args.relay_auth_env, "").strip()
    if args.relay_exits_url and relay_auth:
        try:
            evidence = read_exits(fetch_exits(args.relay_exits_url, relay_auth))
            runs = [replace(run, produce_started=api.produce_started_at(
                args.repo, run.id, now)) for run in runs]
        except Exception as error:            # noqa: BLE001 — never fatal
            print(f"  minting liveness: /stats/exits unavailable ({error}); "
                  f"check skipped this cycle", flush=True)
            evidence = None
    ledger = load_state(args.state_file)
    check = assess_minting(
        runs, evidence, ledger, now,
        threshold=args.stall_seconds, startup=args.stall_startup_seconds)
    save_state(args.state_file, check.ledger)

    plan = plan_cycle(
        runs,
        target=target,
        hard_cap=hard_cap,
        overlap_seconds=args.overlap_seconds,
        max_dispatch=args.max_dispatch_per_cycle,
        max_cancel=args.max_cancel_per_cycle,
        stalled=check.stalled,
        duration_seconds=args.duration_seconds,
    )

    queued = sum(r.slots for r in runs if r.status != "in_progress")
    running = sum(r.slots for r in runs if r.status == "in_progress")
    foreign = [r for r in runs if not r.ours]
    print(
        f"tunnel={args.tunnel} target={target} hard_cap={hard_cap} "
        f"ceiling={args.runner_ceiling}(reserve {args.reserve_slots}) "
        f"alive={sum(r.slots for r in runs)} (running={running} queued={queued}) "
        f"productive={sum(r.slots for r in plan.productive)} "
        f"retiring={sum(r.slots for r in plan.retiring)} "
        f"stalled={sum(r.slots for r in plan.stalled)} "
        f"foreign={sum(r.slots for r in foreign)} "
        f"dispatch={plan.dispatch} cancel={len(plan.cancel)}",
        flush=True,
    )
    if plan.reason:
        print(f"  note: {plan.reason}", flush=True)
    if check.note:
        print(f"  minting: {check.note}", flush=True)
    for run in runs:
        if run in plan.stalled:
            mark = "STALLED"
        elif run in plan.retiring:
            mark = "retiring"
        else:
            mark = "productive"
        mint = check.verdict.get(run.id, "-")
        quiet = check.silence.get(run.id)
        quiet_s = f" quiet={quiet/60:.0f}m" if quiet is not None else ""
        print(f"  run {run.id} {run.status:<11} age={run.age/60:6.1f}m "
              f"x{run.producers} via={run.via} {mark} mint={mint}{quiet_s}",
              flush=True)
    if queued and running:
        print("  (queued producers are waiting on the account's job-concurrency "
              "ceiling; they count toward the target because they will start)",
              flush=True)

    # `plan.lifetimes` is one entry per dispatch. It falls back to the flat
    # configured duration only when the planner was given no duration at all,
    # which the supervisor never does.
    lifetimes = plan.lifetimes or [args.duration_seconds] * plan.dispatch
    dispatch_failures = 0
    for index, lifetime in enumerate(lifetimes):
        inputs = {
            "tunnel": args.tunnel,
            "producers": "1",
            "duration": str(lifetime),
            "token_interval": str(args.token_interval),
            "post_to_relay": str(args.post_to_relay),
            "dispatched_by": "supervisor",
        }
        if args.dry_run:
            print(f"dry-run dispatch {index + 1}/{plan.dispatch}: {inputs}", flush=True)
            continue
        # ⚠️ NEVER let one dispatch abort the rest. On 2026-08-06 a transient
        # HTTP 500 on dispatch 3 of 6 raised out of main(), so dispatches 4-6
        # and the cancel phase below never ran and the tick exited 1 — and the
        # next tick failed at the same point, holding four fleets at 2/12,
        # 2/12, 0/10 and 0/10 while relay minting fell 1198 -> 76 tok/min.
        # `GitHubAPI.cancel` had reasoned this way about its own errors since
        # it was written; dispatch simply never got the same treatment. The
        # tick still ends non-zero (below) so the failure stays visible.
        try:
            api.dispatch(args.repo, args.workflow, args.branch, inputs)
        except RuntimeError as error:
            dispatch_failures += 1
            print(f"dispatch {index + 1}/{plan.dispatch} FAILED "
                  f"({str(error)[:160]}) — continuing with the rest", flush=True)
            time.sleep(3)
            continue
        print(f"dispatched {index + 1}/{plan.dispatch} "
              f"({args.tunnel}, {lifetime}s)", flush=True)
        # A short stagger keeps the fleet's browser-startup and its WARP
        # registrations from landing in the same instant.
        time.sleep(3)

    for run in plan.cancel:
        if args.dry_run:
            print(f"dry-run cancel run={run.id}", flush=True)
            continue
        print(f"cancel run={run.id}: {api.cancel(args.repo, run.id)}", flush=True)
        time.sleep(1)

    # ---- the heavier remedy, deliberately last and deliberately narrow ----
    # A stalled run is normally left alone: it stops counting as productive, a
    # replacement is dispatched, and it clears itself at GitHub's 5-hour wall,
    # leaving its end-of-run summary intact. Cancelling is only worth its cost
    # when hard_cap is the thing standing between the fleet and its target — the
    # case where a hung runner suppresses its own replacement.
    if args.max_stall_cancel_per_cycle > 0 and check.cancellable and plan.blocked_by_cap:
        by_id = {r.id: r for r in runs}
        for run_id in check.cancellable[:args.max_stall_cancel_per_cycle]:
            run = by_id.get(run_id)
            if run is None or not run.ours:
                continue
            quiet = check.silence.get(run_id, 0) / 60
            if args.dry_run:
                print(f"dry-run cancel STALLED run={run_id} quiet={quiet:.0f}m",
                      flush=True)
                continue
            print(f"cancel STALLED run={run_id} (f{run.number}, quiet {quiet:.0f}m, "
                  f"blocking the refill): {api.cancel(args.repo, run_id)}", flush=True)
            time.sleep(1)
    elif check.cancellable and args.max_stall_cancel_per_cycle > 0:
        print(f"  {len(check.cancellable)} stalled run(s) left alone — the refill "
              f"is not blocked by hard_cap, so downgrading them is enough",
              flush=True)

    # Reported only after the whole cycle has run. A red tick is the signal an
    # operator needs, but it must never be the reason a refill was skipped.
    if dispatch_failures:
        print(f"{dispatch_failures}/{plan.dispatch} dispatch(es) failed after "
              f"retries; the rest of the cycle completed", flush=True)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
