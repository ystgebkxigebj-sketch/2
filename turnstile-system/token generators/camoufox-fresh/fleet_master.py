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
from dataclasses import dataclass, field


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

    @property
    def slots(self) -> int:
        """Producer jobs this run contributes (a matrix dispatch can hold many)."""
        return max(1, self.producers)


@dataclass
class Plan:
    productive: list[Run] = field(default_factory=list)
    retiring: list[Run] = field(default_factory=list)
    dispatch: int = 0
    cancel: list[Run] = field(default_factory=list)
    reason: str = ""


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
                           "unknown", 1, 0, "unknown"))
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
        ))
    return sorted(out, key=lambda r: r.age)


def plan_cycle(runs: list[Run], *, target: int, hard_cap: int,
               overlap_seconds: float, max_dispatch: int,
               max_cancel: int) -> Plan:
    """Decide this cycle's dispatches and cancellations.

    A run counts toward the target until it is within `overlap_seconds` of its
    own end. Age is measured from `created_at`, but a producer spends several
    minutes installing Camoufox before it mints, so `created_at + duration`
    predicts the end EARLIER than it really happens. That error is deliberately
    in the safe direction: the successor is dispatched a little early and the two
    overlap, rather than a little late leaving a hole.
    """
    plan = Plan()
    for run in runs:
        remaining = run.duration - run.age if run.duration else float("inf")
        (plan.retiring if remaining <= overlap_seconds else plan.productive).append(run)

    held = sum(r.slots for r in plan.productive)
    alive = sum(r.slots for r in runs)

    if held < target:
        want = target - held
        room = max(0, hard_cap - alive)
        plan.dispatch = min(want, max_dispatch, room)
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
    def __init__(self, token: str) -> None:
        self.token = token

    def request(self, method: str, path: str, body: dict | None = None):
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
        return json.loads(payload) if payload else {}

    def list_runs(self, repo: str, workflow: str) -> list[dict]:
        workflow_id = urllib.parse.quote(workflow, safe="")
        data = self.request(
            "GET", f"repos/{repo}/actions/workflows/{workflow_id}/runs?per_page=100")
        return data.get("workflow_runs", [])

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
    parser.add_argument("--workflow", default="gartic-camoufox-producers.yml")
    parser.add_argument("--branch", default="main")
    parser.add_argument("--tunnel", default="warp", choices=("warp", "vpngate"))
    parser.add_argument("--enabled", default="",
                        help='kill switch; must be exactly "true" to dispatch')
    parser.add_argument("--target", default="0",
                        help="producer jobs to keep alive (string so an unset "
                             "repo variable is a clean 0 rather than a crash)")
    parser.add_argument("--hard-cap", type=int, default=16,
                        help="never let total alive producer jobs exceed this, "
                             "whatever --target says. The account's concurrency "
                             "ceiling is shared with this supervisor's own job.")
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
    if target > args.hard_cap:
        print(f"target {target} exceeds hard cap {args.hard_cap} — clamped", flush=True)
        target = args.hard_cap

    token = os.environ.get("GH_TOKEN", "").strip()
    if not token:
        print("GH_TOKEN is empty", file=sys.stderr)
        return 1
    api = GitHubAPI(token)

    raw = api.list_runs(args.repo, args.workflow)
    runs = classify(raw, args.tunnel, time.time())
    plan = plan_cycle(
        runs,
        target=target,
        hard_cap=args.hard_cap,
        overlap_seconds=args.overlap_seconds,
        max_dispatch=args.max_dispatch_per_cycle,
        max_cancel=args.max_cancel_per_cycle,
    )

    queued = sum(r.slots for r in runs if r.status != "in_progress")
    running = sum(r.slots for r in runs if r.status == "in_progress")
    foreign = [r for r in runs if not r.ours]
    print(
        f"tunnel={args.tunnel} target={target} hard_cap={args.hard_cap} "
        f"alive={sum(r.slots for r in runs)} (running={running} queued={queued}) "
        f"productive={sum(r.slots for r in plan.productive)} "
        f"retiring={sum(r.slots for r in plan.retiring)} "
        f"foreign={sum(r.slots for r in foreign)} "
        f"dispatch={plan.dispatch} cancel={len(plan.cancel)}",
        flush=True,
    )
    if plan.reason:
        print(f"  note: {plan.reason}", flush=True)
    for run in runs:
        mark = "retiring" if run in plan.retiring else "productive"
        print(f"  run {run.id} {run.status:<11} age={run.age/60:6.1f}m "
              f"x{run.producers} via={run.via} {mark}", flush=True)
    if queued and running:
        print("  (queued producers are waiting on the account's job-concurrency "
              "ceiling; they count toward the target because they will start)",
              flush=True)

    for index in range(plan.dispatch):
        inputs = {
            "tunnel": args.tunnel,
            "producers": "1",
            "duration": str(args.duration_seconds),
            "token_interval": str(args.token_interval),
            "post_to_relay": str(args.post_to_relay),
            "dispatched_by": "supervisor",
        }
        if args.dry_run:
            print(f"dry-run dispatch {index + 1}/{plan.dispatch}: {inputs}", flush=True)
            continue
        api.dispatch(args.repo, args.workflow, args.branch, inputs)
        print(f"dispatched {index + 1}/{plan.dispatch} "
              f"({args.tunnel}, {args.duration_seconds}s)", flush=True)
        # A short stagger keeps the fleet's browser-startup and its WARP
        # registrations from landing in the same instant.
        time.sleep(3)

    for run in plan.cancel:
        if args.dry_run:
            print(f"dry-run cancel run={run.id}", flush=True)
            continue
        print(f"cancel run={run.id}: {api.cancel(args.repo, run.id)}", flush=True)
        time.sleep(1)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
