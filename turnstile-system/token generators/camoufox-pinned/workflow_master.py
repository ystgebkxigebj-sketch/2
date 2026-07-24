"""Keep one named GitHub Actions producer run alive per configured slot."""

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
from dataclasses import dataclass
from pathlib import Path


ACTIVE_STATUSES = {"queued", "in_progress", "waiting", "pending", "requested"}
SLOT_RE = re.compile(r"^Camoufox slot (\d+)$")
SHA_RE = re.compile(r"^[0-9a-f]{40}$")

# Paths whose commits actually change what a producer executes. Staleness is
# keyed on these rather than on the raw branch head so that unrelated commits
# (README, docs, other workflows) do not churn the whole fleet.
PRODUCER_PATHS = (
    "turnstile-system/token generators/camoufox-pinned",
    ".github/workflows/gartic-camoufox-pinned.yml",
)


@dataclass(frozen=True)
class RefillPlan:
    active_slots: frozenset[int]
    missing_slots: tuple[int, ...]
    unnamed_active: int


@dataclass(frozen=True)
class RetirePlan:
    stale: tuple[int, ...]
    stale_total: int


def _age_seconds(created_at: str | None, now_epoch: float) -> float:
    """Seconds since a run was created; 0.0 when the timestamp is unusable."""
    if not created_at:
        return 0.0
    try:
        stamp = time.strptime(created_at, "%Y-%m-%dT%H:%M:%SZ")
    except (TypeError, ValueError):
        return 0.0
    return now_epoch - calendar.timegm(stamp)


def plan_retire(
    runs: list[dict],
    stale_shas: frozenset[str],
    *,
    max_in_progress: int,
    max_queued: int,
    min_age_seconds: float,
    now_epoch: float,
) -> RetirePlan:
    """Pick active producer runs that are executing outdated code.

    A run's `head_sha` is pinned when it is dispatched and `actions/checkout`
    checks out exactly that commit, so head_sha is an exact "is this running old
    code?" test. `stale_shas` is resolved by the caller (see
    `GitHubAPI.stale_shas`) rather than compared to the branch head directly, so
    an unrelated docs commit does not mark the whole fleet stale.

    Caps are split by status because cancelling costs something very different
    in each case, and because `if: always()` behaves asymmetrically:

      * `queued`/`pending`/`waiting` — no runner assigned, so the job's steps
        (including its successor-dispatch) never execute. Cancelling costs zero
        supply, but `plan_refill` must provide the replacement. These still MUST
        be retired: their head_sha is already pinned to the old commit, so they
        would check out stale code whenever they eventually start.
      * `in_progress` — cancelling removes live production, but the workflow's
        successor step is `if: always()`, which GitHub still runs on
        cancellation, so the dying run dispatches its own replacement against
        the branch head (i.e. the fixed commit). Capped tightly: with only a
        handful of concurrent jobs, cancelling several at once would drop token
        supply to zero for the minutes a replacement needs to install Camoufox.

    Safety rules: nothing is cancelled unless it is one of ours (`display_title`
    matches `SLOT_RE`) and was dispatched by us; runs younger than
    `min_age_seconds` are skipped as a circuit breaker against a
    cancel/dispatch/cancel loop if the staleness check ever misfires.
    """
    if not stale_shas:
        return RetirePlan((), 0)

    eligible: list[dict] = []
    for run in runs:
        if run.get("status") not in ACTIVE_STATUSES:
            continue
        if run.get("head_sha") not in stale_shas:
            continue
        # Only ever touch our own slot runs — never a human-triggered or legacy run.
        if not SLOT_RE.match(run.get("display_title") or ""):
            continue
        if run.get("event") != "workflow_dispatch":
            continue
        if _age_seconds(run.get("created_at"), now_epoch) < min_age_seconds:
            continue
        eligible.append(run)

    def oldest_first(bucket: list[dict]) -> list[dict]:
        return sorted(bucket, key=lambda run: run.get("created_at") or "")

    running = oldest_first([r for r in eligible if r.get("status") == "in_progress"])
    pending = oldest_first([r for r in eligible if r.get("status") != "in_progress"])

    picked = pending[:max(0, max_queued)] + running[:max(0, max_in_progress)]
    return RetirePlan(tuple(int(run["id"]) for run in picked), len(eligible))


def plan_refill(runs: list[dict], target: int, max_dispatch: int = 0) -> RefillPlan:
    """Pick which slots to start.

    `max_dispatch` caps how many are started in ONE cycle. Ramping the fleet one
    slot at a time matters because every producer holds a proxy open for its
    whole life and burns metered Webshare bandwidth (~0.75 MB per token): a cold
    start of 20 at once commits the full spend before anyone can see whether the
    first one is even healthy. 0 means unlimited (the historical behaviour).
    """
    active = [run for run in runs if run.get("status") in ACTIVE_STATUSES]
    slots: set[int] = set()
    unnamed = 0
    for run in active:
        match = SLOT_RE.match(run.get("display_title") or "")
        if match and 0 <= int(match.group(1)) < target:
            slots.add(int(match.group(1)))
        else:
            unnamed += 1
    capacity = max(0, target - len(active))
    missing = [slot for slot in range(target) if slot not in slots][:capacity]
    if max_dispatch > 0:
        missing = missing[:max_dispatch]
    return RefillPlan(frozenset(slots), tuple(missing), unnamed)


class GitHubAPI:
    def __init__(self, token: str) -> None:
        self.token = token

    def request(self, method: str, path: str, body: dict | None = None) -> dict | list:
        data = json.dumps(body).encode("utf-8") if body is not None else None
        request = urllib.request.Request(
            f"https://api.github.com/{path}",
            data=data,
            method=method,
            headers={
                "Accept": "application/vnd.github+json",
                "Authorization": f"Bearer {self.token}",
                "X-GitHub-Api-Version": "2022-11-28",
                "User-Agent": "camoufox-workflow-master/1",
            },
        )
        try:
            with urllib.request.urlopen(request, timeout=30) as response:
                payload = response.read()
        except urllib.error.HTTPError as error:
            detail = error.read(500).decode("utf-8", errors="replace")
            raise RuntimeError(f"GitHub API HTTP {error.code}: {detail}") from error
        return json.loads(payload) if payload else {}

    def default_branch(self, repo: str) -> str:
        return self.request("GET", f"repos/{repo}")["default_branch"]

    def list_runs(self, repo: str, workflow: str) -> list[dict]:
        workflow_id = urllib.parse.quote(workflow, safe="")
        path = f"repos/{repo}/actions/workflows/{workflow_id}/runs?per_page=100"
        data = self.request("GET", path)
        return data.get("workflow_runs", [])

    def latest_producer_commit(self, repo: str, branch: str) -> str:
        """Newest commit touching producer code (generator dir or its workflow)."""
        best_sha, best_date = "", ""
        for path in PRODUCER_PATHS:
            query = urllib.parse.urlencode({"sha": branch, "path": path, "per_page": 1})
            data = self.request("GET", f"repos/{repo}/commits?{query}")
            if isinstance(data, list) and data:
                commit = data[0]
                date = ((commit.get("commit") or {}).get("committer") or {}).get("date", "")
                if date > best_date:
                    best_date, best_sha = date, commit.get("sha", "")
        return best_sha

    def compare_status(self, repo: str, base: str, head: str) -> str:
        base_q = urllib.parse.quote(base, safe="")
        head_q = urllib.parse.quote(head, safe="")
        data = self.request("GET", f"repos/{repo}/compare/{base_q}...{head_q}")
        return data.get("status", "") if isinstance(data, dict) else ""

    def stale_shas(self, repo: str, branch: str, runs: list[dict]) -> frozenset[str]:
        """Which active runs' commits predate the newest producer-code commit.

        `compare/{run_sha}...{code_sha}` reports `code_sha` relative to the run:
          ahead     -> code_sha descends from the run's commit => run is STALE
          identical -> run is exactly at the code commit       => fresh
          behind    -> run already contains the code commit    => fresh
          diverged  -> force-push/rebase; unknown              => never cancel
        Any lookup failure yields no stale shas, so the caller cancels nothing.
        """
        code_sha = self.latest_producer_commit(repo, branch)
        if not SHA_RE.match(code_sha or ""):
            return frozenset()
        candidates = {
            run.get("head_sha") for run in runs
            if run.get("status") in ACTIVE_STATUSES
        }
        stale: set[str] = set()
        for sha in candidates:
            if not sha or not SHA_RE.match(sha) or sha == code_sha:
                continue
            try:
                if self.compare_status(repo, sha, code_sha) == "ahead":
                    stale.add(sha)
            except RuntimeError as error:
                print(f"compare {sha[:7]} failed, treating as fresh: {error}",
                      file=sys.stderr, flush=True)
        return frozenset(stale)

    def cancel(self, repo: str, run_id: int) -> str:
        """Cancel a run. Benign terminal states are reported, never raised.

        A single already-finished run must not abort the whole supervisor cycle
        (which would also skip the refill dispatches), so 409/404 are outcomes
        rather than errors.
        """
        try:
            self.request("POST", f"repos/{repo}/actions/runs/{run_id}/cancel")
        except RuntimeError as error:
            message = str(error)
            if "HTTP 409" in message:
                return "already-final"
            if "HTTP 404" in message:
                return "gone"
            return f"failed: {message[:120]}"
        return "cancel-requested"

    def dispatch(
        self, repo: str, workflow: str, branch: str, slot: int, runtime: int
    ) -> None:
        workflow_id = urllib.parse.quote(workflow, safe="")
        path = f"repos/{repo}/actions/workflows/{workflow_id}/dispatches"
        self.request(
            "POST",
            path,
            {
                "ref": branch,
                "inputs": {"slot": str(slot), "runtime_minutes": str(runtime)},
            },
        )

def acquire_lock(path: Path) -> int:
    path.parent.mkdir(parents=True, exist_ok=True)
    try:
        descriptor = os.open(path, os.O_CREAT | os.O_EXCL | os.O_WRONLY)
    except FileExistsError as error:
        raise RuntimeError(f"another master appears active: {path}") from error
    os.write(descriptor, str(os.getpid()).encode("ascii"))
    return descriptor


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--repo", required=True, help="OWNER/REPO")
    parser.add_argument("--workflow", default="gartic-camoufox-pinned.yml")
    parser.add_argument("--branch", help="dispatch ref; default branch when omitted")
    parser.add_argument("--target", type=int, default=20)
    parser.add_argument("--runtime-minutes", type=int, default=330)
    parser.add_argument("--poll-seconds", type=int, default=60)
    parser.add_argument(
        "--retire-stale",
        action="store_true",
        help="cancel active runs whose commit predates the newest producer-code commit",
    )
    parser.add_argument(
        "--retire-max-running",
        type=int,
        default=1,
        help="max in_progress runs to cancel per cycle (each is live token supply)",
    )
    parser.add_argument(
        "--retire-max-queued",
        type=int,
        default=20,
        help="max queued/pending runs to cancel per cycle (these produce nothing)",
    )
    parser.add_argument(
        "--retire-min-age-seconds",
        type=int,
        default=600,
        help="never cancel a run younger than this (circuit breaker)",
    )
    parser.add_argument(
        "--max-dispatch-per-cycle",
        type=int,
        default=1,
        help="start at most this many slots per cycle (0 = unlimited). Default 1 "
             "so the fleet ramps one slot at a time instead of committing the "
             "whole metered proxy budget on a cold start",
    )
    parser.add_argument("--once", action="store_true")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--lock-file", type=Path, default=Path(".camoufox-master.lock"))
    args = parser.parse_args()
    if not 1 <= args.target <= 20:
        parser.error("--target must be between 1 and 20")
    if not 1 <= args.runtime_minutes <= 330:
        parser.error("--runtime-minutes must be between 1 and 330")
    if args.poll_seconds < 15:
        parser.error("--poll-seconds must be at least 15")

    token = os.environ.get("GH_TOKEN", "").strip()
    if not token:
        parser.error("GH_TOKEN must contain a token with repository Actions write access")
    api = GitHubAPI(token)
    repo = args.repo
    branch = args.branch or api.default_branch(repo)
    lock_descriptor = acquire_lock(args.lock_file)
    print(
        f"master repo={repo} workflow={args.workflow} branch={branch} "
        f"target={args.target} dry_run={args.dry_run}",
        flush=True,
    )
    try:
        while True:
            try:
                runs = api.list_runs(repo, args.workflow)

                # Refill from the pre-cancel snapshot FIRST, so a slot is never
                # dispatched in the same cycle it is cancelled — that would race
                # the dying run's own always() successor dispatch.
                plan = plan_refill(runs, args.target, args.max_dispatch_per_cycle)
                print(
                    f"active_slots={len(plan.active_slots)} unnamed_active={plan.unnamed_active} "
                    f"dispatching={list(plan.missing_slots)} "
                    f"(cap {args.max_dispatch_per_cycle or 'unlimited'}/cycle)",
                    flush=True,
                )
                for slot in plan.missing_slots:
                    if args.dry_run:
                        print(f"dry-run dispatch slot={slot}", flush=True)
                    else:
                        api.dispatch(repo, args.workflow, branch, slot, args.runtime_minutes)
                        print(f"dispatched slot={slot}", flush=True)
                        time.sleep(2)

                if args.retire_stale:
                    stale = api.stale_shas(repo, branch, runs)
                    retire = plan_retire(
                        runs,
                        stale,
                        max_in_progress=args.retire_max_running,
                        max_queued=args.retire_max_queued,
                        min_age_seconds=args.retire_min_age_seconds,
                        now_epoch=time.time(),
                    )
                    print(
                        f"stale_commits={len(stale)} eligible={retire.stale_total} "
                        f"retiring={len(retire.stale)}",
                        flush=True,
                    )
                    for run_id in retire.stale:
                        if args.dry_run:
                            print(f"dry-run cancel run={run_id}", flush=True)
                            continue
                        print(f"cancel run={run_id}: {api.cancel(repo, run_id)}", flush=True)
                        time.sleep(1)
            except Exception as error:
                print(f"master cycle failed: {error}", file=sys.stderr, flush=True)
                if args.once:
                    return 1
            if args.once:
                return 0
            time.sleep(args.poll_seconds)
    except KeyboardInterrupt:
        print("master stopped", flush=True)
        return 0
    finally:
        os.close(lock_descriptor)
        args.lock_file.unlink(missing_ok=True)


if __name__ == "__main__":
    raise SystemExit(main())
