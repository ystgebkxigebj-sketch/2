"""Keep one named GitHub Actions producer run alive per configured slot."""

from __future__ import annotations

import argparse
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


@dataclass(frozen=True)
class RefillPlan:
    active_slots: frozenset[int]
    missing_slots: tuple[int, ...]
    unnamed_active: int


@dataclass(frozen=True)
class RetirePlan:
    stale: tuple[int, ...]
    stale_total: int


def plan_retire(runs: list[dict], head_sha: str, max_cancel: int) -> RetirePlan:
    """Pick active runs that are executing outdated code.

    A run's `head_sha` is fixed when it is dispatched, and `actions/checkout`
    checks out exactly that commit — so any active run whose head_sha differs
    from the branch head is still running old generator code and must be
    retired. (This is how the 2026-07-23 `action:'join'` fix was rolled out:
    producers minting actionless tokens had to be cycled, not waited out for
    their full 330-minute runtime.)

    Only `max_cancel` runs are returned per cycle so token supply degrades
    gracefully instead of dropping to zero — a replacement needs several
    minutes to install Camoufox before it produces anything.

    Cancelling is self-healing: the producer workflow's successor-dispatch step
    is `if: always()`, which GitHub still runs on cancellation, so each retired
    run dispatches its own replacement on the current default branch.
    """
    if not head_sha:
        return RetirePlan((), 0)
    stale = [
        run for run in runs
        if run.get("status") in ACTIVE_STATUSES
        and run.get("head_sha")
        and run.get("head_sha") != head_sha
    ]
    # Retire longest-running first: they have the most remaining runtime to waste.
    stale.sort(key=lambda run: run.get("created_at") or "")
    ids = tuple(int(run["id"]) for run in stale[:max(0, max_cancel)])
    return RetirePlan(ids, len(stale))


def plan_refill(runs: list[dict], target: int) -> RefillPlan:
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
    return RefillPlan(frozenset(slots), tuple(missing), unnamed)


class GitHubAPI:
    def __init__(self, token: str) -> None:
        self.token = token

    def request(self, method: str, path: str, body: dict | None = None) -> dict:
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

    def branch_head_sha(self, repo: str, branch: str) -> str:
        path = f"repos/{repo}/commits/{urllib.parse.quote(branch, safe='')}"
        return self.request("GET", path).get("sha", "")

    def cancel(self, repo: str, run_id: int) -> bool:
        """Cancel a run. Returns False if it already finished (HTTP 409)."""
        try:
            self.request("POST", f"repos/{repo}/actions/runs/{run_id}/cancel")
        except RuntimeError as error:
            if "HTTP 409" in str(error):
                return False
            raise
        return True

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
        help="cancel active runs whose head_sha != branch head (they run outdated code)",
    )
    parser.add_argument(
        "--retire-max",
        type=int,
        default=4,
        help="max stale runs to cancel per cycle, so supply degrades gradually",
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
                if args.retire_stale:
                    head_sha = api.branch_head_sha(repo, branch)
                    retire = plan_retire(runs, head_sha, args.retire_max)
                    print(
                        f"head_sha={head_sha[:7] or '?'} stale_active={retire.stale_total} "
                        f"retiring={len(retire.stale)}",
                        flush=True,
                    )
                    for run_id in retire.stale:
                        if args.dry_run:
                            print(f"dry-run cancel run={run_id}", flush=True)
                            continue
                        if api.cancel(repo, run_id):
                            print(f"cancelled stale run={run_id}", flush=True)
                        else:
                            print(f"stale run={run_id} already finished", flush=True)
                        time.sleep(1)
                    if retire.stale:
                        # Cancelled runs stay 'active' briefly, which would starve
                        # plan_refill's capacity math. Their own always() successor
                        # step re-dispatches them, so refill can wait a cycle.
                        runs = api.list_runs(repo, args.workflow)
                plan = plan_refill(runs, args.target)
                print(
                    f"active_slots={len(plan.active_slots)} unnamed_active={plan.unnamed_active} "
                    f"missing={list(plan.missing_slots)}",
                    flush=True,
                )
                for slot in plan.missing_slots:
                    if args.dry_run:
                        print(f"dry-run dispatch slot={slot}", flush=True)
                    else:
                        api.dispatch(repo, args.workflow, branch, slot, args.runtime_minutes)
                        print(f"dispatched slot={slot}", flush=True)
                        time.sleep(2)
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
