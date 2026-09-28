"""Run ``gold-profile daily`` for a date range and keep the JSON reports.

This is the same command the EC2 timer runs, executed oldest date first. The
exit-code contract (README of gold_profile_service): 0/12 success, 10 input not
ready, 11 another writer, 1 failure, 2 usage error.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, timedelta
import json
from pathlib import Path
import subprocess
import time


RETRYABLE_OK = {0, 10, 11, 12}


@dataclass(frozen=True)
class DateRun:
    day: date
    exit_code: int
    status: str | None
    seconds: float
    command: list[str]

    def as_dict(self) -> dict:
        return {"date": self.day.isoformat(), "exit_code": self.exit_code, "status": self.status,
                "seconds": round(self.seconds, 1), "command": self.command}


def daily_command(day: date, *, compose_file: str | None, service: str, extra_env: dict[str, str],
                  attempts: int, publish: bool, docker: str = "docker",
                  volumes: tuple[str, ...] = ()) -> list[str]:
    command = [docker, "compose"]
    if compose_file:
        command += ["-f", compose_file]
    command += ["run", "--rm", "--no-deps"]
    for volume in volumes:
        command += ["-v", volume]
    for key, value in extra_env.items():
        command += ["-e", f"{key}={value}"]
    command += [service, "gold-profile", "daily", "--as-of", day.isoformat(), "--attempts", str(attempts)]
    if not publish:
        command.append("--no-publish")
    return command


def last_json(text: str) -> dict | None:
    """The daily report may be one line or pretty-printed; take the last object."""

    lines = text.strip().splitlines()
    decoder = json.JSONDecoder()
    # Scan from the top so a pretty-printed report is returned as a whole rather
    # than as its last nested object; the candidate must consume the remainder.
    for index in range(len(lines)):
        if not lines[index].lstrip().startswith("{"):
            continue
        candidate = "\n".join(lines[index:]).strip()
        try:
            value, end = decoder.raw_decode(candidate)
        except ValueError:
            continue
        if isinstance(value, dict) and end == len(candidate):
            return value
    return None


def run_dates(start: date, end: date, *, log_path: Path, dry_run: bool, keep_going: bool,
              runner=subprocess.run, **command_options) -> list[DateRun]:
    results: list[DateRun] = []
    log_path.parent.mkdir(parents=True, exist_ok=True)
    day = start
    with log_path.open("a", encoding="utf-8") as log:
        while day <= end:
            command = daily_command(day, **command_options)
            if dry_run:
                print(" ".join(command))
                day += timedelta(days=1)
                continue
            started = time.perf_counter()
            completed = runner(command, capture_output=True, text=True, encoding="utf-8", errors="replace")
            report = last_json(completed.stdout or "")
            status = report.get("status") if isinstance(report, dict) else None
            run = DateRun(day, completed.returncode, status, time.perf_counter() - started, command)
            results.append(run)
            log.write(json.dumps({**run.as_dict(), "report": report,
                                  "stderr_tail": (completed.stderr or "")[-2000:]}, ensure_ascii=False) + "\n")
            log.flush()
            print(f"{day} exit={run.exit_code} status={status} {run.seconds:.0f}s")
            if run.exit_code not in RETRYABLE_OK and not keep_going:
                break
            day += timedelta(days=1)
    return results
