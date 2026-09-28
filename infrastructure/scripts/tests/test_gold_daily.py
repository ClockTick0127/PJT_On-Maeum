import os
from pathlib import Path
import shutil
import subprocess
import tempfile
import unittest


SCRIPT = Path(__file__).resolve().parents[1] / "run-gold-daily.sh"
ROOT = Path(__file__).resolve().parents[3]


def executable(path: Path, body: str) -> Path:
    path.write_text("#!/usr/bin/env bash\n" + body, encoding="utf-8", newline="\n")
    path.chmod(0o755)
    return path


def bash_path(path: Path) -> str:
    if os.name != "nt":
        return str(path)
    resolved = path.resolve()
    drive = resolved.drive[0].lower()
    tail = resolved.as_posix()[3:]
    return f"/{drive}/{tail}"


def run_script(tmp_path: Path, *, docker_exit, flock_exit: int = 0, report_enabled=False):
    """``docker_exit`` is one code for every call, or one code per call in order."""

    if os.name == "nt":
        raise unittest.SkipTest("requires a POSIX host with flock")
    if shutil.which("bash") is None:
        raise unittest.SkipTest("bash is unavailable")
    root = tmp_path / "runtime"
    root.mkdir()
    runner = tmp_path / "run-gold-daily.sh"
    shutil.copyfile(SCRIPT, runner)
    runner.chmod(0o755)
    (root / "compose.yaml").write_text("services: {}\n", encoding="utf-8")
    (root / ".env").write_text("POSTGRES_PASSWORD=not-a-real-secret\n", encoding="utf-8")
    calls = tmp_path / "docker.calls"
    codes = [docker_exit] if isinstance(docker_exit, int) else list(docker_exit)
    fake_docker = executable(
        tmp_path / "docker",
        "\n".join([
            f'printf "%s\\n" "$*" >>"{bash_path(calls)}"',
            f'count=$(wc -l <"{bash_path(calls)}")',
            f'codes=({" ".join(str(code) for code in codes)})',
            'index=$((count - 1))',
            '(( index < ${#codes[@]} )) || index=$(( ${#codes[@]} - 1 ))',
            'exit "${codes[$index]}"',
            "",
        ]),
    )
    fake_flock = executable(tmp_path / "flock", f"exit {flock_exit}\n")
    fake_date = executable(
        tmp_path / "date",
        'case "$2" in "2 days ago") echo 2026-09-19;; *) echo 2026-09-20;; esac\n',
    )
    env = {
        **os.environ,
        "NILM_ROOT": bash_path(root),
        "GOLD_DAILY_LOCK_FILE": bash_path(tmp_path / "daily.lock"),
        "GOLD_DAILY_CATCHUP_DAYS": "2",
        "GOLD_DAILY_DOCKER_BIN": bash_path(fake_docker),
        "GOLD_DAILY_FLOCK_BIN": bash_path(fake_flock),
        "GOLD_DAILY_DATE_BIN": bash_path(fake_date),
        "HOUSEHOLD_REPORT_ENABLED": "true" if report_enabled else "false",
    }
    result = subprocess.run(
        ["bash", bash_path(runner)], env=env, text=True, encoding="utf-8",
        capture_output=True, check=False,
    )
    recorded = calls.read_text(encoding="utf-8").splitlines() if calls.exists() else []
    return result, recorded


class GoldDailyRunnerTest(unittest.TestCase):
    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self._tmp.cleanup)
        self.tmp_path = Path(self._tmp.name)

    def test_pending_outbox_is_an_accepted_handoff_and_all_catchup_dates_run(self):
        result, calls = run_script(self.tmp_path, docker_exit=12)

        self.assertEqual(result.returncode, 12)
        self.assertEqual(len(calls), 2)
        self.assertTrue(all("run --rm --no-deps gold-profile" in call for call in calls))
        self.assertTrue(all("--no-publish" in call for call in calls))
        self.assertIn("--as-of 2026-09-19", calls[0])
        self.assertIn("--as-of 2026-09-20", calls[1])

    def test_pipeline_failure_is_propagated_and_stops_catchup(self):
        result, calls = run_script(self.tmp_path, docker_exit=1)

        self.assertEqual(result.returncode, 1)
        self.assertEqual(len(calls), 1)

    def test_reports_follow_only_completed_gold_dates(self):
        result, calls = run_script(self.tmp_path, docker_exit=[10, 12, 0], report_enabled=True)
        self.assertEqual(result.returncode, 10)
        self.assertEqual(len(calls), 3)
        self.assertIn('household-report daily --as-of 2026-09-20', calls[2])

    def test_report_failure_is_propagated(self):
        result, calls = run_script(self.tmp_path, docker_exit=[0, 1], report_enabled=True)
        self.assertEqual(result.returncode, 1)
        self.assertEqual(len(calls), 2)

    def test_an_incomplete_older_date_does_not_starve_newer_dates(self):
        # The oldest date still waits for inputs (10); yesterday hands its outbox off (12).
        result, calls = run_script(self.tmp_path, docker_exit=[10, 12])

        self.assertEqual(len(calls), 2)
        self.assertIn("--as-of 2026-09-19", calls[0])
        self.assertIn("--as-of 2026-09-20", calls[1])
        # The incomplete date stays visible to systemd as a non-success exit.
        self.assertEqual(result.returncode, 10)

    def test_lock_conflict_skips_without_starting_a_container(self):
        result, calls = run_script(self.tmp_path, docker_exit=0, flock_exit=1)

        self.assertEqual(result.returncode, 11)
        self.assertEqual(calls, [])
        self.assertIn('"status":"SKIPPED"', result.stdout)

    def test_systemd_units_and_readme_use_the_deployed_runner_contract(self):
        service = (ROOT / "infrastructure/systemd/gold-profile-daily.service").read_text(
            encoding="utf-8"
        )
        timer = (ROOT / "infrastructure/systemd/gold-profile-daily.timer.in").read_text(
            encoding="utf-8"
        )
        readme = (ROOT / "batch/gold_profile_service/README.md").read_text(
            encoding="utf-8"
        )
        runner_env = (ROOT / "infrastructure/ec2-b/gold-daily.env.example").read_text(
            encoding="utf-8"
        )

        self.assertIn("ExecStart=/opt/nilm/bin/run-gold-daily.sh", service)
        self.assertIn("SuccessExitStatus=11 12", service)
        self.assertIn("Persistent=true", timer)
        self.assertIn("@@ON_CALENDAR@@", timer)
        self.assertIn("install-gold-daily-systemd.sh '*-*-* 06:30:00 Asia/Seoul'", readme)
        self.assertIn("gold-profile daily --as-of 2026-09-19", readme)
        self.assertIn("GOLD_DAILY_CATCHUP_DAYS=3", runner_env)
        self.assertIn("/opt/nilm/gold-daily.env.example", readme)


if __name__ == "__main__":
    unittest.main()
