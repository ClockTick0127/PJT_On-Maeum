import contextlib
import copy
import io
import json
import os
from pathlib import Path
import sys
import subprocess
import tempfile
import unittest
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import release


def fixture():
    sha = "a" * 40
    repository = "docker.io/leejeongmin24/on-maum"
    data = dict(schema=1, repository=repository, git_sha=sha, release_id=sha + "-b4", images={})
    for index, service in enumerate(release.SERVICES, 1):
        digest = "sha256:" + f"{index:064x}"
        data["images"][service] = dict(tag=f'{repository}:{service}-{data["release_id"]}',
                                        digest=digest, reference=f"{repository}@{digest}")
    return data


class ReleaseTests(unittest.TestCase):
    def setUp(self):
        self.data = fixture()
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.root = Path(self.directory.name)
        self.manifest = self.root / "release.json"
        self.environment = patch.dict(os.environ, {
            "IMAGE_REPOSITORY": self.data["repository"], "IMAGE_TAG": self.data["git_sha"],
            "RELEASE_ID": self.data["release_id"],
        })
        self.environment.start()
        self.addCleanup(self.environment.stop)

    def test_rejects_partial_release(self):
        del self.data["images"]["frontend"]
        with self.assertRaises(ValueError):
            release.validate(self.data)

    def test_rejects_different_repository_and_mutable_reference(self):
        for reference in ("docker.io/other/public@sha256:" + "1" * 64,
                          self.data["repository"] + ":latest"):
            with self.subTest(reference=reference):
                data = copy.deepcopy(self.data)
                data["images"]["frontend"]["reference"] = reference
                with self.assertRaises(ValueError):
                    release.validate(data)

    def test_rejects_bad_repository_or_release_id(self):
        for repository in ("https://docker.io/u/r", "docker.io/u/r:latest", "docker.io/u/r/child"):
            with self.assertRaises(ValueError):
                release.identity(repository, self.data["git_sha"], self.data["release_id"])
        with self.assertRaises(ValueError):
            release.identity(self.data["repository"], self.data["git_sha"], "../../escape")

    def test_rejects_stale_manifest_from_other_build(self):
        release.write_json(self.manifest, self.data)
        with patch.dict(os.environ, {"RELEASE_ID": self.data["git_sha"] + "-b5"}):
            with self.assertRaises(ValueError):
                release.read_expected(self.manifest)

    def test_env_preserves_literal_secrets_and_replaces_old_image_keys(self):
        path = self.root / ".env"
        path.write_text("\ufeffPOSTGRES_PASSWORD='a$()#b'\nREGISTRY=old\nIMAGE_TAG=old\n"
                        "API_GATEWAY_IMAGE=old\nAPI_GATEWAY_IMAGE=duplicate\n", encoding="utf-8")
        release.update_env(path, self.data)
        text = path.read_text(encoding="utf-8")
        self.assertIn("POSTGRES_PASSWORD='a$()#b'", text)
        self.assertNotIn("REGISTRY=", text)
        self.assertEqual(text.count("API_GATEWAY_IMAGE="), 1)
        for entry in self.data["images"].values():
            self.assertIn(entry["reference"], text)

    def test_invalid_manifest_cannot_modify_env(self):
        path = self.root / ".env"
        path.write_text("POSTGRES_PASSWORD=unchanged\n")
        del self.data["images"]["frontend"]
        with self.assertRaises(ValueError):
            release.update_env(path, self.data)
        self.assertEqual(path.read_text(), "POSTGRES_PASSWORD=unchanged\n")

    @unittest.skipUnless(os.environ.get("RUN_COMPOSE_TESTS") == "1", "opt-in Docker Compose CLI integration")
    def test_real_compose_maps_digest_images_and_preserves_project_names(self):
        repository_root = Path(__file__).resolve().parents[3]
        for target, services in release.TARGETS.items():
            with self.subTest(target=target):
                directory = repository_root / "infrastructure" / f"ec2-{target}"
                env = self.root / f"{target}.env"
                env.write_text((directory / ".env.example").read_text(encoding="utf-8"))
                release.update_env(env, self.data)
                config = json.loads(subprocess.check_output([
                    "docker", "compose", "--env-file", str(env), "-f", str(directory / "compose.yaml"),
                    # 일회성 배치는 profile 뒤에 있어 기본 출력에서 빠진다.
                    "--profile", "*", "config", "--format", "json"], text=True))
                self.assertEqual(config["name"], "nilm-" + target)
                for service in services:
                    self.assertEqual(config["services"][service]["image"], self.data["images"][service]["reference"])

    def test_snapshot_preserves_original_on_retry_and_excludes_data(self):
        (self.root / ".env").write_text("original")
        (self.root / "database").mkdir()
        (self.root / "database" / "data").write_text("keep")
        release.snapshot(self.root, self.data)
        (self.root / ".env").write_text("new")
        release.snapshot(self.root, self.data)
        backup = self.root / "releases" / self.data["release_id"] / "previous"
        self.assertEqual((backup / ".env").read_text(), "original")
        self.assertFalse((backup / "database").exists())

    def docker_metadata(self, *args):
        if args[0] == "info":
            return {"OSType": "linux", "Architecture": "x86_64"}
        return [{"Os": "linux", "Architecture": "amd64", "RepoDigests": [args[-1].removeprefix("docker.io/")]}]

    def test_b_pulls_analysis_batch_bridge_and_loaders_by_digest(self):
        with patch.object(release, "docker_json", side_effect=self.docker_metadata), \
                patch.object(release.subprocess, "run") as run:
            release.check_images(self.data, "b", pull=True)
        self.assertEqual(run.call_count, 7)
        for service in ("realtime-analysis-service", "aggregation-service", "mqtt-kafka-bridge",
                        "bronze-loader", "session-lake-loader", "power-silver", "gold-profile"):
            self.assertIn(self.data["images"][service]["reference"], str(run.call_args_list))
        self.assertNotIn(self.data["images"]["api-gateway"]["reference"], str(run.call_args_list))

    def test_a_pulls_application_images_and_simulator(self):
        with patch.object(release, "docker_json", side_effect=self.docker_metadata), \
                patch.object(release.subprocess, "run") as run:
            release.check_images(self.data, "a", pull=True)
        self.assertEqual(run.call_count, 5)
        self.assertIn(self.data["images"]["simulator"]["reference"], str(run.call_args_list))
        for service in ("realtime-analysis-service", "aggregation-service", "mqtt-kafka-bridge",
                        "bronze-loader", "session-lake-loader", "power-silver", "gold-profile"):
            self.assertNotIn(self.data["images"][service]["reference"], str(run.call_args_list))

    def test_platform_and_digest_mismatch_block_deployment(self):
        for image in ({"Os": "linux", "Architecture": "arm64", "RepoDigests": []},
                      {"Os": "linux", "Architecture": "amd64", "RepoDigests": []}):
            with self.subTest(image=image), patch.object(release, "docker_json", side_effect=[
                    {"OSType": "linux", "Architecture": "amd64"}, [image]]):
                with self.assertRaises(ValueError):
                    release.check_images(self.data, "b")

    def push_process(self, *args, fail=False, missing_digest=False, **kwargs):
        class Process:
            stdout = io.StringIO("layer: Pushed\n" if missing_digest else
                                 "tag: digest: sha256:" + "1" * 64 + " size: 1234\n")
            def __enter__(self):
                return self
            def __exit__(self, *args):
                return False
            def wait(self):
                return 1 if fail else 0
        return Process()

    def test_publish_creates_manifest_only_after_all_pushes(self):
        with patch.object(release.subprocess, "Popen", side_effect=self.push_process) as push, \
                contextlib.redirect_stdout(io.StringIO()):
            release.publish(self.manifest)
        self.assertEqual(push.call_count, len(release.SERVICES))
        self.assertEqual(
            len(release.load(self.manifest)["images"]),
            len(release.SERVICES),
        )
        for call in push.call_args_list:
            self.assertTrue(call.args[0][2].startswith(self.data["repository"] + ":"))

    def test_failed_second_push_removes_stale_manifest(self):
        release.write_json(self.manifest, self.data)
        with patch.object(release.subprocess, "Popen", side_effect=[self.push_process(), self.push_process(fail=True)]), \
                contextlib.redirect_stdout(io.StringIO()):
            with self.assertRaises(RuntimeError):
                release.publish(self.manifest)
        self.assertFalse(self.manifest.exists())

    def test_missing_digest_cannot_publish_manifest(self):
        with patch.object(release.subprocess, "Popen", return_value=self.push_process(missing_digest=True)), \
                contextlib.redirect_stdout(io.StringIO()):
            with self.assertRaises(ValueError):
                release.publish(self.manifest)
        self.assertFalse(self.manifest.exists())


if __name__ == "__main__":
    unittest.main()
