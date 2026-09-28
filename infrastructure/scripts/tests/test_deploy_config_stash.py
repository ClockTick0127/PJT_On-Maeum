"""prepare.sh runs on the EC2 agents from the 'deploy-config' stash, not a checkout.

Every repository file prepare.sh installs must therefore match the stash includes in
the Jenkinsfile, or the deploy fails with "install: cannot stat" after images were pushed.
"""
from pathlib import Path
import re
import unittest

ROOT = Path(__file__).resolve().parents[3]
JENKINSFILE = ROOT / "Jenkinsfile"
PREPARE = ROOT / "infrastructure" / "scripts" / "prepare.sh"


def stash_includes(name):
    text = JENKINSFILE.read_text(encoding="utf-8")
    match = re.search(rf"stash name: '{re.escape(name)}', includes: '([^']*)'", text)
    if not match:
        raise AssertionError(f"stash '{name}' not found in Jenkinsfile")
    return [pattern.strip() for pattern in match.group(1).split(",") if pattern.strip()]


def ant_pattern(pattern):
    """Translate an Ant-style include pattern into a regular expression."""
    parts = []
    for token in re.split(r"(\*\*/|\*\*|\*)", pattern):
        if token == "**/":
            parts.append(r"(?:.*/)?")
        elif token == "**":
            parts.append(r".*")
        elif token == "*":
            parts.append(r"[^/]*")
        else:
            parts.append(re.escape(token))
    return re.compile("".join(parts) + r"\Z")


def prepare_repository_paths():
    text = PREPARE.read_text(encoding="utf-8")
    paths = set()
    for line in text.splitlines():
        line = line.strip()
        if not re.match(r"(install|cp)\b", line):
            continue
        for match in re.finditer(r'"?(infrastructure/[^\s"]+)"?', line):
            paths.add(match.group(1).replace("$target", "b"))
    return sorted(paths)


class DeployConfigStashTests(unittest.TestCase):
    def test_prepare_paths_are_stashed(self):
        patterns = [ant_pattern(include) for include in stash_includes("deploy-config")]
        paths = prepare_repository_paths()
        self.assertTrue(paths, "prepare.sh should install repository files")
        missing = []
        for path in paths:
            candidate = path
            if not (ROOT / path).is_file():
                # Directories copied with cp -R: require at least one file under them.
                files = [str(p.relative_to(ROOT)).replace("\\", "/")
                         for p in (ROOT / path).rglob("*") if p.is_file()]
                self.assertTrue(files, f"{path} referenced by prepare.sh does not exist")
                candidate = files[0]
            if not any(pattern.match(candidate) for pattern in patterns):
                missing.append(path)
        self.assertEqual(missing, [], "paths installed by prepare.sh but absent from the "
                                      "deploy-config stash includes in Jenkinsfile")

    def test_prepare_paths_exist(self):
        for path in prepare_repository_paths():
            self.assertTrue((ROOT / path).exists(), path)


if __name__ == "__main__":
    unittest.main()
