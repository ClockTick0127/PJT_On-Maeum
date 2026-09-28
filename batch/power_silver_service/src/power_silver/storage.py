"""Lake storage backends: WebHDFS for production, a local directory for tests.

The Spark job never uses this module to read or write measurement data — Spark
talks to the filesystem directly through :meth:`LakeStorage.uri`. What goes
through here is everything that has to be exact and small: manifest JSON, file
metadata for the input snapshot, and the directory renames that publish a run.
"""

from __future__ import annotations

from dataclasses import dataclass
import os
from pathlib import Path, PurePosixPath
import shutil
from typing import Protocol


@dataclass(frozen=True)
class FileStatus:
    path: str
    length: int
    modification_time: int  # epoch milliseconds


def parent_of(path: str) -> str:
    return str(PurePosixPath(path).parent)


class LakeStorage(Protocol):
    """File operations the batch needs outside Spark."""

    def uri(self, path: str) -> str:
        """Return the URI Spark uses for ``path``."""

    def exists(self, path: str) -> bool: ...

    def status(self, path: str) -> FileStatus: ...

    def read_bytes(self, path: str) -> bytes: ...

    def write_bytes(self, path: str, data: bytes) -> None: ...

    def makedirs(self, path: str) -> None: ...

    def list(self, path: str) -> list[str]:
        """Child names of a directory, or an empty list when it is missing."""

    def walk_files(self, path: str) -> list[FileStatus]:
        """Every file under ``path``, recursively, or an empty list when missing."""

    def rename(self, source: str, destination: str) -> None:
        """Move ``source`` to ``destination``; fail when the destination exists."""

    def delete(self, path: str, recursive: bool = False) -> bool: ...


class HdfsLakeStorage:
    """WebHDFS-backed storage using the ``hdfs`` client already used by the project."""

    def __init__(self, client, fs_uri: str) -> None:
        self._client = client
        self._fs_uri = fs_uri.rstrip("/")

    def uri(self, path: str) -> str:
        return f"{self._fs_uri}{path}"

    def exists(self, path: str) -> bool:
        return self._client.status(path, strict=False) is not None

    def status(self, path: str) -> FileStatus:
        raw = self._client.status(path)
        return FileStatus(path, int(raw["length"]), int(raw["modificationTime"]))

    def read_bytes(self, path: str) -> bytes:
        with self._client.read(path) as reader:
            return reader.read()

    def write_bytes(self, path: str, data: bytes) -> None:
        self._client.makedirs(parent_of(path))
        self._client.write(path, data=data, overwrite=True)

    def makedirs(self, path: str) -> None:
        self._client.makedirs(path)

    def list(self, path: str) -> list[str]:
        if not self.exists(path):
            return []
        return sorted(self._client.list(path))

    def walk_files(self, path: str) -> list[FileStatus]:
        if not self.exists(path):
            return []
        found: list[FileStatus] = []
        for directory, _subdirs, files in self._client.walk(path, status=True):
            # hdfs.Client.walk(status=True) returns (path, status) for directories.
            directory_path = directory[0] if isinstance(directory, tuple) else directory
            for name, raw in files:
                found.append(
                    FileStatus(
                        f"{directory_path.rstrip('/')}/{name}",
                        int(raw["length"]),
                        int(raw["modificationTime"]),
                    )
                )
        return sorted(found, key=lambda item: item.path)

    def rename(self, source: str, destination: str) -> None:
        if self.exists(destination):
            raise FileExistsError(destination)
        self._client.makedirs(parent_of(destination))
        self._client.rename(source, destination)

    def delete(self, path: str, recursive: bool = False) -> bool:
        return bool(self._client.delete(path, recursive=recursive))


class LocalLakeStorage:
    """Directory-backed storage with the same semantics, for tests and local runs."""

    def __init__(self, root: str | os.PathLike[str]) -> None:
        self._root = Path(root)

    def resolve(self, path: str) -> Path:
        parts = [part for part in PurePosixPath(path).parts if part not in ("/", "")]
        return self._root.joinpath(*parts)

    def uri(self, path: str) -> str:
        # Path.as_uri()는 '='을 %3D로 인코딩해서 Hadoop이 다른 디렉터리를 만든다.
        # 레이크 경로는 파티션 표기에 '='을 쓰므로 그대로 넘긴다.
        text = self.resolve(path).absolute().as_posix()
        return "file://" + (text if text.startswith("/") else "/" + text)

    def exists(self, path: str) -> bool:
        return self.resolve(path).exists()

    def status(self, path: str) -> FileStatus:
        stat = self.resolve(path).stat()
        return FileStatus(path, stat.st_size, int(stat.st_mtime * 1000))

    def read_bytes(self, path: str) -> bytes:
        return self.resolve(path).read_bytes()

    def write_bytes(self, path: str, data: bytes) -> None:
        target = self.resolve(path)
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(data)

    def makedirs(self, path: str) -> None:
        self.resolve(path).mkdir(parents=True, exist_ok=True)

    def list(self, path: str) -> list[str]:
        target = self.resolve(path)
        if not target.is_dir():
            return []
        return sorted(child.name for child in target.iterdir())

    def walk_files(self, path: str) -> list[FileStatus]:
        root = self.resolve(path)
        if not root.exists():
            return []
        found: list[FileStatus] = []
        for child in sorted(root.rglob("*")):
            if child.is_file():
                relative = child.relative_to(root).as_posix()
                found.append(self.status(f"{path.rstrip('/')}/{relative}"))
        return found

    def rename(self, source: str, destination: str) -> None:
        target = self.resolve(destination)
        if target.exists():
            raise FileExistsError(destination)
        target.parent.mkdir(parents=True, exist_ok=True)
        os.rename(self.resolve(source), target)

    def delete(self, path: str, recursive: bool = False) -> bool:
        target = self.resolve(path)
        if not target.exists():
            return False
        if target.is_dir():
            shutil.rmtree(target) if recursive else target.rmdir()
        else:
            target.unlink()
        return True


def create_storage(settings) -> LakeStorage:
    """Pick the backend the settings ask for."""

    if settings.lake_local_root:
        return LocalLakeStorage(settings.lake_local_root)
    from hdfs import InsecureClient

    return HdfsLakeStorage(
        InsecureClient(settings.hdfs_url, user=settings.hdfs_user),
        settings.lake_fs_uri,
    )
