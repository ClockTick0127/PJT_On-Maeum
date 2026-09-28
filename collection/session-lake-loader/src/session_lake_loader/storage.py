"""Lake storage backends: WebHDFS for production, a local directory for tests."""

from __future__ import annotations

import os
from pathlib import Path, PurePosixPath
import shutil
from typing import Protocol


class LakeStorage(Protocol):
    """Minimal file operations the loader and verifier need on the lake."""

    def exists(self, path: str) -> bool: ...

    def size(self, path: str) -> int: ...

    def read_bytes(self, path: str) -> bytes: ...

    def write_bytes(self, path: str, data: bytes) -> None:
        """Write (or overwrite) a temporary file. Final files are never overwritten."""

    def rename(self, source: str, destination: str) -> None:
        """Move ``source`` to ``destination``; fail when the destination exists."""

    def makedirs(self, path: str) -> None: ...

    def list(self, path: str) -> list[str]:
        """Return child names of a directory, or an empty list when it is missing."""

    def delete(self, path: str, recursive: bool = False) -> bool: ...


def parent_of(path: str) -> str:
    return str(PurePosixPath(path).parent)


class HdfsLakeStorage:
    """WebHDFS-backed storage using the ``hdfs`` client already used by the project."""

    def __init__(self, client) -> None:
        self._client = client

    def exists(self, path: str) -> bool:
        return self._client.status(path, strict=False) is not None

    def size(self, path: str) -> int:
        return int(self._client.status(path)["length"])

    def read_bytes(self, path: str) -> bytes:
        with self._client.read(path) as reader:
            return reader.read()

    def write_bytes(self, path: str, data: bytes) -> None:
        self._client.makedirs(parent_of(path))
        self._client.write(path, data=data, overwrite=True)

    def rename(self, source: str, destination: str) -> None:
        if self.exists(destination):
            raise FileExistsError(destination)
        self._client.rename(source, destination)

    def makedirs(self, path: str) -> None:
        self._client.makedirs(path)

    def list(self, path: str) -> list[str]:
        if not self.exists(path):
            return []
        return sorted(self._client.list(path))

    def delete(self, path: str, recursive: bool = False) -> bool:
        return bool(self._client.delete(path, recursive=recursive))


class LocalLakeStorage:
    """Directory-backed storage with the same semantics, for tests and local runs."""

    def __init__(self, root: str | os.PathLike[str]) -> None:
        self._root = Path(root)

    def resolve(self, path: str) -> Path:
        relative = PurePosixPath(path)
        parts = [part for part in relative.parts if part not in ("/", "")]
        return self._root.joinpath(*parts)

    def exists(self, path: str) -> bool:
        return self.resolve(path).exists()

    def size(self, path: str) -> int:
        return self.resolve(path).stat().st_size

    def read_bytes(self, path: str) -> bytes:
        return self.resolve(path).read_bytes()

    def write_bytes(self, path: str, data: bytes) -> None:
        target = self.resolve(path)
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(data)

    def rename(self, source: str, destination: str) -> None:
        target = self.resolve(destination)
        if target.exists():
            raise FileExistsError(destination)
        target.parent.mkdir(parents=True, exist_ok=True)
        os.rename(self.resolve(source), target)

    def makedirs(self, path: str) -> None:
        self.resolve(path).mkdir(parents=True, exist_ok=True)

    def list(self, path: str) -> list[str]:
        target = self.resolve(path)
        if not target.is_dir():
            return []
        return sorted(child.name for child in target.iterdir())

    def delete(self, path: str, recursive: bool = False) -> bool:
        target = self.resolve(path)
        if not target.exists():
            return False
        if target.is_dir():
            if recursive:
                shutil.rmtree(target)
            else:
                target.rmdir()
        else:
            target.unlink()
        return True
