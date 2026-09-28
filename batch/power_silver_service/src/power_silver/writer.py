"""출력 파일 분할과 저장.

스몰 파일을 줄이려고 출력 파티션 수를 직접 정한다. 읽기 설정인
``spark.sql.files.maxPartitionBytes``는 출력 파일 크기를 보장하지 않는다.
압축 후 행당 바이트는 데이터에 따라 달라지므로, 실제로 쓴 파일을 재어 그 값을 manifest에
남기고 다음 실행이 그 측정치로 분할 수를 정한다.
"""

from __future__ import annotations

from dataclasses import dataclass
import math

from pyspark.sql import DataFrame

from power_silver.storage import LakeStorage


@dataclass(frozen=True)
class WriteResult:
    path: str
    file_count: int
    byte_count: int

    def bytes_per_row(self, row_count: int) -> float | None:
        if row_count <= 0:
            return None
        return self.byte_count / row_count


def plan_partitions(
    row_count: int,
    *,
    bytes_per_row: float,
    target_file_bytes: int,
    max_files: int,
) -> int:
    """목표 파일 크기에 맞는 출력 파티션 수.

    데이터가 적으면 1개로 둔다. 크기를 맞추려고 억지로 합치거나 쪼개지 않는다.
    """

    if row_count <= 0:
        return 1
    estimated = row_count * max(bytes_per_row, 1e-9)
    return max(1, min(max_files, math.ceil(estimated / target_file_bytes)))


def write_parquet(
    frame: DataFrame,
    storage: LakeStorage,
    path: str,
    *,
    partitions: int,
) -> WriteResult:
    """실행별 임시 경로에 저장하고 실제 파일 수·크기를 잰다."""

    prepared = frame.repartition(partitions) if partitions > 1 else frame.coalesce(1)
    prepared.write.mode("overwrite").parquet(storage.uri(path))
    files = [
        item
        for item in storage.walk_files(path)
        if item.path.endswith(".parquet")
    ]
    return WriteResult(
        path=path,
        file_count=len(files),
        byte_count=sum(item.length for item in files),
    )
