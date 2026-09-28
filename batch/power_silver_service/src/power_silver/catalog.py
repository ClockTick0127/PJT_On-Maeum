"""후속 집계가 읽어야 할 실행 버전을 고르고, 다시 계산할 날짜를 찾는다.

``/nilm/silver/power_clean/event_date=.../run_id=*`` 를 통째로 읽으면 같은 날짜가
수정 재처리 횟수만큼 중복된다. 소비자는 반드시 여기를 거쳐 날짜별 활성 실행 하나만
읽는다.

상위 날짜가 재처리되면 그 날짜를 소비한 하위 결과는 낡은 것이 된다. 큐로 알리는 방식은
알림이 유실되면 조용히 틀린 결과가 남는다. 그래서 소비 사실을 :class:`LakeDatasetDependency`
에 남기고, 다시 계산할 날짜는 **"내 활성 결과가 쓴 상위 버전"과 "지금 활성인 상위 버전"을
직접 비교**해서 구한다(:meth:`SilverCatalog.dirty_dates`). 비교가 곧 진실이므로 알림을
놓쳐서 생기는 누락이 없다.
"""

from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass
from datetime import date
from uuid import UUID

from sqlalchemy import and_, select
from sqlalchemy.orm import Session, aliased, sessionmaker

from realtime_analysis.models import LakeDatasetDependency, LakeDatasetVersion

from power_silver.constants import VERSION_ACTIVE


# 다시 계산해야 하는 이유.
DIRTY_NEVER_PROCESSED = "NEVER_PROCESSED"
DIRTY_UPSTREAM_CHANGED = "UPSTREAM_CHANGED"
DIRTY_DEPENDENCY_UNKNOWN = "DEPENDENCY_UNKNOWN"


@dataclass(frozen=True)
class DatasetVersionRef:
    """소비자가 한 번에 고정해야 하는 상위 버전 참조."""

    dataset_name: str
    target_date: date
    run_id: UUID
    version_id: UUID
    output_path: str
    manifest_path: str
    input_snapshot_id: str
    rule_version: str
    config_version: str


@dataclass(frozen=True)
class DirtyDate:
    target_date: date
    reason: str
    upstream_run_id: UUID
    consumed_run_id: UUID | None


def _ref(version: LakeDatasetVersion) -> DatasetVersionRef:
    return DatasetVersionRef(
        dataset_name=version.dataset_name,
        target_date=version.target_date,
        run_id=version.run_id,
        version_id=version.version_id,
        output_path=version.output_path,
        manifest_path=version.manifest_path,
        input_snapshot_id=version.input_snapshot_id,
        rule_version=version.rule_version,
        config_version=version.config_version,
    )


class SilverCatalog:
    def __init__(self, session_factory: sessionmaker[Session]) -> None:
        self._session_factory = session_factory

    def active_versions(
        self, dataset_name: str, dates: Iterable[date]
    ) -> dict[date, DatasetVersionRef]:
        """날짜별 활성 버전을 한 조회로 고정한다.

        경로만 받으면 무엇을 소비했는지 기록할 수 없다. 의존성을 남기려면 run/version ID가
        같은 시점의 값이어야 하므로 한 번에 가져온다.
        """

        wanted = sorted(set(dates))
        if not wanted:
            return {}
        with self._session_factory() as session:
            versions = session.scalars(
                select(LakeDatasetVersion).where(
                    LakeDatasetVersion.dataset_name == dataset_name,
                    LakeDatasetVersion.status == VERSION_ACTIVE,
                    LakeDatasetVersion.target_date.in_(wanted),
                )
            ).all()
        return {version.target_date: _ref(version) for version in versions}

    def active_version(self, dataset_name: str, target_date: date):
        return self.active_versions(dataset_name, [target_date]).get(target_date)

    def active_paths(self, dataset_name: str, dates: Iterable[date]) -> dict[date, str]:
        return {
            target_date: ref.output_path
            for target_date, ref in self.active_versions(dataset_name, dates).items()
        }

    def active_path(self, dataset_name: str, target_date: date) -> str | None:
        ref = self.active_version(dataset_name, target_date)
        return ref.output_path if ref is not None else None

    def missing_dates(
        self, dataset_name: str, dates: Iterable[date]
    ) -> tuple[date, ...]:
        """아직 확정되지 않은 날짜. 루틴 집계는 이 날짜를 표본에서 빼야 한다."""

        wanted = sorted(set(dates))
        available = self.active_versions(dataset_name, wanted)
        return tuple(day for day in wanted if day not in available)

    def dirty_dates(
        self,
        consumer_dataset: str,
        upstream_dataset: str,
        *,
        dates: Iterable[date] | None = None,
    ) -> tuple[DirtyDate, ...]:
        """``consumer_dataset``을 다시 계산해야 하는 날짜.

        세 가지다 — 상위는 확정됐는데 아직 만든 적이 없거나(`NEVER_PROCESSED`), 내 활성
        결과가 쓴 상위 실행이 더 이상 활성이 아니거나(`UPSTREAM_CHANGED`), 무엇을 썼는지
        기록이 없거나(`DEPENDENCY_UNKNOWN`).
        """

        upstream = aliased(LakeDatasetVersion)
        consumer = aliased(LakeDatasetVersion)
        dependency = aliased(LakeDatasetDependency)

        query = (
            select(
                upstream.target_date,
                upstream.run_id,
                consumer.run_id,
                dependency.upstream_run_id,
            )
            .select_from(upstream)
            .outerjoin(
                consumer,
                and_(
                    consumer.dataset_name == consumer_dataset,
                    consumer.target_date == upstream.target_date,
                    consumer.status == VERSION_ACTIVE,
                ),
            )
            .outerjoin(
                dependency,
                and_(
                    dependency.consumer_run_id == consumer.run_id,
                    dependency.upstream_dataset_name == upstream_dataset,
                    dependency.upstream_target_date == upstream.target_date,
                ),
            )
            .where(
                upstream.dataset_name == upstream_dataset,
                upstream.status == VERSION_ACTIVE,
            )
            .order_by(upstream.target_date)
        )
        if dates is not None:
            wanted = sorted(set(dates))
            if not wanted:
                return ()
            query = query.where(upstream.target_date.in_(wanted))

        with self._session_factory() as session:
            rows = session.execute(query).all()

        dirty: list[DirtyDate] = []
        for target_date, upstream_run, consumer_run, consumed_run in rows:
            if consumer_run is None:
                reason = DIRTY_NEVER_PROCESSED
            elif consumed_run is None:
                reason = DIRTY_DEPENDENCY_UNKNOWN
            elif consumed_run != upstream_run:
                reason = DIRTY_UPSTREAM_CHANGED
            else:
                continue
            dirty.append(
                DirtyDate(
                    target_date=target_date,
                    reason=reason,
                    upstream_run_id=upstream_run,
                    consumed_run_id=consumed_run,
                )
            )
        return tuple(dirty)
