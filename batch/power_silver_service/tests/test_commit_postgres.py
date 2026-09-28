"""PostgreSQL에서만 확인할 수 있는 것들.

SQLite 테스트는 잠금의 in-process 대체 경로를 돌린다. 운영 경로인 advisory lock과
"날짜당 활성 버전 하나" 부분 유니크 인덱스는 여기서 확인한다.

    docker run -d --name nilm-silver-test-pg -e POSTGRES_USER=test \
      -e POSTGRES_PASSWORD=test -e POSTGRES_DB=analysis_test \
      -p 127.0.0.1:55433:5432 postgres:18-alpine
    export TEST_DATABASE_URL=postgresql+psycopg://test:test@127.0.0.1:55433/analysis_test
"""

from __future__ import annotations

from datetime import date, datetime, timezone
import os
from uuid import uuid4

import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import sessionmaker

from realtime_analysis.database import Base
from realtime_analysis.models import LakeBatchRun, LakeDatasetVersion

from power_silver.commit import LockNotAcquired, SilverCommitRepository
from power_silver.constants import VERSION_ACTIVE

from test_commit import TARGET, build_manifest, start


pytestmark = pytest.mark.postgres


@pytest.fixture
def pg_session_factory():
    url = os.environ.get("TEST_DATABASE_URL")
    if not url:
        pytest.skip("TEST_DATABASE_URL is not set")
    engine = create_engine(url)
    # 일회용 DB만 지정해야 한다. 스키마를 통째로 비운다.
    with engine.begin() as connection:
        connection.execute(text("DROP SCHEMA public CASCADE"))
        connection.execute(text("CREATE SCHEMA public"))
    Base.metadata.create_all(engine)
    yield sessionmaker(bind=engine, autoflush=False, expire_on_commit=False)
    engine.dispose()


def test_the_advisory_lock_keeps_a_second_connection_out(pg_session_factory):
    first = SilverCommitRepository(pg_session_factory)
    second = SilverCommitRepository(pg_session_factory)

    with first.date_lock(TARGET):
        with pytest.raises(LockNotAcquired):
            with second.date_lock(TARGET):
                pass
        with second.date_lock(date(2026, 9, 18)):
            pass

    with second.date_lock(TARGET):
        pass


def test_publish_and_supersede_round_trip(pg_session_factory):
    repository = SilverCommitRepository(pg_session_factory)
    first = start(repository)
    repository.publish(build_manifest(first.run_id))
    second = start(repository, snapshot_id="snap-2")
    repository.publish(build_manifest(second.run_id, snapshot_id="snap-2"))

    for dataset in ("power_clean", "household_observation_daily"):
        assert repository.active_version(dataset, TARGET).run_id == second.run_id


def test_the_partial_index_refuses_two_active_versions(pg_session_factory):
    repository = SilverCommitRepository(pg_session_factory)
    handle = start(repository)
    repository.publish(build_manifest(handle.run_id))

    other = LakeBatchRun(
        run_id=uuid4(),
        job_name="power-silver-daily",
        target_date=TARGET,
        attempt=99,
        status="RUNNING",
        input_snapshot_id="snap-x",
        rule_version="power-silver-v1",
        config_version="c@1",
        started_at=datetime.now(timezone.utc),
        details={},
    )
    with pg_session_factory.begin() as session:
        session.add(other)

    # 활성 버전을 직접 하나 더 넣으려 하면 DB가 막는다.
    with pytest.raises(IntegrityError):
        with pg_session_factory.begin() as session:
            session.add(
                LakeDatasetVersion(
                    version_id=uuid4(),
                    dataset_name="power_clean",
                    target_date=TARGET,
                    run_id=other.run_id,
                    status=VERSION_ACTIVE,
                    output_path="/nilm/silver/power_clean/other",
                    manifest_path="",
                    row_count=1,
                    input_snapshot_id="snap-x",
                    rule_version="power-silver-v1",
                    config_version="c@1",
                    published_at=datetime.now(timezone.utc),
                )
            )
