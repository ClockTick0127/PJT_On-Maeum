"""Uses the same explicitly disposable PostgreSQL as migration integration tests."""
from uuid import uuid4
import pytest
from sqlalchemy import inspect
from sqlalchemy.orm import sessionmaker
from tests.test_session_lake_postgres import engine, DATABASE_URL
from realtime_analysis.scene_lease import household_lease

pytestmark = [pytest.mark.postgres, pytest.mark.skipif(not DATABASE_URL, reason='TEST_DATABASE_URL not set')]


def test_scene_migration_tables_and_pending_index(engine):
    inspector = inspect(engine)
    assert {'selected_scene_evidence', 'selected_scene_projection', 'selected_scene_usage',
            'selected_scene_outbox'} <= set(inspector.get_table_names())
    assert inspector.get_indexes('selected_scene_outbox')


def test_household_lock_excludes_other_run_and_releases(engine):
    factory = sessionmaker(engine)
    house = 'acceptance-' + str(uuid4())
    with household_lease(factory, house) as alive:
        alive()
        with pytest.raises(RuntimeError, match='already owns'):
            with household_lease(factory, house):
                pytest.fail('Second worker acquired same household')
        with household_lease(factory, house + '-other') as other:
            other()
    with household_lease(factory, house) as reacquired:
        reacquired()
