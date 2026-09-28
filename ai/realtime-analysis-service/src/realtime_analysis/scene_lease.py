"""Hold one PostgreSQL session lock per household across all scene runs."""
from contextlib import contextmanager
from sqlalchemy import text


@contextmanager
def household_lease(session_factory, household_id):
    with session_factory() as session:
        connection = session.connection()
        params = {'key': 'nilm:selected-scene:' + household_id}
        locked = connection.scalar(text('SELECT pg_try_advisory_lock(hashtextextended(:key, 0))'), params)
        if not locked:
            raise RuntimeError('A selected-scene worker already owns this household')
        def check_alive():
            # A broken lease connection must stop the worker before it processes
            # more input through separate pooled DB connections.
            connection.execute(text('SELECT 1'))
        try:
            yield check_alive
        finally:
            connection.execute(text('SELECT pg_advisory_unlock(hashtextextended(:key, 0))'), params)
