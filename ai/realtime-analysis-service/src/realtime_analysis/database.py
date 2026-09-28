"""SQLAlchemy engine and session configuration for analysis_db."""

from sqlalchemy import URL, MetaData, create_engine
from sqlalchemy.engine import Engine
from sqlalchemy.orm import DeclarativeBase, Session, sessionmaker

from realtime_analysis.config import Settings


NAMING_CONVENTION = {
    "ix": "ix_%(column_0_label)s",
    "uq": "uq_%(table_name)s_%(column_0_name)s",
    "ck": "ck_%(table_name)s_%(constraint_name)s",
    "fk": "fk_%(table_name)s_%(column_0_name)s_%(referred_table_name)s",
    "pk": "pk_%(table_name)s",
}


class Base(DeclarativeBase):
    """Declarative base shared by analysis_db models and Alembic."""

    metadata = MetaData(naming_convention=NAMING_CONVENTION)


def build_database_url(settings: Settings) -> URL:
    """Build a safely escaped PostgreSQL URL from individual settings."""

    return URL.create(
        drivername="postgresql+psycopg",
        username=settings.database_user,
        password=settings.database_password,
        host=settings.database_host,
        port=settings.database_port,
        database=settings.database_name,
    )


def create_database_engine(settings: Settings) -> Engine:
    return create_engine(build_database_url(settings), pool_pre_ping=True)


def create_session_factory(settings: Settings) -> sessionmaker[Session]:
    return sessionmaker(
        bind=create_database_engine(settings),
        autoflush=False,
        expire_on_commit=False,
    )
