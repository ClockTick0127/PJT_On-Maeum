"""Collect Gold profile messages from the delivery outbox into ``profiles.json``.

historicalAssessment wants the messages exactly as published (docs/historical-
assessment.md), ordered by ``as_of_date`` then ``profile_revision``. The outbox
row keeps that payload, so no re-serialisation is needed.
"""

from __future__ import annotations

import json
import os
from pathlib import Path
from urllib.parse import quote_plus


def database_url(environ=os.environ) -> str:
    explicit = environ.get("DATABASE_URL")
    if explicit:
        return explicit
    user = quote_plus(environ.get("DATABASE_USER", "nilm_admin"))
    password = quote_plus(environ.get("DATABASE_PASSWORD", "change-me-local"))
    host = environ.get("DATABASE_HOST", "localhost")
    port = environ.get("DATABASE_PORT", "5432")
    name = environ.get("DATABASE_NAME", "analysis_db")
    return f"postgresql+psycopg://{user}:{password}@{host}:{port}/{name}"


def sort_key(payload: dict) -> tuple:
    return (str(payload.get("as_of_date", "")), int(payload.get("profile_revision", 0)))


def fetch_payloads(url: str, household_id: str, *, delivery_mode: str | None) -> list[dict]:
    from sqlalchemy import create_engine, text

    engine = create_engine(url)
    query = "SELECT payload FROM gold_profile_delivery_outbox WHERE household_id = :household"
    params = {"household": household_id}
    if delivery_mode:
        query += " AND delivery_mode = :mode"
        params["mode"] = delivery_mode
    try:
        with engine.connect() as connection:
            rows = connection.execute(text(query), params).fetchall()
    finally:
        engine.dispose()
    payloads = []
    for (payload,) in rows:
        if isinstance(payload, str):
            payload = json.loads(payload)
        payloads.append(payload)
    return sorted(payloads, key=sort_key)


TREATED_AS_ACTIVE_NOTE = (
    "delivery_mode rewritten from SHADOW to ACTIVE by history-generator for a backfill test; "
    "no operational profile was promoted"
)


def treat_as_active(payloads: list[dict]) -> list[dict]:
    """Let historicalAssessment accept SHADOW Gold runs of a backfill.

    The Java resolver only considers ``delivery_mode == ACTIVE`` messages. A
    backfill that never published its Gold runs has only SHADOW rows, so the copy
    used as assessment input is relabelled and annotated. Time fields are untouched.
    """

    rewritten = []
    for payload in payloads:
        copy = dict(payload)
        if copy.get("delivery_mode") != "ACTIVE":
            copy["delivery_mode"] = "ACTIVE"
            copy["history_generator_note"] = TREATED_AS_ACTIVE_NOTE
        rewritten.append(copy)
    return rewritten


VIRTUAL_PUBLISH_NOTE = (
    "effective_from/published_at set by history-generator to the day after as_of_date (KST midnight) "
    "as the virtual application time of a backfill test; the batch execution time is kept in "
    "history_generator_original"
)


def virtual_publish(payloads: list[dict], utc_offset_seconds: int = 9 * 3600) -> list[dict]:
    """Give backfilled Gold messages a virtual application time.

    A Gold run executed today carries today's ``effective_from``/``published_at``,
    so historicalAssessment would refuse it for every past evaluation instant
    (docs/historical-assessment.md). The consumer rule is ``as_of_date <= eval
    date - 1``; the earliest honest application time is therefore the KST
    midnight after ``as_of_date``. Original values are preserved next to the
    rewritten ones and the rewrite is annotated.
    """

    from datetime import date, datetime, timedelta, timezone

    tz = timezone(timedelta(seconds=utc_offset_seconds))
    rewritten = []
    for payload in payloads:
        copy = dict(payload)
        as_of = date.fromisoformat(str(copy["as_of_date"]))
        applied = datetime(as_of.year, as_of.month, as_of.day, tzinfo=tz) + timedelta(days=1)
        copy["history_generator_original"] = {
            "effective_from": payload.get("effective_from"), "published_at": payload.get("published_at"),
        }
        copy["effective_from"] = applied.isoformat()
        copy["published_at"] = applied.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")
        copy["history_generator_virtual_publish_note"] = VIRTUAL_PUBLISH_NOTE
        rewritten.append(copy)
    return rewritten


def write_profiles(payloads: list[dict], output: Path) -> None:
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(payloads, ensure_ascii=False, indent=1), encoding="utf-8")
