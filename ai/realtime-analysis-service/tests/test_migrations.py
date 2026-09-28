from pathlib import Path

from alembic.config import Config
from alembic.script import ScriptDirectory


SERVICE_DIR = Path(__file__).resolve().parents[1]


def test_alembic_graph_has_one_head_and_unique_revisions() -> None:
    config = Config(str(SERVICE_DIR / "alembic.ini"))
    config.set_main_option("script_location", str(SERVICE_DIR / "alembic"))

    scripts = ScriptDirectory.from_config(config)
    revisions = list(scripts.walk_revisions())

    assert scripts.get_heads() == ["20260921_18"]
    assert len({revision.revision for revision in revisions}) == len(revisions)
