"""Durable selected-scene output and window checkpoints."""
from alembic import op
import sqlalchemy as sa

revision = "20260921_17"
down_revision = "20260921_16"
branch_labels = depends_on = None


def upgrade():
    if "selected_scene_evidence" in sa.inspect(op.get_bind()).get_table_names():
        return
    op.create_table("selected_scene_evidence",
        sa.Column("household_id", sa.String(50), primary_key=True),
        sa.Column("run_id", sa.String(100), primary_key=True),
        sa.Column("profile_id", sa.String(100), primary_key=True),
        sa.Column("source_index", sa.BigInteger(), primary_key=True),
        sa.Column("input_sha256", sa.String(64), nullable=False),
        sa.Column("payload", sa.JSON(), nullable=False),
        sa.Column("checkpoint", sa.JSON(), nullable=False))


def downgrade():
    if "selected_scene_evidence" in sa.inspect(op.get_bind()).get_table_names():
        op.drop_table("selected_scene_evidence")
