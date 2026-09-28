"""Isolated scene usage, projection checkpoint and transactional outbox."""
from alembic import op
import sqlalchemy as sa

revision = '20260921_18'
down_revision = '20260921_17'
branch_labels = depends_on = None


def scope():
    return [sa.Column('household_id', sa.String(50), nullable=False),
            sa.Column('run_id', sa.String(100), nullable=False),
            sa.Column('profile_id', sa.String(100), nullable=False)]


def upgrade():
    existing = set(sa.inspect(op.get_bind()).get_table_names())
    if 'selected_scene_projection' not in existing:
        op.create_table('selected_scene_projection', *scope(), sa.Column('state', sa.JSON(), nullable=False),
                        sa.PrimaryKeyConstraint('household_id', 'run_id', 'profile_id'))
    if 'selected_scene_usage' not in existing:
        op.create_table('selected_scene_usage', sa.Column('session_id', sa.String(36), primary_key=True),
                        *scope(), sa.Column('payload', sa.JSON(), nullable=False))
    if 'selected_scene_outbox' not in existing:
        op.create_table('selected_scene_outbox', sa.Column('sequence', sa.Integer(), primary_key=True, autoincrement=True),
                        sa.Column('event_id', sa.String(36), nullable=False, unique=True), *scope(),
                        sa.Column('topic', sa.String(100), nullable=False), sa.Column('payload', sa.JSON(), nullable=False),
                        sa.Column('published', sa.Boolean(), nullable=False))
    indexes = {index['name'] for index in sa.inspect(op.get_bind()).get_indexes('selected_scene_outbox')}
    if 'ix_scene_outbox_pending' not in indexes:
        op.create_index('ix_scene_outbox_pending', 'selected_scene_outbox', ['household_id', 'run_id', 'profile_id', 'published', 'sequence'])


def downgrade():
    existing = set(sa.inspect(op.get_bind()).get_table_names())
    for table in ('selected_scene_outbox', 'selected_scene_usage', 'selected_scene_projection'):
        if table in existing:
            op.drop_table(table)
