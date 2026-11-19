"""Deduplicate authenticated channel callbacks transactionally."""

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision = "e41bf230a9c7"
down_revision = "d37a109ebae2"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "executor_callbacks",
        sa.Column("event_id", sa.Uuid(), nullable=False),
        sa.Column("attempt_id", sa.Uuid(), nullable=False),
        sa.Column("payload_hash", sa.String(64), nullable=False),
        sa.Column("payload", postgresql.JSONB(), nullable=False),
        sa.Column(
            "received_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False
        ),
        sa.PrimaryKeyConstraint("event_id"),
        sa.ForeignKeyConstraint(
            ["attempt_id"], ["executor_attempts.attempt_id"], ondelete="RESTRICT"
        ),
    )


def downgrade() -> None:
    if op.get_bind().scalar(sa.text("SELECT 1 FROM executor_callbacks LIMIT 1")):
        raise RuntimeError("Cannot downgrade while callback receipts exist")
    op.drop_table("executor_callbacks")
