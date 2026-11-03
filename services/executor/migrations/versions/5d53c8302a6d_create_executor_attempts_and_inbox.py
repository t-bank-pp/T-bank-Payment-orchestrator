"""create executor attempts and inbox

Revision ID: 5d53c8302a6d
Revises:
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "5d53c8302a6d"
down_revision: str | Sequence[str] | None = None
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "executor_attempts",
        sa.Column("attempt_id", sa.Uuid(), nullable=False),
        sa.Column("payment_id", sa.Uuid(), nullable=False),
        sa.Column("command_hash", sa.String(length=64), nullable=False),
        sa.Column("payload", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column("state", sa.String(length=32), server_default="queued", nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.CheckConstraint("state = 'queued'", name="ck_executor_attempts_state"),
        sa.PrimaryKeyConstraint("attempt_id"),
    )
    op.create_index(
        op.f("ix_executor_attempts_payment_id"), "executor_attempts", ["payment_id"], unique=False
    )
    op.create_table(
        "inbox",
        sa.Column("event_id", sa.Uuid(), nullable=False),
        sa.Column("attempt_id", sa.Uuid(), nullable=False),
        sa.Column("payload_hash", sa.String(length=64), nullable=False),
        sa.Column("payload", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column(
            "received_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.ForeignKeyConstraint(
            ["attempt_id"], ["executor_attempts.attempt_id"], ondelete="RESTRICT"
        ),
        sa.PrimaryKeyConstraint("event_id"),
    )
    op.create_index(op.f("ix_inbox_attempt_id"), "inbox", ["attempt_id"], unique=False)


def downgrade() -> None:
    op.drop_index(op.f("ix_inbox_attempt_id"), table_name="inbox")
    op.drop_table("inbox")
    op.drop_index(op.f("ix_executor_attempts_payment_id"), table_name="executor_attempts")
    op.drop_table("executor_attempts")
