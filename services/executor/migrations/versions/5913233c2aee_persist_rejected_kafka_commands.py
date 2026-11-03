"""persist rejected Kafka commands

Revision ID: 5913233c2aee
Revises: 5d53c8302a6d
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "5913233c2aee"
down_revision: str | Sequence[str] | None = "5d53c8302a6d"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "executor_rejections",
        sa.Column("topic", sa.String(length=255), nullable=False),
        sa.Column("partition", sa.Integer(), nullable=False),
        sa.Column("kafka_offset", sa.BigInteger(), nullable=False),
        sa.Column("reason", sa.String(length=32), nullable=False),
        sa.Column("raw_key", sa.LargeBinary(), nullable=True),
        sa.Column("raw_value", sa.LargeBinary(), nullable=True),
        sa.Column(
            "received_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.CheckConstraint(
            "reason IN ('INVALID_COMMAND', 'KEY_MISMATCH', 'COMMAND_CONFLICT')",
            name="ck_rejection_reason",
        ),
        sa.CheckConstraint("partition >= 0 AND kafka_offset >= 0", name="ck_rejection_position"),
        sa.PrimaryKeyConstraint("topic", "partition", "kafka_offset"),
    )


def downgrade() -> None:
    op.drop_table("executor_rejections")
