"""Add durable result inbox and rejected Kafka records."""

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "0002_payment_results"
down_revision: str | None = "0001_payment_foundation"
branch_labels: str | None = None
depends_on: str | None = None


def upgrade() -> None:
    op.create_table(
        "inbox",
        sa.Column("event_id", sa.UUID(), nullable=False),
        sa.Column("attempt_id", sa.UUID(), nullable=False),
        sa.Column("payload_hash", sa.String(64), nullable=False),
        sa.Column("payload", postgresql.JSONB(), nullable=False),
        sa.Column(
            "received_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False
        ),
        sa.PrimaryKeyConstraint("event_id", name=op.f("pk_inbox")),
        sa.ForeignKeyConstraint(
            ["attempt_id"],
            ["payment_attempts.attempt_id"],
            name=op.f("fk_inbox_attempt_id_payment_attempts"),
            ondelete="RESTRICT",
        ),
        sa.UniqueConstraint("attempt_id", name=op.f("uq_inbox_attempt_id")),
    )
    op.create_table(
        "payment_rejections",
        sa.Column("topic", sa.String(255), nullable=False),
        sa.Column("partition", sa.Integer(), nullable=False),
        sa.Column("kafka_offset", sa.BigInteger(), nullable=False),
        sa.Column("reason", sa.String(32), nullable=False),
        sa.Column("raw_key", sa.LargeBinary(), nullable=True),
        sa.Column("raw_value", sa.LargeBinary(), nullable=True),
        sa.Column(
            "received_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False
        ),
        sa.PrimaryKeyConstraint(
            "topic", "partition", "kafka_offset", name=op.f("pk_payment_rejections")
        ),
        sa.CheckConstraint(
            "partition >= 0 AND kafka_offset >= 0",
            name=op.f("ck_payment_rejections_valid_position"),
        ),
    )


def downgrade() -> None:
    op.drop_table("payment_rejections")
    op.drop_table("inbox")
