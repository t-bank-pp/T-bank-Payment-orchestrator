"""Create payments, payment_attempts and outbox.

Revision ID: 0001_payment_foundation
Revises: None
"""

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "0001_payment_foundation"
down_revision: str | None = None
branch_labels: str | None = None
depends_on: str | None = None


def upgrade() -> None:
    op.create_table(
        "payments",
        sa.Column("id", sa.UUID(), server_default=sa.text("gen_random_uuid()"), nullable=False),
        sa.Column("idempotency_key", sa.String(100), nullable=False),
        sa.Column("request_hash", sa.String(64), nullable=False),
        sa.Column("amount_minor", sa.BigInteger(), nullable=False),
        sa.Column("currency", sa.String(3), nullable=False),
        sa.Column("recipient", sa.String(255), nullable=False),
        sa.Column("status", sa.String(32), server_default="in_progress", nullable=False),
        sa.Column("version", sa.Integer(), server_default=sa.text("0"), nullable=False),
        sa.Column(
            "created_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False
        ),
        sa.Column(
            "updated_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False
        ),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_payments")),
        sa.UniqueConstraint("idempotency_key", name=op.f("uq_payments_idempotency_key")),
        sa.CheckConstraint("amount_minor > 0", name=op.f("ck_payments_positive_amount")),
        sa.CheckConstraint("version >= 0", name=op.f("ck_payments_nonnegative_version")),
        sa.CheckConstraint(
            "status IN ('routing', 'in_progress', 'pending', "
            "'succeeded', 'failed', 'needs_review')",
            name=op.f("ck_payments_valid_status"),
        ),
    )
    op.create_table(
        "payment_attempts",
        sa.Column(
            "attempt_id", sa.UUID(), server_default=sa.text("gen_random_uuid()"), nullable=False
        ),
        sa.Column("payment_id", sa.UUID(), nullable=False),
        sa.Column("number", sa.Integer(), nullable=False),
        sa.Column("channel_id", sa.String(32), nullable=False),
        sa.Column("outcome", sa.String(16), nullable=True),
        sa.Column("reason_code", sa.String(100), nullable=True),
        sa.Column(
            "decision", postgresql.JSONB(), server_default=sa.text("'{}'::jsonb"), nullable=False
        ),
        sa.Column(
            "created_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False
        ),
        sa.Column("finished_at", sa.DateTime(timezone=True), nullable=True),
        sa.PrimaryKeyConstraint("attempt_id", name=op.f("pk_payment_attempts")),
        sa.ForeignKeyConstraint(
            ["payment_id"],
            ["payments.id"],
            name=op.f("fk_payment_attempts_payment_id_payments"),
            ondelete="RESTRICT",
        ),
        sa.UniqueConstraint(
            "payment_id", "number", name=op.f("uq_payment_attempts_payment_id_number")
        ),
        sa.CheckConstraint("number > 0", name=op.f("ck_payment_attempts_positive_number")),
        sa.CheckConstraint(
            "outcome IS NULL OR outcome IN ('succeeded', 'failed', 'unknown')",
            name=op.f("ck_payment_attempts_valid_outcome"),
        ),
    )
    op.create_table(
        "outbox",
        sa.Column("id", sa.UUID(), server_default=sa.text("gen_random_uuid()"), nullable=False),
        sa.Column("topic", sa.String(255), nullable=False),
        sa.Column("message_key", sa.Text(), nullable=False),
        sa.Column("payload", postgresql.JSONB(), nullable=False),
        sa.Column(
            "created_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False
        ),
        sa.Column("published_at", sa.DateTime(timezone=True), nullable=True),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_outbox")),
    )
    op.create_index(
        "ix_outbox_unpublished_created_at",
        "outbox",
        ["created_at"],
        postgresql_where=sa.text("published_at IS NULL"),
    )


def downgrade() -> None:
    op.drop_index("ix_outbox_unpublished_created_at", table_name="outbox")
    op.drop_table("outbox")
    op.drop_table("payment_attempts")
    op.drop_table("payments")
