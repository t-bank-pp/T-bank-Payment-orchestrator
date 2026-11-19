"""Store and backfill durable attempt result events."""

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision = "a2f47db13e80"
down_revision = "c61b0e6a17a0"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "outbox",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("attempt_id", sa.Uuid(), nullable=False),
        sa.Column("topic", sa.String(255), nullable=False),
        sa.Column("message_key", sa.String(36), nullable=False),
        sa.Column("payload", postgresql.JSONB(), nullable=False),
        sa.Column(
            "created_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False
        ),
        sa.Column("published_at", sa.DateTime(timezone=True), nullable=True),
        sa.ForeignKeyConstraint(
            ["attempt_id"], ["executor_attempts.attempt_id"], ondelete="RESTRICT"
        ),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("attempt_id"),
    )
    op.create_index(
        "ix_executor_outbox_unpublished",
        "outbox",
        ["created_at"],
        postgresql_where=sa.text("published_at IS NULL"),
    )
    # A single result per completed attempt. Use persisted identities and finish time.
    # The CLI backfill covers results committed by old workers during a rolling upgrade.
    op.execute(
        sa.text("""
        INSERT INTO outbox (id, attempt_id, topic, message_key, payload)
        SELECT event_id, attempt_id, 'attempt.events', payment_id::text,
            jsonb_build_object(
                'event_id', event_id,
                'occurred_at', to_char(finished_at AT TIME ZONE 'UTC',
                    'YYYY-MM-DD"T"HH24:MI:SS.US"Z"'),
                'payment_id', payment_id,
                'attempt_id', attempt_id,
                'correlation_id', payload ->> 'correlation_id',
                'type', 'AttemptResult',
                'version', 1,
                'data', jsonb_build_object(
                    'channel_id', payload -> 'data' ->> 'channel_id',
                    'outcome', state,
                    'reason_code', reason_code,
                    'provider_operation_id', provider_operation_id
                )
            )
        FROM (
            SELECT *, gen_random_uuid() AS event_id FROM executor_attempts
            WHERE state IN ('succeeded', 'failed', 'unknown')
        ) AS completed
        ON CONFLICT (attempt_id) DO NOTHING
    """)
    )


def downgrade() -> None:
    if op.get_bind().scalar(sa.text("SELECT 1 FROM outbox LIMIT 1")):
        raise RuntimeError("Cannot downgrade result outbox while result events exist")
    op.drop_index("ix_executor_outbox_unpublished", table_name="outbox")
    op.drop_table("outbox")
