"""Add durable execution claims and channel results."""

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision = "c61b0e6a17a0"
down_revision = "5913233c2aee"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column("executor_attempts", sa.Column("lease_token", sa.Uuid(), nullable=True))
    for name in ("lease_expires_at", "finished_at"):
        op.add_column(
            "executor_attempts", sa.Column(name, sa.DateTime(timezone=True), nullable=True)
        )
    op.add_column("executor_attempts", sa.Column("reason_code", sa.String(100), nullable=True))
    op.add_column(
        "executor_attempts", sa.Column("provider_operation_id", sa.String(255), nullable=True)
    )
    op.add_column(
        "executor_attempts", sa.Column("result_payload", postgresql.JSONB(), nullable=True)
    )
    op.drop_constraint("ck_executor_attempts_state", "executor_attempts", type_="check")
    op.create_check_constraint(
        "ck_executor_attempts_state",
        "executor_attempts",
        "state IN ('queued','in_progress','succeeded','failed','unknown')",
    )
    op.create_check_constraint(
        "ck_executor_attempts_lease",
        "executor_attempts",
        "(state = 'in_progress' AND lease_token IS NOT NULL AND lease_expires_at IS NOT NULL) "
        "OR (state <> 'in_progress' AND lease_token IS NULL AND lease_expires_at IS NULL)",
    )


def downgrade() -> None:
    # The old schema can only represent queued. Never erase a known/uncertain result.
    if op.get_bind().scalar(
        sa.text("SELECT 1 FROM executor_attempts WHERE state <> 'queued' LIMIT 1")
    ):
        raise RuntimeError("Cannot downgrade execution schema while non-queued attempts exist")
    op.drop_constraint("ck_executor_attempts_lease", "executor_attempts", type_="check")
    op.drop_constraint("ck_executor_attempts_state", "executor_attempts", type_="check")
    op.create_check_constraint(
        "ck_executor_attempts_state", "executor_attempts", "state = 'queued'"
    )
    for name in (
        "result_payload",
        "provider_operation_id",
        "reason_code",
        "finished_at",
        "lease_expires_at",
        "lease_token",
    ):
        op.drop_column("executor_attempts", name)
