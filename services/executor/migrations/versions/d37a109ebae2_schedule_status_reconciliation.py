"""Persist the next status reconciliation time."""

import sqlalchemy as sa
from alembic import op

revision = "d37a109ebae2"
down_revision = "b8f019c0d421"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "executor_attempts",
        sa.Column("next_reconcile_at", sa.DateTime(timezone=True), nullable=True),
    )


def downgrade() -> None:
    op.drop_column("executor_attempts", "next_reconcile_at")
