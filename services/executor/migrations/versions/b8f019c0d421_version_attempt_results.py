"""Version attempt states and allow immutable result refinements."""

import sqlalchemy as sa
from alembic import op

revision = "b8f019c0d421"
down_revision = "a2f47db13e80"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "executor_attempts",
        sa.Column("state_version", sa.Integer(), server_default="0", nullable=False),
    )
    op.execute(
        "UPDATE executor_attempts SET state_version=1 "
        "WHERE state IN ('succeeded','failed','unknown')"
    )
    op.create_check_constraint(
        "ck_executor_attempts_version", "executor_attempts", "state_version >= 0"
    )
    op.add_column(
        "outbox", sa.Column("state_version", sa.Integer(), server_default="1", nullable=False)
    )
    op.drop_constraint("outbox_attempt_id_key", "outbox", type_="unique")
    op.create_unique_constraint(
        "uq_executor_outbox_attempt_version", "outbox", ["attempt_id", "state_version"]
    )


def downgrade() -> None:
    if op.get_bind().scalar(sa.text("SELECT 1 FROM outbox WHERE state_version > 1 LIMIT 1")):
        raise RuntimeError("Cannot downgrade while refined result events exist")
    op.drop_constraint("uq_executor_outbox_attempt_version", "outbox", type_="unique")
    op.create_unique_constraint("outbox_attempt_id_key", "outbox", ["attempt_id"])
    op.drop_column("outbox", "state_version")
    op.drop_constraint("ck_executor_attempts_version", "executor_attempts", type_="check")
    op.drop_column("executor_attempts", "state_version")
