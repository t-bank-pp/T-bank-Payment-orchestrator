"""Track result versions while retaining legacy v1 inbox hashes."""

import sqlalchemy as sa
from alembic import op

revision = "0003_payment_refinements"
down_revision = "0002_payment_results"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "payment_attempts",
        sa.Column("result_version", sa.Integer(), server_default="0", nullable=False),
    )
    op.add_column(
        "payment_attempts", sa.Column("provider_operation_id", sa.String(255), nullable=True)
    )
    op.execute("UPDATE payment_attempts SET result_version=1 WHERE outcome IS NOT NULL")
    op.execute(
        "UPDATE payment_attempts a SET provider_operation_id="
        "i.payload->'data'->>'provider_operation_id' FROM inbox i "
        "WHERE a.attempt_id=i.attempt_id"
    )
    op.create_check_constraint(
        "nonnegative_result_version", "payment_attempts", "result_version >= 0"
    )
    op.add_column(
        "inbox", sa.Column("state_version", sa.Integer(), server_default="1", nullable=False)
    )
    op.drop_constraint("uq_inbox_attempt_id", "inbox", type_="unique")
    op.create_unique_constraint(
        "uq_inbox_attempt_id_state_version", "inbox", ["attempt_id", "state_version"]
    )


def downgrade() -> None:
    if op.get_bind().scalar(sa.text("SELECT 1 FROM inbox WHERE state_version > 1 LIMIT 1")):
        raise RuntimeError("Cannot downgrade while refined payment results exist")
    op.drop_constraint("uq_inbox_attempt_id_state_version", "inbox", type_="unique")
    op.create_unique_constraint("uq_inbox_attempt_id", "inbox", ["attempt_id"])
    op.drop_column("inbox", "state_version")
    op.drop_constraint(
        op.f("ck_payment_attempts_nonnegative_result_version"), "payment_attempts", type_="check"
    )
    op.drop_column("payment_attempts", "provider_operation_id")
    op.drop_column("payment_attempts", "result_version")
