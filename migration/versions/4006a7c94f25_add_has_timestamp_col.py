"""Add has timestamp col

Revision ID: 4006a7c94f25
Revises: 8ff84a0bb8f8
Create Date: 2023-01-03 10:53:44.569973

"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision = "4006a7c94f25"
down_revision = "8ff84a0bb8f8"
branch_labels = None
depends_on = None


def upgrade():
    op.add_column(
        "materializedmetadata", sa.Column("has_created_ts", sa.Boolean(), nullable=True)
    )
    op.execute("UPDATE materializedmetadata SET has_created_ts = False")


def downgrade():
    op.drop_column("materializedmetadata", "has_created_ts")
