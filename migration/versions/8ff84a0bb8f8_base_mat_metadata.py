"""Base mat metadata

Revision ID: 8ff84a0bb8f8
Revises: 
Create Date: 2023-01-03 10:50:35.866055

"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.engine import reflection
from sqlalchemy import engine_from_config

# revision identifiers, used by Alembic.
revision = "8ff84a0bb8f8"
down_revision = None
branch_labels = None
depends_on = None


def get_tables():
    config = op.get_context().config
    engine = engine_from_config(
        config.get_section(config.config_ini_section), prefix="sqlalchemy."
    )
    inspector = reflection.Inspector.from_engine(engine)
    return inspector.get_table_names()


def upgrade():
    tables = get_tables()
    if "materializedmetadata" not in tables:
        op.create_table(
            "materializedmetadata",
            sa.Column("id", sa.Integer(), nullable=False),
            sa.Column("schema", sa.String(length=100), nullable=False),
            sa.Column("table_name", sa.String(length=100), nullable=False),
            sa.Column("row_count", sa.Integer(), nullable=False),
            sa.Column("materialized_timestamp", sa.DateTime(), nullable=False),
            sa.Column("segmentation_source", sa.String(length=255), nullable=True),
            sa.Column("is_merged", sa.Boolean(), nullable=True),
            sa.PrimaryKeyConstraint("id"),
        )


def downgrade():
    op.drop_table("materializedmetadata")
