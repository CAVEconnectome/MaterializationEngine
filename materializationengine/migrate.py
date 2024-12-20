import logging

import click
from dynamicannotationdb.migration import DynamicMigration, run_alembic_migration
from flask import current_app
from flask.cli import AppGroup
from sqlalchemy.engine.url import make_url

from materializationengine.info_client import (
    get_datastacks,
    get_relevant_datastack_info,
)

logger = logging.getLogger(__name__)

migrator = AppGroup("migrator")


def get_allowed_aligned_volumes():
    with current_app.app_context():
        datastacks = get_datastacks()
        aligned_volumes = []
        for datastack in datastacks:
            aligned_volume_name, pcg_table_name = get_relevant_datastack_info(datastack)
            aligned_volumes.append(aligned_volume_name)
        return aligned_volumes


def migrate_static_schemas(sql_url: str, aligned_volume: str):
    sql_base_uri = sql_url.rpartition("/")[0]
    logging.info(sql_base_uri)
    sql_uri = make_url(f"{sql_base_uri}/{aligned_volume}")
    return run_alembic_migration(str(sql_uri))


@migrator.command()
def auto_migrate():
    sql_uri = current_app.config["SQLALCHEMY_DATABASE_URI"]
    aligned_volumes = get_allowed_aligned_volumes()
    for aligned_volume in aligned_volumes:
        logger.info(f"Migrating {aligned_volume}")
        migration_status = migrate_static_schemas(sql_uri, aligned_volume)
        logger.info(f"Migrated {aligned_volume} with {migration_status}")


def validate_aligned_volume(ctx, param, value):
    if value is None:
        return None
    allowed_volumes = get_allowed_aligned_volumes()
    if value not in allowed_volumes:
        raise click.BadParameter(f"Must be one of: {', '.join(allowed_volumes)}")
    return value


@migrator.command(help="Migrate metadata schemas")
@click.option(
    "--sql_url",
    prompt=True,
    default=lambda: current_app.config["SQLALCHEMY_DATABASE_URI"],
    show_default="SQL URL from config",
)
@click.option(
    "-a",
    "--aligned_volume",
    prompt="Target Aligned Volume",
    help="Aligned Volume database to migrate",
    callback=validate_aligned_volume,
)
def migrate(sql_url: str, aligned_volume: str):
    migration_status = migrate_static_schemas(sql_url, aligned_volume)
    click.echo(migration_status)


@migrator.command(help="Migrate dynamic annotation schemas")
@click.option(
    "--sql_url",
    prompt=True,
    default=lambda: current_app.config["SQLALCHEMY_DATABASE_URI"],
    show_default="SQL URL from config",
)
@click.option(
    "-a",
    "--aligned_volume",
    prompt="Target Aligned Volume",
    help="Aligned Volume database to migrate",
    callback=validate_aligned_volume,
)
@click.option(
    "--dry_run", prompt="Dry Run", help="Test migration before running", default=True
)
def migrate_annotation_schemas(sql_url: str, aligned_volume: str, dry_run: bool = True):
    migrator = DynamicMigration(sql_url, aligned_volume)
    migrations = migrator.upgrade_annotation_models(dry_run=dry_run)
    click.echo(migrations)


@migrator.command(help="Alter constraint on DELETE")
@click.option(
    "--sql_url",
    prompt=True,
    default=lambda: current_app.config["SQLALCHEMY_DATABASE_URI"],
    show_default="SQL URL from config",
)
@click.option(
    "-a",
    "--aligned_volume",
    prompt="Target Aligned Volume",
    help="Aligned Volume database to migrate",
    callback=validate_aligned_volume,
)
@click.option(
    "--dry_run", prompt="Dry Run", help="Test migration before running", default=True
)
def migrate_foreign_key_constraints(
    sql_url: str, aligned_volume: str, dry_run: bool = True
):
    migrator = DynamicMigration(sql_url, aligned_volume)
    fkey_constraint_mapping = migrator.apply_cascade_option_to_tables(dry_run=dry_run)
    click.echo(fkey_constraint_mapping)
