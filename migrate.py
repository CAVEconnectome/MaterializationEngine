import click
from dynamicannotationdb.migration import DynamicMigration, run_alembic_migration
from run import application
from sqlalchemy.engine.url import make_url

from materializationengine.info_client import (
    get_datastacks,
    get_relevant_datastack_info,
)


def get_allowed_aligned_volumes():
    with application.app_context():
        datastacks = get_datastacks()
        aligned_volumes = []
        for datastack in datastacks:
            aligned_volume_name, pcg_table_name = get_relevant_datastack_info(datastack)
            aligned_volumes.append(aligned_volume_name)
    return aligned_volumes


@click.group(help="Migration tools")
def migrator():
    pass


@migrator.command(help="Migrate metadata schemas")
@click.option(
    "--sql_url",
    prompt=True,
    default=lambda: application.config["SQLALCHEMY_DATABASE_URI"],
    show_default="SQL URL from config",
)
@click.option(
    "-a",
    "--aligned_volume",
    prompt="Target Aligned Volume",
    help="Aligned Volume database to migrate",
    type=click.Choice(get_allowed_aligned_volumes()),
)
def migrate_static_schemas(sql_url: str, aligned_volume: str):
    sql_base_uri = sql_url.rpartition("/")[0]
    sql_uri = make_url(f"{sql_base_uri}/{aligned_volume}")
    migrator = run_alembic_migration(str(sql_uri))
    click.echo(migrator)


@migrator.command(help="Migrate dynamic annotation schemas")
@click.option(
    "--sql_url",
    prompt=True,
    default=lambda: application.config["SQLALCHEMY_DATABASE_URI"],
    show_default="SQL URL from config",
)
@click.option(
    "-a",
    "--aligned_volume",
    prompt="Target Aligned Volume",
    help="Aligned Volume database to migrate",
    type=click.Choice(get_allowed_aligned_volumes()),
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
    default=lambda: application.config["SQLALCHEMY_DATABASE_URI"],
    show_default="SQL URL from config",
)
@click.option(
    "-a",
    "--aligned_volume",
    prompt="Target Aligned Volume",
    help="Aligned Volume database to migrate",
    type=click.Choice(get_allowed_aligned_volumes()),
)
@click.option(
    "--dry_run", prompt="Dry Run", help="Test migration before running", default=True
)
def migrate_annotation_schemas(sql_url: str, aligned_volume: str, dry_run: bool = True):
    migrator = DynamicMigration(sql_url, aligned_volume)
    fkey_constraint_mapping = migrator.apply_cascade_option_to_tables(dry_run=dry_run)
    click.echo(fkey_constraint_mapping)


if __name__ == "__main__":
    migrator()
