from flask_admin import Admin, AdminIndexView, BaseView, expose
from flask_admin.contrib.sqla import ModelView
from dynamicannotationdb.models import AnalysisVersion, AnalysisTable
from flask import request, current_app, g, render_template
from middle_auth_client import auth_required, auth_requires_admin
from dynamicannotationdb.migration import DynamicMigration, run_alembic_migration
from sqlalchemy.engine.url import make_url
from materializationengine.info_client import (
    get_datastacks,
    get_relevant_datastack_info,
)


class MatAdminIndexView(AdminIndexView):
    @expose("/", methods=["GET"])
    @auth_required
    def index(self):
        return super(MatAdminIndexView, self).index()

    def is_accessible(self):
        @auth_required
        def helper():
            return True

        return helper()


def setup_admin(app, db):
    admin = Admin(app, name="materializationengine", index_view=MatAdminIndexView())
    admin.add_view(MigrationView(name="Migration"))
    admin.add_view(ModelView(AnalysisVersion, db.session))
    admin.add_view(ModelView(AnalysisTable, db.session))
    return admin


def get_allowed_aligned_volumes():
    with current_app.app_context():
        datastacks = get_datastacks()
        aligned_volumes = []
        for datastack in datastacks:
            aligned_volume_name, pcg_table_name = get_relevant_datastack_info(datastack)
            aligned_volumes.append(aligned_volume_name)
    return aligned_volumes


class MigrationView(BaseView):
    def is_accessible(self):
        @auth_required
        def helper():
            return True

        return helper() and g.get("auth_user", {}).get("admin", False)

    def inaccessible_callback(self, name, **kwargs):
        return render_template("admin/403.html"), 403

    @expose("/")
    @auth_requires_admin
    def index(self):
        aligned_volumes = get_allowed_aligned_volumes()
        return self.render("admin/migration.html", aligned_volumes=aligned_volumes)

    @expose("/migrate_static_schemas", methods=["POST"])
    @auth_requires_admin
    def migrate_static_schemas(self):
        sql_url = request.form.get("sql_url")
        aligned_volume = request.form.get("aligned_volume")
        sql_base_uri = sql_url.rpartition("/")[0]
        sql_uri = make_url(f"{sql_base_uri}/{aligned_volume}")
        migrator = run_alembic_migration(str(sql_uri))
        return self.render(
            "admin/migration.html",
            message=migrator,
            aligned_volumes=get_allowed_aligned_volumes(),
        )

    @expose("/migrate_annotation_schemas", methods=["POST"])
    @auth_requires_admin
    def migrate_annotation_schemas(self):
        sql_url = request.form.get("sql_url")
        aligned_volume = request.form.get("aligned_volume")
        dry_run = request.form.get("dry_run") == "true"
        migrator = DynamicMigration(sql_url, aligned_volume)
        migrations = migrator.upgrade_annotation_models(dry_run=dry_run)
        return self.render(
            "admin/migration.html",
            message=migrations,
            aligned_volumes=get_allowed_aligned_volumes(),
        )

    @expose("/migrate_foreign_key_constraints", methods=["POST"])
    @auth_requires_admin
    def migrate_foreign_key_constraints(self):
        sql_url = request.form.get("sql_url")
        aligned_volume = request.form.get("aligned_volume")
        dry_run = request.form.get("dry_run") == "true"
        migrator = DynamicMigration(sql_url, aligned_volume)
        fkey_constraint_mapping = migrator.apply_cascade_option_to_tables(
            dry_run=dry_run
        )
        return self.render(
            "admin/migration.html",
            message=fkey_constraint_mapping,
            aligned_volumes=get_allowed_aligned_volumes(),
        )
