from dynamicannotationdb.models import AnalysisTable, AnalysisVersion
from flask_admin import Admin
from flask_admin.contrib.sqla import ModelView


def setup_admin(app, db):
    admin = Admin(app, name="materializationengine")
    admin.add_view(ModelView(AnalysisVersion, db.session))
    admin.add_view(ModelView(AnalysisTable, db.session))
    return admin
