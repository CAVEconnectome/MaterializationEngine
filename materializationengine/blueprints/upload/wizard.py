from flask import Blueprint, render_template, session, jsonify, current_app

__version__ = "4.35.0"


wizard_bp = Blueprint('wizard', __name__, url_prefix='/materialize/wizard')

@wizard_bp.route("/")
def wizard():
    try:
        if "current_step" not in session:
            session["current_step"] = 0

        current_step = session["current_step"]

        return render_template(
            "/csv_upload/main.html",
            current_step=current_step,
            total_steps=5,
            version=current_app.config.get("VERSION", __version__),
        )
    except Exception as e:
        current_app.logger.error(f"Error rendering wizard: {str(e)}")
        return jsonify({"status": "error", "message": str(e)}), 500