# Run a test server.
import os

from werkzeug.serving import WSGIRequestHandler

from materializationengine.app import create_app
from materializationengine.celery_worker import create_celery

HOME = os.path.expanduser("~")

application = create_app()
celery = create_celery(application)

if __name__ == "__main__":

    WSGIRequestHandler.protocol_version = "HTTP/1.1"

    application.run(host='0.0.0.0',
                    port=int(os.environ.get('FLASK_RUN_PORT', 8080)),
                    debug=True,
                    threaded=True,
                    ssl_context='adhoc')
