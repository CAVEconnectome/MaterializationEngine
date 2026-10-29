"""Every Celery task runs in a Flask app context, including the ones the beat schedules
name and those with a base of their own (LockedTask).

configure_app used to push an app context that was never popped, which covered any
task that missed ContextTask. Since that was removed (#265), a task without it fails
on its first current_app access: run_periodic_materialization did, on minnie.

Checked in a subprocess: the test session's Celery app is already configured, and the
failure depends on the order in which create_celery configures a fresh one.
"""

import json
import subprocess
import sys
import textwrap

SCHEDULES = [
    {
        "name": "daily",
        "minute": 10,
        "hour": 8,
        "task": "run_periodic_materialization",
        "datastack_params": {"days_to_expire": 2, "merge_tables": False, "datastack": "test"},
    },
    {"name": "live", "minute": 0, "hour": "*", "task": "run_periodic_database_update",
     "datastack_params": {"datastack": "test"}},
    {"name": "expired", "minute": 0, "hour": 10, "task": "remove_expired_databases",
     "datastack_params": {"delete_threshold": 3}},
]

SCRIPT = textwrap.dedent(
    """
    import json, sys
    import flask
    from materializationengine.app import create_app
    from materializationengine.celery_worker import create_celery

    app = create_app(config_name="testing")
    app.config["BEAT_SCHEDULES"] = json.loads(sys.argv[1])
    celery = create_celery(app)
    celery.loader.import_default_modules()
    tasks = {n: t for n, t in celery.tasks.items() if not n.startswith("celery.")}
    assert not flask.has_app_context()
    outside = []
    for name, task in tasks.items():
        # the task's __call__ (ContextTask, LockedTask) with its body replaced
        task.run = lambda *a, **k: flask.has_app_context()
        if not task():
            outside.append(name)
    print(json.dumps({"count": len(tasks), "outside_app_context": sorted(outside)}))
    """
)


class TestContextTask:
    def test_every_task_runs_in_an_app_context(self):
        out = subprocess.run(
            [sys.executable, "-c", SCRIPT, json.dumps(SCHEDULES)],
            capture_output=True,
            text=True,
            timeout=120,
        )
        assert out.returncode == 0, out.stderr[-3000:]
        result = json.loads(out.stdout.strip().splitlines()[-1])
        assert result["count"] > 50
        assert result["outside_app_context"] == []
