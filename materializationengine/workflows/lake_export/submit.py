"""Submitting a lake export as a RayJob (rayjobs/entrypoints/lake_export.py).

The export endpoints call this: the entrypoint's command line, and the job's workers
sized from the deployment's settings -- worker_cpus CPUs each (one range stream per
CPU), worker_memory, and as many as the job's streams need unless capped. The
autoscaler starts workers only as tasks queue, so a job of small tables uses few.
"""

import json
import shlex
from typing import Dict, List, Optional, Sequence

from materializationengine.workflows.lake_export.settings import ExportSettings

MODULE = "materializationengine.rayjobs.entrypoints.lake_export"


def entrypoint(datastack: str, version: int, job_id: str, tables: Optional[Sequence[str]] = None,
               version_job: bool = False, views: bool = False, output_specs: Optional[List[Dict]] = None,
               config: Optional[Dict] = None, name_suffix: Optional[str] = None,
               max_streams: Optional[int] = None) -> str:
    """The RayJob's command line (every value shell-quoted)."""
    parts = ["python", "-m", MODULE, "--datastack", datastack, "--version", str(int(version)), "--job-id", job_id]
    if tables:
        parts += ["--tables", *tables]
    if version_job:
        parts.append("--version-job")
    if views:
        parts.append("--views")
    if output_specs is not None:
        parts += ["--output-specs", json.dumps(output_specs)]
    if config is not None:
        parts += ["--config", json.dumps(config)]
    if name_suffix:
        parts += ["--name-suffix", name_suffix]
    if max_streams:
        parts += ["--max-streams", str(int(max_streams))]
    return " ".join(shlex.quote(p) for p in parts)


def submit(settings: ExportSettings, command: str, name_prefix: str, metadata: Dict,
           max_streams: Optional[int] = None, num_workers: Optional[int] = None, views: bool = False) -> str:
    """Create the RayJob; its name. num_workers overrides the sizing (?workers=); views: the
    job has a views stage, so its cluster may grow to the stage's workers."""
    from materializationengine.rayjobs import submit_rayjob

    return submit_rayjob(
        entrypoint=command,
        name_prefix=name_prefix,
        num_workers=num_workers or settings.workers_for(max_streams, views=views),
        metadata=metadata,
        worker_cpus=settings.worker_cpus,
        worker_memory=settings.worker_memory,
    )
