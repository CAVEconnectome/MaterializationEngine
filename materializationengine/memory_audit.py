"""Per-request memory accounting, designed to survive the request being killed.

Why this exists
---------------
On minniev7 the api pods run on e2-small nodes (1358Mi allocatable) whose memory is
99% committed by requests, and the materialize container has no memory limit. A request
that allocates a few hundred MB therefore does not fail cleanly -- it pushes the *node*
over its eviction threshold, and the kubelet evicts the pod or the kernel OOM-kills
uwsgi mid-request. Measured 2026-08-18: 8 kernel OOM kills in 3h (uwsgi x4), pods
evicted at ~970Mi against a 670Mi request, and 5 nodes flapping NotReady.

The requests responsible are invisible in the ordinary logs, for two reasons:

1. A worker killed by SIGKILL runs no exception handler, no ``after_request``, and no
   ``teardown_request``. Anything that logs on the way *out* of a request logs nothing
   at all for exactly the requests we need to identify.
2. The v3 query endpoints take the table and filters in the POST *body*, so the access
   log collapses every one of them onto a single URI. 127 of the 219 requests killed by
   harakiri in 24h were ``POST /materialize/api/v3/datastack/minnie65_public/query`` --
   one line, 127 different queries.

So this module logs on the way *in* as well as on the way out. A ``start`` record with
no matching ``end`` record is a request that died mid-flight, and it carries enough
identity (table, filter shapes, limit) to find the query that did it. Pair them up with:

    jq -c 'select(.source=="memory_audit")' \
      | jq -s 'group_by(.rid) | map(select(length==1)) | .[][0]'

Cost is one ``/proc/self/status`` read per request boundary (~30us), so this is left on
in production; set ``MEMORY_AUDIT_ENABLED=false`` to disable.
"""

import json
import logging
import os
import time
import uuid

from flask import g, request

logger = logging.getLogger("materializationengine.memory_audit")

# /proc is read directly rather than via psutil because VmHWM (the peak RSS the worker
# has ever reached) has no portable psutil equivalent, and it is the field that reveals a
# request whose transient allocation dwarfs its steady state -- a pandas frame that is
# built, copied, serialised and freed can set a new high-water mark hundreds of MB above
# the RSS visible once the request returns.
_STATUS = "/proc/self/status"
_KEYS = ("VmRSS", "VmHWM")


def _mem_kb():
    """Current and peak RSS in KiB, or (None, None) off Linux (e.g. a dev mac)."""
    try:
        out = {}
        with open(_STATUS, "rb") as fh:
            for raw in fh:
                if raw.startswith(b"Vm"):
                    name, _, rest = raw.partition(b":")
                    key = name.decode()
                    if key in _KEYS:
                        out[key] = int(rest.split()[0])
                        if len(out) == len(_KEYS):
                            break
        return out.get("VmRSS"), out.get("VmHWM")
    except Exception:
        return None, None


def _mb(kb):
    return None if kb is None else round(kb / 1024.0, 1)


def _filter_values(v, budget, max_values):
    """Filter values verbatim when the list is short, otherwise just its length.

    A LIST in the output means "these are the actual values"; an INT means "this many values,
    elided". Consumers tell them apart by type.

    Values are stringified on purpose. A root id such as 864691135406097394 exceeds 2**53, so
    any consumer that re-parses the log line as JSON with double-precision numbers silently
    corrupts its low digits -- destroying the one property the id is logged for.

    ``budget`` is a single-element list used as a shared counter across the whole summary, so a
    body filtering many columns cannot add up to an enormous log line even when every
    individual column sits under ``max_values``.
    """
    if isinstance(v, (list, tuple)):
        n = len(v)
        if n > max_values or n > budget[0]:
            return n
        budget[0] -= n
        return [str(x) for x in v]
    if budget[0] <= 0:
        return 1
    budget[0] -= 1
    return [str(v)]


def _summarize_filters(body, max_keys=12, max_values=8, total_values=64):
    """Shape of a query body, plus the filter values when there are few enough to be useful.

    Filter dicts routinely hold hundreds of thousands of root ids, and logging those verbatim
    would reproduce the memory problem being diagnosed -- so anything longer than
    ``max_values`` is recorded as a count, and a global ``total_values`` budget bounds the
    record no matter how many columns are filtered.

    Short lists ARE logged, because shape alone proved insufficient in practice: on 2026-08-18
    two root ids were OOM-killing api pods with oversized synapse queries, and every audit
    record showed an indistinguishable ``{synapses_pni_2: {post_pt_root_id: 1}}`` -- one value,
    value unknown -- so the culprits had to be recovered from the caller's logs instead. A
    one-value filter is precisely the case where the value is both tiny to log and decisive.
    """
    q = {}
    budget = [total_values]
    for key in ("table", "limit", "offset", "timestamp", "desired_resolution"):
        val = body.get(key)
        if val is not None and not isinstance(val, (dict, list)):
            q[key] = val
    for key in (
        "filter_in_dict",
        "filter_equal_dict",
        "filter_greater_dict",
        "filter_less_dict",
        "filter_out_dict",
        "filter_spatial_dict",
        "filter_regex_dict",
    ):
        spec = body.get(key)
        if not isinstance(spec, dict):
            continue
        shape = {}
        for tbl, cols in list(spec.items())[:max_keys]:
            if isinstance(cols, dict):
                # {table: {column: [values]}} -> the values, or a count when too long
                shape[tbl] = {
                    c: _filter_values(v, budget, max_values)
                    for c, v in list(cols.items())[:max_keys]
                }
            elif isinstance(cols, (list, tuple)):
                shape[tbl] = _filter_values(cols, budget, max_values)
        if shape:
            q[key] = shape
    for key in ("joins", "select_columns", "suffixes"):
        val = body.get(key)
        if isinstance(val, (list, tuple)):
            q[f"n_{key}"] = len(val)
        elif isinstance(val, dict):
            q[f"n_{key}"] = len(val)
    return q


def _identity(max_body_bytes, max_filter_values=8):
    """Enough of the request to identify the query, cheap and never raising."""
    ident = {}
    try:
        length = request.content_length or 0
        if length:
            ident["body_bytes"] = length
        if request.method not in ("POST", "PUT", "PATCH"):
            return ident
        # Deliberately skipped for very large bodies: get_json caches the raw bytes and
        # the parsed object, so introspecting a 50MB filter payload would add tens of MB
        # to the very request most likely to be near the eviction threshold. The size
        # alone already flags it.
        if length > max_body_bytes:
            ident["body_introspected"] = False
            return ident
        body = request.get_json(silent=True)
        if isinstance(body, dict):
            ident.update(_summarize_filters(body, max_values=max_filter_values))
    except Exception:
        pass
    return ident


def init_memory_audit(app):
    """Register the request hooks. Safe to call unconditionally."""
    if not app.config.get("MEMORY_AUDIT_ENABLED", True):
        app.logger.info("memory_audit disabled")
        return

    warn_delta_mb = float(app.config.get("MEMORY_AUDIT_WARN_DELTA_MB", 100))
    max_body_bytes = int(app.config.get("MEMORY_AUDIT_MAX_BODY_BYTES", 2 * 1024 * 1024))
    # Filters at or below this length are logged with their VALUES, which is what makes an
    # offending request identifiable; longer ones degrade to a count. Keep it small -- a filter
    # can legitimately carry hundreds of thousands of root ids.
    max_filter_values = int(app.config.get("MEMORY_AUDIT_MAX_FILTER_VALUES", 8))
    # /health runs on every probe on every pod and never allocates, so its start/end pair
    # is pure noise -- except that it is also the endpoint whose stalls take a pod out of
    # the Service (35 of 219 harakiri kills in 24h were GET /health). It is therefore not
    # skipped on the way in; it is only quiet on the way out when nothing interesting
    # happened. See _should_log_end.
    quiet_paths = set(app.config.get("MEMORY_AUDIT_QUIET_PATHS", ["/health"]))
    pid = os.getpid()

    def _emit(payload, level=logging.INFO):
        payload["source"] = "memory_audit"
        payload["pid"] = pid
        try:
            logger.log(level, json.dumps(payload, default=str))
        except Exception:
            pass

    @app.before_request
    def _memory_audit_start():
        try:
            rss, hwm = _mem_kb()
            g._ma = {
                "rid": uuid.uuid4().hex[:12],
                "t0": time.monotonic(),
                "rss": rss,
                "hwm": hwm,
            }
            payload = {
                "phase": "start",
                "rid": g._ma["rid"],
                "method": request.method,
                "path": request.path,
                "endpoint": request.endpoint,
                "rss_mb": _mb(rss),
            }
            payload.update(_identity(max_body_bytes, max_filter_values))
            _emit(payload)
        except Exception:
            pass

    def _should_log_end(state, delta_mb, dur_s):
        if request.path not in quiet_paths:
            return True
        # A quiet path still reports when it misbehaved, since a slow or memory-hungry
        # /health is the leading indicator of the pod dropping out of the Service.
        return dur_s >= 5 or (delta_mb or 0) >= 10

    @app.teardown_request
    def _memory_audit_end(exc=None):
        state = getattr(g, "_ma", None)
        if not state:
            return
        try:
            rss, hwm = _mem_kb()
            dur_s = round(time.monotonic() - state["t0"], 3)
            delta_mb = None
            if rss is not None and state["rss"] is not None:
                delta_mb = round((rss - state["rss"]) / 1024.0, 1)
            hwm_grew_mb = None
            if hwm is not None and state["hwm"] is not None:
                hwm_grew_mb = round((hwm - state["hwm"]) / 1024.0, 1)

            if not _should_log_end(state, delta_mb, dur_s):
                return

            payload = {
                "phase": "end",
                "rid": state["rid"],
                "method": request.method,
                "path": request.path,
                "endpoint": request.endpoint,
                "dur_s": dur_s,
                "rss_before_mb": _mb(state["rss"]),
                "rss_after_mb": _mb(rss),
                "delta_mb": delta_mb,
                # Peak for the life of the worker; hwm_grew_mb > 0 means this request set
                # a new high-water mark, which is the transient cost the pod actually paid
                # even if delta_mb came back near zero.
                "peak_mb": _mb(hwm),
                "hwm_grew_mb": hwm_grew_mb,
                "exc": type(exc).__name__ if exc else None,
            }
            hot = max(delta_mb or 0, hwm_grew_mb or 0) >= warn_delta_mb
            _emit(payload, logging.WARNING if hot else logging.INFO)
        except Exception:
            pass

    app.logger.info(
        "memory_audit enabled (warn_delta_mb=%s, max_body_bytes=%s, max_filter_values=%s)",
        warn_delta_mb,
        max_body_bytes,
        max_filter_values,
    )
