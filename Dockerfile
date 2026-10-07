ARG BASE_IMAGE=tiangolo/uwsgi-nginx-flask:python3.12

# Build stage: compiles the Python dependencies (uwsgi has no wheel and needs gcc).
# Only its installed files are copied into the image, so gcc and uv stay out of it.
FROM ${BASE_IMAGE} AS deps
COPY --from=ghcr.io/astral-sh/uv:0.9 /uv /usr/bin/uv
RUN apt-get update && apt-get install -y --no-install-recommends gcc libc6-dev \
  && rm -rf /var/lib/apt/lists/*
ENV UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy \
    UV_PYTHON_DOWNLOADS=0 \
    UV_PROJECT_ENVIRONMENT=/usr/local/
WORKDIR /app
COPY uv.lock pyproject.toml ./
RUN --mount=type=cache,target=/root/.cache/uv \
  UV_VENV_ARGS="--system-site-packages" uv sync --frozen --no-install-project --no-default-groups --group deploy

FROM ${BASE_IMAGE}
# Postgres 18 client (pg_dump/psql for the upload transfer) and pg_repack (table maintenance).
# The apt lists are removed so they do not ship in the layer.
RUN apt-get update && apt-get install -y --no-install-recommends curl ca-certificates gnupg lsb-release \
  && install -d /usr/share/postgresql-common/pgdg \
  && curl -fsSL https://www.postgresql.org/media/keys/ACCC4CF8.asc \
       -o /usr/share/postgresql-common/pgdg/apt.postgresql.org.asc \
  && echo "deb [signed-by=/usr/share/postgresql-common/pgdg/apt.postgresql.org.asc] https://apt.postgresql.org/pub/repos/apt $(lsb_release -cs)-pgdg main" \
       > /etc/apt/sources.list.d/pgdg.list \
  && apt-get update \
  && apt-get install -y --no-install-recommends postgresql-client-18 postgresql-18-repack \
  && rm -rf /var/lib/apt/lists/*
RUN mkdir -p /home/nginx/.cloudvolume/secrets /home/nginx/tmp/shutdown \
  && chown -R nginx /home/nginx \
  && usermod -d /home/nginx -s /bin/bash nginx

# The dependencies the build stage installed into /usr/local (site-packages and their scripts:
# celery, uwsgi, ray, ...). The base image ships its own, newer flask/werkzeug/click/uwsgi;
# uv sync replaced them with the locked versions in the build stage, so site-packages is
# replaced whole here. Copying it over the base's would leave both versions' files and
# metadata side by side. No gcloud SDK: nothing uses it since the Cloud SQL imports and
# exports went to the Admin REST API (cloudsql_admin.py).
RUN rm -rf /usr/local/lib/python3.12/site-packages
COPY --from=deps /usr/local/lib/python3.12/site-packages /usr/local/lib/python3.12/site-packages
COPY --from=deps /usr/local/bin /usr/local/bin

ENV UWSGI_INI=/app/uwsgi.ini \
    PATH="/app/.venv/bin:$PATH" \
    PYTHONNOUSERSITE=1

COPY override/timeout.conf /etc/nginx/conf.d/timeout.conf
COPY --chmod=755 gracefully_shutdown_celery.sh /home/nginx/
RUN chmod +x /entrypoint.sh
WORKDIR /app

COPY . /app
