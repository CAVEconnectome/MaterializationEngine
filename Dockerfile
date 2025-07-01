FROM ghcr.io/astral-sh/uv:python3.12-bookworm-slim AS builder
RUN apt-get update && apt-get install -y gcc

# Enable bytecode compilation
ENV UV_COMPILE_BYTECODE=1
ENV UV_LINK_MODE=copy
ENV UV_PYTHON_DOWNLOADS=0

# Install the project's dependencies using the lockfile and settings
WORKDIR /app

# Copy only the necessary files for dependency installation
COPY uv.lock pyproject.toml ./

RUN --mount=type=cache,target=/root/.cache/uv \
  uv sync --frozen --no-install-project --no-default-groups

# COPY . ./
# RUN --mount=type=cache,target=/root/.cache/uv \
#   uv sync --frozen --no-default-groups


FROM tiangolo/uwsgi-nginx-flask:python3.12
COPY --from=builder --chown=nginx:nginx /app/.venv /venv
ENV UWSGI_INI /app/uwsgi.ini
ENV PATH="/venv/bin:$PATH"
ENV PYTHONHOME="/venv"
ENV PYTHONNOUSERSITE=1
# Symlink python3 and python to venv's python
RUN ln -sf /venv/bin/python3 /usr/local/bin/python3 && \
    ln -sf /venv/bin/python /usr/local/bin/python
RUN ln -sf /venv/bin/pip /usr/local/bin/pip

RUN mkdir -p /home/nginx/.cloudvolume/secrets \
  && chown -R nginx /home/nginx \
  && usermod -d /home/nginx -s /bin/bash nginx 
# COPY requirements.txt /app/.
# RUN python -m pip install --upgrade pip
# RUN pip install -r requirements.txt
# Install gcloud SDK as root and set permissions
# Install gcloud SDK as root

COPY override/timeout.conf /etc/nginx/conf.d/timeout.conf
COPY gracefully_shutdown_celery.sh /home/nginx
RUN chmod +x /home/nginx/gracefully_shutdown_celery.sh
RUN mkdir -p /home/nginx/tmp/shutdown 
RUN chmod +x /entrypoint.sh
WORKDIR /app
USER nginx
RUN curl -sSL https://sdk.cloud.google.com | bash
ENV PATH /venv/bin:/home/nginx/google-cloud-sdk/bin:/root/google-cloud-sdk/bin:$PATH
USER root
COPY . /app
