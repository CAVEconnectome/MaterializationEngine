FROM tiangolo/uwsgi-nginx-flask:python3.12 AS builder
RUN apt-get update && apt-get install -y gcc
RUN pip install uv
# Enable bytecode compilation
ENV UV_COMPILE_BYTECODE=1
ENV UV_LINK_MODE=copy
ENV UV_PYTHON_DOWNLOADS=0
RUN mkdir -p /home/nginx/.cloudvolume/secrets \
  && chown -R nginx /home/nginx \
  && usermod -d /home/nginx -s /bin/bash nginx 
# COPY requirements.txt /app/.
# RUN python -m pip install --upgrade pip
# RUN pip install -r requirements.txt
# Install gcloud SDK as root and set permissions
# Install gcloud SDK as root
USER nginx
RUN curl -sSL https://sdk.cloud.google.com | bash
ENV PATH /app/.venv/bin:/home/nginx/google-cloud-sdk/bin:/root/google-cloud-sdk/bin:$PATH
USER root
# Install the project's dependencies using the lockfile and settings
WORKDIR /app

# Copy only the necessary files for dependency installation
COPY uv.lock pyproject.toml ./

ENV UV_PROJECT_ENVIRONMENT="/usr/local/"
RUN --mount=type=cache,target=/root/.cache/uv \
  UV_VENV_ARGS="--system-site-packages" uv sync --frozen --no-install-project --no-default-groups

# COPY . ./
# RUN --mount=type=cache,target=/root/.cache/uv \
#   uv sync --frozen --no-default-groups

ENV UWSGI_INI /app/uwsgi.ini
ENV PATH="/app/.venv/bin:$PATH"
ENV PYTHONNOUSERSITE=1


COPY override/timeout.conf /etc/nginx/conf.d/timeout.conf
COPY gracefully_shutdown_celery.sh /home/nginx
RUN chmod +x /home/nginx/gracefully_shutdown_celery.sh
RUN mkdir -p /home/nginx/tmp/shutdown 
RUN chmod +x /entrypoint.sh
WORKDIR /app

COPY . /app
