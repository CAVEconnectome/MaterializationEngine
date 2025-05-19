FROM debian:bookworm-slim AS gcloud_builder

RUN apt-get update && \
    apt-get install -y --no-install-recommends curl ca-certificates gnupg bash python3-setuptools debianutils && \
    rm -rf /var/lib/apt/lists/*

RUN curl -sSL https://sdk.cloud.google.com | bash -s -- --disable-prompts --install-dir=/opt/gcloud_install_temp

FROM tiangolo/uwsgi-nginx-flask:python3.10

USER root

ENV UWSGI_INI=/app/uwsgi.ini
ENV PATH=/opt/google-cloud-sdk/bin:$PATH

COPY --from=gcloud_builder /opt/gcloud_install_temp/google-cloud-sdk /opt/google-cloud-sdk


RUN apt-get update && \
    apt-get install -y --no-install-recommends postgresql-client && \
    rm -rf /var/lib/apt/lists/* && \
    mkdir -p /home/nginx/.cloudvolume/secrets && \
    chown -R nginx /home/nginx && \
    usermod -d /home/nginx -s /bin/bash nginx

WORKDIR /app

COPY requirements.txt /app/
RUN python -m pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt

COPY . /app
COPY override/timeout.conf /etc/nginx/conf.d/timeout.conf
COPY gracefully_shutdown_celery.sh /home/nginx/


RUN chmod +x /home/nginx/gracefully_shutdown_celery.sh && \
    mkdir -p /home/nginx/tmp/shutdown && \
    chmod +x /app/entrypoint.sh # Assuming entrypoint.sh is copied by 'COPY . /app'


USER root
