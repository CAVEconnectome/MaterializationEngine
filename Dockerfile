FROM tiangolo/uwsgi-nginx-flask:python3.10

USER root

ENV UWSGI_INI /app/uwsgi.ini
ENV PATH /home/nginx/google-cloud-sdk/bin:/root/google-cloud-sdk/bin:$PATH

RUN apt-get update && \
    apt-get install -y --no-install-recommends postgresql-client curl && \
    rm -rf /var/lib/apt/lists/* && \
    mkdir -p /home/nginx/.cloudvolume/secrets && \
    chown -R nginx /home/nginx && \
    usermod -d /home/nginx -s /bin/bash nginx

WORKDIR /app
COPY requirements.txt /app/
RUN python -m pip install --upgrade pip && \
    pip install -r requirements.txt

COPY . /app
COPY override/timeout.conf /etc/nginx/conf.d/timeout.conf
COPY gracefully_shutdown_celery.sh /home/nginx/
RUN chmod +x /home/nginx/gracefully_shutdown_celery.sh && \
    mkdir -p /home/nginx/tmp/shutdown && \
    chmod +x /entrypoint.sh

RUN curl -sSL https://sdk.cloud.google.com | bash

USER root
