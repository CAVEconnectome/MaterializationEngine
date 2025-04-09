FROM tiangolo/uwsgi-nginx-flask:python3.12

COPY . /app
COPY override/timeout.conf /etc/nginx/conf.d/timeout.conf
COPY gracefully_shutdown_celery.sh /home/nginx/


RUN chmod +x /home/nginx/gracefully_shutdown_celery.sh && \
    mkdir -p /home/nginx/tmp/shutdown && \
    chmod +x /app/entrypoint.sh # Assuming entrypoint.sh is copied by 'COPY . /app'


USER root
