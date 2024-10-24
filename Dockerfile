FROM tiangolo/uwsgi-nginx-flask:python3.10

ENV UWSGI_INI /app/uwsgi.ini
RUN mkdir -p /home/nginx/.cloudvolume/secrets \
  && chown -R nginx /home/nginx \
  && usermod -d /home/nginx -s /bin/bash nginx 
COPY requirements.txt /app/.
RUN python -m pip install --upgrade pip
RUN pip install -r requirements.txt
# Install gcloud SDK as root and set permissions
# Install gcloud SDK as root
COPY . /app
COPY override/timeout.conf /etc/nginx/conf.d/timeout.conf
COPY gracefully_shutdown_celery.sh /home/nginx
RUN chmod +x /home/nginx/gracefully_shutdown_celery.sh
RUN mkdir -p /home/nginx/tmp/shutdown 
RUN chmod +x /entrypoint.sh
WORKDIR /app
USER nginx
RUN curl -sSL https://sdk.cloud.google.com | bash
ENV PATH /home/nginx/google-cloud-sdk/bin:/root/google-cloud-sdk/bin:$PATH
USER root
