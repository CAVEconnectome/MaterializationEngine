FROM gcr.io/neuromancer-seung-import/pychunkedgraph:graph-tool_dracopy

ENV UWSGI_INI /app/uwsgi.ini
COPY requirements.txt /app/.
RUN mv /usr/local/lib/python3.7/site-packages/pip/_internal/index /usr/local/lib/python3.7/site-packages/pip/_internal/index_bak
RUN python -m pip install --upgrade pip
RUN pip install -r requirements.txt
COPY . /app
COPY override/timeout.conf /etc/nginx/conf.d/timeout.conf
COPY gracefully_shutdown_celery.sh /home/nginx
RUN chmod +x /home/nginx/gracefully_shutdown_celery.sh
RUN mkdir -p /home/nginx/tmp/shutdown 
RUN chmod +x /entrypoint.sh
WORKDIR /app