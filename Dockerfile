FROM gcr.io/neuromancer-seung-import/pychunkedgraph:graph-tool_dracopy

ENV UWSGI_INI /app/uwsgi.ini
RUN pip install --upgrade pip
COPY requirements.txt /app/.
RUN pip install -r requirements.txt
COPY . /app
RUN chmod +x /entrypoint.sh
WORKDIR /app