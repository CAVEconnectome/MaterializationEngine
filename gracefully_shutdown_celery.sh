#!/bin/bash -e
echo "*** WARNING: Celery worker shutdown started"

# set celery broker url
export CELERY_BROKER_URL=redis://:${REDIS_PASSWORD}@${REDIS_HOST}:${REDIS_PORT}/0

# prevent the worker accepting new tasks
echo "*** EVENT: Worker not accepting new tasks"
celery control --broker $CELERY_BROKER_URL --destination worker.process@$HOSTNAME cancel_consumer process
sleep 5
# loop until all active task are finished
echo "*** EVENT: Waiting for remaining tasks to finish"
while : ; do
    IS_ACTIVE=$( celery inspect --broker $CELERY_BROKER_URL --destination worker.process@$HOSTNAME --json active | python3 -c "import json; active_tasks = json.loads(input())['worker.process@$HOSTNAME']; print(len(active_tasks))")
    if (( $IS_ACTIVE > 0 ))
    then
        sleep 10
        dt=$(date '+%d/%m/%Y %H:%M:%S');
        echo -n -e "*** STATUS: Number of tasks being run on worker.process@${HOSTNAME} is: ${IS_ACTIVE}. Current time: ${dt}, waiting...\r"
    else
        echo -n -e "*** STATUS: Number of tasks being run on worker.process@${HOSTNAME} is: ${IS_ACTIVE}. Current time: ${dt}, stopping...\r"
        echo "*** EVENT: No active running tasks found, shutting down"
        break
    fi

done
echo "*** EVENT: Sending kill signal to sidecars..."
touch /home/nginx/tmp/shutdown/kill_sidecar



