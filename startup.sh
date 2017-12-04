#!/bin/sh
rm -f celeryd.pid
rm -f celerybeat.pid
export DPP_REDIS_HOST=redis
export DPP_CELERY_BROKER=redis://redis:6379/6
dpp init
echo "Deleting `redis-cli -n 6 -h redis KEYS '*' | wc -l` keys"
redis-cli -h redis -n 6 FLUSHDB
echo "Remaining `redis-cli -n 6 -h redis KEYS '*' | wc -l` keys"
SCHEDULER=1 python3 -m celery -b $DPP_CELERY_BROKER -A datapackage_pipelines.app -l INFO beat &
python3 -m celery -n mgmt -b $DPP_CELERY_BROKER --concurrency=1 -A datapackage_pipelines.app -Q datapackage-pipelines-management -l INFO worker &
python3 -m celery -n work -b $DPP_CELERY_BROKER --concurrency=4 -A datapackage_pipelines.app -Q datapackage-pipelines -l INFO worker &
dpp serve
