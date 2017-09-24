#!/bin/sh
rm -f celeryd.pid
rm -f celerybeat.pid
dpp init
python3 -m celery -b redis://redis:6379/6 -A datapackage_pipelines.app -l INFO beat &
python3 -m celery -b redis://redis:6379/6 --concurrency=1 -A datapackage_pipelines.app -Q datapackage-pipelines-management -l INFO worker &
python3 -m celery -b redis://redis:6379/6 --concurrency=4 -A datapackage_pipelines.app -Q datapackage-pipelines -l INFO worker &
dpp serve


