FROM frictionlessdata/datapackage-pipelines:latest


RUN apk --update --no-cache add libpq postgresql-dev libffi libffi-dev build-base python3-dev ca-certificates
RUN update-ca-certificates

WORKDIR /app
RUN apk add --update postgresql-client

ADD requirements.txt /app/requirements.txt
RUN pip install -r /app/requirements.txt

ADD . /app

ENTRYPOINT ["/app/startup.sh"]
