FROM frictionlessdata/datapackage-pipelines:latest

ADD . /app

WORKDIR /app
RUN pip install . "rfc3986<1.0"
RUN apk add --update postgresql-client

ENTRYPOINT ["/app/startup.sh"]
