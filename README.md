[![Build Status](https://travis-ci.org/datahq/assembler.svg?branch=master)](https://travis-ci.org/datahq/assembler)

# Assembler

The factory floor on which everything is produced.

### Environment variables

Set of env variables needed to run assembler

```
AWS_ACCESS_KEY_ID=<<aWSsecREtKey>>
AWS_SECRET_ACCESS_KEY=<<aWSsecREtACCesSKey>>
DPP_ELASTICSEARCH=<<https://elastic-search-url.com>>
PKGSTORE_BUCKET=<<s3.bucket.package.store>>
SOURCESPEC_REGISTRY_DB_ENGINE<<postgresql://database.url:port/db_name>>
```
### Tests

To setup Test environment locally you will need several different services to be running in background

* Local [ElasticSearch](elasticsearch.com) (5.0 or higher) running on port 9200
* moto_server provided by [moto](https://github.com/spulec/moto) on port 5000
  * run `moto_server`
* Postgresql database 9.5 or higher
  * psql -U postgres -c "create user datahub password 'secret' createdb;"
  * psql -U postgres -c "create database datahq owner=datahub;"
