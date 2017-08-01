import logging

from datapackage_pipelines.utilities.extended_json import LazyJsonLine
from datapackage_pipelines.utilities.extended_json import json

from datapackage_pipelines.wrapper import spew, ingest

parameters, datapackage, res_iter = ingest()
res_name = parameters.get('resource', datapackage['resources'][0]['name'])


def show_sample(res):
    logging.info('SAMPLE OF LINES from %s', res.spec['name'])
    for i, row in enumerate(res):
        if i < 10:
            if isinstance(row, LazyJsonLine):
                logging.info('#%s: %s', i, row._evaluate())
            else:
                logging.info('#%s: %r', i, row)
        yield row


def process_resources(res_iter_):
    for res in res_iter_:
        logging.info('? from %s', res.spec['name'])
        if res.spec['name'] == res_name:
            yield show_sample(res)
        else:
            yield res


logging.info(json.dumps(datapackage, indent=2))

spew(datapackage, process_resources(res_iter))
