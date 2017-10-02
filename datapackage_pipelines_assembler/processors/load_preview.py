import collections

from datapackage_pipelines.wrapper import spew, ingest

parameters, datapackage, res_iter = ingest()
limit = int(parameters.get('limit', 200))


def generate_preview(res):
    for i, row in enumerate(res):
        if i < limit:
            yield row


def process_resources(res_iter_):
    for res in res_iter_:
        if res.spec['rowcount'] > limit:
            yield generate_preview(res)
        else:
            collections.deque(res, maxlen=0)
            yield []


spew(datapackage, process_resources(res_iter))
