from datapackage_pipelines.wrapper import spew, ingest

parameters, datapackage, res_iter = ingest()


def generate_preview(res):
    for i, row in enumerate(res):
        if i < int(parameters.get('limit', 10000)):
            yield row


def process_resources(res_iter_):
    for res in res_iter_:
        yield generate_preview(res)


spew(datapackage, process_resources(res_iter))
