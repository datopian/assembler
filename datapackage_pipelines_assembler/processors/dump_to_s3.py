import itertools

import copy
from tableschema_elasticsearch import Storage

from datapackage_pipelines.wrapper import ingest, spew
from datapackage_pipelines_aws.processors.dump.to_s3 import S3Dumper


SCHEMA = {
    'fields': [
        {'name': 'id', 'type': 'string'},
        {'name': 'name', 'type': 'string'},
        {'name': 'title', 'type': 'string'},
        {'name': 'description', 'type': 'string'},
        {'name': 'datapackage', 'type': 'object', 'es:index': False},
        {'name': 'datahub', 'type': 'object',
         'es:schema': {
             'fields': [
                 {'name': 'owner', 'type': 'string'},
                 {'name': 'ownerid', 'type': 'string'},
                 {'name': 'findability', 'type': 'string'},
             ]
         }
         },
    ],
    'primaryKey': ['id']
}


def create_index(index_name):
    storage = Storage()
    storage.create(index_name, ('dataset', SCHEMA))


def modify_datapackage(dp):
    dp['resources'].append({
        'name': '__datasets',
        'path': 'nonexistent',
        'schema': SCHEMA
    })
    return dp


def dataset_resource(dp):
    ret = dict(
        (k, dp.get(k))
        for k in [
            'id',
            'name',
            'title',
            'description',
            'datahub'
        ]
    )
    dp = copy.deepcopy(dp)
    dp['resources'].pop()
    ret['datapackage'] = dp
    yield ret


class MyS3Dumper(S3Dumper):
    
    def prepare_datapackage(self, datapackage, params):
        datapackage = super(MyS3Dumper, self).prepare_datapackage(datapackage, params)
        return modify_datapackage(datapackage)

    def handle_resources(self, datapackage, resource_iterator, parameters, stats):
        yield from super(MyS3Dumper, self).handle_resources(datapackage, resource_iterator, parameters, stats)
        yield dataset_resource(datapackage)


if __name__ == "__main__":
    MyS3Dumper()()