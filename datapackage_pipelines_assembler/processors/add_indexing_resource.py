import itertools

from tableschema_elasticsearch import Storage

from datapackage_pipelines.wrapper import ingest, spew

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
    ret['datapackage'] = dp
    yield ret


if __name__ == "__main__":
    parameters, dp, res_iter = ingest()
    spew(modify_datapackage(dp),
         itertools.chain(res_iter, [dataset_resource(dp)]))
