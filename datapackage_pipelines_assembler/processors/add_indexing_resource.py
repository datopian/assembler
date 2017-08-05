import os

import copy
import datapackage
import itertools

from datapackage_pipelines.wrapper import ingest, spew

parameters, dp, res_iter = ingest()


def modify_datapackage(dp):
    dp['resources'].append({
        'name': '__datasets',
        'path': 'nonexistent',
        'schema': {
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
                    ]
                 }
                },
            ],
            'primaryKey': ['id']
        }
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


spew(modify_datapackage(dp),
     itertools.chain(res_iter, [dataset_resource(dp)]))
