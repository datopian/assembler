import os

import copy
import datapackage
import itertools

from datapackage_pipelines.wrapper import ingest, spew

parameters, dp, res_iter = ingest()


def modify_datapackage(dp):
    dp['resources'].append({
        'name': 'datasets',
        'path': 'nonexistent',
        'schema': {
            'fields': [
                {'name': 'id', 'type': 'string'},
                {'name': 'name', 'type': 'string'},
                {'name': 'title', 'type': 'string'},
                {'name': 'description', 'type': 'string'},
                {'name': 'resources', 'type': 'array',
                 'es:itemType': 'object', 'es:index': False},
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
    yield dp


spew(modify_datapackage(dp),
     itertools.chain(res_iter, [dataset_resource(dp)]))
