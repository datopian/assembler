import datetime
import itertools

from datapackage_pipelines.utilities.resources import PROP_STREAMING
from datapackage_pipelines.wrapper import ingest, spew

SCHEMA = {
    'fields': [
        {'name': 'timestamp', 'type': 'datetime'},
        {'name': 'event_entity', 'type': "string"},
        {'name': 'event_action', 'type': "string"},
        {'name': 'owner', 'type': 'string'},
        {'name': 'ownerid', 'type': 'string'},
        {'name': 'dataset', 'type': 'string'},
        {'name': 'status', 'type': 'string'},
        {'name': 'messsage', 'type': 'string'},
        {'name': 'findability', 'type': 'string'},
        {'name': 'payload', 'type': 'object', 'es:index': False}
    ],
    'primaryKey': ['ownerid', 'timestamp']
}


def modify_datapackage(dp):
    dp['resources'].append({
        'name': '__events',
        PROP_STREAMING: True,
        'path': 'nonexistent',
        'schema': SCHEMA
    })
    return dp


def dataset_resource(dp):
    yield dict(
        timestamp=datetime.datetime.now(),
        event_entity='flow',
        event_action='finished',
        owner=dp['datahub'].get('owner'),
        ownerid=dp['datahub'].get('ownerid'),
        dataset=dp['name'],
        status='OK',
        messsage='',
        findability=dp['datahub'].get('findability'),
        payload={'flow-id': parameters['flow-id']}
    )


if __name__ == "__main__":
    parameters, dp, res_iter = ingest()
    spew(modify_datapackage(dp),
         itertools.chain(res_iter, [dataset_resource(dp)]))
