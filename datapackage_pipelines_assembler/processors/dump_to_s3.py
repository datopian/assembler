import copy
import datetime
import os

import filemanager
from datapackage_pipelines.utilities.resources import PROP_STREAMING

from datapackage_pipelines_aws.s3_dumper import S3Dumper


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
                 {'name': 'flowid', 'type': 'string'},
                 {'name': 'stats', 'type': 'object', 'es:schema': {
                    'fields': [
                        {'name': 'rowcount', 'type': 'integer'},
                        {'name': 'bytes', 'type': 'integer'}
                    ]}}
             ]}
         },
    ],
    'primaryKey': ['id']
}


def modify_datapackage(dp):
    dp['resources'].append({
        'name': '__datasets',
        PROP_STREAMING: True,
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

    def __init__(self):
        super(MyS3Dumper, self).__init__()
        self.fm = filemanager.FileManager(os.environ.get('FILEMANAGER_DATABASE_URL'))

    def initialize(self, params):
        super(MyS3Dumper, self).initialize(params)
        self.final = params.get('final', False)

    def prepare_datapackage(self, datapackage, params):
        datapackage = super(MyS3Dumper, self).prepare_datapackage(datapackage, params)
        if self.final:
            return modify_datapackage(datapackage)
        else:
            return datapackage

    def handle_datapackage(self, datapackage, parameters, stats):
        if self.final:
            dp = copy.deepcopy(datapackage)
            dp['resources'].pop()
        else:
            dp = datapackage
        return super(MyS3Dumper, self).handle_datapackage(dp, parameters, stats)

    def handle_resources(self, datapackage, resource_iterator, parameters, stats):
        yield from super(MyS3Dumper, self).handle_resources(datapackage, resource_iterator, parameters, stats)
        if self.final:
            yield dataset_resource(datapackage)

    def put_object(self, **kwargs):
        super(MyS3Dumper, self).put_object(**kwargs)
        datahub = self.datapackage['datahub']
        self.fm.add_file(
            kwargs['Bucket'],
            kwargs['Key'],
            datahub.get('findability'),
            datahub.get('owner'),
            datahub.get('ownerid'),
            self.datapackage.get('id'),
            datahub.get('flowid'),
            os.stat(kwargs['Body'].name).st_size,
            datetime.datetime.now()
        )


if __name__ == "__main__":
    MyS3Dumper()()
