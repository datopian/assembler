import os
import json

from datapackage_pipelines.generators import (
    GeneratorBase,
)

import logging
log = logging.getLogger(__name__)


ROOT_PATH = os.path.join(os.path.dirname(__file__), '..')
SCHEMA_FILE = os.path.join(
    os.path.dirname(__file__), 'schemas/assembler_spec_schema.json')


class Generator(GeneratorBase):

    @classmethod
    def get_schema(cls):
        return json.load(open(SCHEMA_FILE))

    @classmethod
    def generate_pipeline(cls, source):
        meta = source['meta']
        pipeline_id = '{owner}/{dataset}'.format(**meta)

        ownerid = meta['owner']
        owner = meta.get('username')
        findability = meta.get('findability', 'published')

        inputs = source.get('inputs', [])
        assert len(inputs) == 1, 'Only supporting one input atm'

        input = inputs[0]
        assert input['kind'] == 'datapackage', 'Only supporting datapackage inputs atm'

        parameters = input.get('parameters', {})

        yield pipeline_id, {
            'pipeline': [
                {
                    'run': 'load_metadata',
                    'parameters': {
                        'url': input['url']
                    }
                },
                {
                    'run': 'assembler.update_metadata',
                    'parameters': {
                        'ownerid': ownerid,
                        'owner': owner,
                        'findability': findability,
                        'stats': {
                            'rowcount': 0,
                            'bytes': 0,
                        },
                        'id': pipeline_id
                    }
                },
                {
                    'run': 'assembler.load_modified_resources',
                    'parameters': {
                        'url': input['url'],
                        'action': 'derived',
                        'resource-mapping': parameters.get('resource-mapping', {})
                    }
                },
                {
                    'run': 'stream_remote_resources'
                },
                {
                    'run': 'set_types'
                },
                {
                    'run': 'assembler.sample'
                },
                {
                    'run': 'assembler.load_modified_resources',
                    'parameters': {
                        'url': input['url'],
                        'action': 'others',
                        'resource-mapping': parameters.get('resource-mapping', {})
                    }
                },
                # {
                #     'run': 'dump.to_path',
                #     'parameters': {
                #         'force-format': False,
                #         'handle-non-tabular': True,
                #         'out-path': './out',
                #         'counters': {
                #             "datapackage-rowcount": "datahub.stats.rowcount",
                #             "datapackage-bytes": "datahub.stats.bytes",
                #             "datapackage-hash": "datahub.hash",
                #             "resource-rowcount": "rowcount",
                #         }
                #
                #     }
                # },
                {
                    'run': 'aws.dump.to_s3',
                    'parameters': {
                        'force-format': False,
                        'handle-non-tabular': True,
                        'bucket': os.environ['PKGSTORE_BUCKET'],
                        'path': '{}/latest'.format(pipeline_id),
                        'counters': {
                            "datapackage-rowcount": "datahub.stats.rowcount",
                            "datapackage-bytes": "datahub.stats.bytes",
                            "datapackage-hash": "datahub.hash",
                            "resource-rowcount": "rowcount",
                            "resource-bytes": "bytes",
                            "resource-hash": "hash",
                        }
                    }
                },
                {
                    'run': 'assembler.add_indexing_resource'
                },
                {
                    'run': 'elasticsearch.dump.to_index',
                    'parameters': {
                        'indexes': {
                            'datasets': [
                                {
                                    'resource-name': 'datasets',
                                    'doc-type': 'dataset'
                                }
                            ]
                        }
                    }
                },
            ]
        }
