import os
import json
import pkgutil

from datapackage_pipelines.generators import (
    GeneratorBase,
    steps,
    slugify,
    SCHEDULE_DAILY
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
                    'run': 'assembler.load_modified_resources',
                    'parameters': {
                        'url': input['url'],
                        'tabular': True,
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
                    'run': 'assembler.load_modified_resources',
                    'parameters': {
                        'url': input['url'],
                        'tabular': False,
                        'resource-mapping': parameters.get('resource-mapping', {})
                    }
                },
                {
                    'run': 'aws.dump.to_s3',
                    'parameters': {
                        'bucket': os.environ['PKGSTORE_BUCKET'],
                        'path': '{}/latest'.format(pipeline_id)
                    }
                }
            ]
        }