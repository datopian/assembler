import os
import json

from datapackage_pipelines.generators import (
    GeneratorBase, steps
)
from .nodes.planner import planner

from .processors.dump_to_s3 import create_index

import logging
log = logging.getLogger(__name__)


ROOT_PATH = os.path.join(os.path.dirname(__file__), '..')
SCHEMA_FILE = os.path.join(
    os.path.dirname(__file__), 'schemas/assembler_spec_schema.json')


def dump_steps(*parts):
    if os.environ.get('ASSEMBLER_LOCAL'):
        return [('dump.to_path',
                {
                    'force-format': False,
                    'handle-non-tabular': True,
                    'out-path': '/'.join(str(p) for p in parts),
                    'counters': {
                        "datapackage-rowcount": "datahub.stats.rowcount",
                        "datapackage-bytes": "datahub.stats.bytes",
                        "datapackage-hash": "datahub.hash",
                        "resource-rowcount": "rowcount",
                        "resource-bytes": "bytes",
                        "resource-hash": "hash",
                    }
                })]
    else:
        return [('assembler.dump_to_s3',
                {
                    'force-format': False,
                    'handle-non-tabular': True,
                    'bucket': os.environ['PKGSTORE_BUCKET'],
                    'path': '/'.join(str(p) for p in parts),
                    'counters': {
                        "datapackage-rowcount": "datahub.stats.rowcount",
                        "datapackage-bytes": "datahub.stats.bytes",
                        "datapackage-hash": "datahub.hash",
                        "resource-rowcount": "rowcount",
                        "resource-bytes": "bytes",
                        "resource-hash": "hash",
                    }
                })]


def s3_path(*parts):
    if os.environ.get('ASSEMBLER_LOCAL'):
        path = '/'.join(str(p) for p in parts)
        return path
    else:
        path = '/'.join(str(p) for p in parts)
        bucket = os.environ['PKGSTORE_BUCKET']
        return 'https://{}/{}'.format(bucket, path)


class Generator(GeneratorBase):

    @classmethod
    def get_schema(cls):
        return json.load(open(SCHEMA_FILE))

    @classmethod
    def generate_pipeline(cls, source):
        meta = source['meta']

        def pipeline_id(r=None):
            if r is not None:
                return '{ownerid}/{dataset}:{suffix}'.format(**meta, suffix=r)
            else:
                return '{ownerid}/{dataset}'.format(**meta)

        ownerid = meta['ownerid']
        owner = meta.get('owner')
        findability = meta.get('findability', 'published')
        update_time = meta.get('update_time')

        inputs = source.get('inputs', [])
        assert len(inputs) == 1, 'Only supporting one input atm'

        input = inputs[0]
        assert input['kind'] == 'datapackage', 'Only supporting datapackage inputs atm'


        urls = []
        inner_pipeline_ids = []
        for inner_pipeline_id, pipeline_steps, dependencies \
                in planner(input, source.get('processing', [])):
            inner_pipeline_id = pipeline_id(inner_pipeline_id)
            inner_pipeline_ids.append(inner_pipeline_id)

            urls.append(s3_path(inner_pipeline_id, 'datapackage.json'))

            pipeline_steps.extend(dump_steps(inner_pipeline_id))
            dependencies = [dict(pipeline=pipeline_id(r)) for r in dependencies]

            pipeline = {
                'pipeline': steps(*pipeline_steps),
                'dependencies': dependencies
            }
            # print('yielding', inner_pipeline_id, pipeline)
            yield inner_pipeline_id, pipeline

        dependencies = [dict(pipeline='./'+pid) for pid in inner_pipeline_ids]
        # print(dependencies)
        final_steps = [
            ('load_metadata',
             {
                'url': input['url']
             }),
            ('assembler.update_metadata',
             {
                'ownerid': ownerid,
                'owner': owner,
                'findability': findability,
                'stats': {
                    'rowcount': 0,
                    'bytes': 0,
                },
                'modified': update_time,
                'id': pipeline_id()
             }),
            ('assembler.load_modified_resources',
             {
                 'urls': urls
             }),
            ('assembler.sample',),
        ]
        final_steps.extend(dump_steps(pipeline_id(), 'latest'))
        final_steps.append(
            ('elasticsearch.dump.to_index',
             {
                 'indexes': {
                     'datahub': [
                         {
                             'resource-name': '__datasets',
                             'doc-type': 'dataset'
                         }
                     ]
                 }
             })
        )
        pipeline = {
            'update_time': update_time,
            'dependencies': dependencies,
            'pipeline': steps(*final_steps)
        }
        # print('yielding', pipeline_id(), pipeline)
        yield pipeline_id(), pipeline