from flowmanager.models import FlowRegistry

import os
import json

from datapackage_pipelines.generators import GeneratorBase

import logging

log = logging.getLogger(__name__)

ROOT_PATH = os.path.join(os.path.dirname(__file__), '..')
SCHEMA_FILE = os.path.join(
    os.path.dirname(__file__), 'schemas/assembler_spec_schema.json')
DB_ENGINE = os.environ.get('SOURCESPEC_REGISTRY_DB_ENGINE')
REGISTRY = FlowRegistry(DB_ENGINE)


class Generator(GeneratorBase):

    @classmethod
    def get_schema(cls):
        return json.load(open(SCHEMA_FILE))

    @classmethod
    def generate_pipeline(cls, source):
        count = 0
        for pipeline in REGISTRY.list_pipelines():  # type: Pipelines
            yield pipeline.pipeline_id, pipeline.pipeline_details
            count += 1
        logging.error('assember sent %d pipelines', count)
