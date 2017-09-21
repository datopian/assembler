import os
from typing import List
from typing import Tuple

import datapackage
from copy import deepcopy

from .node_collector import collect_artifacts
from .base_processing_node import ProcessingArtifact


datapackage_cache = {}


def planner(datapackage_input, processing, outputs):
    parameters = datapackage_input.get('parameters')
    datapackage_url = datapackage_input['url']
    resource_info = datapackage_input.get('resource-info')

    # Create resource_info if missing
    if resource_info is None:
        if datapackage_url not in datapackage_cache:
            resource_info = []
            dp = datapackage.DataPackage(datapackage_url)
            for resource in dp.resources:  # type: Resource
                resource.descriptor['url'] = resource.source
                resource_info.append(deepcopy(resource.descriptor))
            datapackage_cache[datapackage_url] = resource_info
        resource_info = datapackage_cache[datapackage_url]
    else:
        for descriptor in resource_info:
            path = descriptor['path']
            if path.startswith('http'):
                url = path
            else:
                url = os.path.join(os.path.dirname(datapackage_url), path)
            descriptor['url'] = url

    # print('PLAN resource_info', resource_info)

    # Add types for all resources
    resource_mapping = parameters.get('resource-mapping', {})
    # print('PLAN resource_mapping', resource_mapping)
    for descriptor in resource_info:
        path = descriptor['path']
        name = descriptor['name']

        # Extract format from original path
        base, extension = os.path.splitext(descriptor['url'])
        extension = extension[1:]
        if extension and 'format' not in descriptor:
            descriptor['format'] = extension

        # Map original path if needed
        mapping = resource_mapping.get(path, resource_mapping.get(name))
        if mapping is not None:
            descriptor['url'] = mapping

        # Augment url with format hint
        if 'format' in descriptor:
            if not descriptor['url'].endswith(descriptor['format']):
                descriptor['url'] += '#.{}'.format(descriptor['format'])

        print(descriptor['url'])
        print(descriptor['format'])

        is_geojson = (
            (descriptor.get('format') == 'geojson') or
            (descriptor['url'].endswith('.geojson'))
        )

        # Hacky way to handle geojson files atm
        if is_geojson:
            schema = descriptor.get('schema')
            if schema is not None:
                del descriptor['schema']
                descriptor['geojsonSchema'] = schema

        if 'schema' in descriptor:
            descriptor['datahub'] = {
                'type': 'source/tabular'
            }
        else:
            descriptor['datahub'] = {
                'type': 'source/non-tabular'
            }

    # print('PLAN AFTER resource_info', resource_info)

    # Processing on resources
    processed_resources = set(p['input'] for p in processing)

    updated_resource_info = []
    for ri in resource_info:
        if ri['datahub']['type'] != 'source/tabular':
            updated_resource_info.append(ri)
            continue

        if ri['name'] not in processed_resources:
            updated_resource_info.append(ri)
            continue

        for p in processing:
            if p['input'] == ri['name']:
                ri_ = deepcopy(ri)
                if 'tabulator' in p:
                    ri_.update(p['tabulator'])
                    ri_['name'] = p['output']
                    updated_resource_info.append(ri_)
    resource_info = dict(
        (ri['name'], ri)
        for ri in updated_resource_info
    )

    # Create processing artifacts
    artifacts = [
        ProcessingArtifact(ri['datahub']['type'],
                           ri['name'],
                           [], [],
                           [],
                           False)
        for ri in resource_info.values()
    ]

    for derived_artifact in collect_artifacts(artifacts, outputs):
        pipeline_steps: List[Tuple] = [
            ('add_metadata', {'name': derived_artifact.resource_name}),
        ]
        needs_streaming = False
        for required_artifact in derived_artifact.required_streamed_artifacts:
            ri = resource_info[required_artifact.resource_name]
            if 'resource' in ri:
                pipeline_steps.append(
                    ('load_resource', ri)
                )
            else:
                pipeline_steps.append(
                    ('add_resource', ri)
                )
                needs_streaming = True

        if needs_streaming:
            pipeline_steps.extend([
                ('assembler.sample',),
                ('stream_remote_resources',)
            ])

        for required_artifact in derived_artifact.required_other_artifacts:
            pipeline_steps.append(
                ('add_resource', resource_info[required_artifact.resource_name])
            )

        pipeline_steps.extend(derived_artifact.pipeline_steps)
        pipeline_steps.extend([
            ('assembler.sample',),
        ])
        dependencies = [ra.resource_name
                        for ra in (derived_artifact.required_streamed_artifacts +
                                   derived_artifact.required_other_artifacts)
                        if ra.datahub_type not in ('source/tabular', 'source/non-tabular')]
        datapackage_url = yield derived_artifact.resource_name, pipeline_steps, dependencies

        resource_info[derived_artifact.resource_name] = {
            'resource': derived_artifact.resource_name,
            'url': datapackage_url
        }
