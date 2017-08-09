import os

import copy
import datapackage

from datapackage_pipelines.wrapper import process
from datapackage_pipelines.generators import slugify

DERIVED_BASE = 'data'
OTHERS_BASE = 'extra'
DERIVED_FORMATS = ['csv', 'json']


def modify_datapackage(dp, parameters, stats):

    action = parameters['action']
    resource_mapping = parameters['resource-mapping']
    remote_dp = datapackage.DataPackage(parameters['url'])
    resources = []
    for i, res in enumerate(remote_dp.resources):

        descriptor = res.descriptor
        name = descriptor.get('name', descriptor.get('path'))
        if name in resource_mapping:

            is_geojson = (
                (descriptor.get('format') == 'geojson') or
                (descriptor.get('path', '').endswith('.geojson'))
            )

            # Hacky way to handle geojson files atm
            if is_geojson:
                schema = descriptor.get('schema')
                if schema is not None:
                    del descriptor['schema']
                    descriptor['geojsonSchema'] = schema

            tabular_resource = 'schema' in descriptor

            assert 'path' in descriptor

            # Add name if missing
            if 'name' not in descriptor:
                descriptor['name'] = slugify(descriptor['path'], to_lower=True, separator='_')

            # Set url from mapping
            descriptor['url'] = resource_mapping[name]

            if action == 'derived' and tabular_resource:
                # Add format if missing
                base, extension = os.path.splitext(descriptor['path'])
                extension = extension[1:]
                if extension and 'format' not in descriptor:
                    descriptor['format'] = extension

                for format in DERIVED_FORMATS:
                    descriptor_cp = copy.deepcopy(descriptor)

                    # Fix path
                    descriptor_cp['path'] = os.path.join(DERIVED_BASE, format, base + '.' + format)
                    descriptor_cp['datahub'] = {
                        'type': 'derived/'+format,
                        'derivedFrom': [
                            descriptor_cp['name']
                        ]
                    }
                    descriptor_cp['name'] += '_' + format
                    resources.append(descriptor_cp)

            if action == 'others' and tabular_resource:
                del descriptor['path']
                descriptor['datahub'] = {
                    'type': 'source/tabular',
                }
                resources.append(descriptor)

            if action == 'others' and not tabular_resource:

                descriptor_cp = copy.deepcopy(descriptor)

                # Fix path
                descriptor_cp['path'] = os.path.join(OTHERS_BASE, descriptor_cp['path'])
                descriptor_cp['datahub'] = {
                    'type': 'non-tabular',
                }

                resources.append(descriptor_cp)

    dp['resources'].extend(resources)
    return dp


process(modify_datapackage=modify_datapackage)
