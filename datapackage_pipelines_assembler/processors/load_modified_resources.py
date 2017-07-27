import os

import copy
import datapackage

from datapackage_pipelines.wrapper import process
from datapackage_pipelines.generators import slugify


SETTINGS = {
    True: [
        ('.datahub/json', '.json'),
        ('', '')
    ],
    False: [
        ('', '')
    ]
}


def modify_datapackage(dp, parameters, stats):
    resource_mapping = parameters['resource-mapping']
    remote_dp = datapackage.DataPackage(parameters['url'])
    for res in remote_dp.resources:
        descriptor = res.descriptor
        name = descriptor.get('name', descriptor.get('path'))
        if name in resource_mapping:
            tabular_resource = ('schema' in descriptor)
            if parameters['tabular'] == tabular_resource:
                assert 'path' in descriptor

                # Add name if missing
                if 'name' not in descriptor:
                    descriptor['name'] = slugify(descriptor['path'], to_lower=True, separator='_')

                # Add format if missing
                _, extension = os.path.splitext(descriptor['path'])
                if extension and 'format' not in descriptor:
                    descriptor['format'] = extension[1:]

                # Set url from mapping
                descriptor['url'] = resource_mapping[name]

                for basepath, extension in SETTINGS[tabular_resource]:
                    descriptor_cp = copy.deepcopy(descriptor)

                    # Fix path
                    descriptor_cp['path'] = os.path.join(basepath, descriptor_cp['path'])
                    if not descriptor_cp['path'].endswith(extension):
                        descriptor_cp['path'] += extension

                    if extension:
                        descriptor_cp['name'] += '_' + extension
                    dp['resources'].append(descriptor_cp)
    return dp

process(modify_datapackage=modify_datapackage)
