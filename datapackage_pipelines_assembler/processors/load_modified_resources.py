import os

import datapackage

from datapackage_pipelines.wrapper import process
from datapackage_pipelines.generators import slugify


def modify_datapackage(dp, parameters, stats):
    resource_mapping = parameters['resource-mapping']
    remote_dp = datapackage.DataPackage(parameters['url'])
    for res in remote_dp.resources:
        descriptor = res.descriptor
        name = descriptor.get('name', descriptor.get('path'))
        if name in resource_mapping:
            if parameters['tabular'] == ('schema' in descriptor):
                assert 'path' in descriptor
                if 'name' not in descriptor:
                    descriptor['name'] = slugify(descriptor['path'], to_lower=True, separator='_')
                _, extension = os.path.splitext(descriptor['path'])
                if extension and 'format' not in descriptor:
                    descriptor['format'] = extension[1:]
                descriptor['url'] = resource_mapping[name]
                dp['resources'].append(descriptor)
    return dp

process(modify_datapackage=modify_datapackage)
