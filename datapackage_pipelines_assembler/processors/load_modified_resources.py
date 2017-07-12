import datapackage

from datapackage_pipelines.wrapper import process


def modify_datapackage(dp, parameters, stats):
    resource_mapping = parameters['resource-mapping']
    remote_dp = datapackage.DataPackage(parameters['url'])
    for res in remote_dp.resources:
        descriptor = res.descriptor
        name = descriptor['name']
        if name in resource_mapping:
            assert 'path' in descriptor
            descriptor['url'] = resource_mapping[name]
            del descriptor['path']
        dp['resources'].append(descriptor)
    return dp

process(modify_datapackage=modify_datapackage)
