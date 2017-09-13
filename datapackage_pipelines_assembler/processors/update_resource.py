from datapackage_pipelines.wrapper import process


def modify_datapackage(dp, parameters, stats):
    for resource in dp['resources']:
        if resource['name'] == parameters['name']:
            resource.update(parameters['update'])
    return dp


process(modify_datapackage=modify_datapackage)
