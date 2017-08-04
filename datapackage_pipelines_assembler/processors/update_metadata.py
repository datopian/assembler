from datapackage_pipelines.wrapper import process


def modify_datapackage(dp, parameters, stats):
    id = parameters.pop('id')
    dp['id'] = id
    dp['datahub'] = parameters
    return dp


process(modify_datapackage=modify_datapackage)
