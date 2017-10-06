from datapackage_pipelines.wrapper import process


def modify_datapackage(dp, parameters, stats):
    dp['resources'] = []
    return dp


process(modify_datapackage=modify_datapackage)
