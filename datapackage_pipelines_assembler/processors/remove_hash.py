from os import path

from datapackage_pipelines.wrapper import process


def modify_datapackage(dp, parameters, stats):
    for resource in dp['resources']:
        fpath = resource['path']
        dir_name = path.dirname(fpath)
        file_name = path.basename(fpath)
        head, tail = path.split(dir_name)
        if tail == resource.get('hash'):
            resource['path'] = path.join(head, file_name)
    return dp


process(modify_datapackage=modify_datapackage)
