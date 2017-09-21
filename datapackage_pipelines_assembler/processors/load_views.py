from datapackage_pipelines.wrapper import process
from datapackage import Package


def modify_datapackage(dp, parameters, stats):
    views = dp.get('views', [])
    package = Package(dp)
    for resource in package.resources:
        view = {
            'name': 'datahub-preview-{}'.format(resource.name),
            'specType': 'table',
            'datahub': {
                'type': 'preview'
            },
            'transform': {
                'limit': int(parameters.get('limit', 10000))
            },
            'resources': [resource.name]
        }
        views.append(view)
    dp['views'] = views
    return dp


process(modify_datapackage=modify_datapackage)
