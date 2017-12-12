import os

import copy
import datapackage
import logging
from datapackage import Resource  # noqa
from datapackage_pipelines.utilities.resources import PROP_STREAMED_FROM
from datapackage_pipelines.utilities.resources import PROP_STREAMING

from datapackage_pipelines.wrapper import process

DERIVED_BASE = 'data'
OTHERS_BASE = 'extra'
DERIVED_FORMATS = ['csv', 'json']


def modify_datapackage(dp, parameters, stats):

    urls = parameters['urls']
    views = dp.get('views', [])
    row_count = 0
    bytes = 0

    for url in urls:
        logging.info('URL: %s', url)
        dp_ = datapackage.DataPackage(url)
        view = dp_.descriptor.get('views', [])
        views += view
        # Skip creation of preview resources if original resource is already small
        datahub = dp_.descriptor['datahub']
        if 'stats' in datahub and datahub['stats'].get('rowcount') == 0:
            continue

        for resource_ in dp_.resources:
            resource: Resource = resource_
            descriptor = copy.deepcopy(resource.descriptor)
            if descriptor['datahub']['type'] == 'derived/csv':
                row_count += int(descriptor.get('rowcount', 0))
            bytes += int(descriptor.get('bytes', 0))
            source = resource.source
            if os.environ.get('ASSEMBLER_LOCAL'):
                descriptor[PROP_STREAMED_FROM] = source
            else:
                descriptor['path'] = source.replace('s3.amazonaws.com/', '')
            if PROP_STREAMING in descriptor:
                del descriptor[PROP_STREAMING]
            dp['resources'].append(descriptor)

    dp['views'] = views
    dp['datahub'].setdefault('stats', {}).update(dict(
        rowcount=row_count,
        bytes=bytes
    ))

    return dp


process(modify_datapackage=modify_datapackage)
