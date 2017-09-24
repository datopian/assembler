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

    for url in urls:
        logging.info('URL: %s', url)
        dp_ = datapackage.DataPackage(url)
        view = dp_.descriptor.get('views', [])
        views += view

        for resource_ in dp_.resources:
            resource: Resource = resource_
            descriptor = copy.deepcopy(resource.descriptor)
            source = resource.source
            if os.environ.get('ASSEMBLER_LOCAL'):
                descriptor[PROP_STREAMED_FROM] = source
            else:
                descriptor['path'] = source
            if PROP_STREAMING in descriptor:
                del descriptor[PROP_STREAMING]
            dp['resources'].append(descriptor)

    dp['views'] = views

    return dp


process(modify_datapackage=modify_datapackage)
