from tempfile import mkdtemp
from os import path

from datapackage_pipelines.wrapper import spew, ingest
from datapackage_pipelines.utilities.resources import PROP_STREAMED_FROM

parameters, datapackage, res_iter = ingest()
readme_name = path.join(mkdtemp(), 'README.md')


def exctract_readme(datapackage_):
    readme = datapackage_.pop('readme', None)
    if readme is None:
        return datapackage_

    with open(readme_name, 'w') as f:
        f.write(readme)

    datapackage_['resources'].append({
        PROP_STREAMED_FROM: readme_name,
        'name': 'readme',
        'format': 'md',
        'path': 'README.md'
    })

    return datapackage_


spew(exctract_readme(datapackage), res_iter)
