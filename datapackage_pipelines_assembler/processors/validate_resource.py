import json
from tempfile import mkdtemp
from os import path

from datapackage import Package
from goodtables import validate
from datapackage_pipelines.wrapper import spew, ingest
from datapackage_pipelines.utilities.resources import PROP_STREAMED_FROM

parameters, datapackage, res_iter = ingest()
report_name = path.join(mkdtemp(), 'validation_report.json')


def generate_report(datapackage_):
    reports = []
    dp = Package(datapackage_)

    for resource in dp.resources:
        # Handle if skip_rows is integer
        skip_rows = resource.descriptor.get('skip_rows', [])
        if isinstance(skip_rows, int):
            skip_rows = [a for a in range(skip_rows + 1)]
        resource.descriptor['skip_rows'] = skip_rows

        # Validate
        report = validate(
            resource.descriptor[PROP_STREAMED_FROM],
            **resource.descriptor
        )
        report["resource"] = resource.name
        reports.append(report)

    with open(report_name, 'w') as f:
        json.dump(reports, f)


generate_report(datapackage)

datapackage['name'] += '-report'
datapackage['resources'] = [{
    PROP_STREAMED_FROM: report_name,
    'name': 'validation_report',
    'format': 'json',
    'path': 'data/validation_report.json',
    'datahub': {
      'type': "derived/report",
    },
    'description': 'Validation report for tabular data'
}]

spew(datapackage, [])
