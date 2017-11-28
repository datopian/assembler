import json

from datapackage import Package
from goodtables import validate
from datapackage_pipelines.wrapper import spew, ingest
from datapackage_pipelines.utilities.resources import PROP_STREAMED_FROM

parameters, datapackage, res_iter = ingest()
report_name = 'datapackage_report.json'


def generate_report(datapackage_):
    reports = []
    dp = Package(datapackage_)

    for resource in dp.resources:
        report = validate(
            resource.descriptor['path'],
            preset='datapackage',
            schema=resource.descriptor,
        )
        reports.append(report)

    with open(report_name, 'w') as f:
        json.dump(reports, f)


generate_report(datapackage)

dp_report = {
    'name': datapackage['name']+'-report',
    'resources': [{
        PROP_STREAMED_FROM: report_name,
        'name': 'datapackage_report',
        'format': 'json',
        'path': 'data/datapackage_report.json',
        'datahub': {
          'type': "derived/report",
        },
        'description': 'Validation report for tabular data'
    }]
}


spew(dp_report, [])
