import hashlib
import json

from datapackage import Package
from goodtables import validate
from datapackage_pipelines.wrapper import spew, ingest
from datapackage_pipelines.utilities.resources import PROP_STREAMED_FROM

parameters, datapackage, res_iter = ingest()
report_name = 'validation_report.json'


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

    hasher = hash_handler(open(report_name, 'rb'))
    return hasher.hexdigest()


def hash_handler(tfile):
    tfile.seek(0)
    hasher = hashlib.md5()
    data = 'x'
    while len(data) > 0:
        data = tfile.read(1024)
        if isinstance(data, str):
            hasher.update(data.encode('utf8'))
        elif isinstance(data, bytes):
            hasher.update(data)
    return hasher


report_hash = generate_report(datapackage)

datapackage['name'] += '-report'
datapackage['resources'] = [{
    PROP_STREAMED_FROM: report_name,
    'name': 'validation_report',
    'format': 'json',
    'path': 'data/validation_report.json',
    'datahub': {
      'type': "derived/report",
    },
    'hash': report_hash,
    'description': 'Validation report for tabular data'
}]

spew(datapackage, [])
