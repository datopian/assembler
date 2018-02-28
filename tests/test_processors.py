import os
import unittest

try:
    from unittest.mock import Mock, patch
except ImportError:
    from mock import Mock, patch

from datapackage_pipelines.utilities.lib_test_helpers import mock_processor_test
from datapackage_pipelines.utilities.resources import PROP_STREAMED_FROM

import datapackage_pipelines_assembler.processors


class TestIndeedProccessors(unittest.TestCase):

    def test_load_previews(self):

        # Path to the processor we want to test
        processor_dir = os.path.dirname(datapackage_pipelines_assembler.processors.__file__)
        processor_path = os.path.join(processor_dir, 'load_preview.py')

        datapackage = {
            "name": "test",
            "resources": [
                { "name": "test-resource", "path": "testing/test.csv", "schema": {
                    "fields": [ {"name": "test", "type": "string"} ]
                }}
            ]
        }

        class TempList(list):
            pass

        resources = TempList([{'id': '%d'%i} for i in range(15)])
        resources.spec = {'rowcount': 15}


        # Trigger the processor with mock `ingest` and capture what it will
        # returned to `spew`.
        spew_args, _ = mock_processor_test(processor_path, ({'limit': 10}, datapackage,[resources]))

        spew_dp = spew_args[0]
        spew_res_iter = spew_args[1]

        dp_resources = spew_dp['resources']

        spew_res_iter_contents = list(spew_res_iter)
        rows = list(list(spew_res_iter_contents)[0])

        # should have 10 rows as limit is set to 10 in params
        self.assertEqual(len(rows), 10)

        spew_args, _ = mock_processor_test(processor_path, ({'limit': 20}, datapackage,[resources]))

        spew_dp = spew_args[0]
        spew_res_iter = spew_args[1]

        dp_resources = spew_dp['resources']

        spew_res_iter_contents = list(spew_res_iter)
        rows = list(list(spew_res_iter_contents)[0])

        # should have 0 rows as original reource is already small
        self.assertEqual(len(rows), 0)


    def test_validate_resource(self):

        # Path to the processor we want to test
        processor_dir = os.path.dirname(datapackage_pipelines_assembler.processors.__file__)
        processor_path = os.path.join(processor_dir, 'validate_resource.py')

        datapackage = {
            "name": "test",
            "resources": [
                {
                    "name": "test-resource",
                    PROP_STREAMED_FROM: 'data/sample_birthdays.csv',
                    "path": "data/sample_birthdays.csv",
                    "schema": {
                        "fields": [
                            {"name": "date", "type": "date"},
                            {"name": "first_name", "type": "string"},
                            {"name": "last_name", "type": "string"}
                        ]
                    }
                }
            ]
        }

        spew_args, _ = mock_processor_test(processor_path, ({}, datapackage,[[]]))

        spew_dp = spew_args[0]
        spew_res_iter = spew_args[1]

        expected = {
            'name': 'validation_report',
            'format': 'json',
            'path': 'data/validation_report.json',
            'datahub': {'type': 'derived/report'},
            'description': 'Validation report for tabular data'
        }
        result = spew_dp['resources'][0]

        # Check resource metadata has hash in it
        self.assertTrue('hash' in result)

        # Delete PROP_STREAMED_FROM as it varies (includes tempdir)
        del result[PROP_STREAMED_FROM]
        # Delete hash as it is not constant (report includes number of seconds it needed to be prepered)
        del result['hash']

        self.assertDictEqual(expected, result)


    def test_extract_readme(self):

        # Path to the processor we want to test
        processor_dir = os.path.dirname(datapackage_pipelines_assembler.processors.__file__)
        processor_path = os.path.join(processor_dir, 'extract_readme.py')

        datapackage = {
            "name": "test",
            "resources": [],
            "readme": "Hellow world"
        }

        spew_args, _ = mock_processor_test(processor_path, ({}, datapackage,[[]]))

        spew_dp = spew_args[0]
        spew_res_iter = spew_args[1]

        expected = {
            "name": "test",
            "resources": [{
              "format": "md",
              "name": "readme",
              "path": "README.md"
            }]
        }

        del spew_dp['resources'][0][PROP_STREAMED_FROM]

        self.assertDictEqual(expected, spew_dp)
