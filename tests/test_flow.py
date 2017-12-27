import json
import jwt
import os
import requests
import subprocess
import time
import unittest
import yaml

import boto3
from elasticsearch import Elasticsearch

ES_SERVER = os.environ['DPP_ELASTICSEARCH'] = 'http://localhost:9200'
ES_SERVER = os.environ['EVENTS_ELASTICSEARCH_HOST'] = 'http://localhost:9200'
S3_SERVER = os.environ['S3_ENDPOINT_URL'] = 'http://localhost:5000/'
os.environ['PKGSTORE_BUCKET'] = 'testing.datahub.io'
os.environ['AWS_ACCESS_KEY_ID'] = 'foo'
os.environ['AWS_SECRET_ACCESS_KEY'] = 'bar'
os.environ['SOURCESPEC_REGISTRY_DB_ENGINE'] = DB_ENGINE = 'postgres://datahub:secret@localhost/datahq'
os.environ['FILEMANAGER_DATABASE_URL'] = 'postgres://datahub:secret@localhost/datahq'
os.environ['DATABASE_URL'] = 'postgres://datahub:secret@localhost/datahq'
os.environ['FLOWMANAGER_HOOK_URL'] = 'http://localhost:4000/source/update'


import filemanager
import flowmanager.controllers
from flowmanager.models import FlowRegistry
from sqlalchemy import create_engine

upload = flowmanager.controllers.upload
configs = flowmanager.controllers.CONFIGS

private_key = open('tests/private.pem').read()
public_key = open('tests/public.pem').read()
info_successful = 'http://localhost:4000/source/datahub/%s/successful'
info_latest = 'http://localhost:4000/source/datahub/%s/latest'

registry = FlowRegistry(DB_ENGINE)


def run_factory(dir='.', config=configs):
    os.chdir(dir)
    flow = yaml.load(open('assembler.source-spec.yaml'))
    token = generate_token(flow['meta']['owner'])
    response = upload(token, flow, registry, public_key, config=config)


    try:
        out = subprocess.check_output(['dpp', 'run', 'dirty'], stderr=subprocess.STDOUT)
        os.remove('.dpp.db')
    except subprocess.CalledProcessError as e:
        print(e.output.decode('utf8'))
        raise


def generate_token(owner):
    ret = {
        'userid': owner,
        'permissions': {},
        'service': ''
    }
    token = jwt.encode(ret, private_key, algorithm='RS256').decode('ascii')
    return token


class TestFlow(unittest.TestCase):

    def setUp(self):
        fm = filemanager.FileManager(os.environ['FILEMANAGER_DATABASE_URL'])
        filemanager.models.Base.metadata.create_all(fm.engine)
        es = Elasticsearch(hosts=[ES_SERVER])
        es.indices.delete(index='datahub', ignore=[400, 404])
        es.indices.delete(index='events', ignore=[400, 404])
        es.indices.flush()

        # you will need to add pipelines table to this list while developing
        for tbl in ('dataset', 'dataset_revision', 'storedfiles'):
            try:
                create_engine(DB_ENGINE).execute('DELETE FROM %s' % tbl)
            except:
                pass


        self.s3 = boto3.resource(
            service_name='s3',
            endpoint_url=S3_SERVER,
        )
        self.bucket_name = os.environ['PKGSTORE_BUCKET']
        self.bucket = self.s3.Bucket(self.bucket_name)
        try:
            self.bucket.create()
        except:
            pass
        for obj in self.bucket.objects.all():
                obj.delete()


    def test_coppies_accross_the_non_tabular_source(self):
        config = {'allowed_types': ['source/non-tabular']}
        run_factory(os.path.join(os.path.dirname(
            os.path.realpath(__file__)), 'inputs/non_tabular'), config=config)
        res = requests.get(
            '{}{}/datahub/non-tabular/1/datapackage.json'.format(S3_SERVER, self.bucket_name)).json()

        paths = dict(
            (r['name'], r['path'])
            for r in res['resources']
        )
        self.assertEqual(len(paths), 1)
        path = paths['test-geojson']
        res = requests.get(path)
        self.assertEqual(res.status_code, 200)

        # Specstore
        res = requests.get(info_latest % 'non-tabular')
        self.assertEqual(res.status_code, 200)
        res = requests.get(info_successful % 'non-tabular')
        self.assertEqual(res.status_code, 200)

        info = res.json()
        self.assertEqual(info['state'], 'SUCCEEDED')
        self.assertEqual(len(info['pipelines']), 2)
        self.assertEqual(info['pipelines']['datahub/non-tabular/1']['status'], 'SUCCEEDED')
        self.assertEqual(info['pipelines']['datahub/non-tabular/1/test-geojson']['status'], 'SUCCEEDED')


    def test_coppies_accross_the_tabular_source(self):
        config = {'allowed_types': ['source/tabular']}
        run_factory(os.path.join(os.path.dirname(
            os.path.realpath(__file__)), 'inputs/single_file'), config=config)
        res = requests.get(
            '{}{}/datahub/single-file/1/datapackage.json'.format(S3_SERVER, self.bucket_name)).json()

        paths = dict(
            (r['name'], r['path'])
            for r in res['resources']
        )
        self.assertEqual(len(paths), 1)
        path = paths['birthdays']
        res = requests.get(path)
        self.assertEqual(res.status_code, 200)

        # Specstore
        res = requests.get(info_latest % 'single-file')
        self.assertEqual(res.status_code, 200)
        res = requests.get(info_successful % 'single-file')
        self.assertEqual(res.status_code, 200)

        info = res.json()
        self.assertEqual(info['state'], 'SUCCEEDED')
        self.assertEqual(len(info['pipelines']), 2)
        self.assertEqual(info['pipelines']['datahub/single-file/1']['status'], 'SUCCEEDED')
        self.assertEqual(info['pipelines']['datahub/single-file/1/birthdays']['status'], 'SUCCEEDED')


    def test_generates_only_derived_csv(self):
        config = {'allowed_types': ['source/tabular', 'derived/csv']}
        run_factory(os.path.join(os.path.dirname(
            os.path.realpath(__file__)), 'inputs/single_file'), config=config)
        res = requests.get(
            '{}{}/datahub/single-file/1/datapackage.json'.format(S3_SERVER, self.bucket_name)).json()

        paths = dict(
            (r['name'], r['path'])
            for r in res['resources']
        )
        self.assertEqual(len(paths), 2)
        path = paths['birthdays_csv']
        res = requests.get(path)
        self.assertEqual(res.status_code, 200)

        # Specstore
        res = requests.get(info_latest % 'single-file')
        self.assertEqual(res.status_code, 200)
        res = requests.get(info_successful % 'single-file')
        self.assertEqual(res.status_code, 200)

        info = res.json()
        self.assertEqual(info['state'], 'SUCCEEDED')
        self.assertEqual(len(info['pipelines']), 3)
        self.assertEqual(info['pipelines']['datahub/single-file/1']['status'], 'SUCCEEDED')
        self.assertEqual(info['pipelines']['datahub/single-file/1/birthdays']['status'], 'SUCCEEDED')
        self.assertEqual(info['pipelines']['datahub/single-file/1/birthdays_csv']['status'], 'SUCCEEDED')


    def test_generates_only_derived_json(self):
        config = {'allowed_types': ['source/tabular', 'derived/json']}
        run_factory(os.path.join(os.path.dirname(
            os.path.realpath(__file__)), 'inputs/single_file'), config=config)
        res = requests.get(
            '{}{}/datahub/single-file/1/datapackage.json'.format(S3_SERVER, self.bucket_name)).json()

        paths = dict(
            (r['name'], r['path'])
            for r in res['resources']
        )
        self.assertEqual(len(paths), 2)
        path = paths['birthdays_json']
        res = requests.get(path)
        self.assertEqual(res.status_code, 200)

        # Specstore
        res = requests.get(info_latest % 'single-file')
        self.assertEqual(res.status_code, 200)
        res = requests.get(info_successful % 'single-file')
        self.assertEqual(res.status_code, 200)

        info = res.json()
        self.assertEqual(info['state'], 'SUCCEEDED')
        self.assertEqual(len(info['pipelines']), 3)
        self.assertEqual(info['pipelines']['datahub/single-file/1']['status'], 'SUCCEEDED')
        self.assertEqual(info['pipelines']['datahub/single-file/1/birthdays']['status'], 'SUCCEEDED')
        self.assertEqual(info['pipelines']['datahub/single-file/1/birthdays_json']['status'], 'SUCCEEDED')


    def test_generates_only_derived_zip(self):
        config = {'allowed_types': ['source/tabular', 'derived/zip']}
        run_factory(os.path.join(os.path.dirname(
            os.path.realpath(__file__)), 'inputs/single_file'), config=config)
        res = requests.get(
            '{}{}/datahub/single-file/1/datapackage.json'.format(S3_SERVER, self.bucket_name)).json()

        paths = dict(
            (r['name'], r['path'])
            for r in res['resources']
        )
        self.assertEqual(len(paths), 2)
        path = paths['single-file_zip']
        res = requests.get(path)
        self.assertEqual(res.status_code, 200)

        # Specstore
        res = requests.get(info_latest % 'single-file')
        self.assertEqual(res.status_code, 200)
        res = requests.get(info_successful % 'single-file')
        self.assertEqual(res.status_code, 200)

        info = res.json()
        self.assertEqual(info['state'], 'SUCCEEDED')
        self.assertEqual(len(info['pipelines']), 3)
        self.assertEqual(info['pipelines']['datahub/single-file/1']['status'], 'SUCCEEDED')
        self.assertEqual(info['pipelines']['datahub/single-file/1/birthdays']['status'], 'SUCCEEDED')
        self.assertEqual(info['pipelines']['datahub/single-file/1/single-file_zip']['status'], 'SUCCEEDED')


    def test_generates_reports(self):
        config = {'allowed_types': ['source/tabular', 'derived/report']}
        run_factory(os.path.join(os.path.dirname(
            os.path.realpath(__file__)), 'inputs/single_file'), config=config)
        res = requests.get(
            '{}{}/datahub/single-file/1/datapackage.json'.format(S3_SERVER, self.bucket_name)).json()

        paths = dict(
            (r['name'], r['path'])
            for r in res['resources']
        )
        self.assertEqual(len(paths), 2)
        path = paths['validation_report']
        res = requests.get(path)
        self.assertEqual(res.status_code, 200)
        report = res.json()
        self.assertTrue(report[0]['valid'])

        # Specstore
        res = requests.get(info_latest % 'single-file')
        self.assertEqual(res.status_code, 200)
        res = requests.get(info_successful % 'single-file')
        self.assertEqual(res.status_code, 200)

        info = res.json()
        self.assertEqual(info['state'], 'SUCCEEDED')
        self.assertEqual(len(info['pipelines']), 3)
        self.assertEqual(info['pipelines']['datahub/single-file/1']['status'], 'SUCCEEDED')
        self.assertEqual(info['pipelines']['datahub/single-file/1/birthdays']['status'], 'SUCCEEDED')
        self.assertEqual(info['pipelines']['datahub/single-file/1/validation_report']['status'], 'SUCCEEDED')


    def test_generates_invalid_reports(self):
        config = {'allowed_types': ['source/tabular', 'derived/report']}
        run_factory(os.path.join(os.path.dirname(
            os.path.realpath(__file__)), 'inputs/invalid_file'), config=config)
        res = requests.get(
                '{}{}/datahub/invalid-file/1/datapackage.json'.format(S3_SERVER, self.bucket_name)).json()

        paths = dict(
            (r['name'], r['path'])
            for r in res['resources']
        )
        self.assertEqual(len(paths), 2)
        path = paths['validation_report']
        res = requests.get(path)
        self.assertEqual(res.status_code, 200)
        report = res.json()
        self.assertFalse(report[0]['valid'])
        self.assertEqual(report[0]['error-count'], 20)
        self.assertEqual(report[0]['tables'][0]['errors'][0]['code'], 'type-or-format-error')

        # Specstore
        res = requests.get(info_latest % 'invalid-file')
        self.assertEqual(res.status_code, 200)
        res = requests.get(info_successful % 'invalid-file')
        self.assertEqual(res.status_code, 200)

        info = res.json()
        self.assertEqual(info['state'], 'SUCCEEDED')
        self.assertEqual(len(info['pipelines']), 3)
        self.assertEqual(info['pipelines']['datahub/invalid-file/1']['status'], 'SUCCEEDED')
        self.assertEqual(info['pipelines']['datahub/invalid-file/1/birthdays']['status'], 'SUCCEEDED')
        self.assertEqual(info['pipelines']['datahub/invalid-file/1/validation_report']['status'], 'SUCCEEDED')


    def test_all_pipeline_statuses_are_updated_after_fail(self):
        config = {'allowed_types': ['source/tabular', 'derived/report', 'derived/csv']}
        run_factory(os.path.join(os.path.dirname(
            os.path.realpath(__file__)), 'inputs/invalid_file'), config=config)

        # Specstore
        time.sleep(5)
        res = requests.get(info_latest % 'invalid-file')
        self.assertEqual(res.status_code, 200)

        info = res.json()
        self.assertEqual(info['state'], 'FAILED')
        self.assertEqual(len(info['pipelines']), 4)
        self.assertEqual(info['pipelines']['datahub/invalid-file/1']['status'], 'FAILED')
        self.assertEqual(info['pipelines']['datahub/invalid-file/1/birthdays']['status'], 'SUCCEEDED')
        self.assertEqual(info['pipelines']['datahub/invalid-file/1/birthdays_csv']['status'], 'FAILED')
        self.assertEqual(info['pipelines']['datahub/invalid-file/1/validation_report']['status'], 'SUCCEEDED')


    def test_generates_without_preview_if_small_enough(self):
        config = {'allowed_types': [
            'source/tabular', 'derived/csv', 'derived/json', 'derived/preview']}
        run_factory(os.path.join(os.path.dirname(
            os.path.realpath(__file__)), 'inputs/single_file'), config=config)
        res = requests.get(
            '{}{}/datahub/single-file/1/datapackage.json'.format(S3_SERVER, self.bucket_name)).json()

        paths = dict(
            (r['name'], r['path'])
            for r in res['resources']
        )
        self.assertEqual(len(paths), 3)
        path = paths['birthdays_json']
        res = requests.get(path)
        self.assertEqual(res.status_code, 200)
        path = paths['birthdays_csv']
        res = requests.get(path)
        self.assertEqual(res.status_code, 200)
        self.assertIsNone(paths.get('birthdays_csv_preview'))

        # Specstore
        res = requests.get(info_latest % 'single-file')
        self.assertEqual(res.status_code, 200)
        res = requests.get(info_successful % 'single-file')
        self.assertEqual(res.status_code, 200)

        info = res.json()
        self.assertEqual(info['state'], 'SUCCEEDED')
        self.assertEqual(len(info['pipelines']), 5)
        self.assertEqual(info['pipelines']['datahub/single-file/1']['status'], 'SUCCEEDED')
        self.assertEqual(info['pipelines']['datahub/single-file/1/birthdays']['status'], 'SUCCEEDED')
        self.assertEqual(info['pipelines']['datahub/single-file/1/birthdays_csv']['status'], 'SUCCEEDED')
        self.assertEqual(info['pipelines']['datahub/single-file/1/birthdays_json']['status'], 'SUCCEEDED')
        self.assertEqual(info['pipelines']['datahub/single-file/1/birthdays_csv_preview']['status'], 'SUCCEEDED')


    def test_generates_preview(self):
        config = {'allowed_types': [
            'source/tabular', 'derived/csv', 'derived/preview']}
        run_factory(os.path.join(os.path.dirname(
            os.path.realpath(__file__)), 'inputs/preview'), config=config)
        res = requests.get(
            '{}{}/datahub/needs-preview/1/datapackage.json'.format(S3_SERVER, self.bucket_name)).json()

        paths = dict(
            (r['name'], r['path'])
            for r in res['resources']
        )
        self.assertEqual(len(paths), 3)
        path = paths['test-preview_csv_preview']
        res = requests.get(path)
        self.assertEqual(res.status_code, 200)
        self.assertEqual(len(res.json()), 2000)
        path = paths['test-preview_csv']
        res = requests.get(path)
        self.assertEqual(res.status_code, 200)

        # Specstore
        res = requests.get(info_latest % 'needs-preview')
        self.assertEqual(res.status_code, 200)
        res = requests.get(info_successful % 'needs-preview')
        self.assertEqual(res.status_code, 200)

        info = res.json()
        self.assertEqual(info['state'], 'SUCCEEDED')
        self.assertEqual(len(info['pipelines']), 4)
        self.assertEqual(info['pipelines']['datahub/needs-preview/1']['status'], 'SUCCEEDED')
        self.assertEqual(info['pipelines']['datahub/needs-preview/1/test-preview']['status'], 'SUCCEEDED')
        self.assertEqual(info['pipelines']['datahub/needs-preview/1/test-preview_csv']['status'], 'SUCCEEDED')
        self.assertEqual(info['pipelines']['datahub/needs-preview/1/test-preview_csv_preview']['status'], 'SUCCEEDED')


    def test_single_file(self):
        run_factory(os.path.join(os.path.dirname(
            os.path.realpath(__file__)), 'inputs/single_file'))

        res = requests.get(
            '{}{}/datahub/single-file/1/datapackage.json'.format(S3_SERVER, self.bucket_name)).json()

        paths = dict(
            (r['name'], r['path'])
            for r in res['resources']
        )
        path = paths['birthdays']
        assert path.startswith('{}{}/datahub/single-file/birthdays/data'.format(S3_SERVER, self.bucket_name))
        res = requests.get(path)

        exp_csv = open('../../outputs/csv/sample_birthdays.csv').read()
        self.assertEqual(res.status_code, 200)
        self.assertEqual(exp_csv, res.text)

        path = paths['birthdays_csv']
        assert path.startswith('{}{}/datahub/single-file/birthdays_csv/data'.format(S3_SERVER, self.bucket_name))
        res = requests.get(path)
        self.assertEqual(res.status_code, 200)
        self.assertEqual(exp_csv.replace('\n', '\r\n'), res.text)


        path = paths['birthdays_json']
        assert path.startswith('{}{}/datahub/single-file/birthdays_json/data'.format(S3_SERVER, self.bucket_name))
        res = requests.get(path)
        self.assertEqual(res.status_code, 200)
        exp_json = json.load(open('../../outputs/json/sample_birthdays.json'))
        self.assertListEqual(exp_json, res.json())

        path = paths['validation_report']
        assert path.startswith('{}{}/datahub/single-file/validation_report/data'.format(S3_SERVER, self.bucket_name))
        res = requests.get(path)
        self.assertEqual(res.status_code, 200)
        report = res.json()
        self.assertTrue(report[0]['valid'])

        path = paths['single-file_zip']
        assert path.startswith('{}{}/datahub/single-file/single-file_zip/data'.format(S3_SERVER, self.bucket_name))
        res = requests.get(path)
        self.assertEqual(res.status_code, 200)
        # TODO: compare zip files

        # Elasticsearch
        res = requests.get('http://localhost:9200/datahub/_search')
        self.assertEqual(res.status_code, 200)

        meta = res.json()
        hits = [hit['_source'] for hit in meta['hits']['hits']
            if hit['_source']['datapackage']['name'] == 'single-file']

        self.assertEqual(len(hits), 1)

        datahub = hits[0]['datahub']
        datapackage = hits[0]['datapackage']
        self.assertEqual(datahub['findability'],'published')
        self.assertEqual(datahub['owner'],'datahub')
        self.assertEqual(datahub['stats']['rowcount'], 20)
        self.assertEqual(len(datapackage['resources']), 5)

        time.sleep(5)
        res = requests.get('http://localhost:9200/events/_search')
        self.assertEqual(res.status_code, 200)

        events = res.json()
        hits = [hit['_source'] for hit in events['hits']['hits']
            if hit['_source']['dataset'] == 'single-file']
        self.assertEqual(len(hits), 1)

        event = hits[0]
        self.assertEqual(event['dataset'],'single-file')
        self.assertEqual(event['event_action'],'finish')
        self.assertEqual(event['event_entity'], 'flow')
        self.assertEqual(event['owner'], 'datahub')
        self.assertEqual(event['status'], 'OK')

        # Specstore
        res = requests.get(info_latest % 'single-file')
        self.assertEqual(res.status_code, 200)
        res = requests.get(info_successful % 'single-file')
        self.assertEqual(res.status_code, 200)

        info = res.json()
        self.assertEqual(info['state'], 'SUCCEEDED')
        self.assertEqual(len(info['pipelines']), 7)
        self.assertEqual(info['pipelines']['datahub/single-file/1']['status'], 'SUCCEEDED')
        self.assertEqual(info['pipelines']['datahub/single-file/1/birthdays']['status'], 'SUCCEEDED')
        self.assertEqual(info['pipelines']['datahub/single-file/1/birthdays_csv']['status'], 'SUCCEEDED')
        self.assertEqual(info['pipelines']['datahub/single-file/1/birthdays_json']['status'], 'SUCCEEDED')
        self.assertEqual(info['pipelines']['datahub/single-file/1/single-file_zip']['status'], 'SUCCEEDED')
        self.assertEqual(info['pipelines']['datahub/single-file/1/validation_report']['status'], 'SUCCEEDED')
        self.assertEqual(info['pipelines']['datahub/single-file/1/birthdays_csv_preview']['status'], 'SUCCEEDED')


    def test_multiple_file(self):
        run_factory(os.path.join(os.path.dirname(
            os.path.realpath(__file__)), 'inputs/multiple_files'))

        res = requests.get(
            '{}{}/datahub/multiple-files/1/datapackage.json'.format(S3_SERVER, self.bucket_name)).json()

        paths = dict(
            (r['name'], r['path'])
            for r in res['resources']
        )

        path = paths['birthdays']
        assert path.startswith('{}{}/datahub/multiple-files/birthdays/data'.format(S3_SERVER, self.bucket_name))
        res = requests.get(path)
        exp_csv = open('../../outputs/csv/sample_birthdays.csv').read()
        self.assertEqual(res.status_code, 200)
        self.assertEqual(exp_csv, res.text)

        path = paths['birthdays_csv']
        assert path.startswith('{}{}/datahub/multiple-files/birthdays_csv/data'.format(S3_SERVER, self.bucket_name))
        res = requests.get(path)
        self.assertEqual(res.status_code, 200)
        self.assertEqual(exp_csv.replace('\n', '\r\n'), res.text)

        path = paths['birthdays_json']
        assert path.startswith('{}{}/datahub/multiple-files/birthdays_json/data'.format(S3_SERVER, self.bucket_name))
        res = requests.get(path)
        self.assertEqual(res.status_code, 200)
        exp_json = json.load(open('../../outputs/json/sample_birthdays.json'))
        self.assertListEqual(exp_json, res.json())

        path = paths['emails']
        assert path.startswith('{}{}/datahub/multiple-files/emails/data'.format(S3_SERVER, self.bucket_name))
        res = requests.get(path)
        exp_csv = open('../../outputs/csv/sample_emails.csv').read()
        self.assertEqual(res.status_code, 200)
        self.assertEqual(exp_csv, res.text)

        path = paths['emails_csv']
        assert path.startswith('{}{}/datahub/multiple-files/emails_csv/data'.format(S3_SERVER, self.bucket_name))
        res = requests.get(path)
        self.assertEqual(res.status_code, 200)
        self.assertEqual(exp_csv.replace('\n', '\r\n'), res.text)

        path = paths['emails_json']
        assert path.startswith('{}{}/datahub/multiple-files/emails_json/data'.format(S3_SERVER, self.bucket_name))
        res = requests.get(path)
        self.assertEqual(res.status_code, 200)
        exp_json = json.load(open('../../outputs/json/sample_emails.json'))
        self.assertListEqual(exp_json, res.json())

        path = paths['validation_report']
        assert path.startswith('{}{}/datahub/multiple-files/validation_report/data'.format(S3_SERVER, self.bucket_name))
        res = requests.get(path)
        self.assertEqual(res.status_code, 200)
        report = res.json()
        self.assertEqual(len(report), 2)
        self.assertTrue(report[0]['valid'])
        self.assertTrue(report[1]['valid'])

        path = paths['multiple-files_zip']
        assert path.startswith('{}{}/datahub/multiple-files/multiple-files_zip/data'.format(S3_SERVER, self.bucket_name))
        res = requests.get(path)
        self.assertEqual(res.status_code, 200)

        # Elasticsearch
        res = requests.get('http://localhost:9200/datahub/_search')
        self.assertEqual(res.status_code, 200)

        meta = res.json()
        hits = [hit['_source'] for hit in meta['hits']['hits']
            if hit['_source']['datapackage']['name'] == 'multiple-files']
        self.assertEqual(len(hits), 1)

        datahub = hits[0]['datahub']
        datapackage = hits[0]['datapackage']
        self.assertEqual(datahub['findability'],'published')
        self.assertEqual(datahub['owner'],'datahub')
        self.assertEqual(datahub['stats']['rowcount'], 40)
        self.assertEqual(len(datapackage['resources']), 8)

        time.sleep(5)
        res = requests.get('http://localhost:9200/events/_search')
        self.assertEqual(res.status_code, 200)

        events = res.json()
        hits = [hit['_source'] for hit in events['hits']['hits']
            if hit['_source']['dataset'] == 'multiple-files']
        self.assertEqual(len(hits), 1)

        event = hits[0]
        self.assertEqual(event['event_action'],'finish')
        self.assertEqual(event['event_entity'], 'flow')
        self.assertEqual(event['owner'], 'datahub')
        self.assertEqual(event['status'], 'OK')

        # Specstore
        res = requests.get(info_latest % 'multiple-files')
        self.assertEqual(res.status_code, 200)
        res = requests.get(info_successful % 'multiple-files')
        self.assertEqual(res.status_code, 200)

        info = res.json()
        self.assertEqual(info['state'], 'SUCCEEDED')
        self.assertEqual(len(info['pipelines']), 11)
        self.assertEqual(info['pipelines']['datahub/multiple-files/1']['status'], 'SUCCEEDED')
        self.assertEqual(info['pipelines']['datahub/multiple-files/1/birthdays']['status'], 'SUCCEEDED')
        self.assertEqual(info['pipelines']['datahub/multiple-files/1/birthdays_csv']['status'], 'SUCCEEDED')
        self.assertEqual(info['pipelines']['datahub/multiple-files/1/birthdays_json']['status'], 'SUCCEEDED')
        self.assertEqual(info['pipelines']['datahub/multiple-files/1/birthdays_csv_preview']['status'], 'SUCCEEDED')
        self.assertEqual(info['pipelines']['datahub/multiple-files/1/emails']['status'], 'SUCCEEDED')
        self.assertEqual(info['pipelines']['datahub/multiple-files/1/emails_csv']['status'], 'SUCCEEDED')
        self.assertEqual(info['pipelines']['datahub/multiple-files/1/emails_json']['status'], 'SUCCEEDED')
        self.assertEqual(info['pipelines']['datahub/multiple-files/1/emails_csv_preview']['status'], 'SUCCEEDED')
        self.assertEqual(info['pipelines']['datahub/multiple-files/1/multiple-files_zip']['status'], 'SUCCEEDED')
        self.assertEqual(info['pipelines']['datahub/multiple-files/1/validation_report']['status'], 'SUCCEEDED')


    def test_excel_file(self):
        run_factory(os.path.join(os.path.dirname(
            os.path.realpath(__file__)), 'inputs/excel'))

        res = requests.get(
            '{}{}/datahub/excel/1/datapackage.json'.format(S3_SERVER, self.bucket_name)).json()
        paths = dict(
            (r['name'], r['path'])
            for r in res['resources']
        )

        path = paths['birthdays']
        assert path.startswith('{}{}/datahub/excel/birthdays/data'.format(S3_SERVER, self.bucket_name))
        res = requests.get(path)
        self.assertEqual(res.status_code, 200)

        exp_csv = open('../../outputs/csv/sample_birthdays.csv').read()

        path = paths['birthdays_csv']
        assert path.startswith('{}{}/datahub/excel/birthdays_csv/data'.format(S3_SERVER, self.bucket_name))
        res = requests.get(path)
        self.assertEqual(res.status_code, 200)
        self.assertEqual(exp_csv.replace('\n', '\r\n'), res.text)

        path = paths['birthdays_json']
        assert path.startswith('{}{}/datahub/excel/birthdays_json/data'.format(S3_SERVER, self.bucket_name))
        res = requests.get(path)
        self.assertEqual(res.status_code, 200)
        exp_json = json.load(open('../../outputs/json/sample_birthdays.json'))
        self.assertListEqual(exp_json, res.json())

        path = paths['validation_report']
        assert path.startswith('{}{}/datahub/excel/validation_report/data'.format(S3_SERVER, self.bucket_name))
        res = requests.get(path)
        self.assertEqual(res.status_code, 200)
        report = res.json()
        self.assertTrue(report[0]['valid'])

        path = paths['excel_zip']
        assert path.startswith('{}{}/datahub/excel/excel_zip/data'.format(S3_SERVER, self.bucket_name))
        res = requests.get(path)
        self.assertEqual(res.status_code, 200)

        # Elasticsearch
        res = requests.get('http://localhost:9200/datahub/_search')
        self.assertEqual(res.status_code, 200)

        meta = res.json()
        hits = [hit['_source'] for hit in meta['hits']['hits']
            if hit['_source']['datapackage']['name'] == 'excel']
        self.assertEqual(len(hits), 1)

        datahub = hits[0]['datahub']
        datapackage = hits[0]['datapackage']
        self.assertEqual(datahub['findability'],'published')
        self.assertEqual(datahub['owner'],'datahub')
        self.assertEqual(datahub['stats']['rowcount'], 20)
        self.assertEqual(len(datapackage['resources']), 5)

        time.sleep(5)
        res = requests.get('http://localhost:9200/events/_search')
        self.assertEqual(res.status_code, 200)

        events = res.json()
        hits = [hit['_source'] for hit in events['hits']['hits']
            if hit['_source']['dataset'] == 'excel']
        self.assertEqual(len(hits), 1)

        event = hits[0]
        self.assertEqual(event['event_action'],'finish')
        self.assertEqual(event['event_entity'], 'flow')
        self.assertEqual(event['owner'], 'datahub')
        self.assertEqual(event['status'], 'OK')

        # Specstore
        res = requests.get(info_latest % 'excel')
        self.assertEqual(res.status_code, 200)
        res = requests.get(info_successful % 'excel')
        self.assertEqual(res.status_code, 200)

        info = res.json()
        self.assertEqual(info['state'], 'SUCCEEDED')
        self.assertEqual(len(info['pipelines']), 7)
        self.assertEqual(info['pipelines']['datahub/excel/1']['status'], 'SUCCEEDED')
        self.assertEqual(info['pipelines']['datahub/excel/1/birthdays']['status'], 'SUCCEEDED')
        self.assertEqual(info['pipelines']['datahub/excel/1/birthdays_csv']['status'], 'SUCCEEDED')
        self.assertEqual(info['pipelines']['datahub/excel/1/birthdays_json']['status'], 'SUCCEEDED')
        self.assertEqual(info['pipelines']['datahub/excel/1/excel_zip']['status'], 'SUCCEEDED')
        self.assertEqual(info['pipelines']['datahub/excel/1/validation_report']['status'], 'SUCCEEDED')
        self.assertEqual(info['pipelines']['datahub/excel/1/birthdays_csv_preview']['status'], 'SUCCEEDED')


    def test_needs_processing(self):
        run_factory(os.path.join(os.path.dirname(
            os.path.realpath(__file__)), 'inputs/needs_processing'))

        res = requests.get(
            '{}{}/datahub/single-file-processed/1/datapackage.json'.format(S3_SERVER, self.bucket_name)).json()
        paths = dict(
            (r['name'], r['path'])
            for r in res['resources']
        )

        path = paths['birthdays']
        assert path.startswith('{}{}/datahub/single-file-processed/birthdays/data'.format(S3_SERVER, self.bucket_name))
        res = requests.get(path)
        exp_csv = open('../../outputs/csv/sample_birthdays_invalid.csv').read()
        self.assertEqual(res.status_code, 200)
        self.assertEqual(exp_csv, res.text)

        path = paths['birthdays_csv']
        assert path.startswith('{}{}/datahub/single-file-processed/birthdays_csv/data'.format(S3_SERVER, self.bucket_name))
        res = requests.get(path)
        exp_csv = open('../../outputs/csv/sample_birthdays.csv').read()
        self.assertEqual(res.status_code, 200)
        self.assertEqual(exp_csv.replace('\n', '\r\n'), res.text)

        path = paths['birthdays_json']
        assert path.startswith('{}{}/datahub/single-file-processed/birthdays_json/data'.format(S3_SERVER, self.bucket_name))
        res = requests.get(path)
        self.assertEqual(res.status_code, 200)
        exp_json = json.load(open('../../outputs/json/sample_birthdays.json'))
        self.assertListEqual(exp_json, res.json())

        path = paths['validation_report']
        assert path.startswith('{}{}/datahub/single-file-processed/validation_report/data'.format(S3_SERVER, self.bucket_name))
        res = requests.get(path)
        self.assertEqual(res.status_code, 200)
        report = res.json()
        self.assertTrue(report[0]['valid'])

        path = paths['single-file-processed_zip']
        assert path.startswith('{}{}/datahub/single-file-processed/single-file-processed_zip/data'.format(S3_SERVER, self.bucket_name))
        res = requests.get(path)
        self.assertEqual(res.status_code, 200)

        # Elasticsearch
        time.sleep(5)
        res = requests.get('http://localhost:9200/datahub/_search')
        self.assertEqual(res.status_code, 200)

        meta = res.json()
        hits = [hit['_source'] for hit in meta['hits']['hits']
            if hit['_source']['datapackage']['name'] == 'single-file-processed']
        self.assertEqual(len(hits), 1)

        datahub = hits[0]['datahub']
        datapackage = hits[0]['datapackage']
        self.assertEqual(datahub['findability'],'published')
        self.assertEqual(datahub['owner'],'datahub')
        self.assertEqual(datahub['stats']['rowcount'], 20)
        self.assertEqual(len(datapackage['resources']), 5)

        res = requests.get('http://localhost:9200/events/_search')
        self.assertEqual(res.status_code, 200)

        events = res.json()
        hits = [hit['_source'] for hit in events['hits']['hits']
            if hit['_source']['dataset'] == 'single-file-processed']
        self.assertEqual(len(hits), 1)

        event = hits[0]
        self.assertEqual(event['event_action'],'finish')
        self.assertEqual(event['event_entity'], 'flow')
        self.assertEqual(event['owner'], 'datahub')
        self.assertEqual(event['status'], 'OK')

        # Specstore
        res = requests.get(info_latest % 'single-file-processed')
        self.assertEqual(res.status_code, 200)
        res = requests.get(info_successful % 'single-file-processed')
        self.assertEqual(res.status_code, 200)

        info = res.json()
        self.assertEqual(info['state'], 'SUCCEEDED')
        self.assertEqual(len(info['pipelines']), 7)
        self.assertEqual(info['pipelines']['datahub/single-file-processed/1']['status'], 'SUCCEEDED')
        self.assertEqual(info['pipelines']['datahub/single-file-processed/1/birthdays']['status'], 'SUCCEEDED')
        self.assertEqual(info['pipelines']['datahub/single-file-processed/1/birthdays_csv']['status'], 'SUCCEEDED')
        self.assertEqual(info['pipelines']['datahub/single-file-processed/1/birthdays_json']['status'], 'SUCCEEDED')
        self.assertEqual(info['pipelines']['datahub/single-file-processed/1/single-file-processed_zip']['status'], 'SUCCEEDED')
        self.assertEqual(info['pipelines']['datahub/single-file-processed/1/validation_report']['status'], 'SUCCEEDED')
        self.assertEqual(info['pipelines']['datahub/single-file-processed/1/birthdays_csv_preview']['status'], 'SUCCEEDED')


    def test_needs_processing_dpp(self):
        run_factory(os.path.join(os.path.dirname(
            os.path.realpath(__file__)), 'inputs/needs_processing_dpp'))

        res = requests.get(
            '{}{}/datahub/single-file-processed-dpp/1/datapackage.json'.format(S3_SERVER, self.bucket_name)).json()
        paths = dict(
            (r['name'], r['path'])
            for r in res['resources']
        )

        path = paths['birthdays']
        assert path.startswith('{}{}/datahub/single-file-processed-dpp/birthdays/data'.format(S3_SERVER, self.bucket_name))
        res = requests.get(path)
        exp_csv = open('../../outputs/csv/sample_birthdays.csv').read()
        self.assertEqual(res.status_code, 200)
        self.assertEqual(exp_csv, res.text)

        path = paths['birthdays_csv']
        assert path.startswith('{}{}/datahub/single-file-processed-dpp/birthdays_csv/data'.format(S3_SERVER, self.bucket_name))
        res = requests.get(path)
        exp_csv = open('../../outputs/csv/sorted_birthdays.csv').read()
        self.assertEqual(res.status_code, 200)
        self.assertEqual(exp_csv.replace('\n', '\r\n'), res.text)

        path = paths['birthdays_json']
        assert path.startswith('{}{}/datahub/single-file-processed-dpp/birthdays_json/data'.format(S3_SERVER, self.bucket_name))
        res = requests.get(path)
        self.assertEqual(res.status_code, 200)
        exp_json = json.load(open('../../outputs/json/sorted_birthdays.json'))
        self.assertListEqual(exp_json, res.json())

        path = paths['validation_report']
        assert path.startswith('{}{}/datahub/single-file-processed-dpp/validation_report/data'.format(S3_SERVER, self.bucket_name))
        res = requests.get(path)
        self.assertEqual(res.status_code, 200)
        report = res.json()
        self.assertTrue(report[0]['valid'])

        path = paths['single-file-processed-dpp_zip']
        assert path.startswith('{}{}/datahub/single-file-processed-dpp/single-file-processed-dpp_zip/data'.format(S3_SERVER, self.bucket_name))
        res = requests.get(path)
        self.assertEqual(res.status_code, 200)

        # Elasticsearch
        time.sleep(5)
        res = requests.get('http://localhost:9200/datahub/_search')
        self.assertEqual(res.status_code, 200)

        meta = res.json()
        hits = [hit['_source'] for hit in meta['hits']['hits']
            if hit['_source']['datapackage']['name'] == 'single-file-processed-dpp']
        self.assertEqual(len(hits), 1)

        datahub = hits[0]['datahub']
        datapackage = hits[0]['datapackage']
        self.assertEqual(datahub['findability'],'published')
        self.assertEqual(datahub['owner'],'datahub')
        self.assertEqual(datahub['stats']['rowcount'], 20)
        self.assertEqual(len(datapackage['resources']), 5)

        res = requests.get('http://localhost:9200/events/_search')
        self.assertEqual(res.status_code, 200)

        events = res.json()
        hits = [hit['_source'] for hit in events['hits']['hits']
            if hit['_source']['dataset'] == 'single-file-processed-dpp']
        self.assertEqual(len(hits), 1)

        event = hits[0]
        self.assertEqual(event['event_action'],'finish')
        self.assertEqual(event['event_entity'], 'flow')
        self.assertEqual(event['owner'], 'datahub')
        self.assertEqual(event['status'], 'OK')

        # Specstore
        res = requests.get(info_latest % 'single-file-processed-dpp')
        self.assertEqual(res.status_code, 200)
        res = requests.get(info_successful % 'single-file-processed-dpp')
        self.assertEqual(res.status_code, 200)

        info = res.json()
        self.assertEqual(info['state'], 'SUCCEEDED')
        self.assertEqual(len(info['pipelines']), 7)
        self.assertEqual(info['pipelines']['datahub/single-file-processed-dpp/1']['status'], 'SUCCEEDED')
        self.assertEqual(info['pipelines']['datahub/single-file-processed-dpp/1/birthdays']['status'], 'SUCCEEDED')
        self.assertEqual(info['pipelines']['datahub/single-file-processed-dpp/1/birthdays_csv']['status'], 'SUCCEEDED')
        self.assertEqual(info['pipelines']['datahub/single-file-processed-dpp/1/birthdays_json']['status'], 'SUCCEEDED')
        self.assertEqual(info['pipelines']['datahub/single-file-processed-dpp/1/single-file-processed-dpp_zip']['status'], 'SUCCEEDED')
        self.assertEqual(info['pipelines']['datahub/single-file-processed-dpp/1/validation_report']['status'], 'SUCCEEDED')
        self.assertEqual(info['pipelines']['datahub/single-file-processed-dpp/1/birthdays_csv_preview']['status'], 'SUCCEEDED')


    def test_private_dataset(self):
        run_factory(os.path.join(os.path.dirname(
            os.path.realpath(__file__)), 'inputs/private_dataset'))

        res = requests.get(
            '{}{}/datahub/private/1/datapackage.json'.format(S3_SERVER, self.bucket_name))
        self.assertEqual(res.status_code, 403)
        obj = self.s3.Object(self.bucket_name, 'datahub/private/1/datapackage.json')
        dp = obj.get()['Body'].read().decode('utf-8')
        dp = json.loads(dp)
        paths = dict(
            (r['name'], r['path'])
            for r in dp['resources']
        )

        path = paths['birthdays']
        assert path.startswith('{}{}/datahub/private/birthdays/data'.format(S3_SERVER, self.bucket_name))
        res = requests.get(path)

        self.assertEqual(res.status_code, 403)

        path = paths['birthdays_csv']
        assert path.startswith('{}{}/datahub/private/birthdays_csv/data'.format(S3_SERVER, self.bucket_name))
        res = requests.get(path)
        self.assertEqual(res.status_code, 403)


        path = paths['birthdays_json']
        assert path.startswith('{}{}/datahub/private/birthdays_json/data'.format(S3_SERVER, self.bucket_name))
        res = requests.get(path)
        self.assertEqual(res.status_code, 403)

        path = paths['validation_report']
        assert path.startswith('{}{}/datahub/private/validation_report/data'.format(S3_SERVER, self.bucket_name))
        res = requests.get(path)
        self.assertEqual(res.status_code, 403)

        path = paths['private_zip']
        assert path.startswith('{}{}/datahub/private/private_zip/data'.format(S3_SERVER, self.bucket_name))
        res = requests.get(path)
        self.assertEqual(res.status_code, 403)

        # Elasticsearch
        res = requests.get('http://localhost:9200/datahub/_search')
        self.assertEqual(res.status_code, 200)

        meta = res.json()
        hits = [hit['_source'] for hit in meta['hits']['hits']
            if hit['_source']['datapackage']['name'] == 'private']

        self.assertEqual(len(hits), 1)

        datahub = hits[0]['datahub']
        datapackage = hits[0]['datapackage']
        self.assertEqual(datahub['findability'],'private')
        self.assertEqual(datahub['owner'],'datahub')
        self.assertEqual(datahub['stats']['rowcount'], 20)
        self.assertEqual(len(datapackage['resources']), 5)

        time.sleep(5)
        res = requests.get('http://localhost:9200/events/_search')
        self.assertEqual(res.status_code, 200)

        events = res.json()
        hits = [hit['_source'] for hit in events['hits']['hits']
            if hit['_source']['dataset'] == 'private']
        self.assertEqual(len(hits), 1)

        event = hits[0]
        self.assertEqual(event['dataset'],'private')
        self.assertEqual(event['event_action'],'finish')
        self.assertEqual(event['event_entity'], 'flow')
        self.assertEqual(event['owner'], 'datahub')
        self.assertEqual(event['status'], 'OK')

        # Specstore
        res = requests.get(info_latest % 'private')
        self.assertEqual(res.status_code, 200)
        res = requests.get(info_successful % 'private')
        self.assertEqual(res.status_code, 200)

        info = res.json()
        self.assertEqual(info['state'], 'SUCCEEDED')
        self.assertEqual(len(info['pipelines']), 7)
        self.assertEqual(info['pipelines']['datahub/private/1']['status'], 'SUCCEEDED')
        self.assertEqual(info['pipelines']['datahub/private/1/birthdays']['status'], 'SUCCEEDED')
        self.assertEqual(info['pipelines']['datahub/private/1/birthdays_csv']['status'], 'SUCCEEDED')
        self.assertEqual(info['pipelines']['datahub/private/1/birthdays_json']['status'], 'SUCCEEDED')
        self.assertEqual(info['pipelines']['datahub/private/1/private_zip']['status'], 'SUCCEEDED')
        self.assertEqual(info['pipelines']['datahub/private/1/validation_report']['status'], 'SUCCEEDED')
        self.assertEqual(info['pipelines']['datahub/private/1/birthdays_csv_preview']['status'], 'SUCCEEDED')


    def test_elasticsearch_saves_multiple_datasets_and_events(self):
        # Run flow
        run_factory(os.path.join(os.path.dirname(
            os.path.realpath(__file__)), 'inputs/single_file'))
        res = requests.get('http://localhost:9200/datahub/_search')
        meta = res.json()
        self.assertEqual(meta['hits']['total'], 1)
        time.sleep(5)
        res = requests.get('http://localhost:9200/events/_search')
        events = res.json()
        self.assertEqual(events['hits']['total'], 1)

        # Second flow
        run_factory(os.path.join(os.path.dirname(
            os.path.realpath(__file__)), 'inputs/multiple_files'))
        res = requests.get('http://localhost:9200/datahub/_search')
        meta = res.json()
        self.assertEqual(meta['hits']['total'], 2)
        time.sleep(5)
        res = requests.get('http://localhost:9200/events/_search')
        events = res.json()
        self.assertEqual(events['hits']['total'], 2)

        # Third flows
        run_factory(os.path.join(os.path.dirname(
            os.path.realpath(__file__)), 'inputs/excel'))
        res = requests.get('http://localhost:9200/datahub/_search')
        meta = res.json()
        self.assertEqual(meta['hits']['total'], 3)
        time.sleep(5)
        res = requests.get('http://localhost:9200/events/_search')
        events = res.json()
        self.assertEqual(events['hits']['total'], 3)

    ## TODO run flow, update metadata, run again
    # def test_quick_succession_local(self):
    #     start_time = time.time()
    #     run_factory(os.path.join(os.path.dirname(
    #         os.path.realpath(__file__)), 'inputs/local/needs_processing'))
    #     time_elapsed_first_run = time.time() - start_time
    #     start_time = time.time()
    #     run_factory(os.path.join(os.path.dirname(
    #         os.path.realpath(__file__)), 'inputs/local/needs_processing'))
    #     elapsed_time_second_run = time.time() - start_time
    #     self.assertTrue(time_elapsed_first_run > elapsed_time_second_run)
