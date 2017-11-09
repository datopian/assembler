import json
import jwt
import os
import requests
import subprocess
import unittest
import yaml

import boto3
from elasticsearch import Elasticsearch

import flowmanager.controllers
from flowmanager.models import FlowRegistry
from sqlalchemy import create_engine

upload = flowmanager.controllers.upload

private_key = open('tests/private.pem').read()
public_key = open('tests/public.pem').read()

ES_SERVER = os.environ['DPP_ELASTICSEARCH'] = 'http://localhost:9200'
S3_SERVER = os.environ['S3_ENDPOINT_URL'] = 'http://localhost:5000/'
os.environ['PKGSTORE_BUCKET'] = 'testing.datahub.io'
os.environ['AWS_ACCESS_KEY_ID'] = 'foo'
os.environ['AWS_SECRET_ACCESS_KEY'] = 'bar'
os.environ['SOURCESPEC_REGISTRY_DB_ENGINE'] = DB_ENGINE = 'postgres://datahub:secret@localhost/datahq'

registry = FlowRegistry(DB_ENGINE)


def run_factory(dir='.'):
    os.chdir(dir)
    flow = yaml.load(open('assembler.source-spec.yaml'))
    token = generate_token(flow['meta']['owner'])
    response = upload(token, flow, registry, public_key)

    subprocess.call(['dpp', 'run', 'dirty'],
                    stdout=subprocess.DEVNULL,
                    stderr=subprocess.DEVNULL
                    )
    revision = registry.get_revision_by_dataset_id(response['id'])
    registry.delete_pipelines(response['id'] + '/' + str(revision['revision']))
    os.remove('.dpp.db')

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
        es = Elasticsearch(hosts=[ES_SERVER])
        es.indices.delete(index='datahub', ignore=[400, 404])
        es.indices.delete(index='events', ignore=[400, 404])
        es.indices.flush()
        for tbl in ('pipelines', 'dataset', 'dataset_revision'):
            try:
                create_engine(DB_ENGINE).execute('DELETE FROM %s' % tbl)
            except:
                pass
        s3 = boto3.resource(
            service_name='s3',
            endpoint_url=S3_SERVER,
        )
        self.bucket_name = os.environ['PKGSTORE_BUCKET']
        self.bucket = s3.Bucket(self.bucket_name)
        try:
            for obj in self.bucket.objects.all():
                obj.delete()
                self.bucket.delete()
        except:
            pass

    def test_single_file(self):
        run_factory(os.path.join(os.path.dirname(
            os.path.realpath(__file__)), 'inputs/single_file'))

        res = requests.get(
            '{}{}/datahub/single-file/latest/datapackage.json'.format(S3_SERVER, self.bucket_name)).json()

        paths = dict(
            (r['name'], r['path'])
            for r in res['resources']
        )
        path = paths['birthdays']
        assert path.startswith('{}{}/datahub/single-file/1/birthdays/data'.format(S3_SERVER, self.bucket_name))
        print(path)
        res = requests.get(path)

        exp_csv = open('../../outputs/csv/sample_birthdays.csv').read()
        self.assertEqual(res.status_code, 200)
        self.assertEqual(exp_csv, res.text)

        path = paths['birthdays_csv']
        assert path.startswith('{}{}/datahub/single-file/1/birthdays_csv/data'.format(S3_SERVER, self.bucket_name))
        res = requests.get(path)
        self.assertEqual(res.status_code, 200)
        self.assertEqual(exp_csv.replace('\n', '\r\n'), res.text)


        path = paths['birthdays_json']
        assert path.startswith('{}{}/datahub/single-file/1/birthdays_json/data'.format(S3_SERVER, self.bucket_name))
        res = requests.get(path)
        self.assertEqual(res.status_code, 200)
        exp_json = json.load(open('../../outputs/json/sample_birthdays.json'))
        self.assertListEqual(exp_json, res.json())

        path = paths['datapackage_zip']
        assert path.startswith('{}{}/datahub/single-file/1/datapackage_zip/data'.format(S3_SERVER, self.bucket_name))
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
        self.assertEqual(len(datapackage['resources']), 4)

        res = requests.get('http://localhost:9200/events/_search')
        self.assertEqual(res.status_code, 200)

        events = res.json()
        hits = [hit['_source'] for hit in events['hits']['hits']
            if hit['_source']['dataset'] == 'single-file']
        self.assertEqual(len(hits), 1)

        event = hits[0]
        self.assertEqual(event['dataset'],'single-file')
        self.assertEqual(event['event_action'],'finished')
        self.assertEqual(event['event_entity'], 'flow')
        self.assertEqual(event['owner'], 'datahub')
        self.assertEqual(event['status'], 'OK')


    def test_multiple_file(self):
        run_factory(os.path.join(os.path.dirname(
            os.path.realpath(__file__)), 'inputs/multiple_files'))

        res = requests.get(
            '{}{}/datahub/multiple-files/latest/datapackage.json'.format(S3_SERVER, self.bucket_name)).json()

        paths = dict(
            (r['name'], r['path'])
            for r in res['resources']
        )

        path = paths['birthdays']
        assert path.startswith('{}{}/datahub/multiple-files/1/birthdays/data'.format(S3_SERVER, self.bucket_name))
        res = requests.get(path)
        exp_csv = open('../../outputs/csv/sample_birthdays.csv').read()
        self.assertEqual(res.status_code, 200)
        self.assertEqual(exp_csv, res.text)

        path = paths['birthdays_csv']
        assert path.startswith('{}{}/datahub/multiple-files/1/birthdays_csv/data'.format(S3_SERVER, self.bucket_name))
        res = requests.get(path)
        self.assertEqual(res.status_code, 200)
        self.assertEqual(exp_csv.replace('\n', '\r\n'), res.text)

        path = paths['birthdays_json']
        assert path.startswith('{}{}/datahub/multiple-files/1/birthdays_json/data'.format(S3_SERVER, self.bucket_name))
        res = requests.get(path)
        self.assertEqual(res.status_code, 200)
        exp_json = json.load(open('../../outputs/json/sample_birthdays.json'))
        self.assertListEqual(exp_json, res.json())

        path = paths['emails']
        assert path.startswith('{}{}/datahub/multiple-files/1/emails/data'.format(S3_SERVER, self.bucket_name))
        res = requests.get(path)
        exp_csv = open('../../outputs/csv/sample_emails.csv').read()
        self.assertEqual(res.status_code, 200)
        self.assertEqual(exp_csv, res.text)

        path = paths['emails_csv']
        assert path.startswith('{}{}/datahub/multiple-files/1/emails_csv/data'.format(S3_SERVER, self.bucket_name))
        res = requests.get(path)
        self.assertEqual(res.status_code, 200)
        self.assertEqual(exp_csv.replace('\n', '\r\n'), res.text)

        path = paths['emails_json']
        assert path.startswith('{}{}/datahub/multiple-files/1/emails_json/data'.format(S3_SERVER, self.bucket_name))
        res = requests.get(path)
        self.assertEqual(res.status_code, 200)
        exp_json = json.load(open('../../outputs/json/sample_emails.json'))
        self.assertListEqual(exp_json, res.json())

        path = paths['datapackage_zip']
        assert path.startswith('{}{}/datahub/multiple-files/1/datapackage_zip/data'.format(S3_SERVER, self.bucket_name))
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
        self.assertEqual(len(datapackage['resources']), 7)

        res = requests.get('http://localhost:9200/events/_search')
        self.assertEqual(res.status_code, 200)

        events = res.json()
        hits = [hit['_source'] for hit in events['hits']['hits']
            if hit['_source']['dataset'] == 'multiple-files']
        self.assertEqual(len(hits), 1)

        event = hits[0]
        self.assertEqual(event['event_action'],'finished')
        self.assertEqual(event['event_entity'], 'flow')
        self.assertEqual(event['owner'], 'datahub')
        self.assertEqual(event['status'], 'OK')


    def test_excel_file(self):
        run_factory(os.path.join(os.path.dirname(
            os.path.realpath(__file__)), 'inputs/excel'))

        res = requests.get(
            '{}{}/datahub/excel/latest/datapackage.json'.format(S3_SERVER, self.bucket_name)).json()
        paths = dict(
            (r['name'], r['path'])
            for r in res['resources']
        )

        path = paths['birthdays']
        assert path.startswith('{}{}/datahub/excel/1/birthdays/data'.format(S3_SERVER, self.bucket_name))
        res = requests.get(path)
        self.assertEqual(res.status_code, 200)

        exp_csv = open('../../outputs/csv/sample_birthdays.csv').read()

        path = paths['birthdays_csv']
        assert path.startswith('{}{}/datahub/excel/1/birthdays_csv/data'.format(S3_SERVER, self.bucket_name))
        res = requests.get(path)
        self.assertEqual(res.status_code, 200)
        self.assertEqual(exp_csv.replace('\n', '\r\n'), res.text)

        path = paths['birthdays_json']
        assert path.startswith('{}{}/datahub/excel/1/birthdays_json/data'.format(S3_SERVER, self.bucket_name))
        res = requests.get(path)
        self.assertEqual(res.status_code, 200)
        exp_json = json.load(open('../../outputs/json/sample_birthdays.json'))
        self.assertListEqual(exp_json, res.json())

        path = paths['datapackage_zip']
        assert path.startswith('{}{}/datahub/excel/1/datapackage_zip/data'.format(S3_SERVER, self.bucket_name))
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
        self.assertEqual(len(datapackage['resources']), 4)

        res = requests.get('http://localhost:9200/events/_search')
        self.assertEqual(res.status_code, 200)

        events = res.json()
        hits = [hit['_source'] for hit in events['hits']['hits']
            if hit['_source']['dataset'] == 'excel']
        self.assertEqual(len(hits), 1)

        event = hits[0]
        self.assertEqual(event['event_action'],'finished')
        self.assertEqual(event['event_entity'], 'flow')
        self.assertEqual(event['owner'], 'datahub')
        self.assertEqual(event['status'], 'OK')

    def test_needs_processing(self):
        run_factory(os.path.join(os.path.dirname(
            os.path.realpath(__file__)), 'inputs/needs_processing'))

        res = requests.get(
            '{}{}/datahub/single-file-processed/latest/datapackage.json'.format(S3_SERVER, self.bucket_name)).json()
        paths = dict(
            (r['name'], r['path'])
            for r in res['resources']
        )

        path = paths['birthdays']
        assert path.startswith('{}{}/datahub/single-file-processed/1/birthdays/data'.format(S3_SERVER, self.bucket_name))
        res = requests.get(path)
        exp_csv = open('../../outputs/csv/sample_birthdays_invalid.csv').read()
        self.assertEqual(res.status_code, 200)
        self.assertEqual(exp_csv, res.text)

        path = paths['birthdays_csv']
        assert path.startswith('{}{}/datahub/single-file-processed/1/birthdays_csv/data'.format(S3_SERVER, self.bucket_name))
        res = requests.get(path)
        exp_csv = open('../../outputs/csv/sample_birthdays.csv').read()
        self.assertEqual(res.status_code, 200)
        self.assertEqual(exp_csv.replace('\n', '\r\n'), res.text)

        path = paths['birthdays_json']
        assert path.startswith('{}{}/datahub/single-file-processed/1/birthdays_json/data'.format(S3_SERVER, self.bucket_name))
        res = requests.get(path)
        self.assertEqual(res.status_code, 200)
        exp_json = json.load(open('../../outputs/json/sample_birthdays.json'))
        self.assertListEqual(exp_json, res.json())


        path = paths['datapackage_zip']
        assert path.startswith('{}{}/datahub/single-file-processed/1/datapackage_zip/data'.format(S3_SERVER, self.bucket_name))
        res = requests.get(path)
        self.assertEqual(res.status_code, 200)

        # Elasticsearch
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
        self.assertEqual(len(datapackage['resources']), 4)

        res = requests.get('http://localhost:9200/events/_search')
        self.assertEqual(res.status_code, 200)

        events = res.json()
        hits = [hit['_source'] for hit in events['hits']['hits']
            if hit['_source']['dataset'] == 'single-file-processed']
        self.assertEqual(len(hits), 1)

        event = hits[0]
        self.assertEqual(event['event_action'],'finished')
        self.assertEqual(event['event_entity'], 'flow')
        self.assertEqual(event['owner'], 'datahub')
        self.assertEqual(event['status'], 'OK')

    def test_elasticsearch_saves_multiple_datasets_and_events(self):
        # Run flow
        run_factory(os.path.join(os.path.dirname(
            os.path.realpath(__file__)), 'inputs/single_file'))
        res = requests.get('http://localhost:9200/datahub/_search')
        meta = res.json()
        res = requests.get('http://localhost:9200/events/_search')
        events = res.json()
        self.assertEqual(meta['hits']['total'], 1)
        self.assertEqual(events['hits']['total'], 1)

        # Second flow
        run_factory(os.path.join(os.path.dirname(
            os.path.realpath(__file__)), 'inputs/multiple_files'))
        res = requests.get('http://localhost:9200/datahub/_search')
        meta = res.json()
        res = requests.get('http://localhost:9200/events/_search')
        events = res.json()
        self.assertEqual(meta['hits']['total'], 2)
        self.assertEqual(events['hits']['total'], 2)

        # Third flows
        run_factory(os.path.join(os.path.dirname(
            os.path.realpath(__file__)), 'inputs/excel'))
        res = requests.get('http://localhost:9200/datahub/_search')
        meta = res.json()
        res = requests.get('http://localhost:9200/events/_search')
        events = res.json()
        self.assertEqual(meta['hits']['total'], 3)
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

    # @classmethod
    # def teardown_class(self):
    #     # for obj in self.bucket.objects.all():
    #     #     obj.delete()
    #     # self.bucket.delete()
