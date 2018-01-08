import datetime
import os

import filemanager

from datapackage_pipelines_aws.s3_dumper import S3Dumper


class MyS3Dumper(S3Dumper):

    def __init__(self):
        super(MyS3Dumper, self).__init__()
        self.fm = filemanager.FileManager(os.environ.get('FILEMANAGER_DATABASE_URL'))

    def put_object(self, **kwargs):
        super(MyS3Dumper, self).put_object(**kwargs)
        datahub = self.datapackage['datahub']
        self.fm.add_file(
            kwargs['Bucket'],
            kwargs['Key'],
            datahub.get('findability'),
            datahub.get('owner'),
            datahub.get('ownerid'),
            self.datapackage.get('id'),
            datahub.get('flowid'),
            os.stat(kwargs['Body'].name).st_size,
            datetime.datetime.now()
        )


if __name__ == "__main__":
    MyS3Dumper()()
