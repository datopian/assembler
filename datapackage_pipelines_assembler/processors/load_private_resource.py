
import os

from datapackage_pipelines.wrapper import get_dependency_datapackage_url
from datapackage_pipelines.lib.load_resource import ResourceLoader
from planner.utilities import s3_path

PKGSTORE_BUCKET = os.environ.get('PKGSTORE_BUCKET')


class PrivateResourceLoader(ResourceLoader):

    def __init__(self):
        super(PrivateResourceLoader, self).__init__()
        url = self.parameters['url']
        if url.startswith('dependency://'):
            url = url[13:]
            url = get_dependency_datapackage_url(url)
            if PKGSTORE_BUCKET in url:
                url = url.split(PKGSTORE_BUCKET)[1][1:]
                if os.path.exists(url):
                    self.parameters['url'] = url

    def process_datapackage(self, dp_):
        for res in dp_.resources:
            cached_path = res.descriptor['path']
            if cached_path.startswith('http'):
                res.descriptor['path'] = s3_path(res.source)
                res.commit()
                res.descriptor['path'] = cached_path  # Keep original paths
        return dp_


if __name__ == '__main__':
    PrivateResourceLoader()()
