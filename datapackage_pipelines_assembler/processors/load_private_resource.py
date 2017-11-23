from datapackage_pipelines.wrapper import ingest
from datapackage_pipelines.lib.load_resource import ResourceLoader
from planner.utilities import s3_path


class PrivateResourceLoader(ResourceLoader):

    def __init__(self):
        super(ResourceLoader, self).__init__()
        self.parameters, self.dp, self.res_iter = ingest()

    def process_datapackage(self, dp_):
        for res in dp_.resources:
            cached_path = res.descriptor['path']
            res.descriptor['path'] = s3_path(res.source)
            res.commit()
            res.descriptor['path'] = cached_path  # Keep original paths
        return dp_


if __name__ == '__main__':
    PrivateResourceLoader()()
