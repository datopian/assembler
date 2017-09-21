from .base_processing_node import BaseProcessingNode, ProcessingArtifact


class DerivedPreviewProcessingNode(BaseProcessingNode):
    def __init__(self, available_artifacts, outputs):
        super(DerivedPreviewProcessingNode, self).__init__(available_artifacts, outputs)
        self.fmt = 'json'

    def get_artifacts(self):
        for artifact in self.available_artifacts:
            if artifact.datahub_type == 'derived/csv':
                datahub_type = 'derived/preview'
                resource_name = artifact.resource_name + '_preview'
                output = ProcessingArtifact(
                    datahub_type, resource_name,
                    [artifact], [],
                    [('assembler.update_resource',
                      {
                          'name': artifact.resource_name,
                          'update': {
                              'name': resource_name,
                              'format': self.fmt,
                              'path': 'data/{}.{}'.format(resource_name, self.fmt),
                              'datahub': {
                                'type': "derived/preview",
                                'derivedFrom': [
                                    artifact.resource_name.replace('_csv', '')
                                ]
                              },
                              "forView": [
                                'datahub-preview-{}'.format(resource_name)
                              ]
                          }
                      }),
                    # TODO pass limit as a parameter
                    ('assembler.load_preview', {}),
                    ('assembler.load_views', {})],
                    True
                )
                yield output
