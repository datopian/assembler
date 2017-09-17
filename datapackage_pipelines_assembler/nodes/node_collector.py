from .base_processing_node import ProcessingArtifact
from .basic_nodes import DerivedCSVProcessingNode, DerivedJSONProcessingNode, NonTabularProcessingNode

ORDERED_NODE_CLASSES = [
    NonTabularProcessingNode,
    DerivedCSVProcessingNode,
    DerivedJSONProcessingNode,
]


def collect_artifacts(artifacts, outputs):
    for cls in ORDERED_NODE_CLASSES:
        node = cls(artifacts, outputs)
        yield from node.get_artifacts()

