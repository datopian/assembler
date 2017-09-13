from .base_processing_node import ProcessingArtifact
from .basic_nodes import DerivedCSVProcessingNode, DerivedJSONProcessingNode, NonTabularProcessingNode

ORDERED_NODE_CLASSES = [
    NonTabularProcessingNode,
    DerivedCSVProcessingNode,
    DerivedJSONProcessingNode,
]


def collect_artifacts(artifacts):
    for cls in ORDERED_NODE_CLASSES:
        node = cls(artifacts)
        yield from node.get_artifacts()

