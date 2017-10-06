from .basic_nodes import DerivedCSVProcessingNode, DerivedJSONProcessingNode, \
    NonTabularProcessingNode
from .view_nodes import DerivedPreviewProcessingNode
from .output_nodes import OutputToZipProcessingNode

ORDERED_NODE_CLASSES = [
    DerivedCSVProcessingNode,
    DerivedPreviewProcessingNode,
    DerivedJSONProcessingNode,
    OutputToZipProcessingNode,
    NonTabularProcessingNode,
]


def collect_artifacts(artifacts, outputs):
    for cls in ORDERED_NODE_CLASSES:
        node = cls(artifacts, outputs)
        ret = list(node.get_artifacts())
        artifacts.extend(ret)
        yield from ret
