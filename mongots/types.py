from datetime import datetime
from typing import Any
from typing import Dict
from typing import List
from typing import Tuple
from typing import Union


Filters = Dict[str, Any]
Groupby = List[str]
Number = Union[int, float]
Tags = Dict[str, Any]

MetadataTags = Dict[str, List[Any]]
MetadataTimeRange = Union[None, Tuple[datetime, datetime]]

PipelineStageMatch = Dict[str, Filters]
PipelineStageProject = Dict[str, Any]
PipelineStageUnwind = Dict[str, Any]
PipelineStage = Union[
    PipelineStageMatch,
    PipelineStageProject,
    PipelineStageUnwind,
]
Pipeline = List[PipelineStage]
