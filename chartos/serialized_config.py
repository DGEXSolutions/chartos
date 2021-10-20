from dataclasses import dataclass, field
from dataclasses_json import dataclass_json
from typing import List, Optional


@dataclass_json
@dataclass
class SerializedField:
    name: str
    description: str
    type: str


@dataclass_json
@dataclass
class SerializedView:
    name: str
    on_field: str
    fields: List[str]
    # defaults to 1 hour
    cache_duration: Optional[int] = None


@dataclass_json
@dataclass
class SerializedLayer:
    name: str
    versionned: bool
    fields: List[SerializedField]
    views: List[SerializedView]


@dataclass_json
@dataclass
class SerializedConfig:
    name: str
    description: str
    layers: List[SerializedLayer]
