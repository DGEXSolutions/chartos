from pydantic import BaseModel
from typing import List, Optional


class SerializedField(BaseModel):
    name: str
    description: str
    type: str


class SerializedView(BaseModel):
    name: str
    on_field: str
    fields: Optional[List[str]] = None
    exclude_fields: Optional[List[str]] = None
    # defaults to 1 hour
    cache_duration: Optional[int] = None


class SerializedLayer(BaseModel):
    name: str
    versioned: bool
    fields: List[SerializedField]
    views: List[SerializedView]


class SerializedConfig(BaseModel):
    name: str
    description: str
    layers: List[SerializedLayer]
