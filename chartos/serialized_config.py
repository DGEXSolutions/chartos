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
    id_field_name: str  # this is returned as mvt metadata
    fields: List[SerializedField]
    views: List[SerializedView]
    description: Optional[str] = None
    attribution: Optional[str] = None


class SerializedConfig(BaseModel):
    name: str
    description: str
    layers: List[SerializedLayer]
