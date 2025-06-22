from pydantic import BaseModel, HttpUrl
from typing import Optional
from datetime import datetime

class SourceConfig(BaseModel):
    source_id: int
    source_name: str
    source_type: str
    connection_string: str
    database_name: Optional[str]
    schema_name: Optional[str]
    object_name: Optional[str]
    file_format: Optional[str]
    delimiter: Optional[str]
    has_header: Optional[bool]
    load_type: Optional[str]
    active: Optional[bool]
    frequency: Optional[str]
    last_update: Optional[datetime]
    description: Optional[str]
    