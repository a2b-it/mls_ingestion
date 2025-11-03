
from __future__ import annotations
from typing import Dict, Any, Optional, List, Literal
from pydantic import BaseModel, Field, HttpUrl, validator

InputFormat = Literal["json", "xml"]

ResponseSource = Literal["status", "header", "json", "xml"]

class ResponseField(BaseModel):
    name: str
    source: ResponseSource = "json"
    expr: Optional[str] = None

class ResponseSpec(BaseModel):
    fields: List[ResponseField]

class AuthConfig(BaseModel):
    type: Literal["none", "basic", "bearer", "api_key"] = "none"
    username: Optional[str] = None
    password: Optional[str] = None
    token: Optional[str] = None
    header: Optional[str] = None
    query_param: Optional[str] = None
    value: Optional[str] = None

class RequestSpec(BaseModel):
    method: Literal["GET","POST"] = "GET"
    url: str
    headers: Dict[str, str] = Field(default_factory=dict)
    params: Dict[str, Any] = Field(default_factory=dict)
    body: Optional[Dict[str, Any]] = None
    timeout_seconds: float = 30.0

class MappingField(BaseModel):
    # For JSON use JMESPath; for XML use XPath.
    name: str
    expr: str

class MappingSpec(BaseModel):
    # Either specify a "root" iterable (JMES/xpath for arrays), or leave None and map scalar fields.
    root: Optional[str] = None
    fields: List[MappingField]

class SinkSpec(BaseModel):
    type: Literal["ndjson","csv"] = "ndjson"
    path: str
    mode: Literal["overwrite","append"] = "overwrite"

class SourceConfig(BaseModel):
    name: str
    mls_id: int
    input_format: InputFormat
    auth: AuthConfig = Field(default_factory=AuthConfig)
    request: RequestSpec
    mapping: MappingSpec
    response: Optional[ResponseSpec] = None
    sink: SinkSpec
    paginate: Optional[Dict[str, Any]] = None  # e.g., {"type":"page","param":"page","start":1,"limit":100, "max_pages":10}

class AppConfig(BaseModel):
    sources: List[SourceConfig]

    @validator("sources")
    def ensure_unique_mls(cls, v: List[SourceConfig]):
        seen = set()
        for s in v:
            if s.mls_id in seen:
                raise ValueError(f"Duplicate mls_id {s.mls_id}")
            seen.add(s.mls_id)
        return v
