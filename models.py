from pydantic import BaseModel, Field
from typing import Literal

SourceType = Literal["directory", "comparison", "review", "alternative", "media", "unknown"]


class SourceRecord(BaseModel):
    url:         str
    domain:      str
    source_type: SourceType
    snippet:     str = ""
    title:       str = ""
    relevance:   float = 0.0


class CompetitorProfile(BaseModel):
    model_config = {"extra": "ignore"}
    
    name:           str
    domain:         str
    pricing:        str = ""
    products:       list[str] = Field(default_factory=list)
    website_notes:  str = ""
    sw_stack:       list[str] = Field(default_factory=list)
    exposure:       str = ""
    useful_sources: list[SourceRecord] = Field(default_factory=list)

    def sources_by_type(self) -> dict[str, list[str]]:
        out: dict[str, list[str]] = {}
        for s in self.useful_sources:
            out.setdefault(s.source_type, []).append(s.url)
        return out
