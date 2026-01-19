from typing import List

from pydantic import BaseModel, Field


class SearchRecord(BaseModel):
    """Record of a single kernel search operation."""

    search_str: str
    datetime: str
    file_name: str
    amount: int = 0


class KernelSearch(BaseModel):
    """Container for a list of kernel search records."""

    search: List[SearchRecord] = Field(default_factory=list)


class Memory(BaseModel):
    """Top-level memory structure storing kernel search history."""

    kernels: KernelSearch = KernelSearch()
