from dataclasses import dataclass
from pathlib import Path
from typing import Protocol


@dataclass(frozen=True)
class FetchResult:
    status: int
    body: bytes


class UpstreamError(Exception):
    def __init__(self, message: str, *, retryable: bool):
        super().__init__(message)
        self.retryable = retryable


class FeedAdapter(Protocol):
    name: str

    def fetch(self) -> FetchResult: ...


class FixtureAdapter:
    def __init__(self, name: str, path: Path):
        self.name = name
        self._body = Path(path).read_bytes()

    def set_body(self, body: bytes) -> None:
        self._body = body

    def fetch(self) -> FetchResult:
        return FetchResult(status=200, body=self._body)
