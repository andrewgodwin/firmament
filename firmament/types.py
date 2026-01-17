from typing import TypedDict


class FileVersionMeta(TypedDict):
    mtime: int
    size: int


class LocalVersionData(TypedDict):
    content_hash: str | None
    mtime: int
    size: int


FileVersionData = dict[str, FileVersionMeta]
