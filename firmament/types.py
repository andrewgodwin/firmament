from typing import Literal, TypedDict


class FileVersionMeta(TypedDict):
    mtime: int
    size: int


FileVersionData = dict[str, FileVersionMeta]


class LocalVersionData(TypedDict):
    content_hash: str | None
    mtime: int
    size: int
    last_hashed: int | None


PathRequestType = Literal["full", "on-demand", "download-once", "ignore"]
