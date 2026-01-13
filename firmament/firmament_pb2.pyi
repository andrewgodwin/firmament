from typing import ClassVar as _ClassVar
from typing import Iterable as _Iterable
from typing import Mapping as _Mapping
from typing import Optional as _Optional
from typing import Union as _Union

from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from google.protobuf.internal import containers as _containers

DESCRIPTOR: _descriptor.FileDescriptor

class Content(_message.Message):
    __slots__ = ["sha256sum"]
    SHA256SUM_FIELD_NUMBER: _ClassVar[int]
    sha256sum: str
    def __init__(self, sha256sum: _Optional[str] = ...) -> None: ...

class ContentDatabase(_message.Message):
    __slots__ = ["contents"]
    CONTENTS_FIELD_NUMBER: _ClassVar[int]
    contents: _containers.RepeatedCompositeFieldContainer[Content]
    def __init__(
        self, contents: _Optional[_Iterable[_Union[Content, _Mapping]]] = ...
    ) -> None: ...

class FileVersion(_message.Message):
    __slots__ = ["content", "mtime", "path", "size"]
    CONTENT_FIELD_NUMBER: _ClassVar[int]
    MTIME_FIELD_NUMBER: _ClassVar[int]
    PATH_FIELD_NUMBER: _ClassVar[int]
    SIZE_FIELD_NUMBER: _ClassVar[int]
    content: str
    mtime: int
    path: str
    size: int
    def __init__(
        self,
        path: _Optional[str] = ...,
        content: _Optional[str] = ...,
        mtime: _Optional[int] = ...,
        size: _Optional[int] = ...,
    ) -> None: ...

class FileVersionDatabase(_message.Message):
    __slots__ = ["file_versions"]
    FILE_VERSIONS_FIELD_NUMBER: _ClassVar[int]
    file_versions: _containers.RepeatedCompositeFieldContainer[FileVersion]
    def __init__(
        self, file_versions: _Optional[_Iterable[_Union[FileVersion, _Mapping]]] = ...
    ) -> None: ...
