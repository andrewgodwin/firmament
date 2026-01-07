import enum

BLOCK_SIZE = 64 * 1024 * 1024  # 64MB


class FILE_TYPES(enum.Enum):
    REGULAR = 1
    EXECUTABLE = 2
    SYMLINK = 3  # Unused right now but reserved
