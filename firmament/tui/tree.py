from dataclasses import dataclass, field
from enum import Enum
from typing import TYPE_CHECKING

from rich.style import Style
from rich.text import Text
from textual.widgets import Tree
from textual.widgets.tree import TreeNode

from firmament.constants import DELETED_CONTENT_HASH
from firmament.types import PathRequestType

if TYPE_CHECKING:
    from firmament.config import Config


class FileStatus(Enum):
    """Status of a file in the sync system"""

    AVAILABLE = "A"  # Has FileVersion but no LocalVersion
    LOCAL = "L"  # Has both FileVersion and LocalVersion
    DELETED = "X"  # Most recent FileVersion is DELETED_CONTENT_HASH


@dataclass
class TreeNodeData:
    """Data associated with each tree node"""

    path: str
    name: str
    is_directory: bool
    status: FileStatus | None  # None for directories
    path_request: PathRequestType | None  # Direct PathRequest if set
    effective_path_request: PathRequestType  # Resolved (inherited) PathRequest
    backend_count: int = 0  # Number of backends that have this file's content
    children: dict[str, "TreeNodeData"] = field(default_factory=dict)


def build_tree(config: "Config") -> TreeNodeData:
    """
    Build a tree structure from flat FileVersion paths.
    """
    root = TreeNodeData(
        path="/",
        name="<root>",
        is_directory=True,
        status=None,
        path_request=config.path_requests.get("/"),
        effective_path_request="full",
        children={},
    )

    # Get all file paths from FileVersions
    for file_path in config.file_versions.keys():
        # Paths start with /, so split produces ["", "dir", "file", ...]
        parts = file_path.split("/")[1:]  # Skip empty first element
        current = root
        current_path = ""

        # Create/traverse directory nodes
        for part in parts[:-1]:
            current_path = f"{current_path}/{part}"
            if part not in current.children:
                current.children[part] = TreeNodeData(
                    path=current_path,
                    name=part,
                    is_directory=True,
                    status=None,
                    path_request=config.path_requests.get(current_path),
                    effective_path_request=config.path_requests.resolve_status(
                        current_path
                    ),
                    children={},
                )
            current = current.children[part]

        # Create file node
        filename = parts[-1]
        has_local = file_path in config.local_versions

        # Count backends that have the most recent version of this file
        content_hash, _ = config.file_versions.most_recent_content(file_path)

        # Determine file status
        if content_hash == DELETED_CONTENT_HASH:
            status = FileStatus.DELETED
        elif has_local:
            status = FileStatus.LOCAL
        else:
            status = FileStatus.AVAILABLE

        backend_count = 0
        if content_hash and content_hash != DELETED_CONTENT_HASH:
            backends = config.content_backends.get(content_hash, [])
            backend_count = len(backends) if backends else 0

        current.children[filename] = TreeNodeData(
            path=file_path,
            name=filename,
            is_directory=False,
            status=status,
            path_request=config.path_requests.get(file_path),
            effective_path_request=config.path_requests.resolve_status(file_path),
            backend_count=backend_count,
            children={},
        )

    return root


class FileTree(Tree[TreeNodeData]):
    """Custom tree widget for displaying file sync status"""

    DEFAULT_CSS = """
    FileTree {
        background: $surface;
    }
    FileTree > .tree--cursor {
        background: $surface-lighten-1;
    }
    FileTree:focus > .tree--cursor {
        background: $accent;
    }
    """

    BINDINGS = [
        ("left", "collapse", "Collapse"),
        ("right", "expand", "Expand"),
    ]

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.show_root = True
        self.guide_depth = 3

    def action_collapse(self) -> None:
        """Collapse the current directory node, or move to parent if on a file or collapsed dir"""
        if self.cursor_node:
            if self.cursor_node.is_expanded:
                self.cursor_node.collapse()
            elif self.cursor_node.parent:
                self.select_node(self.cursor_node.parent)

    def action_expand(self) -> None:
        """Expand the current directory node"""
        if self.cursor_node and not self.cursor_node.is_expanded:
            self.cursor_node.expand()

    def render_label(
        self, node: TreeNode[TreeNodeData], base_style: Style, style: Style
    ) -> Text:
        """Render node label with status and path request indicators"""
        data = node.data
        if data is None:
            return Text(str(node.label), style=style)

        label = Text(style=style)

        # Status indicator
        if data.is_directory:
            label.append("[D] ", style=Style(color="grey70") + style)
        elif data.status == FileStatus.DELETED:
            label.append("[X] ", style=Style(color="red") + style)
        elif data.status == FileStatus.LOCAL:
            label.append("[L] ", style=Style(color="green") + style)
        else:
            label.append("[A] ", style=Style(color="yellow") + style)

        # PathRequest indicator
        req = data.path_request  # Direct request (not inherited)
        eff = data.effective_path_request  # Effective (possibly inherited)

        if req is not None:
            # Has direct PathRequest set
            req_char = self._path_request_char(req)
            label.append(f"<{req_char}> ", style=Style(color="cyan", bold=True) + style)
        elif eff != "full" or data.path == "/":
            # Inherited non-default, or root node (always show root's effective status)
            req_char = self._path_request_char(eff)
            label.append(f"({req_char}) ", style=Style(color="cyan", dim=True) + style)
        else:
            label.append("     ", style=style)

        # Node name
        if data.is_directory:
            # Don't append "/" to root node
            if data.path == "/":
                label.append(data.name, style=Style(color="blue", bold=True) + style)
            else:
                label.append(
                    data.name + "/", style=Style(color="blue", bold=True) + style
                )
        else:
            label.append(data.name, style=style)
            # Show backend count for files
            if data.backend_count > 0:
                label.append(f" ({data.backend_count})", style=Style(dim=True) + style)

        return label

    @staticmethod
    def _path_request_char(pr: PathRequestType) -> str:
        """Single character representation of PathRequestType"""
        return {
            "full": "F",
            "on-demand": "O",
            "download-once": "D",
            "ignore": "I",
        }[pr]
