from dataclasses import dataclass, field
from pathlib import Path
from typing import TYPE_CHECKING

from textual.app import App, ComposeResult
from textual.widgets import Footer, Tree
from textual.widgets.tree import TreeNode as TextualTreeNode

from firmament.constants import (
    PATH_REQUEST_DOWNLOAD_ONCE,
    PATH_REQUEST_IGNORE,
    PATH_REQUEST_ON_DEMAND,
    PATH_REQUEST_SYNC,
)

if TYPE_CHECKING:
    from firmament.config import Config


@dataclass
class FileTreeNode:
    """
    Represents a node in the file tree.
    """

    path: str
    name: str
    is_dir: bool
    is_local: bool
    is_remote: bool
    size: int | None
    path_request: str
    path_request_explicit: bool  # True if set explicitly, False if inherited
    children: dict[str, "FileTreeNode"] = field(default_factory=dict)


def get_path_request_safe(config: "Config", path: str) -> str:
    """
    Safe version of PathRequestsStore.get that doesn't have infinite recursion.

    The original PathRequestsStore.get() method has a bug on line 97 where it calls
    self.get() recursively instead of self.requests.get().
    """
    if not path.startswith("/"):
        raise ValueError("Path must start with /")

    path_obj = Path(path)
    while True:
        current_path = str(path_obj)
        if current_path in config.path_requests.requests:
            return config.path_requests.requests[current_path]
        if path_obj == path_obj.parent:
            break
        path_obj = path_obj.parent

    # Default
    return config.path_requests.requests.get("/", "OD")


def build_file_tree(config: "Config", remote: str) -> FileTreeNode:
    """
    Build unified tree from local and remote files.
    """
    # Root node
    root = FileTreeNode(
        path="",
        name="/",
        is_dir=True,
        is_local=True,
        is_remote=True,
        size=None,
        path_request="OD",
        path_request_explicit=False,
        children={},
    )

    # Phase 1: Scan remote files
    try:
        remote_files = config.rclone.get_all_files(f"{remote}")
        for file_info in remote_files:
            path = file_info["Path"]
            parts = path.split("/")

            # Insert into tree, creating intermediate directories
            current = root
            for i, part in enumerate(parts):
                is_file = i == len(parts) - 1
                if part not in current.children:
                    current.children[part] = FileTreeNode(
                        path="/".join(parts[: i + 1]),
                        name=part,
                        is_dir=not is_file,
                        is_remote=True,
                        is_local=False,
                        size=file_info.get("Size") if is_file else None,
                        path_request="OD",
                        path_request_explicit=False,
                        children={},
                    )
                else:
                    # Update existing node
                    current.children[part].is_remote = True
                    if is_file and "Size" in file_info:
                        current.children[part].size = file_info["Size"]
                current = current.children[part]
    except Exception as e:
        print(f"Warning: Failed to fetch remote files: {e}")

    # Phase 2: Scan local files
    for local_path in config.root_path.rglob("*"):
        # Skip .firmament
        if ".firmament" in local_path.parts:
            continue

        try:
            rel_path = local_path.relative_to(config.root_path)
            parts = list(rel_path.parts)

            # Insert or update
            current = root
            for i, part in enumerate(parts):
                is_file = (i == len(parts) - 1) and local_path.is_file()
                if part not in current.children:
                    current.children[part] = FileTreeNode(
                        path="/".join(parts[: i + 1]),
                        name=part,
                        is_dir=not is_file,
                        is_remote=False,
                        is_local=True,
                        size=local_path.stat().st_size if is_file else None,
                        path_request="OD",
                        path_request_explicit=False,
                        children={},
                    )
                else:
                    # Update existing node
                    current.children[part].is_local = True
                    if is_file:
                        try:
                            current.children[part].size = local_path.stat().st_size
                        except OSError:
                            pass  # Ignore stat errors
                current = current.children[part]
        except (OSError, ValueError):
            # Skip files we can't read
            continue

    # Phase 3: Apply path requests (recursive)
    def apply_requests(node: FileTreeNode):
        full_path = f"/{node.path}" if node.path else "/"
        node.path_request = get_path_request_safe(config, full_path)
        # Check if this path is explicitly set or inherited
        node.path_request_explicit = full_path in config.path_requests.requests
        for child in node.children.values():
            apply_requests(child)

    apply_requests(root)
    return root


def format_size(size_bytes: int) -> str:
    """
    Format file size in human-readable format.
    """
    size = float(size_bytes)
    for unit in ["B", "KB", "MB", "GB", "TB"]:
        if size < 1024.0:
            return f"{size:.1f}{unit}"
        size /= 1024.0
    return f"{size:.1f}PB"


class BrowseApp(App):
    """
    Interactive file browser TUI.
    """

    BINDINGS = [
        ("q", "quit", "Quit"),
        ("r", "refresh", "Refresh"),
        ("left", "collapse_node", "Collapse"),
        ("right", "expand_node", "Expand"),
        ("o", "set_on_demand", "On Demand"),
        ("s", "set_sync", "Sync"),
        ("d", "set_download_once", "Download Once"),
        ("i", "set_ignore", "Ignore"),
        ("x", "unset_request", "Unset"),
    ]

    CSS = """
    Tree {
        height: 100%;
    }
    """

    def __init__(self, config: "Config", remote: str):
        super().__init__()
        self.config = config
        self.remote = remote
        self.file_tree: FileTreeNode | None = None

    def compose(self) -> ComposeResult:
        yield Tree("Loading...")
        yield Footer()

    def on_mount(self) -> None:
        """
        Load tree data when app starts.
        """
        tree = self.query_one(Tree)
        tree.loading = True

        # Build tree data
        self.file_tree = build_file_tree(self.config, self.remote)

        # Populate Textual tree
        tree.clear()
        tree.root.label = "/"
        if self.file_tree is not None:
            self.populate_tree(tree.root, self.file_tree)
        tree.root.expand()
        tree.loading = False

    def populate_tree(
        self, textual_node: TextualTreeNode, data_node: FileTreeNode
    ) -> None:
        """
        Recursively populate Textual tree from data tree.
        """
        # Sort directories first, then files, alphabetically within each group
        sorted_children = sorted(
            data_node.children.items(), key=lambda item: (not item[1].is_dir, item[0])
        )

        for child_name, child_data in sorted_children:
            # Format label with status indicators
            label = self.format_node_label(child_data)

            # Add to tree
            # For files (non-directories), disable the expand arrow
            tree_node = textual_node.add(
                label, data=child_data, allow_expand=child_data.is_dir
            )

            # If directory, add children
            if child_data.is_dir and child_data.children:
                self.populate_tree(tree_node, child_data)

    def format_node_label(self, node: FileTreeNode) -> str:
        """
        Format a node's display label.
        """
        # Map two-letter codes to single letters
        request_short = {
            "OD": "O",
            "DO": "D",
            "SY": "S",
            "IG": "I",
        }
        short_request = request_short.get(node.path_request, node.path_request)

        # Use brackets for explicit
        if not node.path_request_explicit:
            short_request = f"_{short_request}_"
        else:
            short_request = f"\\[{short_request}]"

        # Color mapping for path requests using hex colors
        request_colors = {
            "\\[O]": "#FFD700",  # Gold
            "_O_": "#B8860B",  # Dark goldenrod
            "\\[D]": "#00FF00",  # Lime
            "_D_": "#006400",  # Dark green
            "\\[S]": "#1E90FF",  # Dodger blue
            "_S_": "#4682B4",  # Steel blue
            "\\[I]": "#808080",  # Grey
            "_I_": "#696969",  # Dim grey
        }
        request_color = request_colors.get(short_request, "white")

        # Base name
        name = node.name
        if node.is_dir:
            # Directories: just path request and name (no location)
            name = f"[bold cyan]{name}/[/]"
            label = f"[{request_color}]{short_request}[/{request_color}] {name}"
        else:
            # Files: show location indicator where arrow would be
            if node.is_local and node.is_remote:
                location = "B"
                location_color = "blue"
            elif node.is_local:
                location = "L"
                location_color = "green"
            elif node.is_remote:
                location = "R"
                location_color = "red"
            else:
                location = "?"
                location_color = "white"

            # Format: location/request name size
            label = (
                f"[{location_color}]{location}[/{location_color}]"
                f"[white] [/white]"
                f"[{request_color}]{short_request}[/{request_color}] {name}"
            )

            # Add size for files
            if node.size is not None:
                label += f" [dim]{format_size(node.size)}[/]"

        return label

    def action_expand_node(self) -> None:
        """
        Expand the currently selected directory node.
        """
        tree = self.query_one(Tree)
        if tree.cursor_node is not None:
            tree.cursor_node.expand()

    def action_collapse_node(self) -> None:
        """
        Collapse the currently selected directory node.
        """
        tree = self.query_one(Tree)
        if tree.cursor_node is not None:
            tree.cursor_node.collapse()

    def _set_path_request(self, request: str) -> None:
        """
        Set the path request for the currently selected node.
        """
        tree = self.query_one(Tree)
        if tree.cursor_node is not None and tree.cursor_node.data is not None:
            node: FileTreeNode = tree.cursor_node.data
            full_path = f"/{node.path}" if node.path else "/"
            self.config.path_requests.set(full_path, request)  # type:ignore
            self.config.path_requests.save()

            # Update the node's data and label without full refresh
            node.path_request = request
            node.path_request_explicit = True
            tree.cursor_node.label = self.format_node_label(node)

    def _unset_path_request(self) -> None:
        """
        Unset the explicit path request for the currently selected node.
        """
        tree = self.query_one(Tree)
        if tree.cursor_node is not None and tree.cursor_node.data is not None:
            node: FileTreeNode = tree.cursor_node.data
            full_path = f"/{node.path}" if node.path else "/"
            if full_path in self.config.path_requests.requests:
                del self.config.path_requests.requests[full_path]
                self.config.path_requests.save()

                # Update the node's data and label without full refresh
                node.path_request = get_path_request_safe(self.config, full_path)
                node.path_request_explicit = False
                tree.cursor_node.label = self.format_node_label(node)

    def action_set_on_demand(self) -> None:
        """
        Set the selected path to ON_DEMAND.
        """
        self._set_path_request(PATH_REQUEST_ON_DEMAND)

    def action_set_sync(self) -> None:
        """
        Set the selected path to SYNC.
        """
        self._set_path_request(PATH_REQUEST_SYNC)

    def action_set_download_once(self) -> None:
        """
        Set the selected path to DOWNLOAD_ONCE.
        """
        self._set_path_request(PATH_REQUEST_DOWNLOAD_ONCE)

    def action_set_ignore(self) -> None:
        """
        Set the selected path to IGNORE.
        """
        self._set_path_request(PATH_REQUEST_IGNORE)

    def action_unset_request(self) -> None:
        """
        Unset the explicit path request, allowing it to inherit.
        """
        self._unset_path_request()

    def action_refresh(self) -> None:
        """
        Refresh the tree.
        """
        self.on_mount()
