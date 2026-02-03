from dataclasses import dataclass, field
from pathlib import Path
from typing import TYPE_CHECKING

from textual.app import App, ComposeResult
from textual.widgets import Footer, Tree
from textual.widgets.tree import TreeNode as TextualTreeNode

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
        for child_name, child_data in sorted(data_node.children.items()):
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
        # Color mapping for path requests
        request_colors = {
            "OD": "yellow",
            "DO": "green",
            "SY": "blue",
            "IG": "red",
        }
        base_color = request_colors.get(node.path_request, "white")

        # Use dim color for inherited, bright for explicit
        color = f"dim {base_color}" if not node.path_request_explicit else base_color

        # Format path request with brackets/parens for explicit/inherited
        bracket_open = "[" if node.path_request_explicit else "("
        bracket_close = "]" if node.path_request_explicit else ")"
        request_label = (
            f"[{color}]{bracket_open}{node.path_request}{bracket_close}[/{color}]"
        )

        # Base name
        name = node.name
        if node.is_dir:
            name = f"[bold cyan]{name}/[/]"

        # Start with path request
        label = f"{request_label} {name}"

        # Add location indicators
        locations = []
        if node.is_local:
            locations.append("L")
        if node.is_remote:
            locations.append("R")
        if locations:
            label += f" [dim yellow]({''.join(locations)})[/]"

        # Add size for files
        if not node.is_dir and node.size is not None:
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

    def action_refresh(self) -> None:
        """
        Refresh the tree.
        """
        self.on_mount()
