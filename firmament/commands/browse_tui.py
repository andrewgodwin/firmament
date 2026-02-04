from collections import deque
from dataclasses import dataclass, field
from pathlib import Path
from typing import TYPE_CHECKING

from textual import work
from textual.app import App, ComposeResult
from textual.widgets import Footer, Tree
from textual.widgets.tree import TreeNode as TextualTreeNode
from textual.worker import get_current_worker

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
    local_count: int = 0  # Recursive count of local files in subtree
    remote_count: int = 0  # Recursive count of remote files in subtree
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
        self._path_to_tnode: dict[str, TextualTreeNode] = {}

    def compose(self) -> ComposeResult:
        yield Tree("Loading...")
        yield Footer()

    def on_mount(self) -> None:
        """
        Load tree data when app starts.
        """
        tree = self.query_one(Tree)

        # Create root
        self.file_tree = FileTreeNode(
            path="",
            name="/",
            is_dir=True,
            is_local=True,
            is_remote=True,
            size=None,
            path_request=get_path_request_safe(self.config, "/"),
            path_request_explicit="/" in self.config.path_requests.requests,
        )

        tree.clear()
        tree.root.data = self.file_tree
        tree.root.label = self.format_node_label(self.file_tree)
        self._path_to_tnode = {"": tree.root}
        tree.root.expand()

        # Start background scans in parallel
        self._scan_remote_files()
        self._scan_local_files()

    @work(thread=True)
    def _scan_remote_files(self) -> None:
        """
        Scan remote files in the background, updating the tree progressively.
        """
        worker = get_current_worker()
        try:
            batch: list = []
            for file_info in self.config.rclone.get_all_files(self.remote):
                if worker.is_cancelled:
                    return
                batch.append(file_info)
                if len(batch) >= 100:
                    self.call_from_thread(self._process_remote_files, batch)
                    batch = []
            if batch:
                self.call_from_thread(self._process_remote_files, batch)
        except Exception:
            pass

    @work(thread=True)
    def _scan_local_files(self) -> None:
        """
        Scan local files in the background (breadth-first), updating the tree
        progressively.
        """
        worker = get_current_worker()
        queue: deque[Path] = deque([self.config.root_path])
        while queue:
            if worker.is_cancelled:
                return
            dir_path = queue.popleft()

            entries: list[tuple[list[str], bool, int | None]] = []
            subdirs: list[Path] = []
            try:
                for entry in dir_path.iterdir():
                    if ".firmament" in entry.parts:
                        continue
                    try:
                        rel = entry.relative_to(self.config.root_path)
                    except ValueError:
                        continue
                    is_dir = entry.is_dir()
                    size = None
                    if not is_dir:
                        try:
                            size = entry.stat().st_size
                        except OSError:
                            pass
                    entries.append((list(rel.parts), is_dir, size))
                    if is_dir:
                        subdirs.append(entry)
            except OSError:
                continue

            if entries:
                self.call_from_thread(self._process_local_entries, entries)
            queue.extend(sorted(subdirs, key=lambda p: p.name))

    def _ensure_node(
        self,
        parts: list[str],
        is_dir: bool,
        is_remote: bool,
        is_local: bool,
        size: int | None,
        dirty_dirs: set[str] | None = None,
    ) -> None:
        """
        Walk/create a path in the data tree and Textual tree.
        """
        assert self.file_tree is not None
        current = self.file_tree
        ancestors: list[tuple[FileTreeNode, str]] = [(self.file_tree, "")]
        delta_local = False
        delta_remote = False

        for i, part in enumerate(parts):
            is_leaf = i == len(parts) - 1
            child_path = "/".join(parts[: i + 1])
            full_path = f"/{child_path}"

            if part not in current.children:
                node = FileTreeNode(
                    path=child_path,
                    name=part,
                    is_dir=is_dir if is_leaf else True,
                    is_local=is_local,
                    is_remote=is_remote,
                    size=size if is_leaf else None,
                    path_request=get_path_request_safe(self.config, full_path),
                    path_request_explicit=full_path
                    in self.config.path_requests.requests,
                )
                current.children[part] = node

                # Add to Textual tree
                parent_path = "/".join(parts[:i])
                parent_tnode = self._path_to_tnode.get(parent_path)
                if parent_tnode is not None:
                    tnode = parent_tnode.add(
                        self.format_node_label(node),
                        data=node,
                        allow_expand=node.is_dir,
                    )
                    self._path_to_tnode[child_path] = tnode
                    # Keep children sorted: directories first, then alphabetically
                    parent_tnode._children.sort(
                        key=lambda tn: (
                            (not tn.data.is_dir, tn.data.name)
                            if tn.data
                            else (True, "")
                        )
                    )

                if is_leaf and not node.is_dir:
                    delta_local = is_local
                    delta_remote = is_remote
            else:
                existing = current.children[part]
                changed = False
                if is_remote and not existing.is_remote:
                    existing.is_remote = True
                    changed = True
                if is_local and not existing.is_local:
                    existing.is_local = True
                    changed = True
                if is_leaf and size is not None and existing.size != size:
                    existing.size = size
                    changed = True
                if changed:
                    tnode = self._path_to_tnode.get(child_path)
                    if tnode is not None:
                        tnode.label = self.format_node_label(existing)

                if is_leaf and not existing.is_dir:
                    delta_local = is_local and not existing.is_local
                    delta_remote = is_remote and not existing.is_remote

            current = current.children[part]
            if not is_leaf and current.is_dir:
                ancestors.append((current, child_path))

        # Propagate file counts to all ancestor directories
        if not is_dir and (delta_local or delta_remote):
            for anc_node, anc_path in ancestors:
                if delta_local:
                    anc_node.local_count += 1
                if delta_remote:
                    anc_node.remote_count += 1
                if dirty_dirs is not None:
                    dirty_dirs.add(anc_path)

    def _refresh_dirty_dirs(self, dirty_dirs: set[str]) -> None:
        """
        Refresh Textual tree labels for directories whose counts changed.
        """
        for path in dirty_dirs:
            tnode = self._path_to_tnode.get(path)
            if tnode is not None and tnode.data is not None:
                tnode.label = self.format_node_label(tnode.data)

    def _process_remote_files(self, remote_files: list) -> None:
        """
        Add remote files to the data tree and Textual tree.
        """
        dirty_dirs: set[str] = set()
        for file_info in remote_files:
            path = file_info["Path"]
            parts = path.split("/")
            size = file_info.get("Size")
            self._ensure_node(
                parts,
                is_dir=False,
                is_remote=True,
                is_local=False,
                size=size,
                dirty_dirs=dirty_dirs,
            )
        self._refresh_dirty_dirs(dirty_dirs)

    def _process_local_entries(
        self, entries: list[tuple[list[str], bool, int | None]]
    ) -> None:
        """
        Add local entries to the data tree and Textual tree.
        """
        dirty_dirs: set[str] = set()
        for parts, is_dir, size in entries:
            self._ensure_node(
                parts,
                is_dir=is_dir,
                is_remote=False,
                is_local=True,
                size=size,
                dirty_dirs=dirty_dirs,
            )
        self._refresh_dirty_dirs(dirty_dirs)

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
            # Directories: path request, name, and file counts
            name = f"[bold cyan]{name}/[/]"
            label = f"[{request_color}]{short_request}[/{request_color}] {name}"
            if node.local_count or node.remote_count:
                label += (
                    f" [#006400]{node.local_count}L[/]"
                    f" [#4682B4]{node.remote_count}R[/]"
                )
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
