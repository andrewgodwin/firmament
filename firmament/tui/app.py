from datetime import datetime

from rich.text import Text
from textual.app import App, ComposeResult
from textual.binding import Binding
from textual.containers import Container, Horizontal
from textual.widgets import Footer, Static, Tree
from textual.widgets.tree import TreeNode

from firmament.config import Config
from firmament.constants import DELETED_CONTENT_HASH
from firmament.types import PathRequestType

from .tree import FileStatus, FileTree, TreeNodeData, build_tree


class FirmamentTUI(App[None]):
    """
    Firmament File Sync TUI Application.
    """

    CSS = """
    Screen {
        layout: grid;
        grid-size: 1;
        grid-rows: 1fr auto;
    }

    #main-container {
        height: 100%;
    }

    #tree-container {
        width: 1fr;
        height: 100%;
        border: solid green;
    }

    #file-tree {
        scrollbar-gutter: stable;
    }

    #details-container {
        width: 40;
        height: 100%;
        border: solid blue;
        padding: 1;
    }

    #details-content {
        width: 100%;
        height: 100%;
    }

    #legend {
        height: auto;
        background: $surface-darken-1;
        padding: 0 1;
    }
    """

    BINDINGS = [
        Binding("q", "quit", "Quit"),
        Binding("r", "refresh", "Refresh"),
        Binding("f", "set_full", "FULL"),
        Binding("o", "set_on_demand", "ON_DEMAND"),
        Binding("d", "set_download_once", "DOWNLOAD_ONCE"),
        Binding("i", "set_ignore", "IGNORE"),
        Binding("c", "clear_request", "Clear"),
        Binding("ctrl+d", "delete_local", "Delete Local"),
    ]

    def __init__(self, config: Config):
        super().__init__()
        self.config = config

    def compose(self) -> ComposeResult:
        yield Horizontal(
            Container(FileTree("Firmament", id="file-tree"), id="tree-container"),
            Container(
                Static("Select a file to view details", id="details-content"),
                id="details-container",
            ),
            id="main-container",
        )
        yield Static(self._legend_text(), id="legend")
        yield Footer()

    def on_mount(self) -> None:
        """
        Initialize the tree on mount.
        """
        self.refresh_tree()
        # Focus the tree for keyboard navigation
        self.query_one("#file-tree", FileTree).focus()
        # Set up auto-refresh every 2 seconds
        self.set_interval(2, self.refresh_tree)

    def refresh_tree(self) -> None:
        """
        Rebuild tree from current data, preserving expanded state.
        """
        tree = self.query_one("#file-tree", FileTree)

        # Save expanded paths before clearing
        expanded_paths = self._get_expanded_paths(tree.root)

        tree.clear()

        tree_data = build_tree(self.config)
        tree.root.data = tree_data
        # Update root label to trigger re-render with new data
        tree.root.set_label(tree_data.name)
        self._populate_tree(tree.root, tree_data, expanded_paths)
        tree.root.expand()

    def _get_expanded_paths(self, node: TreeNode[TreeNodeData]) -> set[str]:
        """
        Recursively collect paths of expanded nodes.
        """
        expanded = set()
        if node.is_expanded and node.data:
            expanded.add(node.data.path)
        for child in node.children:
            expanded.update(self._get_expanded_paths(child))
        return expanded

    def _populate_tree(
        self,
        parent: TreeNode[TreeNodeData],
        data: TreeNodeData,
        expanded_paths: set[str] | None = None,
    ) -> None:
        """
        Recursively populate tree nodes.
        """
        if expanded_paths is None:
            expanded_paths = set()

        # Sort: directories first, then alphabetically
        sorted_children = sorted(
            data.children.values(),
            key=lambda x: (not x.is_directory, x.name.lower()),
        )

        for child_data in sorted_children:
            if child_data.is_directory:
                node = parent.add(child_data.name, data=child_data)
                self._populate_tree(node, child_data, expanded_paths)
                # Restore expanded state
                if child_data.path in expanded_paths:
                    node.expand()
            else:
                parent.add_leaf(child_data.name, data=child_data)

    def _legend_text(self) -> Text:
        text = Text()
        # File status indicators
        text.append("[A]", style="bold yellow")
        text.append("vailable ")
        text.append("[L]", style="bold green")
        text.append("ocal ")
        text.append("[X]", style="bold red")
        text.append(" Deleted | ")
        # Path request indicators
        text.append("<F>", style="bold cyan")
        text.append("ull ")
        text.append("<O>", style="bold cyan")
        text.append("n Demand ")
        text.append("<D>", style="bold cyan")
        text.append("ownload Once ")
        text.append("<I>", style="bold cyan")
        text.append("gnore | ")
        text.append("(x)", style="dim cyan")
        text.append(" = inherited")
        return text

    def on_tree_node_highlighted(self, event: Tree.NodeHighlighted) -> None:
        """
        Update details pane when a tree node is highlighted.
        """
        self._update_details(event.node.data)

    def _update_details(self, data: TreeNodeData | None) -> None:
        """
        Update the details pane with information about the selected node.
        """
        details = self.query_one("#details-content", Static)

        if data is None:
            details.update("Select a file to view details")
            return

        text = Text()

        # Path
        text.append("Path: ", style="bold")
        text.append(f"{data.path}\n\n")

        # Type
        text.append("Type: ", style="bold")
        if data.is_directory:
            text.append("Directory\n\n", style="blue")
        else:
            text.append("File\n\n")

        # Status (for files only)
        if data.status:
            text.append("Status: ", style="bold")
            if data.status == FileStatus.DELETED:
                text.append("Deleted\n", style="red")
            elif data.status == FileStatus.LOCAL:
                text.append("Local\n", style="green")
            else:
                text.append("Available\n", style="yellow")
            text.append("\n")

        # PathRequest
        text.append("PathRequest: ", style="bold")
        if data.path_request:
            text.append(f"{data.path_request} ", style="cyan")
            text.append("(direct)\n")
        else:
            text.append(f"{data.effective_path_request} ", style="dim cyan")
            text.append("(inherited)\n")
        text.append("\n")

        # File versions (for files only)
        if not data.is_directory and data.path:
            versions = self.config.file_versions.get(data.path)
            local = self.config.local_versions.get(data.path)
            local_hash = local["content_hash"] if local else None

            if versions:
                text.append("Versions:\n", style="bold")
                # Sort by mtime descending
                sorted_versions = sorted(
                    versions.items(),
                    key=lambda x: x[1]["mtime"],
                    reverse=True,
                )
                for content_hash, meta in sorted_versions:
                    mtime = datetime.fromtimestamp(meta["mtime"])
                    # Handle deleted versions specially
                    if content_hash == DELETED_CONTENT_HASH:
                        text.append("  [X] ", style="bold red")
                        text.append("DELETED\n", style="red")
                        text.append(f"        {mtime:%Y-%m-%d %H:%M}\n", style="dim")
                        continue
                    size = self._format_size(meta["size"])
                    # Mark if this version is local
                    if content_hash == local_hash:
                        text.append("  [L] ", style="bold green")
                    else:
                        text.append("      ")
                    text.append(f"{content_hash[:12]}...\n", style="green")
                    text.append(f"        {mtime:%Y-%m-%d %H:%M}\n", style="dim")
                    text.append(f"        {size}\n", style="dim")
                    # Show which backends have this content
                    backends = self.config.content_backends.get(content_hash, [])
                    if backends:
                        text.append(
                            f"        Backends: {', '.join(backends)}\n", style="dim"
                        )

        details.update(text)

    @staticmethod
    def _format_size(size: float) -> str:
        """
        Format size in human-readable units.
        """
        for unit in ["B", "KB", "MB", "GB"]:
            if size < 1024:
                return f"{size:.1f} {unit}"
            size /= 1024
        return f"{size:.1f} TB"

    def action_set_full(self) -> None:
        self._set_path_request("full")

    def action_set_on_demand(self) -> None:
        self._set_path_request("on-demand")

    def action_set_download_once(self) -> None:
        self._set_path_request("download-once")

    def action_set_ignore(self) -> None:
        self._set_path_request("ignore")

    def action_clear_request(self) -> None:
        """
        Clear direct PathRequest (inherit from parent)
        """
        tree = self.query_one("#file-tree", FileTree)
        node = tree.cursor_node
        if node and node.data:
            path = node.data.path
            if path and path in self.config.path_requests:
                del self.config.path_requests[path]
                self.refresh_tree()
                self.notify(f"Cleared PathRequest for {path}")

    def _set_path_request(self, request_type: PathRequestType) -> None:
        """
        Set PathRequest for selected node.
        """
        tree = self.query_one("#file-tree", FileTree)
        node = tree.cursor_node
        if node and node.data:
            path = node.data.path
            if path:  # Don't set on root
                self.config.path_requests[path] = request_type
                self.refresh_tree()
                self.notify(f"Set {path} to {request_type}")

    def action_refresh(self) -> None:
        """
        Refresh tree from datastore.
        """
        self.refresh_tree()
        self.notify("Tree refreshed")

    def action_delete_local(self) -> None:
        """
        Delete local copy of file if path request allows it.
        """
        tree = self.query_one("#file-tree", FileTree)
        node = tree.cursor_node
        if not node or not node.data:
            return

        data = node.data
        if data.is_directory:
            self.notify("Cannot delete directories", severity="warning")
            return

        if data.status != FileStatus.LOCAL:
            self.notify("File is not local", severity="warning")
            return

        # Check if path request allows deletion (on-demand or download-once)
        effective_request = data.effective_path_request
        if effective_request not in ("on-demand", "download-once"):
            self.notify(
                f"Cannot delete: path request is '{effective_request}'",
                severity="warning",
            )
            return

        # Delete the file from disk and remove from local_versions
        path = data.path
        disk_path = self.config.disk_path(path)
        try:
            disk_path.unlink()
            del self.config.local_versions[path]
            self.refresh_tree()
            self.notify(f"Deleted local copy: {path}")
        except Exception as e:
            self.notify(f"Failed to delete: {e}", severity="error")
