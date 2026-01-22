import pytest

from firmament.constants import DELETED_CONTENT_HASH
from firmament.datastore import (
    ContentBackends,
    DiskDatastore,
    FileVersion,
    LocalVersion,
    PathRequest,
)


@pytest.fixture
def datastore(tmp_path):
    """Create a basic DiskDatastore for testing."""
    ds = DiskDatastore[dict](tmp_path / "test-db")
    yield ds
    ds.close()


@pytest.fixture
def local_version(tmp_path):
    """Create a LocalVersion datastore."""
    ds = LocalVersion(tmp_path / "local-version-db")
    yield ds
    ds.close()


@pytest.fixture
def file_version(tmp_path):
    """Create a FileVersion datastore."""
    ds = FileVersion(tmp_path / "file-version-db")
    yield ds
    ds.close()


@pytest.fixture
def path_request(tmp_path):
    """Create a PathRequest datastore."""
    ds = PathRequest(tmp_path / "path-request-db")
    yield ds
    ds.close()


@pytest.fixture
def content_backends(tmp_path):
    """Create a ContentBackends datastore."""
    ds = ContentBackends(tmp_path / "content-backends-db")
    yield ds
    ds.close()


class TestDiskDatastoreBasicOperations:
    """Tests for basic CRUD operations."""

    def test_set_and_get(self, datastore):
        datastore.set("key1", {"value": 42})
        assert datastore.get("key1") == {"value": 42}

    def test_get_missing_key_returns_none(self, datastore):
        assert datastore.get("nonexistent") is None

    def test_get_missing_key_returns_default(self, datastore):
        assert datastore.get("nonexistent", default={"default": True}) == {
            "default": True
        }

    def test_delete_existing_key(self, datastore):
        datastore.set("key1", {"value": 1})
        datastore.delete("key1")
        assert datastore.get("key1") is None

    def test_delete_missing_key_raises(self, datastore):
        with pytest.raises(KeyError):
            datastore.delete("nonexistent")

    def test_set_overwrites_existing(self, datastore):
        datastore.set("key1", {"old": True})
        datastore.set("key1", {"new": True})
        assert datastore.get("key1") == {"new": True}


class TestDiskDatastoreDictInterface:
    """Tests for dict-like interface."""

    def test_getitem(self, datastore):
        datastore.set("key1", {"value": 1})
        assert datastore["key1"] == {"value": 1}

    def test_getitem_missing_raises(self, datastore):
        with pytest.raises(KeyError):
            _ = datastore["nonexistent"]

    def test_setitem(self, datastore):
        datastore["key1"] = {"value": 1}
        assert datastore.get("key1") == {"value": 1}

    def test_delitem(self, datastore):
        datastore["key1"] = {"value": 1}
        del datastore["key1"]
        assert datastore.get("key1") is None

    def test_delitem_missing_raises(self, datastore):
        with pytest.raises(KeyError):
            del datastore["nonexistent"]

    def test_contains_true(self, datastore):
        datastore["key1"] = {"value": 1}
        assert "key1" in datastore

    def test_contains_false(self, datastore):
        assert "nonexistent" not in datastore

    def test_len_empty(self, datastore):
        assert len(datastore) == 0

    def test_len_with_items(self, datastore):
        datastore["key1"] = {"value": 1}
        datastore["key2"] = {"value": 2}
        datastore["key3"] = {"value": 3}
        assert len(datastore) == 3


class TestDiskDatastoreIteration:
    """Tests for iteration methods."""

    def test_keys(self, datastore):
        datastore["a"] = {"v": 1}
        datastore["b"] = {"v": 2}
        datastore["c"] = {"v": 3}
        assert set(datastore.keys()) == {"a", "b", "c"}

    def test_values(self, datastore):
        datastore["a"] = {"v": 1}
        datastore["b"] = {"v": 2}
        assert {v["v"] for v in datastore.values()} == {1, 2}

    def test_items(self, datastore):
        datastore["a"] = {"v": 1}
        datastore["b"] = {"v": 2}
        items = dict(datastore.items())
        assert items == {"a": {"v": 1}, "b": {"v": 2}}

    def test_all(self, datastore):
        datastore["a"] = {"v": 1}
        datastore["b"] = {"v": 2}
        assert datastore.all() == {"a": {"v": 1}, "b": {"v": 2}}

    def test_keys_empty(self, datastore):
        assert list(datastore.keys()) == []

    def test_values_empty(self, datastore):
        assert list(datastore.values()) == []

    def test_items_empty(self, datastore):
        assert list(datastore.items()) == []


class TestDiskDatastoreBulkOperations:
    """Tests for bulk operations."""

    def test_set_all_replaces_everything(self, datastore):
        datastore["old1"] = {"v": 1}
        datastore["old2"] = {"v": 2}

        datastore.set_all({"new1": {"v": 10}, "new2": {"v": 20}})

        assert "old1" not in datastore
        assert "old2" not in datastore
        assert datastore["new1"] == {"v": 10}
        assert datastore["new2"] == {"v": 20}

    def test_set_all_empty_clears_database(self, datastore):
        datastore["key1"] = {"v": 1}
        datastore["key2"] = {"v": 2}

        datastore.set_all({})

        assert len(datastore) == 0


class TestDiskDatastorePersistence:
    """Tests for data persistence."""

    def test_data_persists_after_close_reopen(self, tmp_path):
        db_path = tmp_path / "persist-db"

        ds1 = DiskDatastore[dict](db_path)
        ds1["key1"] = {"persistent": True}
        ds1.close()

        ds2 = DiskDatastore[dict](db_path)
        assert ds2["key1"] == {"persistent": True}
        ds2.close()

    def test_creates_directory_if_not_exists(self, tmp_path):
        db_path = tmp_path / "nested" / "path" / "db"
        ds = DiskDatastore[dict](db_path)
        ds["key"] = {"value": 1}
        ds.close()

        assert db_path.exists()


class TestDiskDatastoreDataTypes:
    """Tests for various data types."""

    def test_string_value(self, datastore):
        datastore["key"] = "string value"
        assert datastore["key"] == "string value"

    def test_int_value(self, datastore):
        datastore["key"] = 42
        assert datastore["key"] == 42

    def test_list_value(self, datastore):
        datastore["key"] = [1, 2, 3, "four"]
        assert datastore["key"] == [1, 2, 3, "four"]

    def test_nested_dict_value(self, datastore):
        datastore["key"] = {"nested": {"deeply": {"value": True}}}
        assert datastore["key"] == {"nested": {"deeply": {"value": True}}}

    def test_none_value(self, datastore):
        datastore["key"] = None
        assert datastore["key"] is None
        assert "key" in datastore

    def test_unicode_key(self, datastore):
        datastore["键"] = {"unicode": True}
        assert datastore["键"] == {"unicode": True}

    def test_unicode_value(self, datastore):
        datastore["key"] = {"emoji": "🎉", "chinese": "中文"}
        assert datastore["key"] == {"emoji": "🎉", "chinese": "中文"}


class TestLocalVersion:
    """Tests for LocalVersion-specific functionality."""

    def test_key_must_start_with_slash(self, local_version):
        with pytest.raises(ValueError, match="must start with /"):
            local_version["invalid/path"] = {
                "content_hash": None,
                "mtime": 0,
                "size": 0,
                "last_hashed": None,
            }

    def test_valid_key_with_slash(self, local_version):
        local_version["/valid/path"] = {
            "content_hash": "abc123",
            "mtime": 1000,
            "size": 100,
            "last_hashed": 1000,
        }
        assert local_version["/valid/path"]["content_hash"] == "abc123"

    def test_by_content_hash_found(self, local_version):
        local_version["/file1"] = {
            "content_hash": "hash1",
            "mtime": 1000,
            "size": 100,
            "last_hashed": 1000,
        }
        local_version["/file2"] = {
            "content_hash": "hash2",
            "mtime": 2000,
            "size": 200,
            "last_hashed": 2000,
        }

        path, data = local_version.by_content_hash("hash2")
        assert path == "/file2"
        assert data["size"] == 200

    def test_by_content_hash_not_found(self, local_version):
        local_version["/file1"] = {
            "content_hash": "hash1",
            "mtime": 1000,
            "size": 100,
            "last_hashed": 1000,
        }

        with pytest.raises(KeyError):
            local_version.by_content_hash("nonexistent")

    def test_all_content_hashes(self, local_version):
        local_version["/file1"] = {
            "content_hash": "hash1",
            "mtime": 1000,
            "size": 100,
            "last_hashed": 1000,
        }
        local_version["/file2"] = {
            "content_hash": "hash2",
            "mtime": 2000,
            "size": 200,
            "last_hashed": 2000,
        }
        local_version["/file3"] = {
            "content_hash": None,
            "mtime": 3000,
            "size": 300,
            "last_hashed": None,
        }

        hashes = local_version.all_content_hashes()
        assert hashes == {"hash1", "hash2"}

    def test_without_content_hashes(self, local_version):
        local_version["/hashed"] = {
            "content_hash": "abc",
            "mtime": 1000,
            "size": 100,
            "last_hashed": 1000,
        }
        local_version["/unhashed1"] = {
            "content_hash": None,
            "mtime": 2000,
            "size": 200,
            "last_hashed": None,
        }
        local_version["/unhashed2"] = {
            "content_hash": None,
            "mtime": 3000,
            "size": 300,
            "last_hashed": None,
        }

        unhashed = set(local_version.without_content_hashes())
        assert unhashed == {"/unhashed1", "/unhashed2"}

    def test_not_in_file_versions(self, local_version, file_version):
        local_version["/file1"] = {
            "content_hash": "hash1",
            "mtime": 1000,
            "size": 100,
            "last_hashed": 1000,
        }
        local_version["/file2"] = {
            "content_hash": "hash2",
            "mtime": 2000,
            "size": 200,
            "last_hashed": 2000,
        }
        local_version["/file3"] = {
            "content_hash": None,
            "mtime": 3000,
            "size": 300,
            "last_hashed": None,
        }

        # Only file1 has a matching FileVersion
        file_version["/file1"] = {"hash1": {"mtime": 1000, "size": 100}}

        not_in_fv = dict(local_version.not_in_file_versions(file_version))
        assert "/file1" not in not_in_fv
        assert "/file2" in not_in_fv
        assert "/file3" not in not_in_fv  # No content_hash, so excluded


class TestFileVersion:
    """Tests for FileVersion-specific functionality."""

    def test_key_must_start_with_slash(self, file_version):
        with pytest.raises(ValueError, match="must start with /"):
            file_version["invalid"] = {}

    def test_set_with_content_creates_path(self, file_version):
        file_version.set_with_content(
            "/new/file", "hash1", {"mtime": 1000, "size": 100}
        )

        assert "/new/file" in file_version
        assert file_version["/new/file"]["hash1"] == {"mtime": 1000, "size": 100}

    def test_set_with_content_merges_hashes(self, file_version):
        file_version.set_with_content("/file", "hash1", {"mtime": 1000, "size": 100})
        file_version.set_with_content("/file", "hash2", {"mtime": 2000, "size": 200})

        data = file_version["/file"]
        assert "hash1" in data
        assert "hash2" in data
        assert data["hash1"]["mtime"] == 1000
        assert data["hash2"]["mtime"] == 2000

    def test_most_recent_content(self, file_version):
        file_version["/file"] = {
            "old_hash": {"mtime": 1000, "size": 100},
            "new_hash": {"mtime": 2000, "size": 200},
            "mid_hash": {"mtime": 1500, "size": 150},
        }

        content_hash, meta = file_version.most_recent_content("/file")
        assert content_hash == "new_hash"
        assert meta["mtime"] == 2000

    def test_most_recent_content_missing_path(self, file_version):
        content_hash, meta = file_version.most_recent_content("/nonexistent")
        assert content_hash is None
        assert meta is None

    def test_deleted_paths(self, file_version):
        file_version["/active"] = {"hash1": {"mtime": 1000, "size": 100}}
        file_version["/deleted"] = {
            "hash1": {"mtime": 1000, "size": 100},
            DELETED_CONTENT_HASH: {"mtime": 2000, "size": 0},
        }
        file_version["/also_deleted"] = {
            DELETED_CONTENT_HASH: {"mtime": 1000, "size": 0}
        }

        deleted = set(file_version.deleted_paths())
        assert deleted == {"/deleted", "/also_deleted"}


class TestPathRequest:
    """Tests for PathRequest-specific functionality."""

    def test_key_must_start_with_slash(self, path_request):
        with pytest.raises(ValueError, match="must start with /"):
            path_request["invalid"] = "full"

    def test_resolve_status_exact_match(self, path_request):
        path_request["/exact/path"] = "full"
        assert path_request.resolve_status("/exact/path") == "full"

    def test_resolve_status_parent_match(self, path_request):
        path_request["/parent"] = "ignore"
        assert path_request.resolve_status("/parent/child/grandchild") == "ignore"

    def test_resolve_status_closest_parent(self, path_request):
        path_request["/a"] = "full"
        path_request["/a/b"] = "on-demand"

        assert path_request.resolve_status("/a/b/c") == "on-demand"
        assert path_request.resolve_status("/a/other") == "full"

    def test_resolve_status_default_on_demand(self, path_request):
        assert path_request.resolve_status("/unset/path") == "on-demand"

    def test_all_status_types(self, path_request):
        path_request["/full"] = "full"
        path_request["/on-demand"] = "on-demand"
        path_request["/download-once"] = "download-once"
        path_request["/ignore"] = "ignore"

        assert path_request.resolve_status("/full/file") == "full"
        assert path_request.resolve_status("/on-demand/file") == "on-demand"
        assert path_request.resolve_status("/download-once/file") == "download-once"
        assert path_request.resolve_status("/ignore/file") == "ignore"


class TestContentBackends:
    """Tests for ContentBackends-specific functionality."""

    def test_store_backend_list(self, content_backends):
        content_backends["hash123"] = ["local", "s3-backup"]
        assert content_backends["hash123"] == ["local", "s3-backup"]

    def test_empty_backend_list(self, content_backends):
        content_backends["hash123"] = []
        assert content_backends["hash123"] == []

    def test_update_backend_list(self, content_backends):
        content_backends["hash123"] = ["local"]
        backends = content_backends["hash123"]
        backends.append("s3")
        content_backends["hash123"] = backends

        assert content_backends["hash123"] == ["local", "s3"]
