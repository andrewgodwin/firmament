import io
import shutil
import subprocess
from unittest.mock import MagicMock, patch

import pytest

from firmament.backends.base import BackendError
from firmament.backends.rclone_s3 import RcloneS3Backend


class MockProcess:
    """
    Mock for subprocess.Popen.
    """

    def __init__(
        self, returncode: int | None = None, stdout: str = "", stderr: str = ""
    ):
        self.returncode = returncode
        self._stdout = stdout
        self._stderr = stderr

    def poll(self):
        return self.returncode

    def communicate(self, timeout=None):
        return self._stdout, self._stderr

    def terminate(self):
        pass

    def kill(self):
        pass

    def wait(self, timeout=None):
        pass


@pytest.fixture
def mock_rclone_subprocess():
    """
    Patch subprocess.Popen for unit testing.
    """
    with patch("firmament.backends.rclone_s3.subprocess.Popen") as mock_popen:
        mock_process = MockProcess()
        mock_popen.return_value = mock_process
        yield {"popen": mock_popen, "process": mock_process}


@pytest.fixture
def mock_socket_connect():
    """
    Patch socket to simulate successful connection.
    """
    with patch("firmament.backends.rclone_s3.socket.socket") as mock_socket_class:
        mock_socket = MagicMock()
        mock_socket.__enter__ = MagicMock(return_value=mock_socket)
        mock_socket.__exit__ = MagicMock(return_value=False)
        # Simulate successful connection
        mock_socket.connect.return_value = None
        mock_socket_class.return_value = mock_socket
        yield mock_socket_class


@pytest.fixture
def mock_boto3():
    """
    Patch boto3 for unit testing.
    """
    with patch("firmament.backends.s3.boto3") as mock:
        client = MagicMock()
        mock.client.return_value = client
        yield {"boto3": mock, "client": client}


class TestRcloneS3BackendInit:
    """
    Tests for RcloneS3Backend initialization.
    """

    def test_config_file_generation(
        self, mock_rclone_subprocess, mock_socket_connect, mock_boto3, tmp_path
    ):
        """
        Should generate a temporary rclone config file.
        """
        with (
            patch("firmament.backends.rclone_s3.tempfile.mkstemp") as mock_mkstemp,
            patch("firmament.backends.rclone_s3.os.write"),
            patch("firmament.backends.rclone_s3.os.close"),
        ):
            config_path = str(tmp_path / "test.conf")
            mock_mkstemp.return_value = (5, config_path)

            backend = RcloneS3Backend(
                name="test-backend",
                rclone_remote_type="drive",
                rclone_remote_config={
                    "client_id": "test-client-id",
                    "client_secret": "test-secret",
                    "token": '{"access_token":"xxx"}',
                },
            )

            # Config file should be tracked
            assert backend._config_path == config_path
            backend.close()

    def test_rclone_binary_not_found(self, mock_socket_connect, mock_boto3):
        """
        Should raise BackendError when rclone binary not found.
        """
        with (
            patch("firmament.backends.rclone_s3.subprocess.Popen") as mock_popen,
            patch("firmament.backends.rclone_s3.tempfile.mkstemp") as m,
        ):
            m.return_value = (5, "/tmp/test.conf")
            with (
                patch("firmament.backends.rclone_s3.os.write"),
                patch("firmament.backends.rclone_s3.os.close"),
                patch("firmament.backends.rclone_s3.os.unlink"),
            ):
                mock_popen.side_effect = FileNotFoundError()

                with pytest.raises(BackendError) as exc_info:
                    RcloneS3Backend(
                        name="test-backend",
                        rclone_remote_type="drive",
                        rclone_remote_config={},
                    )

                assert "rclone binary not found" in str(exc_info.value)

    def test_rclone_process_crash_on_startup(self, mock_socket_connect, mock_boto3):
        """
        Should capture stderr when rclone exits unexpectedly.
        """
        with (
            patch("firmament.backends.rclone_s3.subprocess.Popen") as mock_popen,
            patch("firmament.backends.rclone_s3.tempfile.mkstemp") as m,
        ):
            m.return_value = (5, "/tmp/test.conf")
            with (
                patch("firmament.backends.rclone_s3.os.write"),
                patch("firmament.backends.rclone_s3.os.close"),
                patch("firmament.backends.rclone_s3.os.unlink"),
            ):
                mock_process = MockProcess(
                    returncode=1, stderr="Error: remote not found"
                )
                mock_popen.return_value = mock_process

                with pytest.raises(BackendError) as exc_info:
                    RcloneS3Backend(
                        name="test-backend",
                        rclone_remote_type="drive",
                        rclone_remote_config={},
                    )

                assert "remote not found" in str(exc_info.value)

    def test_auto_port_selection(
        self, mock_rclone_subprocess, mock_socket_connect, mock_boto3
    ):
        """
        Should auto-select an available port when none specified.
        """
        with patch("firmament.backends.rclone_s3.tempfile.mkstemp") as m:
            m.return_value = (5, "/tmp/test.conf")
            with (
                patch("firmament.backends.rclone_s3.os.write"),
                patch("firmament.backends.rclone_s3.os.close"),
                patch.object(
                    RcloneS3Backend, "_find_available_port", return_value=54321
                ),
            ):
                backend = RcloneS3Backend(
                    name="test-backend",
                    rclone_remote_type="local",
                    rclone_remote_config={},
                )

                assert backend._port == 54321
                backend.close()


class TestRcloneS3BackendCommandBuilding:
    """
    Tests for rclone command construction.
    """

    def test_basic_command(
        self, mock_rclone_subprocess, mock_socket_connect, mock_boto3
    ):
        """
        Should build correct basic command.
        """
        with patch("firmament.backends.rclone_s3.tempfile.mkstemp") as m:
            m.return_value = (5, "/tmp/test.conf")
            with (
                patch("firmament.backends.rclone_s3.os.write"),
                patch("firmament.backends.rclone_s3.os.close"),
                patch.object(
                    RcloneS3Backend, "_find_available_port", return_value=8080
                ),
            ):
                backend = RcloneS3Backend(
                    name="test-backend",
                    rclone_remote_type="drive",
                    rclone_remote_config={"client_id": "test"},
                    remote_path="backups/firmament",
                )

                # Verify command was called correctly
                call_args = mock_rclone_subprocess["popen"].call_args[0][0]
                assert call_args[0] == "rclone"
                assert call_args[1:3] == ["serve", "s3"]
                assert "firmament:backups/firmament" in call_args
                assert "--config" in call_args
                assert "--addr" in call_args
                assert "127.0.0.1:8080" in call_args
                assert "--auth-key" in call_args

                backend.close()

    def test_extra_flags(self, mock_rclone_subprocess, mock_socket_connect, mock_boto3):
        """
        Should include extra rclone flags.
        """
        with patch("firmament.backends.rclone_s3.tempfile.mkstemp") as m:
            m.return_value = (5, "/tmp/test.conf")
            with (
                patch("firmament.backends.rclone_s3.os.write"),
                patch("firmament.backends.rclone_s3.os.close"),
                patch.object(
                    RcloneS3Backend, "_find_available_port", return_value=8080
                ),
            ):
                backend = RcloneS3Backend(
                    name="test-backend",
                    rclone_remote_type="drive",
                    rclone_remote_config={},
                    extra_rclone_flags=["--vfs-cache-mode=full", "--verbose"],
                )

                call_args = mock_rclone_subprocess["popen"].call_args[0][0]
                assert "--vfs-cache-mode=full" in call_args
                assert "--verbose" in call_args

                backend.close()

    def test_custom_binary_path(
        self, mock_rclone_subprocess, mock_socket_connect, mock_boto3
    ):
        """
        Should use custom rclone binary path.
        """
        with patch("firmament.backends.rclone_s3.tempfile.mkstemp") as m:
            m.return_value = (5, "/tmp/test.conf")
            with (
                patch("firmament.backends.rclone_s3.os.write"),
                patch("firmament.backends.rclone_s3.os.close"),
                patch.object(
                    RcloneS3Backend, "_find_available_port", return_value=8080
                ),
            ):
                backend = RcloneS3Backend(
                    name="test-backend",
                    rclone_remote_type="local",
                    rclone_remote_config={},
                    rclone_binary="/usr/local/bin/rclone",
                )

                call_args = mock_rclone_subprocess["popen"].call_args[0][0]
                assert call_args[0] == "/usr/local/bin/rclone"

                backend.close()


class TestRcloneS3BackendBucketMapping:
    """
    Tests for bucket name and prefix mapping.
    """

    def test_bucket_from_remote_path(
        self, mock_rclone_subprocess, mock_socket_connect, mock_boto3
    ):
        """
        First path component should become bucket name.
        """
        with patch("firmament.backends.rclone_s3.tempfile.mkstemp") as m:
            m.return_value = (5, "/tmp/test.conf")
            with (
                patch("firmament.backends.rclone_s3.os.write"),
                patch("firmament.backends.rclone_s3.os.close"),
                patch.object(
                    RcloneS3Backend, "_find_available_port", return_value=8080
                ),
            ):
                backend = RcloneS3Backend(
                    name="test-backend",
                    rclone_remote_type="local",
                    rclone_remote_config={},
                    remote_path="backups/firmament/data",
                )

                assert backend.bucket == "backups"
                assert backend.prefix == "firmament/data"

                backend.close()

    def test_default_bucket_no_path(
        self, mock_rclone_subprocess, mock_socket_connect, mock_boto3
    ):
        """
        Should use 'data' bucket when no remote_path.
        """
        with patch("firmament.backends.rclone_s3.tempfile.mkstemp") as m:
            m.return_value = (5, "/tmp/test.conf")
            with (
                patch("firmament.backends.rclone_s3.os.write"),
                patch("firmament.backends.rclone_s3.os.close"),
                patch.object(
                    RcloneS3Backend, "_find_available_port", return_value=8080
                ),
            ):
                backend = RcloneS3Backend(
                    name="test-backend",
                    rclone_remote_type="local",
                    rclone_remote_config={},
                )

                assert backend.bucket == "data"
                assert backend.prefix == ""

                backend.close()


class TestRcloneS3BackendCleanup:
    """
    Tests for subprocess cleanup.
    """

    def test_close_terminates_process(
        self, mock_rclone_subprocess, mock_socket_connect, mock_boto3
    ):
        """
        Close() should terminate the subprocess.
        """
        with patch("firmament.backends.rclone_s3.tempfile.mkstemp") as m:
            m.return_value = (5, "/tmp/test.conf")
            with (
                patch("firmament.backends.rclone_s3.os.write"),
                patch("firmament.backends.rclone_s3.os.close"),
                patch("firmament.backends.rclone_s3.os.path.exists", return_value=True),
            ):
                with patch("firmament.backends.rclone_s3.os.unlink") as mock_unlink:
                    # Use MagicMock with poll returning None (process still running)
                    mock_process = MagicMock()
                    mock_process.poll.return_value = None
                    mock_rclone_subprocess["popen"].return_value = mock_process

                    backend = RcloneS3Backend(
                        name="test-backend",
                        rclone_remote_type="local",
                        rclone_remote_config={},
                    )

                    backend.close()

                    mock_process.terminate.assert_called_once()
                    mock_unlink.assert_called()

    def test_double_close_safe(
        self, mock_rclone_subprocess, mock_socket_connect, mock_boto3
    ):
        """
        Calling close() twice should be safe.
        """
        with patch("firmament.backends.rclone_s3.tempfile.mkstemp") as m:
            m.return_value = (5, "/tmp/test.conf")
            with (
                patch("firmament.backends.rclone_s3.os.write"),
                patch("firmament.backends.rclone_s3.os.close"),
                patch("firmament.backends.rclone_s3.os.path.exists", return_value=True),
            ):
                with patch("firmament.backends.rclone_s3.os.unlink"):
                    mock_process = MagicMock()
                    mock_process.poll.return_value = None
                    mock_rclone_subprocess["popen"].return_value = mock_process

                    backend = RcloneS3Backend(
                        name="test-backend",
                        rclone_remote_type="local",
                        rclone_remote_config={},
                    )

                    backend.close()
                    backend.close()  # Should not raise

                    # terminate should only be called once
                    assert mock_process.terminate.call_count == 1


class TestRcloneS3BackendStr:
    """
    Tests for string representation.
    """

    def test_str_with_path(
        self, mock_rclone_subprocess, mock_socket_connect, mock_boto3
    ):
        """
        String should show remote type and path.
        """
        with patch("firmament.backends.rclone_s3.tempfile.mkstemp") as m:
            m.return_value = (5, "/tmp/test.conf")
            with (
                patch("firmament.backends.rclone_s3.os.write"),
                patch("firmament.backends.rclone_s3.os.close"),
                patch.object(
                    RcloneS3Backend, "_find_available_port", return_value=8080
                ),
            ):
                backend = RcloneS3Backend(
                    name="test-backend",
                    rclone_remote_type="drive",
                    rclone_remote_config={},
                    remote_path="backups/data",
                )

                result = str(backend)
                assert "drive" in result
                assert "backups/data" in result
                assert "8080" in result

                backend.close()


# Integration tests - require rclone to be installed


def _rclone_has_serve_s3() -> bool:
    """
    Check if rclone has serve s3 capability (requires v1.63+).
    """
    try:
        result = subprocess.run(
            ["rclone", "serve", "s3", "--help"],
            capture_output=True,
            text=True,
            timeout=5,
        )
        return result.returncode == 0
    except Exception:
        return False


@pytest.fixture
def rclone_integration_backend(tmp_path):
    """
    Create RcloneS3Backend for integration testing using local filesystem.

    This uses rclone's 'local' backend to test without external dependencies. Requires
    rclone v1.63+ to be installed (for serve s3 support).
    """
    if not shutil.which("rclone"):
        pytest.skip("rclone not installed, skipping integration test")

    if not _rclone_has_serve_s3():
        pytest.skip("rclone serve s3 not available (requires rclone v1.63+)")

    # Create a temporary directory for the "remote"
    remote_dir = tmp_path / "remote_storage"
    remote_dir.mkdir()

    # Create the bucket directory (required by rclone serve s3)
    bucket_dir = remote_dir / "testbucket"
    bucket_dir.mkdir()

    backend = RcloneS3Backend(
        name="integration-test",
        rclone_remote_type="local",
        rclone_remote_config={"nounc": "true"},
        remote_path=str(remote_dir / "testbucket"),
    )

    yield backend

    backend.close()


@pytest.mark.integration
class TestRcloneS3BackendIntegration:
    """
    Integration tests that require rclone to be installed.

    Run with: pytest -m integration tests/test_rclone_s3_backend.py
    """

    def test_write_and_read_roundtrip(self, rclone_integration_backend):
        """
        Test write/read roundtrip through rclone serve s3.
        """
        content = b"Hello, Rclone World!"
        path = rclone_integration_backend.remote_database_path("test-file")

        rclone_integration_backend.remote_write_io(path, io.BytesIO(content))

        result = io.BytesIO()
        rclone_integration_backend.remote_read_io(path, result)
        assert result.getvalue() == content

    def test_exists_and_delete(self, rclone_integration_backend):
        """
        Test exists and delete operations.
        """
        path = rclone_integration_backend.remote_database_path("exists-test")

        # Should not exist initially
        assert rclone_integration_backend.remote_exists(path) is False

        # Write something
        rclone_integration_backend.remote_write_io(path, io.BytesIO(b"test"))

        # Now should exist
        assert rclone_integration_backend.remote_exists(path) is True

        # Delete
        rclone_integration_backend.remote_delete(path)

        # Should not exist again
        assert rclone_integration_backend.remote_exists(path) is False
