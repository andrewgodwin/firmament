"""
Backend that uses rclone serve s3 to expose any rclone remote as S3.
"""

from __future__ import annotations

import atexit
import os
import secrets
import socket
import subprocess
import tempfile
import threading
import time
from typing import Any, ClassVar

from botocore.exceptions import ClientError

from .base import BackendError
from .s3 import S3Backend


class RcloneS3Backend(S3Backend):
    """
    A backend that uses rclone serve s3 to expose any rclone remote as an S3-compatible
    endpoint.

    This allows firmament to use any rclone-supported backend (Google Drive, Dropbox,
    OneDrive, etc.) by running rclone serve s3 as a subprocess and connecting to it via
    boto3.

    The rclone remote configuration is stored in the firmament config and a temporary
    rclone.conf is generated at runtime. The subprocess is started on initialization and
    cleaned up when the backend is closed or when the process exits.
    """

    type_aliases = ["rclone", "rclone-s3"]

    # Track all active instances for cleanup
    _active_instances: ClassVar[set[RcloneS3Backend]] = set()
    _atexit_registered: ClassVar[bool] = False

    def __init__(
        self,
        name: str,
        rclone_remote_type: str,
        rclone_remote_config: dict[str, Any],
        remote_path: str = "",
        encryption_key: str | None = None,
        rclone_binary: str = "rclone",
        serve_port: int | None = None,
        serve_host: str = "127.0.0.1",
        startup_timeout: float = 10.0,
        extra_rclone_flags: list[str] | None = None,
    ):
        """
        Initialize an RcloneS3Backend.

        Args:
            name: Backend name for firmament.
            rclone_remote_type: The rclone remote type (e.g., "drive", "dropbox", "s3").
            rclone_remote_config: Key-value config for the rclone remote.
            remote_path: Path within the remote (e.g., "backups/firmament").
            encryption_key: Optional encryption key for firmament encryption.
            rclone_binary: Path to rclone binary (default: "rclone").
            serve_port: Port for rclone serve s3 (auto-selects if None).
            serve_host: Host to bind rclone serve s3 (default: "127.0.0.1").
            startup_timeout: Timeout waiting for server to start (seconds).
            extra_rclone_flags: Additional flags to pass to rclone serve s3.
        """
        # Store configuration before starting subprocess
        self.rclone_remote_type = rclone_remote_type
        self.rclone_remote_config = rclone_remote_config
        self.remote_path = remote_path.strip("/")
        self.rclone_binary = rclone_binary
        self.serve_host = serve_host
        self.startup_timeout = startup_timeout
        self.extra_rclone_flags = extra_rclone_flags or []

        # Generate random credentials for S3 auth
        self._access_key = secrets.token_urlsafe(16)
        self._secret_key = secrets.token_urlsafe(32)

        # Auto-select port if not provided
        if serve_port is None:
            self._port = self._find_available_port()
        else:
            self._port = serve_port

        # Process and config file management
        self._process: subprocess.Popen | None = None
        self._config_path: str | None = None
        self._lock = threading.Lock()
        self._closed = False

        # Register atexit handler once
        if not RcloneS3Backend._atexit_registered:
            atexit.register(RcloneS3Backend._cleanup_all_instances)
            RcloneS3Backend._atexit_registered = True

        # Generate temp config and start the rclone subprocess
        self._generate_rclone_config()
        self._start_rclone_server()

        # Track this instance for cleanup
        RcloneS3Backend._active_instances.add(self)

        # Work out bucket name and prefix from remote_path
        # rclone serve s3 exposes top-level directories as buckets
        if self.remote_path:
            parts = self.remote_path.split("/")
            bucket_name = parts[0]
            prefix = "/".join(parts[1:]) if len(parts) > 1 else ""
        else:
            bucket_name = "data"
            prefix = ""

        # Store for later use
        self._bucket_name = bucket_name
        self._prefix = prefix
        self._encryption_key = encryption_key
        self._name = name

        # Initialize parent S3Backend, handling bucket creation
        self._init_s3_backend()

    def _init_s3_backend(self):
        """
        Initialize parent S3Backend, creating bucket if needed.
        """
        try:
            super().__init__(
                bucket=self._bucket_name,
                name=self._name,
                encryption_key=self._encryption_key,
                prefix=self._prefix,
                endpoint_url=f"http://{self.serve_host}:{self._port}",
                access_key_id=self._access_key,
                secret_access_key=self._secret_key,
            )
        except BackendError as e:
            # If bucket doesn't exist, try to create it
            if "does not exist" in str(e) or "404" in str(e):
                self._create_bucket_and_retry()
            else:
                raise

    def _create_bucket_and_retry(self):
        """
        Create the bucket and retry S3Backend initialization.
        """
        import boto3

        # Build client with our credentials
        client = boto3.client(
            "s3",
            endpoint_url=f"http://{self.serve_host}:{self._port}",
            aws_access_key_id=self._access_key,
            aws_secret_access_key=self._secret_key,
        )

        try:
            client.create_bucket(Bucket=self._bucket_name)
        except ClientError as e:
            error_code = e.response.get("Error", {}).get("Code", "")
            # Bucket might already exist, which is fine
            if error_code not in ("BucketAlreadyOwnedByYou", "BucketAlreadyExists"):
                raise BackendError(
                    f"Failed to create bucket '{self._bucket_name}': {e}"
                )

        # Now retry parent init
        super().__init__(
            bucket=self._bucket_name,
            name=self._name,
            encryption_key=self._encryption_key,
            prefix=self._prefix,
            endpoint_url=f"http://{self.serve_host}:{self._port}",
            access_key_id=self._access_key,
            secret_access_key=self._secret_key,
        )

    def _find_available_port(self) -> int:
        """
        Find an available port on localhost.
        """
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.bind(("127.0.0.1", 0))
            s.listen(1)
            return s.getsockname()[1]

    def _generate_rclone_config(self):
        """
        Generate a temporary rclone.conf file with the remote configuration.
        """
        # Build config content in INI format
        lines = ["[firmament]", f"type = {self.rclone_remote_type}"]
        for key, value in self.rclone_remote_config.items():
            # Handle values that might contain special characters
            if isinstance(value, str) and ("\n" in value or "=" in value):
                # Use triple quotes for multiline or special values
                lines.append(f"{key} = {value}")
            else:
                lines.append(f"{key} = {value}")

        config_content = "\n".join(lines) + "\n"

        # Create temp file
        fd, path = tempfile.mkstemp(suffix=".conf", prefix="rclone_firmament_")
        try:
            os.write(fd, config_content.encode("utf-8"))
        finally:
            os.close(fd)

        self._config_path = path

    def _start_rclone_server(self):
        """
        Start the rclone serve s3 subprocess.
        """
        # Build the remote string using our generated "firmament" remote name
        if self.remote_path:
            remote_string = f"firmament:{self.remote_path}"
        else:
            remote_string = "firmament:"

        # Build command
        cmd = [
            self.rclone_binary,
            "serve",
            "s3",
            remote_string,
            "--config",
            self._config_path,
            "--addr",
            f"{self.serve_host}:{self._port}",
            "--auth-key",
            f"{self._access_key},{self._secret_key}",
        ]

        # Add any extra flags
        cmd.extend(self.extra_rclone_flags)

        try:
            self._process = subprocess.Popen(
                cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
            )
        except FileNotFoundError:
            self._cleanup_config_file()
            raise BackendError(
                f"rclone binary not found at '{self.rclone_binary}'. "
                "Please install rclone or provide the correct path."
            )
        except Exception as e:
            self._cleanup_config_file()
            raise BackendError(f"Failed to start rclone serve s3: {e}")

        # Wait for server to be ready
        self._wait_for_server_ready()

    def _wait_for_server_ready(self):
        """
        Wait for the rclone S3 server to accept connections.
        """
        start_time = time.time()

        while time.time() - start_time < self.startup_timeout:
            # Check if process died
            if self._process.poll() is not None:
                stdout, stderr = self._process.communicate()
                self._cleanup_config_file()
                raise BackendError(
                    f"rclone serve s3 exited unexpectedly with code "
                    f"{self._process.returncode}.\nstderr: {stderr}\nstdout: {stdout}"
                )

            # Try to connect
            try:
                with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                    s.settimeout(0.5)
                    s.connect((self.serve_host, self._port))
                    return  # Server is ready
            except (OSError, TimeoutError):
                time.sleep(0.1)

        # Timeout - kill the process
        self._stop_rclone_server()
        self._cleanup_config_file()
        raise BackendError(
            f"rclone serve s3 did not start within {self.startup_timeout} seconds"
        )

    def _stop_rclone_server(self):
        """
        Stop the rclone subprocess.
        """
        if self._process is None:
            return

        try:
            self._process.terminate()
            try:
                self._process.wait(timeout=5.0)
            except subprocess.TimeoutExpired:
                self._process.kill()
                self._process.wait()
        except Exception:
            pass  # Best effort cleanup
        finally:
            self._process = None

    def _cleanup_config_file(self):
        """
        Delete the temporary config file.
        """
        if self._config_path and os.path.exists(self._config_path):
            try:
                os.unlink(self._config_path)
            except Exception:
                pass  # Best effort cleanup
            self._config_path = None

    def close(self):
        """
        Clean up the rclone subprocess and temporary config file.
        """
        with self._lock:
            if self._closed:
                return
            self._closed = True
            self._stop_rclone_server()
            self._cleanup_config_file()
            RcloneS3Backend._active_instances.discard(self)

    @classmethod
    def _cleanup_all_instances(cls):
        """
        Clean up all active instances (called via atexit).
        """
        for instance in list(cls._active_instances):
            instance.close()

    def __str__(self):
        remote_display = (
            f"{self.rclone_remote_type}:{self.remote_path}"
            if self.remote_path
            else self.rclone_remote_type
        )
        return f"Rclone S3 ({remote_display} via localhost:{self._port})"

    def __del__(self):
        """
        Ensure cleanup on garbage collection.
        """
        self.close()
