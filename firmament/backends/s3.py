from collections.abc import Iterator
from typing import BinaryIO

import boto3
from botocore.exceptions import ClientError

from .base import BackendError, BaseBackend, VersionError


class S3Backend(BaseBackend):
    """
    A backend that uses Amazon S3 (or S3-compatible services) to store blocks and files.

    Uses ETags for version tracking to enable optimistic locking on database files.
    """

    type_aliases = ["s3"]
    content_rebuild_interval = 60 * 60

    def __init__(
        self,
        bucket: str,
        name: str,
        encryption_key: str | None = None,
        prefix: str = "",
        region: str | None = None,
        endpoint_url: str | None = None,
        access_key_id: str | None = None,
        secret_access_key: str | None = None,
        storage_class: str | None = None,
    ):
        super().__init__(name=name, encryption_key=encryption_key)
        self.bucket = bucket
        self.prefix = prefix.strip("/")
        self.storage_class = storage_class

        # Build client kwargs
        client_kwargs: dict = {}
        if region:
            client_kwargs["region_name"] = region
        if endpoint_url:
            client_kwargs["endpoint_url"] = endpoint_url
        if access_key_id and secret_access_key:
            client_kwargs["aws_access_key_id"] = access_key_id
            client_kwargs["aws_secret_access_key"] = secret_access_key

        self.client = boto3.client("s3", **client_kwargs)

        # Verify bucket exists and is accessible
        try:
            self.client.head_bucket(Bucket=self.bucket)
        except ClientError as e:
            error_code = e.response.get("Error", {}).get("Code", "")
            if error_code == "404":
                raise BackendError(f"Bucket '{self.bucket}' does not exist")
            elif error_code == "403":
                raise BackendError(f"Access denied to bucket '{self.bucket}'")
            else:
                raise BackendError(f"Cannot access bucket '{self.bucket}': {e}")

    def __str__(self):
        if self.prefix:
            return f"S3 (bucket {self.bucket}, prefix {self.prefix})"
        return f"S3 (bucket {self.bucket})"

    def _full_key(self, path: str) -> str:
        """Combines the prefix with the given path to form the full S3 key."""
        if self.prefix:
            return f"{self.prefix}/{path}"
        return path

    def remote_read_io(
        self,
        path: str,
        target_handle: BinaryIO,
    ) -> str:
        """
        Reads encrypted contents stored at "path" into the passed file handle.

        Returns the ETag as a version identifier.
        """
        key = self._full_key(path)
        try:
            response = self.client.get_object(Bucket=self.bucket, Key=key)
        except ClientError as e:
            error_code = e.response.get("Error", {}).get("Code", "")
            if error_code == "NoSuchKey":
                raise FileNotFoundError(f"Object not found: {key}")
            raise BackendError(f"Failed to read {key}: {e}")

        # Stream through encryptor
        enc_handle = self.encryptor.decrypt_file(response["Body"])
        while True:
            chunk = enc_handle.read(self.encryptor.chunk_size)
            if chunk:
                target_handle.write(chunk)
            else:
                break
        enc_handle.close()

        # Return ETag as version (strip quotes that S3 adds)
        return response["ETag"].strip('"')

    def remote_write_io(
        self,
        path: str,
        source_handle: BinaryIO,
        over_version: str | None = None,
        is_content: bool = False,
    ):
        """
        Writes encrypted contents from the passed file handle into "path".

        If over_version is provided, uses conditional put to ensure we're
        overwriting the expected version. Raises VersionError if the current
        version doesn't match.

        If is_content is True and storage_class is configured, applies the
        storage class to the object.
        """
        key = self._full_key(path)

        # Read and encrypt all content first (S3 put_object needs the full body)
        enc_handle = self.encryptor.encrypt_file(source_handle)
        encrypted_data = enc_handle.read()
        enc_handle.close()

        put_kwargs: dict = {
            "Bucket": self.bucket,
            "Key": key,
            "Body": encrypted_data,
        }

        # Apply storage class for content writes if configured
        if is_content and self.storage_class:
            put_kwargs["StorageClass"] = self.storage_class

        # If we have a version to check against, use conditional put
        if over_version:
            # S3 doesn't support If-Match for put_object directly, so we need to
            # check the current version first and hope for no race condition,
            # or use the more complex approach with object locking.
            # For now, we'll do a head check before writing.
            try:
                head_response = self.client.head_object(Bucket=self.bucket, Key=key)
                current_version = head_response["ETag"].strip('"')
                if current_version != over_version:
                    raise VersionError(
                        f"Requested {over_version}, got {current_version}"
                    )
            except ClientError as e:
                error_code = e.response.get("Error", {}).get("Code", "")
                if error_code == "404":
                    # Object doesn't exist, but we expected a version
                    raise VersionError(
                        f"Requested {over_version}, but object does not exist"
                    )
                raise BackendError(f"Failed to check version for {key}: {e}")

        try:
            self.client.put_object(**put_kwargs)
        except ClientError as e:
            raise BackendError(f"Failed to write {key}: {e}")

    def remote_exists(self, path: str) -> bool:
        """Returns if the given remote path exists."""
        key = self._full_key(path)
        try:
            self.client.head_object(Bucket=self.bucket, Key=key)
            return True
        except ClientError as e:
            error_code = e.response.get("Error", {}).get("Code", "")
            if error_code == "404":
                return False
            raise BackendError(f"Failed to check existence of {key}: {e}")

    def remote_delete(self, path: str):
        """Deletes the remote path."""
        key = self._full_key(path)
        try:
            self.client.delete_object(Bucket=self.bucket, Key=key)
        except ClientError as e:
            # S3 delete is idempotent, but other errors should be raised
            raise BackendError(f"Failed to delete {key}: {e}")

    def remote_content_path(self, sha256sum: str) -> str:
        """
        Works out storage path for given sha256sum.

        Uses a 3-char prefix so listing has a max of 4096 entries per prefix.
        """
        cryptsum = self.encryptor.encrypt_identifier(sha256sum)
        return f"content/{cryptsum[:3]}/{cryptsum}"

    def remote_database_path(self, db_name: str) -> str:
        """Works out storage path for given database name."""
        return f"database-{db_name}"

    def remote_content_walk(self) -> Iterator[str]:
        """
        Yields the set of content hashes that are stored on this backend
        by listing all objects under the content/ prefix.
        """
        content_prefix = self._full_key("content/")
        paginator = self.client.get_paginator("list_objects_v2")

        for page in paginator.paginate(Bucket=self.bucket, Prefix=content_prefix):
            for obj in page.get("Contents", []):
                key = obj["Key"]
                # Extract the filename (last component of the key)
                filename = key.rsplit("/", 1)[-1]
                # Skip entries that aren't actual content hashes (check length)
                if len(filename) > 4:
                    yield self.encryptor.decrypt_identifier(filename)
