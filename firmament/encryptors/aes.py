import base64
import io
import os
import struct
from typing import BinaryIO

from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.ciphers.aead import AESGCM, AESSIV
from cryptography.hazmat.primitives.kdf.pbkdf2 import PBKDF2HMAC

from firmament.encryptors.base import BaseEncryptor


class _EncryptingStream(io.RawIOBase):
    """
    A streaming wrapper that encrypts file content in chunks using AES-GCM.

    Each chunk is prefixed with: [4-byte length][12-byte nonce][ciphertext+tag]
    """

    def __init__(self, source: BinaryIO, aesgcm: AESGCM, chunk_size: int):
        self._fh = source
        self._aesgcm = aesgcm
        self._chunk_size = chunk_size
        self._buffer = b""
        self._eof = False

    def _encrypt_chunk(self) -> bytes:
        plaintext = self._fh.read(self._chunk_size)
        if not plaintext:
            self._eof = True
            return b""

        nonce = os.urandom(AESEncryptor.NONCE_SIZE)
        ciphertext = self._aesgcm.encrypt(nonce, plaintext, associated_data=None)
        # Prefix with length so decoder knows chunk boundaries
        chunk_data = nonce + ciphertext
        return struct.pack(">I", len(chunk_data)) + chunk_data

    def read(self, size: int = -1) -> bytes:
        if size == -1:
            # Read all remaining
            while not self._eof:
                self._buffer += self._encrypt_chunk()
            result = self._buffer
            self._buffer = b""
            return result

        while len(self._buffer) < size and not self._eof:
            self._buffer += self._encrypt_chunk()

        result = self._buffer[:size]
        self._buffer = self._buffer[size:]
        return result

    def readinto(self, b):  # type: ignore[override]
        data = self.read(len(b))
        b[: len(data)] = data
        return len(data)

    def close(self):
        self._fh.close()
        super().close()

    def readable(self) -> bool:
        return True

    def writable(self) -> bool:
        return False

    def seekable(self) -> bool:
        return False


class _DecryptingStream(io.RawIOBase):
    """
    A streaming wrapper that decrypts AES-GCM encrypted content in chunks.

    Expects format: [4-byte length][12-byte nonce][ciphertext+tag]...
    """

    def __init__(self, source: BinaryIO, aesgcm: AESGCM):
        self._fh = source
        self._aesgcm = aesgcm
        self._buffer = b""
        self._eof = False

    def _decrypt_chunk(self) -> bytes:
        # Read chunk length prefix
        length_bytes = self._fh.read(4)
        if not length_bytes:
            self._eof = True
            return b""

        chunk_len = struct.unpack(">I", length_bytes)[0]

        # Read nonce + ciphertext
        chunk_data = self._fh.read(chunk_len)
        nonce = chunk_data[: AESEncryptor.NONCE_SIZE]
        ciphertext = chunk_data[AESEncryptor.NONCE_SIZE :]

        return self._aesgcm.decrypt(nonce, ciphertext, associated_data=None)

    def read(self, size: int = -1) -> bytes:
        if size == -1:
            # Read all remaining
            while not self._eof:
                self._buffer += self._decrypt_chunk()
            result = self._buffer
            self._buffer = b""
            return result

        while len(self._buffer) < size and not self._eof:
            self._buffer += self._decrypt_chunk()

        result = self._buffer[:size]
        self._buffer = self._buffer[size:]
        return result

    def readinto(self, b):  # type: ignore[override]
        data = self.read(len(b))
        b[: len(data)] = data
        return len(data)

    def close(self):
        self._fh.close()
        super().close()

    def readable(self) -> bool:
        return True

    def writable(self) -> bool:
        return False

    def seekable(self) -> bool:
        return False


class AESEncryptor(BaseEncryptor):
    """
    An encryptor that uses AES-SIV for identifiers and AES-GCM for files.

    File encryption is done in a streaming fashion with each chunk independently
    encrypted. Format: [4-byte big-endian length][12-byte nonce][ciphertext+16-byte tag]...
    """

    # AES-GCM nonce size (96 bits is recommended)
    NONCE_SIZE = 12

    # AES-GCM tag size
    TAG_SIZE = 16

    def __init__(self, key: str, key_iterations: int = 100000):
        # Derive a 64-byte key for AES-SIV (requires 256 or 512 bit key)
        kdf_siv = PBKDF2HMAC(
            algorithm=hashes.SHA256(),
            length=64,
            salt=b"NaCl",
            iterations=key_iterations,
        )
        siv_key = kdf_siv.derive(key.encode("utf8"))
        self._aessiv = AESSIV(siv_key)

        # Derive a 32-byte key for AES-GCM (256-bit)
        kdf_gcm = PBKDF2HMAC(
            algorithm=hashes.SHA256(),
            length=32,
            salt=b"NaCl",
            iterations=key_iterations,
        )
        gcm_key = kdf_gcm.derive(key.encode("utf8"))
        self._aesgcm = AESGCM(gcm_key)

    def encrypt_identifier(self, identifier: str) -> str:
        ciphertext = self._aessiv.encrypt(
            identifier.encode("utf8"), associated_data=None
        )
        return base64.urlsafe_b64encode(ciphertext).decode("ascii")

    def decrypt_identifier(self, crypttext: str) -> str:
        ciphertext = base64.urlsafe_b64decode(crypttext)
        plaintext = self._aessiv.decrypt(ciphertext, associated_data=None)
        return plaintext.decode("utf8")

    def encrypt_file(self, content: BinaryIO) -> BinaryIO:
        return _EncryptingStream(content, self._aesgcm, self.chunk_size)  # type: ignore[return-value]

    def decrypt_file(self, crypttext: BinaryIO) -> BinaryIO:
        return _DecryptingStream(crypttext, self._aesgcm)  # type: ignore[return-value]
