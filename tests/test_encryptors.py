import io

import pytest

from firmament.encryptors.aes import AESEncryptor


class TestNullEncryptor:
    """
    Tests for the NullEncryptor (passthrough).
    """

    def test_identifier_roundtrip(self, null_encryptor):
        identifier = "abc123def456"
        encrypted = null_encryptor.encrypt_identifier(identifier)
        decrypted = null_encryptor.decrypt_identifier(encrypted)
        assert decrypted == identifier

    def test_identifier_is_passthrough(self, null_encryptor):
        identifier = "test-identifier"
        assert null_encryptor.encrypt_identifier(identifier) == identifier
        assert null_encryptor.decrypt_identifier(identifier) == identifier

    def test_file_roundtrip(self, null_encryptor):
        content = b"Hello, World!"
        encrypted = null_encryptor.encrypt_file(io.BytesIO(content))
        decrypted = null_encryptor.decrypt_file(encrypted)
        assert decrypted.read() == content

    def test_file_is_passthrough(self, null_encryptor):
        content = b"test content"
        source = io.BytesIO(content)
        encrypted = null_encryptor.encrypt_file(source)
        assert encrypted is source


class TestAESEncryptor:
    """
    Tests for the AESEncryptor (AES-SIV for identifiers, AES-GCM for files).
    """

    def test_identifier_roundtrip(self, aes_encryptor):
        identifier = "abc123def456"
        encrypted = aes_encryptor.encrypt_identifier(identifier)
        decrypted = aes_encryptor.decrypt_identifier(encrypted)
        assert decrypted == identifier

    def test_identifier_is_encrypted(self, aes_encryptor):
        identifier = "test-identifier"
        encrypted = aes_encryptor.encrypt_identifier(identifier)
        assert encrypted != identifier

    def test_identifier_deterministic(self, aes_encryptor):
        """
        AES-SIV should produce the same ciphertext for the same plaintext.
        """
        identifier = "same-identifier"
        encrypted1 = aes_encryptor.encrypt_identifier(identifier)
        encrypted2 = aes_encryptor.encrypt_identifier(identifier)
        assert encrypted1 == encrypted2

    def test_identifier_different_inputs_different_outputs(self, aes_encryptor):
        encrypted1 = aes_encryptor.encrypt_identifier("identifier-a")
        encrypted2 = aes_encryptor.encrypt_identifier("identifier-b")
        assert encrypted1 != encrypted2

    def test_file_roundtrip_small(self, aes_encryptor):
        content = b"Hello, World!"
        encrypted = aes_encryptor.encrypt_file(io.BytesIO(content))
        decrypted = aes_encryptor.decrypt_file(encrypted)
        assert decrypted.read() == content

    def test_file_roundtrip_empty(self, aes_encryptor):
        content = b""
        encrypted = aes_encryptor.encrypt_file(io.BytesIO(content))
        decrypted = aes_encryptor.decrypt_file(encrypted)
        assert decrypted.read() == content

    def test_file_roundtrip_exact_chunk_size(self, aes_encryptor):
        """
        Test content that is exactly one chunk.
        """
        content = b"x" * aes_encryptor.chunk_size
        encrypted = aes_encryptor.encrypt_file(io.BytesIO(content))
        decrypted = aes_encryptor.decrypt_file(encrypted)
        assert decrypted.read() == content

    def test_file_roundtrip_multiple_chunks(self, aes_encryptor):
        """
        Test content spanning multiple chunks.
        """
        content = b"y" * (aes_encryptor.chunk_size * 2 + 500)
        encrypted = aes_encryptor.encrypt_file(io.BytesIO(content))
        decrypted = aes_encryptor.decrypt_file(encrypted)
        assert decrypted.read() == content

    def test_file_is_encrypted(self, aes_encryptor):
        content = b"This is secret data that should be encrypted"
        encrypted_stream = aes_encryptor.encrypt_file(io.BytesIO(content))
        encrypted_data = encrypted_stream.read()
        assert encrypted_data != content
        assert content not in encrypted_data

    def test_file_roundtrip_binary_data(self, aes_encryptor):
        """
        Test with binary data including null bytes.
        """
        content = bytes(range(256)) * 100
        encrypted = aes_encryptor.encrypt_file(io.BytesIO(content))
        decrypted = aes_encryptor.decrypt_file(encrypted)
        assert decrypted.read() == content

    def test_file_streaming_read(self, aes_encryptor):
        """
        Test reading decrypted content in small chunks.
        """
        content = b"A" * 5000
        encrypted = aes_encryptor.encrypt_file(io.BytesIO(content))
        decrypted = aes_encryptor.decrypt_file(encrypted)

        result = b""
        while chunk := decrypted.read(100):
            result += chunk
        assert result == content

    def test_different_keys_cannot_decrypt(self):
        encryptor1 = AESEncryptor("key-one", key_iterations=1000)
        encryptor2 = AESEncryptor("key-two", key_iterations=1000)

        content = b"Secret message"
        encrypted_stream = encryptor1.encrypt_file(io.BytesIO(content))
        encrypted_data = encrypted_stream.read()

        with pytest.raises(Exception):
            decrypted = encryptor2.decrypt_file(io.BytesIO(encrypted_data))
            decrypted.read()

    def test_different_keys_cannot_decrypt_identifier(self):
        encryptor1 = AESEncryptor("key-one", key_iterations=1000)
        encryptor2 = AESEncryptor("key-two", key_iterations=1000)

        identifier = "secret-id"
        encrypted = encryptor1.encrypt_identifier(identifier)

        with pytest.raises(Exception):
            encryptor2.decrypt_identifier(encrypted)
