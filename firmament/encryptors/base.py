from typing import BinaryIO


class BaseEncryptor:
    """
    Base class for encryptors
    """

    chunk_size = 1024 * 1024

    def encrypt_identifier(self, identifier: str) -> str:
        raise NotImplementedError()

    def decrypt_identifier(self, crypttext: str) -> str:
        raise NotImplementedError()

    def encrypt_file(self, content: BinaryIO) -> BinaryIO:
        raise NotImplementedError()

    def decrypt_file(self, crypttext: BinaryIO) -> BinaryIO:
        raise NotImplementedError()
