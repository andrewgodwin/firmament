from typing import BinaryIO

from firmament.encryptors.base import BaseEncryptor


class NullEncryptor(BaseEncryptor):
    """
    An encryptor that doesn't do anything.
    """

    def encrypt_identifier(self, identifier: str) -> str:
        return identifier

    def decrypt_identifier(self, crypttext: str) -> str:
        return crypttext

    def encrypt_file(self, content: BinaryIO) -> BinaryIO:
        return content

    def decrypt_file(self, crypttext: BinaryIO) -> BinaryIO:
        return crypttext
