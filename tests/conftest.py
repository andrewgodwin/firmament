import pytest

from firmament.encryptors.aes import AESEncryptor
from firmament.encryptors.null import NullEncryptor


@pytest.fixture
def null_encryptor():
    return NullEncryptor()


@pytest.fixture
def aes_encryptor():
    # Use fewer iterations for faster tests
    return AESEncryptor("test-key", key_iterations=1000)
