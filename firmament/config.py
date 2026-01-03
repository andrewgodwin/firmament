from pathlib import Path
from typing import Annotated, Any

import yaml
from pydantic import AfterValidator, BaseModel
from pydantic.types import PathType

DirectoryPath = Annotated[
    Path, AfterValidator(lambda v: v.expanduser()), PathType("dir")
]


class BackendConfig(BaseModel):

    type: str
    options: dict[str, Any]


class CheckoutConfig(BaseModel):
    """
    Checkout configuration options
    """

    local_path: DirectoryPath
    remote_path: Path


class Config(BaseModel):
    """
    Config file parser
    """

    backends: dict[str, BackendConfig]
    checkouts: list[CheckoutConfig]

    def __init__(self, path: Path):
        with open(path) as fh:
            super().__init__(**yaml.safe_load(fh.read()))
