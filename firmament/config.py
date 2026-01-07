from pathlib import Path
from typing import Annotated, Any

import yaml
from pydantic import AfterValidator, BaseModel
from pydantic.types import PathType

from firmament.backends.base import BaseBackend
from firmament.database import Database

DirectoryPath = Annotated[
    Path, AfterValidator(lambda v: v.expanduser()), PathType("dir")
]
FilePath = Annotated[Path, AfterValidator(lambda v: v.expanduser()), PathType("file")]


class BackendSchema(BaseModel):

    type: str
    options: dict[str, Any]


class ConfigSchema(BaseModel):

    backends: dict[str, BackendSchema]


class Config:
    """
    Config file parser
    """

    backends: dict[str, BaseBackend]

    def __init__(self, root_path: Path):
        # Calculate paths
        self.root_path = root_path.resolve()
        self.meta_path = self.root_path / ".firmament"
        self.config_path = self.meta_path / "config"
        self.database_path = self.meta_path / "database"

        # Read main config in
        with open(self.config_path) as fh:
            self.config_data = ConfigSchema(**yaml.safe_load(fh.read()))

        # Set up backend class instances
        self.backends = {}
        for name, backend_config in self.config_data.backends.items():
            backend_class = BaseBackend.implementation_get(backend_config.type)
            self.backends[name] = backend_class(**backend_config.options)

        # Set up database
        self.database = Database(self.database_path)
