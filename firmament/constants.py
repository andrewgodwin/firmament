from typing import Literal

PATH_REQUEST_ON_DEMAND = "OD"
PATH_REQUEST_IGNORE = "IG"
PATH_REQUEST_SYNC = "SY"
PATH_REQUEST_DOWNLOAD_ONCE = "DO"

PathRequest = Literal["DO", "IG", "OD", "SY"]
