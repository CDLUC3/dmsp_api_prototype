import pathlib
from typing import Annotated, Literal

from cyclopts import Parameter, validators

Directory = Annotated[
    pathlib.Path,
    Parameter(
        validator=validators.Path(
            dir_okay=True,
            file_okay=False,
            exists=True,
        )
    ),
]
LogLevel = Annotated[
    Literal["CRITICAL", "ERROR", "WARNING", "INFO", "DEBUG", "NOTSET"],
    Parameter(help="Python log level"),
]
