import os

from observatory_platform.config import module_file_path


def project_path(*subpaths: str) -> str:
    """Make a path to a file or folder within this project.

    :param subpaths: any sub paths.
    :return: a path to a file or folder.
    """

    path = os.path.join(module_file_path("dmptool_workflows"), *subpaths)
    if not os.path.exists(path):
        raise FileNotFoundError(f"project_path: path or file {path} does not exist!")
    return path
