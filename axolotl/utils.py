# axolotl utils
from os import path
import re


def check_file_exists(file_path):
    matches = re.match(
        "^((?P<type>[a-z]+)://){0,1}(?P<path>[^#\\<\\>\\$\\+%!`&\\*'\\|\\{\\?\"=\\}:\\@]+)$",
        file_path
    )
    if matches == None:
        raise FileNotFoundError("can't recognize filepath {}".format(file_path))
    else:
        matches = matches.groupdict()
        
    if matches["type"] == None:
        matches["type"] = "file"
    
    if matches["type"] == "file":
        return path.exists(matches["path"])
    else:
        raise NotImplementedError()


def is_directory(file_path):
    matches = re.match(
        "^((?P<type>[a-z]+)://){0,1}(?P<path>[^#\\<\\>\\$\\+%!`&\\*'\\|\\{\\?\"=\\}:\\@]+)$",
        file_path
    )
    if matches == None:
        raise FileNotFoundError("can't recognize filepath {}".format(file_path))
    else:
        matches = matches.groupdict()
        
    if matches["type"] == None:
        matches["type"] = "file"
    
    if matches["type"] == "file":
        return path.isdir(matches["path"])
    else:
        raise NotImplementedError()