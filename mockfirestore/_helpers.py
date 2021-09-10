import operator
import random
import string
from datetime import datetime as dt
from functools import reduce
from typing import (Dict, Any, Tuple, TypeVar, Sequence, Iterator)

T = TypeVar('T')
KeyValuePair = Tuple[str, Dict[str, Any]]
Document = Dict[str, Any]
Collection = Dict[str, Document]
Store = Dict[str, Collection]


def get_by_path(data: Dict[str, T], path: Sequence[str], create_nested: bool = False) -> T:
    """Access a nested object in root by item sequence."""

    def get_or_create(a, b):
        if b not in a:
            a[b] = {}
        return a[b]

    if create_nested:
        return reduce(get_or_create, path, data)
    else:
        return reduce(operator.getitem, path, data)


def set_by_path(data: Dict[str, T], path: Sequence[str], value: T, create_nested: bool = True):
    """Set a value in a nested object in root by item sequence."""
    get_by_path(data, path[:-1], create_nested=True)[path[-1]] = value


def delete_by_path(data: Dict[str, T], path: Sequence[str]):
    """Delete a value in a nested object in root by item sequence."""
    del get_by_path(data, path[:-1])[path[-1]]


def generate_random_string():
    return ''.join(random.choice(string.ascii_letters + string.digits) for _ in range(20))


class Timestamp:
    """
    Imitates some properties of `google.protobuf.timestamp_pb2.Timestamp`
    """

    def __init__(self, timestamp: float):
        self._timestamp = timestamp

    @classmethod
    def from_now(cls):
        timestamp = dt.now().timestamp()
        return cls(timestamp)

    @property
    def seconds(self):
        return str(self._timestamp).split('.')[0]

    @property
    def nanos(self):
        return str(self._timestamp).split('.')[1]


def get_document_iterator(document: Dict[str, Any], prefix: str = '') -> Iterator[Tuple[str, Any]]:
    """
    :returns: (dot-delimited path, value,)
    """
    for key, value in document.items():
        if isinstance(value, dict):
            for item in get_document_iterator(value, prefix=key):
                yield item

        if not prefix:
            yield key, value
        else:
            yield '{}.{}'.format(prefix, key), value


# Copyright 2018 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.


import re

_FIELD_PATH_MISSING_TOP = "{!r} is not contained in the data"
_FIELD_PATH_MISSING_KEY = "{!r} is not contained in the data for the key {!r}"
_FIELD_PATH_WRONG_TYPE = (
    "The data at {!r} is not a dictionary, so it cannot contain the key {!r}"
)

_FIELD_PATH_DELIMITER = "."
_BACKSLASH = "\\"
_ESCAPED_BACKSLASH = _BACKSLASH * 2
_BACKTICK = "`"
_ESCAPED_BACKTICK = _BACKSLASH + _BACKTICK

_SIMPLE_FIELD_NAME = re.compile("^[_a-zA-Z][_a-zA-Z0-9]*$")
_LEADING_ALPHA_INVALID = re.compile("^[_a-zA-Z][_a-zA-Z0-9]*[^_a-zA-Z0-9]")
PATH_ELEMENT_TOKENS = [
    ("SIMPLE", r"[_a-zA-Z][_a-zA-Z0-9]*"),  # unquoted elements
    ("QUOTED", r"`(?:\\`|[^`])*?`"),  # quoted elements, unquoted
    ("DOT", r"\."),  # separator
]
TOKENS_PATTERN = "|".join("(?P<{}>{})".format(*pair) for pair in PATH_ELEMENT_TOKENS)
TOKENS_REGEX = re.compile(TOKENS_PATTERN)


def _tokenize_field_path(path: str):
    """Lex a field path into tokens (including dots).

    Args:
        path (str): field path to be lexed.
    Returns:
        List(str): tokens
    """
    pos = 0
    get_token = TOKENS_REGEX.match
    match = get_token(path)
    while match is not None:
        type_ = match.lastgroup
        value = match.group(type_)
        yield value
        pos = match.end()
        match = get_token(path, pos)
    if pos != len(path):
        raise ValueError("Path {} not consumed, residue: {}".format(path, path[pos:]))


def split_field_path(path: str):
    """Split a field path into valid elements (without dots).

    Args:
        path (str): field path to be lexed.
    Returns:
        List(str): tokens
    Raises:
        ValueError: if the path does not match the elements-interspersed-
                    with-dots pattern.
    """
    if not path:
        return []

    elements = []
    want_dot = False

    for element in _tokenize_field_path(path):
        if want_dot:
            if element != ".":
                raise ValueError("Invalid path: {}".format(path))
            else:
                want_dot = False
        else:
            if element == ".":
                raise ValueError("Invalid path: {}".format(path))
            elements.append(element)
            want_dot = True

    if not want_dot or not elements:
        raise ValueError("Invalid path: {}".format(path))

    return elements


def parse_field_path(api_repr: str):
    """Parse a **field path** from into a list of nested field names.

    See :func:`field_path` for more on **field paths**.

    Args:
        api_repr (str):
            The unique Firestore api representation which consists of
            either simple or UTF-8 field names. It cannot exceed
            1500 bytes, and cannot be empty. Simple field names match
            ``'^[_a-zA-Z][_a-zA-Z0-9]*$'``. All other field names are
            escaped by surrounding them with backticks.

    Returns:
        List[str, ...]: The list of field names in the field path.
    """
    # code dredged back up from
    # https://github.com/googleapis/google-cloud-python/pull/5109/files
    field_names = []
    for field_name in split_field_path(api_repr):
        # non-simple field name
        if field_name[0] == "`" and field_name[-1] == "`":
            field_name = field_name[1:-1]
            field_name = field_name.replace(_ESCAPED_BACKTICK, _BACKTICK)
            field_name = field_name.replace(_ESCAPED_BACKSLASH, _BACKSLASH)
        field_names.append(field_name)
    return field_names

# def parse_field_path(api_repr: str):
#     return api_repr.replace("`").split(".")