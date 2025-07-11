from __future__ import annotations

import dataclasses
import re
from abc import ABC
from pathlib import Path
from typing import ClassVar, Literal, Pattern, TypeVar

import structlog

logger = structlog.getLogger(__name__)
T = TypeVar("T", bound="Script")


@dataclasses.dataclass(frozen=True)
class Script(ABC):
    pattern: ClassVar[Pattern[str]]
    type: ClassVar[Literal["V", "R", "A"]]
    name: str
    file_path: Path
    description: str

    @staticmethod
    def get_script_name(file_path: Path) -> str:
        """Script name is the filename without any jinja extension"""
        if file_path.suffixes[-1].upper() == ".JINJA":
            return file_path.stem
        return file_path.name

    @classmethod
    def from_path(cls, file_path: Path, **kwargs) -> T:
        logger.debug("script found", class_name=cls.__name__, file_path=str(file_path))

        # script name is the filename without any jinja extension
        script_name = cls.get_script_name(file_path=file_path)
        name_parts = cls.pattern.search(file_path.name.strip())
        description = name_parts.group("description").replace("_", " ").capitalize()

        return cls(
            name=script_name, file_path=file_path, description=description, **kwargs
        )


@dataclasses.dataclass(frozen=True)
class VersionedScript(Script):
    pattern: ClassVar[re.Pattern[str]] = re.compile(
        r"^(V)(?P<version>.+?)?__(?P<description>.+?)\.", re.IGNORECASE
    )
    type: ClassVar[Literal["V"]] = "V"
    version: str

    @classmethod
    def from_path(cls: T, file_path: Path, **kwargs) -> T:
        name_parts = cls.pattern.search(file_path.name.strip())

        return super().from_path(
            file_path=file_path, version=name_parts.group("version")
        )


@dataclasses.dataclass(frozen=True)
class RepeatableScript(Script):
    pattern: ClassVar[re.Pattern[str]] = re.compile(
        r"^(R)__(?P<description>.+?)\.", re.IGNORECASE
    )
    type: ClassVar[Literal["R"]] = "R"


@dataclasses.dataclass(frozen=True)
class AlwaysScript(Script):
    pattern: ClassVar[re.Pattern[str]] = re.compile(
        r"^(A)__(?P<description>.+?)\.", re.IGNORECASE
    )
    type: ClassVar[Literal["A"]] = "A"


def script_factory(
    file_path: Path,
) -> T | None:
    if VersionedScript.pattern.search(file_path.name.strip()) is not None:
        return VersionedScript.from_path(file_path=file_path)

    elif RepeatableScript.pattern.search(file_path.name.strip()) is not None:
        return RepeatableScript.from_path(file_path=file_path)

    elif AlwaysScript.pattern.search(file_path.name.strip()) is not None:
        return AlwaysScript.from_path(file_path=file_path)

    logger.debug("ignoring non-change file", file_path=str(file_path))


def get_all_scripts_recursively(root_directory: Path):
    all_files: dict[str, T] = dict()
    all_versions = list()
    # Walk the entire directory structure recursively
    sql_pattern = re.compile(r"\.sql(\.jinja)?$", flags=re.IGNORECASE)
    file_paths = root_directory.glob("**/*")
    for file_path in file_paths:
        if file_path.is_dir():
            continue
        if not sql_pattern.search(file_path.name.strip()):
            continue
        script = script_factory(file_path=file_path)
        if script is None:
            continue

        # Throw an error if the script_name already exists
        if script.name.lower() in all_files:
            raise ValueError(
                f"The script name {script.name} exists more than once ("
                f"first_instance {str(all_files[script.name.lower()].file_path)}, "
                f"second instance {str(script.file_path)})"
            )

        all_files[script.name.lower()] = script

        # Throw an error if the same version exists more than once
        if script.type == "V":
            if script.version in all_versions:
                raise ValueError(
                    f"The script version {script.version} exists more than once "
                    f"(second instance {str(script.file_path)})"
                )
            all_versions.append(script.version)

    return all_files
