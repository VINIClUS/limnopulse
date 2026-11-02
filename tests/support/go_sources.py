"""Read this module's Go sources for code-scan tests: import closures, per-file import names
and comment-free code, so a check sees `w.Supervisor` under any alias and ignores comments."""

from __future__ import annotations

import re
from dataclasses import dataclass
from functools import cache
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
MODULE = "github.com/VINIClUS/limnopulse/"
_IMPORT_BLOCK = re.compile(r"^import\s*\((.*?)^\)", re.MULTILINE | re.DOTALL)
_IMPORT_LINE = re.compile(r'^import\s+(?:([\w.]+)\s+)?"([^"]+)"', re.MULTILINE)
_IMPORT_SPEC = re.compile(r'^\s*(?:([\w.]+)\s+)?"([^"]+)"\s*$')
_VERSION = re.compile(r"^v\d+$")


@dataclass(frozen=True)
class GoFile:
    path: Path
    package: str
    # The source without comments; string literals are kept.
    code: str
    # Import path -> local name: the alias, "." for a dot import, "_" for a blank one.
    imports: dict[str, str]

    @property
    def relative(self) -> str:
        return self.path.relative_to(ROOT).as_posix()

    def uses(self, import_path: str, member: str) -> bool:
        """Whether the code refers to `<local name>.<member>` (a regex) of import_path."""
        name = self.imports.get(import_path)
        if name is None or name == "_":
            return False
        if name == ".":
            # A dot import hides every qualified reference; count it as a use.
            return True
        pattern = rf"(?<![\w.]){re.escape(name)}\s*\.\s*(?:{member})"
        return re.search(pattern, self.code) is not None


def strip_comments(source: str) -> str:
    """Remove // and /* */ comments, leaving string, raw string and rune literals intact."""
    out: list[str] = []
    index, length = 0, len(source)
    while index < length:
        char = source[index]
        if source.startswith("//", index):
            end = source.find("\n", index)
            index = length if end < 0 else end
        elif source.startswith("/*", index):
            end = source.find("*/", index + 2)
            index = length if end < 0 else end + 2
            out.append(" ")
        elif char in "\"'":
            end = index + 1
            while end < length and source[end] != char and source[end] != "\n":
                end += 2 if source[end] == "\\" else 1
            out.append(source[index : end + 1])
            index = end + 1
        elif char == "`":
            end = source.find("`", index + 1)
            end = length - 1 if end < 0 else end
            out.append(source[index : end + 1])
            index = end + 1
        else:
            out.append(char)
            index += 1
    return "".join(out)


def _default_name(import_path: str) -> str:
    parts = import_path.split("/")
    if len(parts) > 1 and _VERSION.match(parts[-1]):
        return parts[-2]
    return parts[-1]


def _imports(code: str) -> dict[str, str]:
    specs: list[tuple[str | None, str]] = []
    for block in _IMPORT_BLOCK.findall(code):
        for line in block.splitlines():
            match = _IMPORT_SPEC.match(line)
            if match:
                specs.append((match.group(1), match.group(2)))
    specs.extend((alias or None, path) for alias, path in _IMPORT_LINE.findall(code))
    return {path: alias or _default_name(path) for alias, path in specs}


@cache
def package_files(import_path: str) -> tuple[GoFile, ...]:
    """The non-test Go files of one of this module's packages."""
    directory = ROOT / import_path.removeprefix(MODULE)
    files = []
    for path in sorted(directory.glob("*.go")):
        if path.name.endswith("_test.go"):
            continue
        code = strip_comments(path.read_text(encoding="utf-8"))
        files.append(GoFile(path=path, package=import_path, code=code, imports=_imports(code)))
    return tuple(files)


@cache
def closure(entrypoint: str) -> frozenset[str]:
    """Every package `cmd/<role>` links, following this module's packages transitively."""
    seen: set[str] = set()
    pending = [MODULE + entrypoint]
    while pending:
        package = pending.pop()
        if package in seen:
            continue
        seen.add(package)
        if package.startswith(MODULE):
            for file in package_files(package):
                pending.extend(file.imports)
    return frozenset(seen)


def closure_files(entrypoint: str) -> list[GoFile]:
    """The non-test files of every module package in the entrypoint's closure."""
    return [
        file
        for package in sorted(closure(entrypoint))
        if package.startswith(MODULE)
        for file in package_files(package)
    ]
