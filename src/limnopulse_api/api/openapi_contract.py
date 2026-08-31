from __future__ import annotations

import json
from collections.abc import Iterable
from copy import deepcopy
from typing import Any

import fastapi.routing as fastapi_routing
from fastapi import FastAPI
from fastapi.routing import APIRoute

_CANONICAL_422_DESCRIPTION = "Unprocessable Entity"
_PYTHON_422_DESCRIPTIONS = frozenset(
    {_CANONICAL_422_DESCRIPTION, "Unprocessable Content"}
)
_ERROR_RESPONSE_SCHEMA = {"$ref": "#/components/schemas/ErrorResponse"}
_OPENAPI_METHODS = frozenset(
    {"delete", "get", "head", "options", "patch", "post", "put", "trace"}
)


def iter_effective_api_routes(app: FastAPI) -> Iterable[Any]:
    iter_route_contexts = getattr(fastapi_routing, "iter_route_contexts", None)
    if iter_route_contexts is None:
        yield from (route for route in app.routes if isinstance(route, APIRoute))
        return

    for context in iter_route_contexts(app.routes):
        if isinstance(context.original_route, APIRoute) and context.path_format is not None:
            yield context


def _component_refs(value: Any) -> set[tuple[str, str]]:
    refs: set[tuple[str, str]] = set()

    def visit(node: Any) -> None:
        if isinstance(node, dict):
            ref = node.get("$ref")
            if isinstance(ref, str) and ref.startswith("#/components/"):
                _, _, section, name = ref.split("/", 3)
                refs.add((section, name))
            for child in node.values():
                visit(child)
        elif isinstance(node, list):
            for child in node:
                visit(child)

    visit(value)
    return refs


def _operation_security_refs(paths: Iterable[Any]) -> set[tuple[str, str]]:
    refs: set[tuple[str, str]] = set()
    for path_item in paths:
        if not isinstance(path_item, dict):
            continue
        for method, operation in path_item.items():
            if method not in _OPENAPI_METHODS or not isinstance(operation, dict):
                continue
            security = operation.get("security")
            if not isinstance(security, list):
                continue
            for requirement in security:
                if isinstance(requirement, dict):
                    refs.update(
                        ("securitySchemes", name)
                        for name in requirement
                        if isinstance(name, str)
                    )
    return refs


def _reachable_components(components: dict[str, Any], roots: Iterable[Any]) -> dict[str, Any]:
    root_values = list(roots)
    pending = list(
        _component_refs(root_values) | _operation_security_refs(root_values)
    )
    selected: dict[str, dict[str, Any]] = {}
    while pending:
        section, name = pending.pop()
        values = components.get(section, {})
        if name not in values or name in selected.get(section, {}):
            continue
        selected.setdefault(section, {})[name] = values[name]
        pending.extend(_component_refs(values[name]))
    return {
        section: {name: selected[section][name] for name in sorted(selected[section])}
        for section in sorted(selected)
    }


def _implicit_422_operations(app: FastAPI) -> set[tuple[str, str]]:
    operations: set[tuple[str, str]] = set()
    for route in iter_effective_api_routes(app):
        declared_response = route.responses.get(422, route.responses.get("422"))
        if not isinstance(declared_response, dict) or "description" in declared_response:
            continue
        operations.update(
            (route.path_format, method.lower()) for method in route.methods or ()
        )
    return operations


def _normalize_422_descriptions(
    paths: dict[str, Any], implicit_operations: set[tuple[str, str]]
) -> None:
    for path, path_item in paths.items():
        for method, operation in path_item.items():
            if (path, method) not in implicit_operations or not isinstance(operation, dict):
                continue
            responses = operation.get("responses")
            if not isinstance(responses, dict):
                continue
            response = responses.get("422")
            if not isinstance(response, dict):
                continue
            schema = (
                response.get("content", {})
                .get("application/json", {})
                .get("schema")
            )
            if (
                response.get("description") in _PYTHON_422_DESCRIPTIONS
                and schema == _ERROR_RESPONSE_SCHEMA
            ):
                response["description"] = _CANONICAL_422_DESCRIPTION


def build_v1_openapi_contract(app: FastAPI) -> dict[str, Any]:
    schema = app.openapi()
    paths = {
        path: deepcopy(schema["paths"][path])
        for path in sorted(schema["paths"])
        if path.startswith("/v1/")
    }
    _normalize_422_descriptions(paths, _implicit_422_operations(app))
    return {
        "openapi": schema["openapi"],
        "info": schema["info"],
        "paths": paths,
        "components": _reachable_components(schema.get("components", {}), paths.values()),
    }


def render_v1_openapi_contract(app: FastAPI) -> str:
    return json.dumps(build_v1_openapi_contract(app), indent=2, sort_keys=True) + "\n"
