"""Per-item DynamoDB authorization of the runtime writes (issue #108).

DynamoDB authorizes every item of a `TransactWriteItems` call on its own: a `Put` needs
`dynamodb:PutItem`, an `Update` `dynamodb:UpdateItem`, a `Delete` `dynamodb:DeleteItem` and a
`ConditionCheck` `dynamodb:ConditionCheckItem`, each on the ARN of the table that the item names
(developer guide, "Using IAM with DynamoDB transactions"). IAM has no `dynamodb:TransactWriteItems`
action at all.

These tests derive every transaction item and every standalone write from the sources of the two
runtime principals in `infra/opentofu/iam_runtime.tf`:

- `api`: the Python package `src/limnopulse_api`, except `workers/` (the telemetry ingest worker
  runs as its own IAM user, managed in `ingestion_queues.tf`). The scan also covers the operator-run
  M1 backfill in `migrations/`, which only needs domain-table actions the API holds anyway.
- `workers`: every Go file outside the test files (the binaries under `cmd/` and `internal/`).

Each derived (action, table) pair must be granted by an unconditional attachment in
`iam_runtime.tf` on that table's ARN. A grant on `.../index/*` does not count: item actions are
authorized against the table. Grants that other files attach behind a feature flag
(`ingestion_queues.tf`, `telegram.tf`, `ses.tf`) are out of scope: the base grants must hold
without them. Every source shape the scanners do not understand fails the tests instead of being
guessed.

A third principal, `telemetry_ingest`, runs the telemetry ingest worker (`workers/`). The user and
its grants exist only behind `var.telemetry_ingress` (`ingestion_queues.tf`). Its writes are the
ones reachable from the worker's compose entrypoint. The same scanner derives them over the modules
the worker imports, and they must be granted on the right table by the attachments that come with
the user.
"""

import ast
import copy
import functools
import os
import re
from collections.abc import Callable, Iterable, Iterator, Mapping
from dataclasses import dataclass, field
from pathlib import Path

import pytest

from tests.unit.test_ingestion_compose import ENTRYPOINT

# The minimal HCL reader of the P3-02 infra tests; `tests/api` imports unit-test helpers the same way.
from tests.unit.test_opentofu_infra import _list_items, _matching_brace, _parse_hcl_body, _unquote

ROOT = Path(__file__).resolve().parents[2]
IAM_RUNTIME = ROOT / "infra" / "opentofu" / "iam_runtime.tf"
API_PACKAGE = ROOT / "src" / "limnopulse_api"
API_EXCLUDED = (API_PACKAGE / "workers",)
API_SETTINGS = API_PACKAGE / "core" / "config.py"

ITEM_ACTIONS = {
    "Put": "dynamodb:PutItem",
    "Update": "dynamodb:UpdateItem",
    "Delete": "dynamodb:DeleteItem",
    "ConditionCheck": "dynamodb:ConditionCheckItem",
}
# boto3 client write methods. Functions that only forward their request parameter to one of them
# (`DynamoV2RepositoryBase.put_item_sync`, its async `put_item`, ...) are derived as writes too.
PY_WRITE_METHODS = {
    "put_item": "dynamodb:PutItem",
    "update_item": "dynamodb:UpdateItem",
    "delete_item": "dynamodb:DeleteItem",
    "batch_write_item": "dynamodb:BatchWriteItem",
}
# PartiQL has no single action, and these access paths hide the operation or the table: each
# must be classified by hand before a test can pass.
PY_UNCLASSIFIED_ACCESS = {
    "execute_statement",
    "batch_execute_statement",
    "execute_transaction",
    "Table",
    "batch_writer",
}
GO_STANDALONE_WRITES = {
    "PutItem": "dynamodb:PutItem",
    "UpdateItem": "dynamodb:UpdateItem",
    "DeleteItem": "dynamodb:DeleteItem",
    "BatchWriteItem": "dynamodb:BatchWriteItem",
    "ExecuteStatement": "unclassified:PartiQL",
    "BatchExecuteStatement": "unclassified:PartiQL",
    "ExecuteTransaction": "unclassified:PartiQL",
}
WRITE_ACTIONS = {
    action.lower()
    for action in (
        *ITEM_ACTIONS.values(),
        *PY_WRITE_METHODS.values(),
        *GO_STANDALONE_WRITES.values(),
    )
}

# Hand inventory (issue #108). The derived sets must contain at least these pairs and sites, so a
# scanner that silently loses a shape cannot pass.
API_FLOOR = {
    ("dynamodb:ConditionCheckItem", "domain"),
    ("dynamodb:DeleteItem", "domain"),
    ("dynamodb:PutItem", "domain"),
    ("dynamodb:UpdateItem", "domain"),
    ("dynamodb:PutItem", "audit"),
}
API_STANDALONE_FLOOR = {
    ("dynamodb:PutItem", "domain"),
    ("dynamodb:UpdateItem", "domain"),
}
WORKERS_FLOOR = {
    ("dynamodb:ConditionCheckItem", "domain"),
    ("dynamodb:PutItem", "domain"),
    ("dynamodb:UpdateItem", "domain"),
}
WORKERS_STANDALONE_FLOOR = {("dynamodb:UpdateItem", "domain")}
# Every function that builds an audit-table item today. Each one must resolve to the audit table.
AUDIT_PUT_SITES = {
    ("src/limnopulse_api/adapters/alert_events.py", "DynamoAlertEventRepository._conditioned_put"),
    ("src/limnopulse_api/adapters/alert_rules.py", "DynamoAlertRuleRepository._conditioned_put"),
    (
        "src/limnopulse_api/adapters/notification_preferences.py",
        "DynamoNotificationPreferenceRepository.save",
    ),
    (
        "src/limnopulse_api/adapters/dynamodb_ingest_credentials.py",
        "DynamoIngestCredentialRepository.create_ingest_credential",
    ),
    (
        "src/limnopulse_api/adapters/dynamodb_ingest_credentials.py",
        "DynamoIngestCredentialRepository.revoke_ingest_credential",
    ),
    (
        "src/limnopulse_api/adapters/dynamodb_integrations.py",
        "DynamoIntegrationRepository.transition_integration_account",
    ),
    (
        "src/limnopulse_api/adapters/dynamodb_integrations.py",
        "DynamoIntegrationRepository.transition_device_integration",
    ),
}
STANDALONE_WRITE_SITES = {
    ("src/limnopulse_api/adapters/dynamodb.py", "DynamoDomainRepository.create_pond"),
    ("src/limnopulse_api/adapters/leads.py", "DynamoLeadRepository.create.write"),
    (
        "src/limnopulse_api/adapters/dynamodb_quarantine.py",
        "DynamoQuarantineRepository.put_quarantine_entries",
    ),
    (
        "src/limnopulse_api/adapters/dynamodb_default_site_asset_migration.py",
        "DynamoDefaultSiteAssetMigrationStore.record_migration_receipt",
    ),
}
# Modules that send transactions built elsewhere: they hold no transaction item of their own.
TRANSACTION_FORWARDERS = {
    "src/limnopulse_api/adapters/dynamodb_v2.py",
    "src/limnopulse_api/adapters/dynamodb_default_site_asset_migration.py",
}

# Least-privilege bounds of the unconditional grants. `REQUIRED` must all be present; anything
# beyond `REQUIRED | LEGACY_INDEX_GRANTS` is rejected. The legacy entries are actions granted on
# `index/*` before issue #108: only Query and Scan are authorized on an index, so they authorize nothing
# and may be dropped, but not added to.
_DOMAIN_READS = {("dynamodb:GetItem", "domain"), ("dynamodb:Query", "domain")}
REQUIRED_DYNAMODB_GRANTS = {
    "api": {
        *_DOMAIN_READS,
        ("dynamodb:Query", "domain/index/*"),
        ("dynamodb:PutItem", "domain"),
        ("dynamodb:UpdateItem", "domain"),
        ("dynamodb:ConditionCheckItem", "domain"),
        ("dynamodb:DeleteItem", "domain"),
        ("dynamodb:PutItem", "audit"),
    },
    "workers": {
        *_DOMAIN_READS,
        ("dynamodb:Query", "domain/index/*"),
        ("dynamodb:UpdateItem", "domain"),
        ("dynamodb:ConditionCheckItem", "domain"),
        ("dynamodb:PutItem", "domain"),
    },
}
LEGACY_INDEX_GRANTS = {
    "api": {
        ("dynamodb:GetItem", "domain/index/*"),
        ("dynamodb:PutItem", "domain/index/*"),
        ("dynamodb:UpdateItem", "domain/index/*"),
    },
    "workers": {
        ("dynamodb:GetItem", "domain/index/*"),
        ("dynamodb:UpdateItem", "domain/index/*"),
    },
}
# Flag-gated attachments in iam_runtime.tf: (resource, user, policy). Their policies live in other
# files and are out of scope here, so no new gated attachment may slip a DynamoDB grant in.
GATED_ATTACHMENTS = {
    ("api_telegram_webhook_secret", "api", "telegram_webhook_secret_reader"),
    ("workers_telegram", "workers", "telegram_worker"),
    ("workers_email", "workers", "email_worker"),
}
IAM_RUNTIME_BLOCK_KINDS = {
    ("resource", "aws_iam_user"),
    ("resource", "aws_iam_policy"),
    ("resource", "aws_iam_user_policy_attachment"),
    ("data", "aws_iam_policy_document"),
}

_RESOURCES = {
    "aws_dynamodb_table.domain.arn": "domain",
    '"${aws_dynamodb_table.domain.arn}/index/*"': "domain/index/*",
    "aws_dynamodb_table.audit.arn": "audit",
}
_TABLES = {"domain", "audit"}


def _norm(pairs: set[tuple[str, str]]) -> set[tuple[str, str]]:
    """IAM action names are case-insensitive."""
    return {(action.lower(), resource) for action, resource in pairs}


# --- Python: transaction items and standalone writes -------------------------------------------

_DOMAIN_TABLE_EXPRESSIONS = {"self.table_name", "self.domain_table_name"}
_AUDIT_TABLE_EXPRESSIONS = {"self.audit_table_name"}
_TABLE_ATTRIBUTES = {"table_name", "domain_table_name", "audit_table_name"}
_MAX_DEPTH = 25


@dataclass(eq=False)
class _Function:
    module: str
    node: ast.FunctionDef | ast.AsyncFunctionDef
    parent: "_Function | None"
    is_method: bool
    qualname: str

    def positional(self) -> list[str]:
        return [arg.arg for arg in [*self.node.args.posonlyargs, *self.node.args.args]]

    def params(self) -> set[str]:
        return {*self.positional(), *(arg.arg for arg in self.node.args.kwonlyargs)}


@dataclass(eq=False)
class _Scope:
    function: _Function | None
    bindings: dict[str, tuple[ast.expr, "_Scope"]] = field(default_factory=dict)


@dataclass(frozen=True)
class _Write:
    site: str
    operation: str
    action: str
    tables: frozenset[str]
    standalone: bool


def _api_modules() -> list[Path]:
    return sorted(
        path
        for path in API_PACKAGE.rglob("*.py")
        if not any(path.is_relative_to(excluded) for excluded in API_EXCLUDED)
    )


def _own_nodes(function: ast.AST) -> Iterator[ast.AST]:
    """Nodes of a function body, without descending into nested functions or classes."""
    pending = list(ast.iter_child_nodes(function))
    while pending:
        node = pending.pop()
        yield node
        if not isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef, ast.Lambda)):
            pending.extend(ast.iter_child_nodes(node))


def _name_of(expr: ast.expr) -> str | None:
    if isinstance(expr, ast.Attribute):
        return expr.attr
    if isinstance(expr, ast.Name):
        return expr.id
    return None


def _is_constant_value(expr: ast.expr) -> bool:
    return isinstance(expr, ast.Constant)


class _PythonSources:
    """An index of the API sources and a small resolver for the table each request names.

    A request's table is the value of its `TableName` key, followed through `**` spreads,
    subscripts, helper calls (arguments bound to parameters), local assignments and, for an
    unbound parameter, every call site of the function. Any shape the resolver does not know
    fails the test instead of guessing.
    """

    def __init__(self, sources: list[tuple[str, str]]) -> None:
        self.functions: dict[str, list[_Function]] = {}
        self.calls: dict[str, list[tuple[ast.Call, _Function | None]]] = {}
        self.all_calls: list[tuple[ast.Call, _Function | None, str]] = []
        self.items: list[tuple[str, ast.expr, _Function | None, str]] = []
        self.table_mutations: list[str] = []
        self.stray_operations: list[str] = []
        self.unclassified_access: list[str] = []
        self.table_assignments: list[tuple[str, str, _Function | None, str]] = []
        for module, text in sources:
            tree = ast.parse(text)
            item_keys: set[int] = set()
            self._walk(tree, module, None, None, item_keys)
            self._find_strays(tree, module, item_keys)
            self._find_write_values(tree, module)

    @classmethod
    def from_paths(cls, paths: list[Path]) -> "_PythonSources":
        return cls(
            [
                (path.relative_to(ROOT).as_posix(), path.read_text(encoding="utf-8"))
                for path in paths
            ]
        )

    def _walk(
        self,
        node: ast.AST,
        module: str,
        function: _Function | None,
        cls: str | None,
        item_keys: set[int],
    ) -> None:
        for child in ast.iter_child_nodes(node):
            if isinstance(child, ast.ClassDef):
                self._walk(child, module, function, child.name, item_keys)
                continue
            if isinstance(child, (ast.FunctionDef, ast.AsyncFunctionDef)):
                if child.name in _TABLE_ATTRIBUTES:  # e.g. a `table_name` property
                    self.table_mutations.append(f"{module}:{child.lineno}")
                prefix = function.qualname if function else cls
                qualname = f"{prefix}.{child.name}" if prefix else child.name
                defined = _Function(
                    module, child, function, function is None and bool(cls), qualname
                )
                self.functions.setdefault(child.name, []).append(defined)
                self._walk(child, module, defined, None, item_keys)
                continue
            self._record(child, module, function, item_keys)
            self._walk(child, module, function, cls, item_keys)

    def _record(
        self, node: ast.AST, module: str, function: _Function | None, item_keys: set[int]
    ) -> None:
        where = f"{module}:{getattr(node, 'lineno', '?')}"
        if isinstance(node, ast.Dict):
            for key, value in zip(node.keys, node.values, strict=True):
                if (
                    key is not None
                    and not isinstance(key, ast.Constant)
                    and self._names_table(value)
                ):
                    # `{op: {"TableName": ...}}`: an item whose operation the scan cannot name.
                    self.stray_operations.append(f"{where}: [{ast.unparse(key)}]")
            operations = [
                (key, value)
                for key, value in zip(node.keys, node.values, strict=True)
                if isinstance(key, ast.Constant) and key.value in ITEM_ACTIONS
            ]
            for key, value in operations:
                item_keys.add(id(key))
                if _is_constant_value(value):
                    continue  # `{"Update": "n/a"}` cannot be a transaction item
                assert len(node.keys) == 1, f"{where}: a transaction item has exactly one key"
                self.items.append((key.value, value, function, where))
        elif isinstance(node, ast.Call):
            name = _name_of(node.func)
            if name:
                self.calls.setdefault(name, []).append((node, function))
            self.all_calls.append((node, function, where))
            if name in PY_UNCLASSIFIED_ACCESS or (
                name == "resource" and "boto3" in ast.unparse(node.func)
            ):
                self.unclassified_access.append(f"{where}: {ast.unparse(node.func)}")
            if name == "getattr" and (
                any(
                    isinstance(arg, ast.Constant)
                    and arg.value
                    in {*PY_WRITE_METHODS, *PY_UNCLASSIFIED_ACCESS, "transact_write_items"}
                    for arg in node.args
                )
                or (
                    len(node.args) > 1
                    and not isinstance(node.args[1], ast.Constant)
                    and "client" in ast.unparse(node.args[0])
                )
            ):
                self.unclassified_access.append(f"{where}: {ast.unparse(node)}")
            if (
                name == "setattr"
                and len(node.args) > 1
                and not (
                    isinstance(node.args[1], ast.Constant)
                    and node.args[1].value not in _TABLE_ATTRIBUTES
                )
            ):
                self.table_mutations.append(where)
            if name in {"update", "setdefault"} and self._sets_table_name(node):
                self.table_mutations.append(where)
        if isinstance(node, (ast.Assign, ast.AugAssign, ast.AnnAssign)):
            targets = node.targets if isinstance(node, ast.Assign) else [node.target]
            for target in targets:
                if (
                    isinstance(target, ast.Subscript)
                    and isinstance(target.slice, ast.Constant)
                    and target.slice.value == "TableName"
                ):
                    self.table_mutations.append(where)
                if (
                    function is None
                    and isinstance(target, ast.Name)
                    and target.id in _TABLE_ATTRIBUTES
                ):
                    self.table_mutations.append(where)  # a class-level or module-level table
                if (
                    isinstance(target, ast.Attribute)
                    and ast.unparse(target)
                    in {*_DOMAIN_TABLE_EXPRESSIONS, *_AUDIT_TABLE_EXPRESSIONS}
                    and node.value is not None
                ):
                    self.table_assignments.append(
                        (target.attr, ast.unparse(node.value), function, where)
                    )
            if isinstance(node, ast.AugAssign) and self._names_table(node.value):
                self.table_mutations.append(where)

    @staticmethod
    def _names_table(expr: ast.expr) -> bool:
        return isinstance(expr, ast.Dict) and any(
            isinstance(key, ast.Constant) and key.value == "TableName" for key in expr.keys
        )

    def _sets_table_name(self, call: ast.Call) -> bool:
        if any(keyword.arg == "TableName" for keyword in call.keywords):
            return True
        if call.args and isinstance(call.args[0], ast.Constant):
            return call.args[0].value == "TableName"
        return any(self._names_table(arg) for arg in call.args)

    def _find_strays(self, tree: ast.AST, module: str, item_keys: set[int]) -> None:
        """Operation names outside an item key or a subscript read: items the scan cannot see."""
        allowed = set(item_keys)
        for node in ast.walk(tree):
            if (
                isinstance(node, ast.Subscript)
                and isinstance(node.ctx, ast.Load)
                and isinstance(node.slice, ast.Constant)
            ):
                allowed.add(id(node.slice))
        for node in ast.walk(tree):
            if isinstance(node, ast.Constant) and node.value in ITEM_ACTIONS:
                if id(node) not in allowed:
                    self.stray_operations.append(f"{module}:{node.lineno}: {node.value!r}")
            elif isinstance(node, ast.keyword) and node.arg in ITEM_ACTIONS:
                self.stray_operations.append(f"{module}:{node.value.lineno}: {node.arg}=")

    def _find_write_values(self, tree: ast.AST, module: str) -> None:
        """A write method used other than as a call or a positional call argument."""
        allowed: set[int] = set()
        for node in ast.walk(tree):
            if isinstance(node, ast.Call):
                allowed.add(id(node.func))
                allowed.update(id(arg) for arg in node.args)
        for node in ast.walk(tree):
            if (
                isinstance(node, ast.Attribute)
                and node.attr in {*PY_WRITE_METHODS, *PY_UNCLASSIFIED_ACCESS}
                and id(node) not in allowed
            ):
                self.unclassified_access.append(f"{module}:{node.lineno}: {ast.unparse(node)}")

    # Resolution -------------------------------------------------------------------------------

    def lookup(self, name: str, module: str | None) -> list[_Function]:
        candidates = self.functions.get(name, [])
        local = [function for function in candidates if function.module == module]
        return local or candidates

    def bind(self, function: _Function, call: ast.Call, caller: _Scope) -> _Scope:
        positional = function.positional()
        if function.is_method and isinstance(call.func, ast.Attribute):
            positional = positional[1:]
        bindings: dict[str, tuple[ast.expr, _Scope]] = {}
        for name, argument in zip(positional, call.args, strict=False):
            assert not isinstance(argument, ast.Starred), ast.unparse(call)
            bindings[name] = (argument, caller)
        for keyword in call.keywords:
            assert keyword.arg is not None, ast.unparse(call)
            bindings[keyword.arg] = (keyword.value, caller)
        return _Scope(function, bindings)

    def request_tables(self, expr: ast.expr, scope: _Scope, depth: int = 0) -> set[str]:
        """The tables named by the request dict that `expr` evaluates to."""
        self._guard(expr, depth)
        if isinstance(expr, ast.Dict):
            table: ast.expr | None = None
            spreads: list[ast.expr] = []
            for key, value in zip(expr.keys, expr.values, strict=True):
                if key is None:
                    # A later spread may override an explicit TableName: not modelled.
                    assert table is None, f"a spread after TableName: {ast.unparse(expr)}"
                    spreads.append(value)
                elif isinstance(key, ast.Constant) and key.value == "TableName":
                    table = value
            if table is not None:
                return self.table(table, scope, depth + 1)
            assert len(spreads) == 1, f"cannot resolve the request {ast.unparse(expr)}"
            return self.request_tables(spreads[0], scope, depth + 1)
        if isinstance(expr, ast.Subscript) and isinstance(expr.slice, ast.Constant):
            return self.member_tables(expr.value, expr.slice.value, scope, depth + 1)
        if isinstance(expr, ast.Call):
            return self._returns(expr, scope, depth, self.request_tables)
        if isinstance(expr, ast.Name):
            return self._name(expr.id, scope, depth, self.request_tables)
        if isinstance(expr, ast.IfExp):
            return self.request_tables(expr.body, scope, depth + 1) | self.request_tables(
                expr.orelse, scope, depth + 1
            )
        raise AssertionError(f"cannot resolve the request {ast.unparse(expr)}")

    def member_tables(self, expr: ast.expr, key: object, scope: _Scope, depth: int) -> set[str]:
        """The tables of the request stored under `key` of the dict that `expr` evaluates to."""
        self._guard(expr, depth)

        def member(value: ast.expr, inner: _Scope, level: int) -> set[str]:
            return self.member_tables(value, key, inner, level)

        if isinstance(expr, ast.Dict):
            values = [
                value
                for item_key, value in zip(expr.keys, expr.values, strict=True)
                if isinstance(item_key, ast.Constant) and item_key.value == key
            ]
            assert values, f"no {key!r} in {ast.unparse(expr)}"
            return self.request_tables(values[-1], scope, depth + 1)
        if isinstance(expr, ast.Call):
            return self._returns(expr, scope, depth, member)
        if isinstance(expr, ast.Name):
            return self._name(expr.id, scope, depth, member)
        raise AssertionError(f"cannot resolve {ast.unparse(expr)}[{key!r}]")

    def table(self, expr: ast.expr, scope: _Scope, depth: int = 0) -> set[str]:
        """The table kind that a `TableName` expression evaluates to."""
        self._guard(expr, depth)
        if isinstance(expr, ast.IfExp):
            return self.table(expr.body, scope, depth + 1) | self.table(
                expr.orelse, scope, depth + 1
            )
        text = ast.unparse(expr)
        if text in _AUDIT_TABLE_EXPRESSIONS:
            return {"audit"}
        if text in _DOMAIN_TABLE_EXPRESSIONS:
            return {"domain"}
        if isinstance(expr, ast.Name):
            return self._name(expr.id, scope, depth, self.table)
        if isinstance(expr, ast.Call):
            return self._returns(expr, scope, depth, self.table)
        raise AssertionError(f"unknown table expression {text}")

    def _guard(self, expr: ast.expr, depth: int) -> None:
        assert depth <= _MAX_DEPTH, f"resolution too deep at {ast.unparse(expr)}"

    def _name(
        self,
        name: str,
        scope: _Scope,
        depth: int,
        resolve: Callable[[ast.expr, _Scope, int], set[str]],
    ) -> set[str]:
        current: _Scope | None = scope
        while current is not None:
            if name in current.bindings:
                expr, caller = current.bindings[name]
                return resolve(expr, caller, depth + 1)
            function = current.function
            if function is None:
                break
            assigned = [
                node.value
                for node in _own_nodes(function.node)
                if (
                    isinstance(node, ast.Assign)
                    and any(isinstance(t, ast.Name) and t.id == name for t in node.targets)
                )
                or (
                    isinstance(node, (ast.AnnAssign, ast.NamedExpr))
                    and isinstance(node.target, ast.Name)
                    and node.target.id == name
                    and node.value is not None
                )
            ]
            if name in function.params():
                # A rebound parameter holds either the caller's value or the new one.
                result = self._call_sites(function, name, depth, resolve)
                return result.union(*(resolve(value, current, depth + 1) for value in assigned))
            if assigned:
                return set().union(*(resolve(value, current, depth + 1) for value in assigned))
            current = _Scope(function.parent) if function.parent else None
        raise AssertionError(f"cannot resolve the name {name}")

    def _returns(
        self,
        call: ast.Call,
        scope: _Scope,
        depth: int,
        resolve: Callable[[ast.expr, _Scope, int], set[str]],
    ) -> set[str]:
        name = _name_of(call.func)
        module = scope.function.module if scope.function else None
        candidates = self.lookup(name, module) if name else []
        assert candidates, f"cannot resolve the call {ast.unparse(call)}"
        result: set[str] = set()
        for function in candidates:
            returns = [
                node.value
                for node in _own_nodes(function.node)
                if isinstance(node, ast.Return) and node.value is not None
            ]
            assert returns, f"{function.qualname} returns nothing"
            bound = self.bind(function, call, scope)
            for value in returns:
                result |= resolve(value, bound, depth + 1)
        return result

    def _call_sites(
        self,
        function: _Function,
        param: str,
        depth: int,
        resolve: Callable[[ast.expr, _Scope, int], set[str]],
    ) -> set[str]:
        result: set[str] = set()
        for call, caller in self.calls.get(function.node.name, []):
            caller_module = caller.module if caller else None
            if not any(f is function for f in self.lookup(function.node.name, caller_module)):
                continue
            bound = self.bind(function, call, _Scope(caller))
            assert param in bound.bindings, f"{ast.unparse(call)} leaves {param} unbound"
            expr, caller_scope = bound.bindings[param]
            result |= resolve(expr, caller_scope, depth + 1)
        assert result, f"no call site binds {function.qualname}({param})"
        return result

    # Writes -----------------------------------------------------------------------------------

    def _write_parts(
        self, call: ast.Call, methods: dict[str, str]
    ) -> Iterator[tuple[str, list[ast.expr], ast.expr | None]]:
        """(method, request expressions, TableName keyword) of each write that `call` makes."""
        name = _name_of(call.func)
        if name in methods:
            requests = [*call.args, *(kw.value for kw in call.keywords if kw.arg is None)]
            table = next((kw.value for kw in call.keywords if kw.arg == "TableName"), None)
            yield name, requests, table
        for argument in call.args:
            reference = _name_of(argument)
            if reference in methods and isinstance(argument, (ast.Attribute, ast.Name)):
                others = [arg for arg in call.args if arg is not argument]
                yield reference, [*others, *(kw.value for kw in call.keywords)], None

    def _write_sites(
        self,
    ) -> list[tuple[str, str, list[ast.expr], ast.expr | None, _Function, str]]:
        methods = dict(PY_WRITE_METHODS)
        while True:
            sites = []
            forwarders: dict[str, str] = {}
            for call, function, where in self.all_calls:
                for method, requests, table in self._write_parts(call, methods):
                    assert function is not None, f"{where}: a write outside a function"
                    forwards = (
                        table is None
                        and requests
                        and all(
                            isinstance(r, ast.Name)
                            and r.id in function.params()
                            and not self._rebinds(function, r.id)
                            for r in requests
                        )
                    )
                    if forwards:
                        forwarders[function.node.name] = methods[method]
                    else:
                        sites.append((method, methods[method], requests, table, function, where))
            for name, action in forwarders.items():
                assert methods.get(name, action) == action, f"{name} forwards two actions"
            if set(forwarders) <= set(methods):
                return sites
            methods.update(forwarders)

    @staticmethod
    def _rebinds(function: _Function, name: str) -> bool:
        for node in _own_nodes(function.node):
            targets = (
                node.targets
                if isinstance(node, ast.Assign)
                else [node.target]
                if isinstance(node, (ast.AnnAssign, ast.AugAssign, ast.NamedExpr))
                else []
            )
            if any(isinstance(target, ast.Name) and target.id == name for target in targets):
                return True
        return False

    def writes(self, reached: set[_Function] | None = None) -> list[_Write]:
        """Every derived write, or with `reached` only those built in one of these functions.

        Writes outside `reached` are skipped before resolution: their call sites may lie outside
        the sources.
        """
        writes: list[_Write] = []
        for operation, value, function, where in self.items:
            assert function is not None, f"{where}: a transaction item outside a function"
            if reached is not None and function not in reached:
                continue
            tables = self.request_tables(value, _Scope(function))
            site = f"{function.module}::{function.qualname}"
            writes.append(
                _Write(site, operation, ITEM_ACTIONS[operation], frozenset(tables), False)
            )
        for method, action, requests, table, function, where in self._write_sites():
            if reached is not None and function not in reached:
                continue
            scope = _Scope(function)
            if table is not None:
                tables = self.table(table, scope)
            else:
                assert requests, f"{where}: cannot resolve the table of {method}"
                tables = set().union(*(self.request_tables(r, scope) for r in requests))
            site = f"{function.module}::{function.qualname}"
            writes.append(_Write(site, method, action, frozenset(tables), True))
        return writes


@functools.cache
def _api_sources() -> _PythonSources:
    return _PythonSources.from_paths(_api_modules())


@functools.cache
def _api_writes() -> tuple[_Write, ...]:
    return tuple(_api_sources().writes())


# --- Go: transaction items and standalone writes -----------------------------------------------

_GO_TYPES = "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
_GO_DYNAMODB = "github.com/aws/aws-sdk-go-v2/service/dynamodb"
_GO_TABLE_NAMES = {"aws.String(store.Table)", "&store.Table", "mutation.TableName"}
# Values of `Table:` fields, captured up to the first comma.
_GO_TABLE_SOURCES = {"config.DynamoDBTable", "table", 'envOr("DYNAMODB_DOMAIN_TABLE"'}


def _go_files() -> list[Path]:
    files: list[Path] = []
    for directory, subdirectories, names in os.walk(ROOT):
        subdirectories[:] = [
            name
            for name in subdirectories
            if not name.startswith(".") and name not in {"node_modules", "vendor"}
        ]
        files.extend(
            Path(directory) / name
            for name in names
            if name.endswith(".go") and not name.endswith("_test.go")
        )
    return sorted(files)


def _strip_go_comments(text: str, *, blank_strings: bool = False) -> str:
    """Drop // and /* */ comments outside string, rune and raw-string literals; keep newlines.

    With `blank_strings`, literal contents are dropped too, so only code remains.
    """
    kept: list[str] = []
    index = 0
    while index < len(text):
        char = text[index]
        if char in "\"'`":
            if char == "`":
                end = text.index("`", index + 1)
            else:
                end = index + 1
                while text[end] != char:
                    end += 2 if text[end] == "\\" else 1
            literal = text[index : end + 1]
            if blank_strings:
                literal = char + "\n" * literal.count("\n") + char
            kept.append(literal)
            index = end + 1
        elif text.startswith("//", index):
            newline = text.find("\n", index)
            index = len(text) if newline == -1 else newline
        elif text.startswith("/*", index):
            end = text.index("*/", index + 2)
            kept.append("\n" * text.count("\n", index, end))
            index = end + 2
        else:
            kept.append(char)
            index += 1
    return "".join(kept)


@functools.cache
def _go_sources() -> tuple[tuple[str, str], ...]:
    return tuple(
        (path.relative_to(ROOT).as_posix(), _strip_go_comments(path.read_text(encoding="utf-8")))
        for path in _go_files()
    )


def _go_alias(text: str, import_path: str) -> str | None:
    match = re.search(
        rf'^\s*(?:import\s+)?([\w.]+\s+)?"{re.escape(import_path)}"', text, re.MULTILINE
    )
    if match is None:
        return None
    alias = (match.group(1) or import_path.rsplit("/", 1)[1]).strip()
    assert alias != ".", f"dot import of {import_path}"
    return alias


def _go_writes_in(relative: str, text: str) -> list[_Write]:
    """Every mention of a transaction item type or a write input type, all on the domain table.

    `text` has its comments stripped; the mentions are searched in code only, outside literals.
    """
    writes: list[_Write] = []
    imports = text
    text = _strip_go_comments(text, blank_strings=True)
    types_alias = _go_alias(imports, _GO_TYPES)
    if types_alias:
        pattern = rf"\b{re.escape(types_alias)}\.(Put|Update|Delete|ConditionCheck)\b"
        for match in re.finditer(pattern, text):
            line = text.count("\n", 0, match.start()) + 1
            operation = match.group(1)
            writes.append(
                _Write(
                    f"{relative}:{line}",
                    operation,
                    ITEM_ACTIONS[operation],
                    frozenset({"domain"}),
                    False,
                )
            )
    dynamodb_alias = _go_alias(imports, _GO_DYNAMODB)
    if dynamodb_alias:
        names = "|".join(GO_STANDALONE_WRITES)
        pattern = rf"\b{re.escape(dynamodb_alias)}\.({names})Input\b"
        for match in re.finditer(pattern, text):
            line = text.count("\n", 0, match.start()) + 1
            operation = match.group(1)
            writes.append(
                _Write(
                    f"{relative}:{line}",
                    operation,
                    GO_STANDALONE_WRITES[operation],
                    frozenset({"domain"}),
                    True,
                )
            )
    return writes


@functools.cache
def _go_writes() -> tuple[_Write, ...]:
    return tuple(write for path, text in _go_sources() for write in _go_writes_in(path, text))


# --- iam_runtime.tf ----------------------------------------------------------------------------


def _iam_blocks() -> list:
    return _parse_hcl_body(IAM_RUNTIME.read_text(encoding="utf-8"))[1]


def _statement_grants(document) -> set[tuple[str, str]]:
    """The DynamoDB grants of a policy document; other statements must not touch DynamoDB."""
    assert set(document.attrs) <= {"version"}, document.labels
    assert {block.kind for block in document.blocks} == {"statement"}, document.labels
    grants: set[tuple[str, str]] = set()
    for statement in document.nested("statement"):
        actions = [_unquote(item) for item in _list_items(statement.attrs["actions"])]
        if not any(action.lower().startswith("dynamodb:") for action in actions):
            scope = [statement.attrs["actions"], statement.attrs.get("resources", "")]
            assert "dynamodb" not in repr(scope).lower(), statement.attrs
            continue
        assert set(statement.attrs) <= {"sid", "effect", "actions", "resources"}, statement.attrs
        assert statement.blocks == [], f"{document.labels}: conditions are not modelled here"
        assert statement.attrs.get("effect", '"Allow"') == '"Allow"'
        for resource in _list_items(statement.attrs["resources"]):
            assert resource in _RESOURCES, f"unexpected resource {resource}"
            for action in actions:
                assert action.lower().startswith("dynamodb:"), f"mixed statement: {action}"
                assert "*" not in action, action
                grants.add((action.lower(), _RESOURCES[resource]))
    return grants


@functools.cache
def _attachments() -> tuple[dict[str, set[tuple[str, str]]], frozenset[tuple[str, str, str]]]:
    """Unconditional DynamoDB grants per IAM user, and the flag-gated attachments."""
    blocks = _iam_blocks()
    kinds = {(block.kind, block.labels[0]) for block in blocks}
    assert kinds <= IAM_RUNTIME_BLOCK_KINDS, kinds - IAM_RUNTIME_BLOCK_KINDS
    documents = {
        block.labels[1]: block
        for block in blocks
        if block.kind == "data" and block.labels[0] == "aws_iam_policy_document"
    }
    policies = {
        block.labels[1]: block
        for block in blocks
        if block.kind == "resource" and block.labels[0] == "aws_iam_policy"
    }
    grants: dict[str, set[tuple[str, str]]] = {}
    gated: set[tuple[str, str, str]] = set()
    for block in blocks:
        if block.kind != "resource" or block.labels[0] != "aws_iam_user_policy_attachment":
            continue
        user = re.fullmatch(r"aws_iam_user\.(\w+)\.name", block.attrs["user"])
        policy = re.fullmatch(r"aws_iam_policy\.(\w+)(\[0\])?\.arn", block.attrs["policy_arn"])
        assert user is not None and policy is not None, block.labels
        assert "for_each" not in block.attrs, block.labels
        if "count" in block.attrs:
            # Flag-gated: the base grants must not depend on an optional feature.
            assert policy.group(2) == "[0]", block.labels
            gated.add((block.labels[1], user.group(1), policy.group(1)))
            continue
        document = re.fullmatch(
            r"data\.aws_iam_policy_document\.(\w+)\.json", policies[policy.group(1)].attrs["policy"]
        )
        assert document is not None, policy.group(1)
        grants.setdefault(user.group(1), set()).update(
            _statement_grants(documents[document.group(1)])
        )
    return grants, frozenset(gated)


def _runtime_grants() -> dict[str, set[tuple[str, str]]]:
    return _attachments()[0]


def _required(writes: tuple[_Write, ...] | list[_Write]) -> set[tuple[str, str]]:
    return {(write.action, table) for write in writes for table in write.tables}


def _table_grants(grants: set[tuple[str, str]]) -> set[tuple[str, str]]:
    return {(action, resource) for action, resource in grants if resource in _TABLES}


def _uncovered(writes: tuple[_Write, ...], granted: set[tuple[str, str]]) -> list[str]:
    return sorted(
        f"{write.site} {write.operation} -> {write.action} on {table}"
        for write in writes
        for table in write.tables
        if (write.action.lower(), table) not in granted
    )


# --- Tests: grants cover every derived write ---------------------------------------------------


def test_api_policy_grants_every_transaction_item_and_write_on_its_table() -> None:
    granted = _table_grants(_runtime_grants()["api"])

    assert _uncovered(_api_writes(), granted) == []


def test_workers_policy_grants_every_transaction_item_and_write_on_its_table() -> None:
    granted = _table_grants(_runtime_grants()["workers"])

    assert _uncovered(_go_writes(), granted) == []


def test_unconditional_dynamodb_grants_stay_within_least_privilege_bounds() -> None:
    grants, gated = _attachments()

    assert gated == GATED_ATTACHMENTS
    for user, required in REQUIRED_DYNAMODB_GRANTS.items():
        actual = grants[user]
        # IAM has no dynamodb:TransactWriteItems action: granting it authorizes nothing.
        assert not {a for a, _ in actual if a.startswith("dynamodb:transact")}, user
        missing = _norm(required) - actual
        extra = actual - _norm(required | LEGACY_INDEX_GRANTS[user])
        assert (missing, extra) == (set(), set()), user


def test_runtime_write_grants_are_all_used_by_a_derived_write() -> None:
    grants = _runtime_grants()
    derived = {"api": _required(_api_writes()), "workers": _required(_go_writes())}

    for user, required in derived.items():
        writes = {grant for grant in _table_grants(grants[user]) if grant[0] in WRITE_ACTIONS}
        assert writes <= _norm(required), user


# --- Tests: the derivation sees what it should -------------------------------------------------


def test_derived_inventory_contains_the_known_writes() -> None:
    api_writes = _api_writes()
    go_writes = _go_writes()

    assert _required(api_writes) >= API_FLOOR
    assert _required([w for w in api_writes if w.standalone]) >= API_STANDALONE_FLOOR
    assert _required(go_writes) >= WORKERS_FLOOR
    assert _required([w for w in go_writes if w.standalone]) >= WORKERS_STANDALONE_FLOOR
    sites = {tuple(write.site.split("::")) for write in api_writes if write.standalone}
    assert sites >= STANDALONE_WRITE_SITES
    audit_sites = {
        tuple(write.site.split("::"))
        for write in api_writes
        if "audit" in write.tables and write.operation == "Put"
    }
    assert audit_sites >= AUDIT_PUT_SITES
    # The audit table is append-only: no Update, Delete or ConditionCheck item and no standalone
    # write names it.
    assert {(w.action, w.standalone) for w in api_writes if "audit" in w.tables} == {
        ("dynamodb:PutItem", False)
    }


def test_every_transaction_module_and_package_yields_a_derived_item() -> None:
    sources = _api_sources()
    with_items = {write.site.split("::")[0] for write in _api_writes() if not write.standalone}
    for path in _api_modules():
        module = path.relative_to(ROOT).as_posix()
        text = path.read_text(encoding="utf-8")
        if "transact_write_items" in text or "TransactItems" in text:
            assert module in with_items | TRANSACTION_FORWARDERS, module
    assert TRANSACTION_FORWARDERS.isdisjoint(with_items)
    # A bare "Put"/"Update"/"Delete"/"ConditionCheck" literal outside an item key may build an
    # item the scan cannot see; a non-item use needs another spelling.
    assert sources.stray_operations == []
    assert sources.unclassified_access == []

    go_packages = {write.site.rsplit("/", 1)[0] for write in _go_writes() if not write.standalone}
    transacting = {
        path.rsplit("/", 1)[0] for path, text in _go_sources() if ".TransactWriteItems(" in text
    }
    assert transacting
    assert transacting <= go_packages


# --- Tests: assumptions behind the table resolution --------------------------------------------


def test_api_tables_come_only_from_the_domain_and_audit_settings() -> None:
    sources = _api_sources()
    assert sources.table_mutations == []

    settings = next(
        node
        for node in ast.walk(ast.parse(API_SETTINGS.read_text(encoding="utf-8")))
        if isinstance(node, ast.ClassDef) and node.name == "Settings"
    )
    fields = {
        node.target.id
        for node in settings.body
        if isinstance(node, ast.AnnAssign) and isinstance(node.target, ast.Name)
    }
    assert {name for name in fields if "table" in name.lower()} == {
        "dynamodb_domain_table",
        "dynamodb_audit_table",
    }
    for path in _api_modules():
        text = path.read_text(encoding="utf-8")
        assert set(re.findall(r"\bdynamodb_\w+_table\b", text)) <= fields, path
        if path != API_SETTINGS:
            literals = {
                node.value for node in ast.walk(ast.parse(text)) if isinstance(node, ast.Constant)
            }
            assert "LimnopulseAudit" not in literals, path

    # `self.table_name` / `self.domain_table_name` / `self.audit_table_name` are only ever set
    # from the constructor parameter of the same name.
    assert sources.table_assignments
    for attribute, value, function, where in sources.table_assignments:
        assert value == attribute, where
        assert function is not None and attribute in function.params(), where

    # The audit table setting, directly or through a name it is assigned to, only ever reaches
    # an `audit_table_name` keyword.
    passed = 0
    for path in _api_modules():
        tree = ast.parse(path.read_text(encoding="utf-8"))
        tainted = {
            name
            for node in ast.walk(tree)
            if isinstance(node, (ast.Assign, ast.AnnAssign))
            and node.value is not None
            and "dynamodb_audit_table" in ast.unparse(node.value)
            for target in (node.targets if isinstance(node, ast.Assign) else [node.target])
            if (name := _name_of(target))
        }

        def carries_audit(expr: ast.expr, tainted: set[str] = tainted) -> bool:
            return "dynamodb_audit_table" in ast.unparse(expr) or any(
                isinstance(node, (ast.Name, ast.Attribute)) and _name_of(node) in tainted
                for node in ast.walk(expr)
            )

        for call in (node for node in ast.walk(tree) if isinstance(node, ast.Call)):
            for argument in call.args:
                assert not carries_audit(argument), (path, ast.unparse(call))
            for keyword in call.keywords:
                if carries_audit(keyword.value):
                    assert keyword.arg == "audit_table_name", (path, ast.unparse(call))
                    passed += 1
    assert passed


def test_go_workers_only_reach_the_domain_table() -> None:
    names: set[str] = set()
    sources: set[str] = set()
    for path, text in _go_sources():
        assert re.search(r"audit[_-]?table|limnopulseaudit", text, re.IGNORECASE) is None, path
        assert set(re.findall(r'"(DYNAMODB_\w*TABLE\w*)"', text)) <= {"DYNAMODB_DOMAIN_TABLE"}
        names |= {m.strip() for m in re.findall(r"\bTableName\s*(?::|=[^=])\s*([^,}\n]+)", text)}
        sources |= {m.strip() for m in re.findall(r"\bTable\s*(?::|=[^=])\s*([^,}\n]+)", text)}
        for binding in re.findall(r'"(\w+)"\s*:\s*&config\.DynamoDBTable\b', text):
            assert binding == "DYNAMODB_DOMAIN_TABLE", path
        for default in re.findall(r"\bDynamoDBTable\s*:\s*([^,}\n]+)", text):
            assert default.strip() == '"LimnopulseDomain"', path
        if re.search(r"\bTable\s*:\s*table\b", text):
            for assigned in re.findall(r"\btable\s*:?=\s*([^\n]+)", text):
                assert "DYNAMODB_DOMAIN_TABLE" in assigned, path

    assert names
    assert names <= _GO_TABLE_NAMES
    assert sources
    assert sources <= _GO_TABLE_SOURCES


# --- Tests: the scanners on fixtures -----------------------------------------------------------


def _fixture(source: str) -> _PythonSources:
    return _PythonSources([("fixture.py", source)])


def _fixture_tables(source: str) -> list[tuple[str, set[str]]]:
    return [(write.operation, set(write.tables)) for write in _fixture(source).writes()]


def test_python_resolver_follows_helpers_and_fails_closed() -> None:
    helpers = """
class Repo:
    def _put(self, table_name, item):
        return {"Put": {"TableName": table_name, "Item": item}}
    def _audit(self, item):
        return {**self._base(item), "TableName": self.audit_table_name}
    def _base(self, item):
        request = {"TableName": self.table_name, "Item": item}
        return request
    def _check(self, key, audit=False):
        if audit:
            return {"TableName": self.audit_table_name, "Key": key}
        return {"TableName": self.domain_table_name, "Key": key}
    def write(self, item, flag):
        self._put(self.domain_table_name, item)
        self._put(table_name=self.audit_table_name, item=item)
        chosen = self.audit_table_name if flag else self.table_name
        def nested():
            return {"TableName": chosen}
        return [
            {"Update": self._base(item)},
            {"Put": self._audit(item)},
            {"ConditionCheck": {**self._base(item), "ConditionExpression": "x"}},
            {"Delete": self._check(item)},
            {"ConditionCheck": nested()},
            {"Update": "not an item"},
        ]
"""
    assert sorted(_fixture_tables(helpers), key=repr) == sorted(
        [
            ("Put", {"domain", "audit"}),
            ("Update", {"domain"}),
            ("Put", {"audit"}),
            ("ConditionCheck", {"domain"}),
            ("Delete", {"domain", "audit"}),
            ("ConditionCheck", {"domain", "audit"}),
        ],
        key=repr,
    )

    unknown = """
class Repo:
    def write(self, item):
        return [{"Put": {"TableName": self.domain_not_audit_table_name, "Item": item}}]
"""
    with pytest.raises(AssertionError, match="unknown table expression"):
        _fixture(unknown).writes()

    override = """
class Repo:
    def write(self, item):
        return [{"Put": {"TableName": self.audit_table_name, **self._base(item)}}]
"""
    with pytest.raises(AssertionError, match="a spread after TableName"):
        _fixture(override).writes()


def test_python_standalone_writes_resolve_through_forwarders() -> None:
    source = """
class Base:
    def _write(self, operation, request):
        return operation(**request)
    def put_item_sync(self, request):
        return self._write(self.client.put_item, request)
    async def put_item(self, request):
        await to_thread(self.put_item_sync, request)

class Repo(Base):
    def _conditioned(self, item):
        return {"Put": {"TableName": self.table_name, "Item": item}}
    def create(self, item):
        self.client.put_item(**self._conditioned(item)["Put"])
    def log(self, item):
        self.client.update_item(TableName=self.audit_table_name, Key=item)
    async def record(self, item):
        await self.put_item({"TableName": self.audit_table_name, "Item": item})
    def rebinding(self, request):
        request = {**request, "TableName": self.audit_table_name}
        self.client.update_item(**request)
    def caller(self, item):
        self.rebinding({"TableName": self.table_name, "Key": item})
"""
    standalone = [w for w in _fixture(source).writes() if w.standalone]
    assert sorted((w.site, w.action, set(w.tables)) for w in standalone) == sorted(
        [
            ("fixture.py::Repo.create", "dynamodb:PutItem", {"domain"}),
            ("fixture.py::Repo.log", "dynamodb:UpdateItem", {"audit"}),
            ("fixture.py::Repo.record", "dynamodb:PutItem", {"audit"}),
            ("fixture.py::Repo.rebinding", "dynamodb:UpdateItem", {"domain", "audit"}),
        ]
    )


def test_python_scanner_flags_shapes_it_cannot_resolve() -> None:
    source = """
class Repo:
    def write(self, item, op):
        request = {"TableName": self.table_name, "Item": item}
        request.update({"TableName": self.audit_table_name})
        request.setdefault("TableName", self.audit_table_name)
        request["TableName"] = self.audit_table_name
        built = dict(Delete={"TableName": self.audit_table_name})
        other = {}
        other["ConditionCheck"] = request
        keyed = {op: {"TableName": self.audit_table_name, "Key": item}}
        writer = getattr(self.client, "delete_item")
        boto3.resource("dynamodb").Table(self.audit_table_name).delete_item(Key=item)
        self.client.execute_statement(Statement="DELETE FROM x")
        update = self.client.update_item
        dynamic = getattr(self.client, op)
        setattr(self, "table_name", self.audit_table_name)
        return request["Put"]

class Other:
    table_name = "LimnopulseLeads"
    @property
    def domain_table_name(self):
        return self.audit_table_name
"""
    sources = _fixture(source)

    assert len(sources.table_mutations) == 6
    assert sorted(stray.split(": ")[1] for stray in sources.stray_operations) == [
        "'ConditionCheck'",
        "Delete=",
        "[op]",
    ]
    assert len(sources.unclassified_access) == 6


def test_go_scanner_sees_items_beyond_composite_literals_and_skips_comments() -> None:
    text = _strip_go_comments(
        """package x
import (
    "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
    ddb "github.com/aws/aws-sdk-go-v2/service/dynamodb"
)
// &types.Delete{ a commented-out item demands nothing
/* types.Put{ nor does a block comment */
var check = new(types.ConditionCheck)
var input ddb.DeleteItemInput
const url = "http://example.test/types.Update{"
"""
    )
    writes = _go_writes_in("x.go", text)

    assert sorted((w.operation, w.standalone) for w in writes) == [
        ("ConditionCheck", False),
        ("DeleteItem", True),
    ]


# --- telemetry_ingest: the writes the ingest worker reaches ------------------------------------

TOFU_DIR = ROOT / "infra" / "opentofu"
WORKERS_PACKAGE = API_PACKAGE / "workers"
# The module the compose services run (`python -m limnopulse_api.workers.telemetry_ingest`).
TELEMETRY_INGEST_ENTRY = ENTRYPOINT[-1]
TELEMETRY_INGEST_USER = "aws_iam_user.telemetry_ingest"
TELEMETRY_INGEST_FLOOR = {("dynamodb:PutItem", "domain"), ("dynamodb:UpdateItem", "domain")}
TELEMETRY_INGEST_WRITE_SITES = {
    (
        "src/limnopulse_api/adapters/dynamodb_quarantine.py",
        "DynamoQuarantineRepository.put_quarantine_entries",
    ),
    (
        "src/limnopulse_api/adapters/dynamodb_ingest_state.py",
        "DynamoIngestStateRepository._advance",
    ),
}
# The worker's entry and the reads it makes through the repositories it builds. If the walk
# stopped early, it would derive no write and pass.
TELEMETRY_INGEST_REACHED = {
    ("src/limnopulse_api/workers/telemetry_ingest.py", "main"),
    ("src/limnopulse_api/workers/telemetry_ingest.py", "build_worker"),
    (
        "src/limnopulse_api/adapters/dynamodb_devices_v2.py",
        "DynamoDeviceComponentRepository.get_component",
    ),
    (
        "src/limnopulse_api/adapters/dynamodb_integrations.py",
        "DynamoIntegrationRepository.resolve_external_identity",
    ),
    (
        "src/limnopulse_api/adapters/dynamodb_deployments.py",
        "DynamoDeploymentRepository.get_deployment_at",
    ),
    ("src/limnopulse_api/adapters/dynamodb_sites_assets.py", "DynamoSiteAssetRepository.get_asset"),
}
# Calls that name an attribute or a module at run time. A constant name counts as a mention.
# Anything else must be reviewed here by hand, because the walk cannot follow it.
_DYNAMIC_ATTRIBUTE_CALLS = {"getattr", "attrgetter", "methodcaller"}
_DYNAMIC_CODE = {"__import__", "import_module", "exec", "eval"}
DYNAMIC_ATTRIBUTE_SITES = {
    # Reads the pydantic fields of the envelope it serializes: no function is reached.
    "src/limnopulse_api/domain/ingestion.py::IngressEnvelope.to_json",
}


def _module_path(name: str) -> Path | None:
    path = API_PACKAGE.parent.joinpath(*name.split("."))
    if (path / "__init__.py").is_file():
        return path / "__init__.py"
    module = path.parent / f"{path.name}.py"
    return module if module.is_file() else None


def _imported_names(name: str, path: Path, tree: ast.Module) -> Iterator[str]:
    """Dotted names that `tree` imports, anywhere in it. Not all of them are modules."""
    package = name.split(".") if path.name == "__init__.py" else name.split(".")[:-1]
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            yield from (alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom):
            base = package[: len(package) - node.level + 1] if node.level else []
            module = ".".join([*base, *([node.module] if node.module else [])])
            yield module
            yield from (f"{module}.{alias.name}" for alias in node.names)


@functools.cache
def _telemetry_ingest_modules() -> tuple[tuple[str, str], ...]:
    """(path, text) of every package module the worker entry imports, transitively.

    Imports inside functions count as well. A package module outside this set never runs in the
    worker process, unless it is imported dynamically, which `_dynamic_sites` rejects.
    """
    found: dict[str, Path] = {}
    pending = [TELEMETRY_INGEST_ENTRY]
    while pending:
        name = pending.pop()
        if name in found or name.split(".")[0] != API_PACKAGE.name:
            continue
        path = _module_path(name)
        if path is None:
            continue  # a function, class or constant imported from a module
        found[name] = path
        parts = name.split(".")
        pending.extend(".".join(parts[:index]) for index in range(1, len(parts)))
        pending.extend(_imported_names(name, path, ast.parse(path.read_text(encoding="utf-8"))))
    assert TELEMETRY_INGEST_ENTRY in found
    return tuple(
        sorted(
            (path.relative_to(ROOT).as_posix(), path.read_text(encoding="utf-8"))
            for path in found.values()
        )
    )


def _node_mentions(node: ast.AST) -> set[str]:
    if isinstance(node, ast.Name):
        return {node.id}
    if isinstance(node, ast.Attribute):
        return {node.attr}
    if isinstance(node, ast.Call) and _name_of(node.func) in _DYNAMIC_ATTRIBUTE_CALLS:
        return {
            part
            for argument in node.args
            if isinstance(argument, ast.Constant) and isinstance(argument.value, str)
            for part in argument.value.split(".")
        }
    return set()


def _mentions(node: ast.AST) -> set[str]:
    return set().union(*(_node_mentions(child) for child in ast.walk(node)))


def _import_time_mentions(tree: ast.Module) -> set[str]:
    """Names mentioned by what runs on import: module and class bodies (the `__main__` guard among
    them), decorators and default values, but no function body."""
    names: set[str] = set()
    pending: list[ast.AST] = [tree]
    while pending:
        node = pending.pop()
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            pending.extend(node.decorator_list)
            pending.extend(node.args.defaults)
            pending.extend(default for default in node.args.kw_defaults if default is not None)
            continue
        names |= _node_mentions(node)
        pending.extend(ast.iter_child_nodes(node))
    return names


def _reached(sources: _PythonSources, modules: Iterable[tuple[str, str]]) -> set[_Function]:
    """Every function of `sources` that may run, followed by name. This over-approximates.

    The roots are what runs on import, every decorated function (its decorator may register it)
    and every dunder method (the interpreter calls those). A reached function reaches every
    function, in any module, whose name it mentions as a name or an attribute, and every function
    nested in it. `from m import f as g` makes `g` name `f` too. A name match that is not a real
    call only adds writes, so the coverage test fails closed on it.
    """
    aliases: dict[str, set[str]] = {}
    names: set[str] = set()
    for _, text in modules:
        tree = ast.parse(text)
        names |= _import_time_mentions(tree)
        for node in ast.walk(tree):
            if isinstance(node, (ast.Import, ast.ImportFrom)):
                for alias in node.names:
                    if alias.asname:
                        aliases.setdefault(alias.asname, set()).add(alias.name.rsplit(".")[-1])
    functions = [function for candidates in sources.functions.values() for function in candidates]
    nested: dict[_Function, list[_Function]] = {}
    for function in functions:
        if function.parent is not None:
            nested.setdefault(function.parent, []).append(function)

    def named(mentioned: set[str]) -> Iterator[_Function]:
        for name in mentioned:
            for target in {name, *aliases.get(name, set())}:
                yield from sources.functions.get(target, [])

    pending = [
        function
        for function in functions
        if function.node.decorator_list
        or (function.node.name.startswith("__") and function.node.name.endswith("__"))
    ]
    pending.extend(named(names))
    reached: set[_Function] = set()
    while pending:
        function = pending.pop()
        if function in reached:
            continue
        reached.add(function)
        pending.extend(named(_mentions(function.node)))
        pending.extend(nested.get(function, []))
    return reached


def _dynamic_sites(modules: Iterable[tuple[str, str]]) -> set[str]:
    """`module::qualname` of every attribute or module named at run time, which the walk cannot
    follow."""
    sites: set[str] = set()

    def constant_names(call: ast.Call) -> bool:
        name = _name_of(call.func)
        named = (
            call.args[1:2]
            if name == "getattr"
            else call.args[:1]
            if name == "methodcaller"
            else call.args
        )
        return bool(named) and all(
            isinstance(argument, ast.Constant) and isinstance(argument.value, str)
            for argument in named
        )

    def visit(node: ast.AST, module: str, owner: str) -> None:
        for child in ast.iter_child_nodes(node):
            if isinstance(child, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
                visit(child, module, f"{owner}.{child.name}" if owner else child.name)
                continue
            if (
                _name_of(child) in _DYNAMIC_CODE
                if isinstance(child, (ast.Name, ast.Attribute))
                else isinstance(child, ast.Call)
                and _name_of(child.func) in _DYNAMIC_ATTRIBUTE_CALLS
                and not constant_names(child)
            ):
                sites.add(f"{module}::{owner or '<module>'}")
            visit(child, module, owner)

    for module, text in modules:
        visit(ast.parse(text), module, "")
    return sites


def _worker_writes(modules: tuple[tuple[str, str], ...]) -> tuple[_Write, ...]:
    sources = _PythonSources(list(modules))
    return tuple(sources.writes(_reached(sources, modules)))


@functools.cache
def _telemetry_ingest_sources() -> _PythonSources:
    return _PythonSources(list(_telemetry_ingest_modules()))


@functools.cache
def _telemetry_ingest_writes() -> tuple[_Write, ...]:
    sources = _telemetry_ingest_sources()
    return tuple(sources.writes(_reached(sources, _telemetry_ingest_modules())))


# --- telemetry_ingest: the grants of its user --------------------------------------------------


def _tofu_texts() -> dict[str, str]:
    return {path.name: path.read_text(encoding="utf-8") for path in sorted(TOFU_DIR.glob("*.tf"))}


def _references(block, needle: str) -> bool:
    return any(needle in value for value in block.attrs.values()) or any(
        _references(inner, needle) for inner in block.blocks
    )


def _telemetry_ingest_grants(texts: Mapping[str, str]) -> set[tuple[str, str]]:
    """DynamoDB grants of the `telemetry_ingest` user that hold whenever the user exists.

    These are its policy attachments, each gated exactly like the user (today on
    `var.telemetry_ingress`), or unconditional if the user ever is. A grant behind another flag,
    a reference this reader does not model, or a wildcard fails instead of counting.
    """
    blocks = [block for text in texts.values() for block in _parse_hcl_body(text)[1]]
    users = [
        block
        for block in blocks
        if block.kind == "resource" and block.labels == ("aws_iam_user", "telemetry_ingest")
    ]
    assert len(users) == 1, "exactly one telemetry_ingest user"
    (user,) = users
    assert "permissions_boundary" not in user.attrs, "a permissions boundary is not modelled"
    gate = user.attrs.get("count")
    index = "[0]" if gate is not None else ""
    documents = {
        block.labels[1]: block
        for block in blocks
        if block.kind == "data" and block.labels[0] == "aws_iam_policy_document"
    }
    policies = {
        block.labels[1]: block
        for block in blocks
        if block.kind == "resource" and block.labels[0] == "aws_iam_policy"
    }
    grants: set[tuple[str, str]] = set()
    for block in blocks:
        if block is user or block.kind == "output" or not _references(block, TELEMETRY_INGEST_USER):
            continue
        assert block.kind == "resource" and block.labels[0] == "aws_iam_user_policy_attachment", (
            f"unmodelled reference to the telemetry_ingest user: {block.kind} {block.labels}"
        )
        assert block.attrs["user"] == f"{TELEMETRY_INGEST_USER}{index}.name", block.labels
        policy = re.fullmatch(
            rf"aws_iam_policy\.(\w+){re.escape(index)}\.arn", block.attrs["policy_arn"]
        )
        assert policy is not None, block.labels
        document = re.fullmatch(
            rf"data\.aws_iam_policy_document\.(\w+){re.escape(index)}\.json",
            policies[policy.group(1)].attrs["policy"],
        )
        assert document is not None, policy.group(1)
        statements = documents[document.group(1)]
        for gated in (block, policies[policy.group(1)], statements):
            assert gated.attrs.get("count") == gate, (
                f"{gated.labels[1]} is gated on {gated.attrs.get('count')}, not on {gate}"
            )
        for statement in statements.nested("statement"):
            for action in _list_items(statement.attrs["actions"]):
                assert "*" not in action, (
                    f"wildcard action {_unquote(action)} in {statements.labels[1]}"
                )
        # `_statement_grants` reads an unconditional document; the gate was checked above.
        plain = copy.copy(statements)
        plain.attrs = {name: value for name, value in statements.attrs.items() if name != "count"}
        grants |= _statement_grants(plain)
    assert grants, "no DynamoDB grant reaches the telemetry_ingest user"
    return grants


def _telemetry_ingest_problems(texts: Mapping[str, str], writes: tuple[_Write, ...]) -> list[str]:
    granted = _telemetry_ingest_grants(texts)
    derived = _norm(_required(writes))
    unused = sorted(
        f"unused write grant: {action} on {table}"
        for action, table in _table_grants(granted)
        if action in WRITE_ACTIONS and (action, table) not in derived
    )
    return [*(f"uncovered: {line}" for line in _uncovered(writes, granted)), *unused]


# --- Tests: telemetry_ingest -------------------------------------------------------------------


def test_telemetry_ingest_policy_grants_every_write_the_worker_reaches() -> None:
    assert _telemetry_ingest_problems(_tofu_texts(), _telemetry_ingest_writes()) == []


def test_telemetry_ingest_derivation_reaches_the_known_reads_and_writes() -> None:
    sources = _telemetry_ingest_sources()
    reached = {(f.module, f.qualname) for f in _reached(sources, _telemetry_ingest_modules())}
    writes = _telemetry_ingest_writes()

    assert reached >= TELEMETRY_INGEST_REACHED
    assert _required(writes) >= TELEMETRY_INGEST_FLOOR
    assert {tuple(write.site.split("::")) for write in writes} >= TELEMETRY_INGEST_WRITE_SITES


def test_telemetry_ingest_scan_covers_every_worker_module_and_fails_closed() -> None:
    modules = dict(_telemetry_ingest_modules())
    sources = _telemetry_ingest_sources()

    # The api scan skips `workers/`, so every worker module must run under this principal.
    workers = {path.relative_to(ROOT).as_posix() for path in WORKERS_PACKAGE.rglob("*.py")}
    assert workers <= set(modules)
    assert sources.stray_operations == []
    assert sources.unclassified_access == []
    assert sources.table_mutations == []
    assert _dynamic_sites(modules.items()) == DYNAMIC_ATTRIBUTE_SITES
    # The other modules are api modules, whose table settings the api test already checks.
    for module, text in modules.items():
        if module.startswith("src/limnopulse_api/workers/"):
            assert set(re.findall(r"\bdynamodb_\w+_table\b", text)) <= {"dynamodb_domain_table"}
            assert "LimnopulseAudit" not in text, module
    assert not any(
        where.startswith("src/limnopulse_api/workers/") for *_, where in sources.table_assignments
    )


_WORKER_DOCUMENT = 'data "aws_iam_policy_document" "telemetry_ingest_worker" {'
_WORKER_ATTACHMENT = 'resource "aws_iam_user_policy_attachment" "telemetry_ingest_worker" {'
_DOMAIN_RESOURCES = "resources = [aws_dynamodb_table.domain.arn]"
_QUARANTINE_PUT = (
    "src/limnopulse_api/adapters/dynamodb_quarantine.py::"
    "DynamoQuarantineRepository.put_quarantine_entries put_item -> dynamodb:PutItem on domain"
)
_STATE_UPDATE = (
    "src/limnopulse_api/adapters/dynamodb_ingest_state.py::"
    "DynamoIngestStateRepository._advance update_item -> dynamodb:UpdateItem on domain"
)


def _edit_block(text: str, header: str, old: str, new: str) -> str:
    """Replace `old` once inside the block that opens with `header`, and nowhere else."""
    assert text.count(header) == 1, header
    start = text.index(header)
    end = _matching_brace(text, start + len(header) - 1)
    block = text[start:end]
    assert block.count(old) == 1, f"the mutant anchor {old!r} drifted in {header}"
    return text[:start] + block.replace(old, new) + text[end:]


@pytest.mark.parametrize(
    ("header", "old", "new", "expected"),
    [
        pytest.param(
            _WORKER_DOCUMENT,
            '"dynamodb:PutItem",',
            "",
            f"uncovered: {_QUARANTINE_PUT}",
            id="drop-put-item",
        ),
        pytest.param(
            _WORKER_DOCUMENT,
            '"dynamodb:UpdateItem",',
            "",
            f"uncovered: {_STATE_UPDATE}",
            id="drop-update-item",
        ),
        pytest.param(
            _WORKER_DOCUMENT,
            _DOMAIN_RESOURCES,
            "resources = [aws_dynamodb_table.audit.arn]",
            f"uncovered: {_QUARANTINE_PUT}",
            id="wrong-table",
        ),
        pytest.param(
            _WORKER_DOCUMENT,
            _DOMAIN_RESOURCES,
            'resources = ["${aws_dynamodb_table.domain.arn}/index/*"]',
            f"uncovered: {_STATE_UPDATE}",
            id="index-instead-of-table",
        ),
        pytest.param(
            _WORKER_DOCUMENT,
            '"dynamodb:PutItem",',
            '"dynamodb:*",',
            "wildcard action dynamodb:* in telemetry_ingest_worker",
            id="widen-action",
        ),
        pytest.param(
            _WORKER_DOCUMENT,
            _DOMAIN_RESOURCES,
            'resources = ["*"]',
            'unexpected resource "*"',
            id="widen-resource",
        ),
        pytest.param(
            _WORKER_DOCUMENT,
            '"dynamodb:UpdateItem",',
            '"dynamodb:UpdateItem", "dynamodb:DeleteItem",',
            "unused write grant: dynamodb:deleteitem on domain",
            id="unused-write-grant",
        ),
        pytest.param(
            _WORKER_ATTACHMENT,
            "count = var.telemetry_ingress ? 1 : 0",
            "count = var.telemetry_ingress_worker_grants ? 1 : 0",
            "telemetry_ingest_worker is gated on var.telemetry_ingress_worker_grants ? 1 : 0",
            id="other-gate",
        ),
        pytest.param(
            _WORKER_ATTACHMENT,
            "aws_iam_user.telemetry_ingest[0].name",
            "aws_iam_user.api.name",
            "no DynamoDB grant reaches the telemetry_ingest user",
            id="detached",
        ),
    ],
)
def test_telemetry_ingest_policy_mutants_fail(
    header: str, old: str, new: str, expected: str
) -> None:
    texts = _tofu_texts()
    texts["ingestion_queues.tf"] = _edit_block(texts["ingestion_queues.tf"], header, old, new)

    try:
        problems = _telemetry_ingest_problems(texts, _telemetry_ingest_writes())
    except AssertionError as error:
        problems = [str(error)]

    assert any(expected in problem for problem in problems), problems


_WORKER_MODULE = "src/limnopulse_api/workers/telemetry_ingest.py"
_WORKER_ANCHOR = "    metrics = StructuredLogTelemetryMetrics()\n"


@pytest.mark.parametrize(
    ("reference", "expected"),
    [
        pytest.param(
            "devices.create_component",
            "src/limnopulse_api/adapters/dynamodb_devices_v2.py::"
            "DynamoDeviceComponentRepository.create_component ConditionCheck"
            " -> dynamodb:ConditionCheckItem on domain",
            id="devices",
        ),
        pytest.param(
            "integrations.transition_integration_account",
            "src/limnopulse_api/adapters/dynamodb_integrations.py::"
            "DynamoIntegrationRepository.transition_integration_account Put"
            " -> dynamodb:PutItem on audit",
            id="integrations",
        ),
        pytest.param(
            "DynamoDeploymentRepository(table, dynamodb).end_deployment",
            "src/limnopulse_api/adapters/dynamodb_deployments.py::"
            "DynamoDeploymentRepository.end_deployment Delete -> dynamodb:DeleteItem on domain",
            id="deployments",
        ),
        pytest.param(
            "DynamoSiteAssetRepository(table, dynamodb).create_asset",
            "src/limnopulse_api/adapters/dynamodb_sites_assets.py::"
            "DynamoSiteAssetRepository.create_asset ConditionCheck"
            " -> dynamodb:ConditionCheckItem on domain",
            id="site-assets",
        ),
    ],
)
def test_a_transactional_write_the_worker_starts_to_reach_fails(
    reference: str, expected: str
) -> None:
    """The worker uses these repositories for reads only. Reaching one of their transactions
    needs an item action that `telemetry_ingest` lacks, and would fail with AccessDenied."""
    modules = dict(_telemetry_ingest_modules())
    assert modules[_WORKER_MODULE].count(_WORKER_ANCHOR) == 1
    modules[_WORKER_MODULE] = modules[_WORKER_MODULE].replace(
        _WORKER_ANCHOR, f"    write = {reference}\n{_WORKER_ANCHOR}"
    )

    problems = _telemetry_ingest_problems(_tofu_texts(), _worker_writes(tuple(modules.items())))

    assert f"uncovered: {expected}" in problems


def test_reachability_follows_names_aliases_and_implicit_calls() -> None:
    modules = (
        (
            "pkg/worker.py",
            """
from pkg.repo import make as build

def main():
    store = build()
    return store.read()

@register
def hook():
    return getattr(Repo, "check")

if __name__ == "__main__":
    main()
""",
        ),
        (
            "pkg/repo.py",
            """
class Repo:
    def __init__(self):
        self.table_name = table_name
    def __enter__(self):
        return self
    def read(self):
        def inner():
            return self.helper()
        return inner()
    def helper(self):
        return None
    def check(self, key):
        return {"ConditionCheck": {"TableName": self.table_name, "Key": key}}
    def write(self, key):
        return {"Delete": {"TableName": self.table_name, "Key": key}}

def make():
    return Repo()
""",
        ),
    )
    sources = _PythonSources(list(modules))
    reached = {function.qualname for function in _reached(sources, modules)}

    assert reached == {
        "main",
        "hook",
        "make",
        "Repo.__init__",
        "Repo.__enter__",
        "Repo.read",
        "Repo.read.inner",
        "Repo.helper",
        "Repo.check",
    }
    assert [(write.operation, write.site) for write in _worker_writes(modules)] == [
        ("ConditionCheck", "pkg/repo.py::Repo.check")
    ]


def test_dynamic_names_are_reported_by_site() -> None:
    modules = (
        (
            "pkg/worker.py",
            """
import importlib
from operator import attrgetter, methodcaller

class Loader:
    def load(self, name):
        return importlib.import_module(name)
    def read(self, record, field):
        return getattr(record, field)
    def fixed(self, record):
        return getattr(record, "id"), attrgetter("a.b")(record), methodcaller("close")(record)
    def chosen(self, record, method):
        return methodcaller(method)(record)

handler = __import__("pkg.handlers")
""",
        ),
    )

    assert _dynamic_sites(modules) == {
        "pkg/worker.py::Loader.load",
        "pkg/worker.py::Loader.read",
        "pkg/worker.py::Loader.chosen",
        "pkg/worker.py::<module>",
    }
