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
- `workers`: the Go binaries under `cmd/` and `internal/`.

Each derived (action, table) pair must be granted by an unconditional attachment in
`iam_runtime.tf` on that table's ARN. A grant on `.../index/*` does not count: item actions are
authorized against the table.
"""

import ast
import re
from collections.abc import Callable, Iterator
from dataclasses import dataclass, field
from pathlib import Path

from tests.unit.test_opentofu_infra import _list_items, _parse_hcl_body, _unquote

ROOT = Path(__file__).resolve().parents[2]
IAM_RUNTIME = ROOT / "infra" / "opentofu" / "iam_runtime.tf"
API_PACKAGE = ROOT / "src" / "limnopulse_api"
API_EXCLUDED = (API_PACKAGE / "workers",)
GO_ROOTS = (ROOT / "cmd", ROOT / "internal")

ITEM_ACTIONS = {
    "Put": "dynamodb:PutItem",
    "Update": "dynamodb:UpdateItem",
    "Delete": "dynamodb:DeleteItem",
    "ConditionCheck": "dynamodb:ConditionCheckItem",
}
# Standalone writes. PartiQL maps to no single action: it must be classified by hand, so it maps
# to an action that no policy grants.
PY_STANDALONE_WRITES = {
    "put_item": "dynamodb:PutItem",
    "update_item": "dynamodb:UpdateItem",
    "delete_item": "dynamodb:DeleteItem",
    "batch_write_item": "dynamodb:BatchWriteItem",
    "execute_statement": "unclassified:PartiQL",
    "batch_execute_statement": "unclassified:PartiQL",
    "execute_transaction": "unclassified:PartiQL",
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
WRITE_ACTIONS = {*ITEM_ACTIONS.values(), *PY_STANDALONE_WRITES.values()}

# Hand inventory (issue #108). The derived sets must contain at least these pairs, so a scanner
# that silently finds nothing cannot pass.
API_FLOOR = {
    ("dynamodb:ConditionCheckItem", "domain"),
    ("dynamodb:DeleteItem", "domain"),
    ("dynamodb:PutItem", "domain"),
    ("dynamodb:UpdateItem", "domain"),
    ("dynamodb:PutItem", "audit"),
}
WORKERS_FLOOR = {
    ("dynamodb:ConditionCheckItem", "domain"),
    ("dynamodb:PutItem", "domain"),
    ("dynamodb:UpdateItem", "domain"),
}
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

_TABLE_ACTIONS = {
    "dynamodb:GetItem",
    "dynamodb:PutItem",
    "dynamodb:Query",
    "dynamodb:UpdateItem",
}
_INDEX_READS = {"dynamodb:GetItem", "dynamodb:Query"}
# The exact DynamoDB grants of each runtime principal: the upper bound. Item actions sit on the
# table ARN only; `index/*` keeps only the grants that predate issue #108.
EXPECTED_DYNAMODB_GRANTS = {
    "api": {
        *((action, "domain") for action in _TABLE_ACTIONS),
        *((action, "domain/index/*") for action in _TABLE_ACTIONS),
        ("dynamodb:ConditionCheckItem", "domain"),
        ("dynamodb:DeleteItem", "domain"),
        ("dynamodb:PutItem", "audit"),
    },
    "workers": {
        *((action, "domain") for action in _INDEX_READS | {"dynamodb:UpdateItem"}),
        *((action, "domain/index/*") for action in _INDEX_READS | {"dynamodb:UpdateItem"}),
        ("dynamodb:ConditionCheckItem", "domain"),
        ("dynamodb:PutItem", "domain"),
    },
}

_RESOURCES = {
    "aws_dynamodb_table.domain.arn": "domain",
    '"${aws_dynamodb_table.domain.arn}/index/*"': "domain/index/*",
    "aws_dynamodb_table.audit.arn": "audit",
    "aws_sqs_queue.notification_jobs.arn": "notification_jobs",
}
_TABLES = {"domain", "audit"}

# --- Python: transaction items and standalone writes -------------------------------------------

_DOMAIN_TABLE_EXPRESSIONS = {"self.table_name", "self.domain_table_name"}
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
    principal: str
    site: str
    operation: str
    action: str
    tables: frozenset[str]


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


def _call_name(call: ast.Call) -> str | None:
    if isinstance(call.func, ast.Attribute):
        return call.func.attr
    if isinstance(call.func, ast.Name):
        return call.func.id
    return None


class _PythonSources:
    """An index of the API sources and a small resolver for the table each request names.

    A request's table is the value of its last `TableName` key, followed through `**` spreads,
    helper calls (arguments bound to parameters), local assignments and, for an unbound
    parameter, every call site of the function. Any shape the resolver does not know fails the
    test instead of guessing.
    """

    def __init__(self, paths: list[Path]) -> None:
        self.functions: dict[str, list[_Function]] = {}
        self.calls: dict[str, list[tuple[ast.Call, _Function | None]]] = {}
        self.items: list[tuple[str, ast.expr, _Function | None, str]] = []
        self.standalone: list[tuple[str, _Function | None, str]] = []
        self.table_mutations: list[str] = []
        for path in paths:
            module = path.relative_to(ROOT).as_posix()
            self._walk(ast.parse(path.read_text(encoding="utf-8")), module, None, None)

    def _walk(self, node: ast.AST, module: str, function: _Function | None, cls: str | None):
        for child in ast.iter_child_nodes(node):
            if isinstance(child, ast.ClassDef):
                self._walk(child, module, function, child.name)
                continue
            if isinstance(child, (ast.FunctionDef, ast.AsyncFunctionDef)):
                prefix = function.qualname if function else cls
                qualname = f"{prefix}.{child.name}" if prefix else child.name
                defined = _Function(
                    module, child, function, function is None and bool(cls), qualname
                )
                self.functions.setdefault(child.name, []).append(defined)
                self._walk(child, module, defined, None)
                continue
            self._record(child, module, function)
            self._walk(child, module, function, cls)

    def _record(self, node: ast.AST, module: str, function: _Function | None) -> None:
        where = f"{module}:{getattr(node, 'lineno', '?')}"
        if isinstance(node, ast.Dict):
            operations = [
                key.value
                for key in node.keys
                if isinstance(key, ast.Constant) and key.value in ITEM_ACTIONS
            ]
            if operations:
                assert len(node.keys) == 1, f"{where}: a transaction item has exactly one key"
                self.items.append((operations[0], node.values[0], function, where))
        elif isinstance(node, ast.Call):
            name = _call_name(node)
            if name:
                self.calls.setdefault(name, []).append((node, function))
        elif isinstance(node, ast.Attribute) and node.attr in PY_STANDALONE_WRITES:
            if ast.unparse(node.value).endswith("client"):
                self.standalone.append((node.attr, function, where))
        if isinstance(node, (ast.Assign, ast.AugAssign, ast.AnnAssign)):
            targets = node.targets if isinstance(node, ast.Assign) else [node.target]
            for target in targets:
                if (
                    isinstance(target, ast.Subscript)
                    and isinstance(target.slice, ast.Constant)
                    and target.slice.value == "TableName"
                ):
                    self.table_mutations.append(where)

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
            named = [
                value
                for key, value in zip(expr.keys, expr.values, strict=True)
                if isinstance(key, ast.Constant) and key.value == "TableName"
            ]
            if named:
                return self.table(named[-1], scope, depth + 1)
            spreads = [
                value for key, value in zip(expr.keys, expr.values, strict=True) if key is None
            ]
            if spreads:
                return set().union(
                    *(self.request_tables(spread, scope, depth + 1) for spread in spreads)
                )
        elif isinstance(expr, ast.Call):
            return self._returns(expr, scope, depth, self.request_tables)
        elif isinstance(expr, ast.Name):
            return self._name(expr.id, scope, depth, self.request_tables)
        elif isinstance(expr, ast.IfExp):
            return self.request_tables(expr.body, scope, depth + 1) | self.request_tables(
                expr.orelse, scope, depth + 1
            )
        raise AssertionError(f"cannot resolve the request {ast.unparse(expr)}")

    def table(self, expr: ast.expr, scope: _Scope, depth: int = 0) -> set[str]:
        """The table kind that a `TableName` expression evaluates to."""
        self._guard(expr, depth)
        text = ast.unparse(expr)
        if "audit_table" in text:
            return {"audit"}
        if text in _DOMAIN_TABLE_EXPRESSIONS:
            return {"domain"}
        if isinstance(expr, ast.Name):
            return self._name(expr.id, scope, depth, self.table)
        if isinstance(expr, ast.Call):
            return self._returns(expr, scope, depth, self.table)
        if isinstance(expr, ast.IfExp):
            return self.table(expr.body, scope, depth + 1) | self.table(
                expr.orelse, scope, depth + 1
            )
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
            if name in function.params():
                return self._call_sites(function, name, depth, resolve)
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
        name = _call_name(call)
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

    # Inventory --------------------------------------------------------------------------------

    def writes(self) -> list[_Write]:
        writes: list[_Write] = []
        for operation, value, function, where in self.items:
            assert function is not None, f"{where}: a transaction item outside a function"
            tables = self.request_tables(value, _Scope(function))
            site = f"{function.module}::{function.qualname}"
            writes.append(
                _Write("api", site, operation, ITEM_ACTIONS[operation], frozenset(tables))
            )
        for method, function, where in self.standalone:
            assert function is not None, f"{where}: a standalone write outside a function"
            # A standalone write never reaches the audit table: audit items are appended inside
            # the transaction of the change they record.
            source = ast.unparse(function.node)
            assert "audit_table" not in source, f"{where}: standalone write next to the audit table"
            site = f"{function.module}::{function.qualname}"
            writes.append(
                _Write("api", site, method, PY_STANDALONE_WRITES[method], frozenset({"domain"}))
            )
        return writes


# --- Go: transaction items and standalone writes -----------------------------------------------

_GO_TYPES = "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
_GO_DYNAMODB = "github.com/aws/aws-sdk-go-v2/service/dynamodb"
_GO_TABLE_EXPRESSIONS = {"aws.String(store.Table)", "&store.Table", "mutation.TableName"}


def _go_files() -> list[Path]:
    return sorted(
        path
        for root in GO_ROOTS
        for path in root.rglob("*.go")
        if not path.name.endswith("_test.go")
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


def _go_writes() -> list[_Write]:
    writes: list[_Write] = []
    for path in _go_files():
        text = path.read_text(encoding="utf-8")
        relative = path.relative_to(ROOT).as_posix()
        types_alias = _go_alias(text, _GO_TYPES)
        if types_alias:
            pattern = rf"\b{re.escape(types_alias)}\.(Put|Update|Delete|ConditionCheck)\s*\{{"
            for match in re.finditer(pattern, text):
                line = text.count("\n", 0, match.start()) + 1
                operation = match.group(1)
                writes.append(
                    _Write(
                        "workers",
                        f"{relative}:{line}",
                        operation,
                        ITEM_ACTIONS[operation],
                        frozenset({"domain"}),
                    )
                )
        dynamodb_alias = _go_alias(text, _GO_DYNAMODB)
        if dynamodb_alias:
            names = "|".join(GO_STANDALONE_WRITES)
            pattern = rf"\b{re.escape(dynamodb_alias)}\.({names})Input\s*\{{"
            for match in re.finditer(pattern, text):
                line = text.count("\n", 0, match.start()) + 1
                operation = match.group(1)
                writes.append(
                    _Write(
                        "workers",
                        f"{relative}:{line}",
                        operation,
                        GO_STANDALONE_WRITES[operation],
                        frozenset({"domain"}),
                    )
                )
    return writes


# --- iam_runtime.tf ----------------------------------------------------------------------------


def _iam_blocks() -> list:
    return _parse_hcl_body(IAM_RUNTIME.read_text(encoding="utf-8"))[1]


def _statement_grants(document) -> set[tuple[str, str]]:
    assert set(document.attrs) <= {"version"}, document.labels
    assert {block.kind for block in document.blocks} == {"statement"}, document.labels
    grants: set[tuple[str, str]] = set()
    for statement in document.nested("statement"):
        assert set(statement.attrs) <= {"sid", "effect", "actions", "resources"}, statement.attrs
        assert statement.blocks == [], f"{document.labels}: conditions are not modelled here"
        assert statement.attrs.get("effect", '"Allow"') == '"Allow"'
        actions = [_unquote(item) for item in _list_items(statement.attrs["actions"])]
        for resource in _list_items(statement.attrs["resources"]):
            assert resource in _RESOURCES, f"unexpected resource {resource}"
            for action in actions:
                assert "*" not in action, action
                grants.add((action, _RESOURCES[resource]))
    return grants


def _runtime_grants() -> dict[str, set[tuple[str, str]]]:
    """Grants of every unconditional attachment in iam_runtime.tf, per IAM user."""
    blocks = _iam_blocks()
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
    for block in blocks:
        if block.kind != "resource" or block.labels[0] != "aws_iam_user_policy_attachment":
            continue
        if "count" in block.attrs:
            continue  # flag-gated: the base grants must not depend on an optional feature
        user = re.fullmatch(r"aws_iam_user\.(\w+)\.name", block.attrs["user"])
        policy = re.fullmatch(r"aws_iam_policy\.(\w+)\.arn", block.attrs["policy_arn"])
        assert user is not None and policy is not None, block.labels
        document = re.fullmatch(
            r"data\.aws_iam_policy_document\.(\w+)\.json", policies[policy.group(1)].attrs["policy"]
        )
        assert document is not None, policy.group(1)
        grants.setdefault(user.group(1), set()).update(
            _statement_grants(documents[document.group(1)])
        )
    return grants


def _required(writes: list[_Write]) -> set[tuple[str, str]]:
    return {(write.action, table) for write in writes for table in write.tables}


def _table_grants(grants: set[tuple[str, str]]) -> set[tuple[str, str]]:
    return {(action, resource) for action, resource in grants if resource in _TABLES}


def _api_writes() -> list[_Write]:
    return _PythonSources(_api_modules()).writes()


def _uncovered(writes: list[_Write], granted: set[tuple[str, str]]) -> list[str]:
    return sorted(
        f"{write.site} {write.operation} -> {write.action} on {table}"
        for write in writes
        for table in write.tables
        if (write.action, table) not in granted
    )


# --- Tests -------------------------------------------------------------------------------------


def test_api_policy_grants_every_transaction_item_and_write_on_its_table() -> None:
    writes = _api_writes()
    granted = _table_grants(_runtime_grants()["api"])

    assert _uncovered(writes, granted) == []


def test_workers_policy_grants_every_transaction_item_and_write_on_its_table() -> None:
    writes = _go_writes()
    granted = _table_grants(_runtime_grants()["workers"])

    assert _uncovered(writes, granted) == []


def test_derived_inventory_contains_the_known_transaction_items() -> None:
    api_writes = _api_writes()

    assert _required(api_writes) >= API_FLOOR
    assert _required(_go_writes()) >= WORKERS_FLOOR
    audit_sites = {
        tuple(write.site.split("::"))
        for write in api_writes
        if "audit" in write.tables and write.operation == "Put"
    }
    assert audit_sites >= AUDIT_PUT_SITES
    # The audit table is append-only: no Update, Delete or ConditionCheck item names it.
    assert {write.action for write in api_writes if "audit" in write.tables} == {"dynamodb:PutItem"}


def test_runtime_dynamodb_grants_are_exact_and_carry_no_transact_pseudo_action() -> None:
    grants = _runtime_grants()

    assert set(grants) == {"api", "workers"}
    for user, expected in EXPECTED_DYNAMODB_GRANTS.items():
        dynamodb = {grant for grant in grants[user] if grant[0].startswith("dynamodb:")}
        assert dynamodb == expected, user
    actions = {action for user_grants in grants.values() for action, _ in user_grants}
    # IAM has no dynamodb:TransactWriteItems action: granting it authorizes nothing.
    assert not {action for action in actions if action.startswith("dynamodb:Transact")}


def test_runtime_write_grants_are_all_used_by_a_derived_write() -> None:
    grants = _runtime_grants()
    derived = {"api": _required(_api_writes()), "workers": _required(_go_writes())}

    for user, required in derived.items():
        writes = {grant for grant in _table_grants(grants[user]) if grant[0] in WRITE_ACTIONS}
        assert writes <= required, user


def test_python_resolver_rejects_unknown_tables_and_follows_helpers() -> None:
    def resolve(source: str) -> list[set[str]]:
        tree = ast.parse(source)
        sources = _PythonSources([])
        sources._walk(tree, "fixture.py", None, None)
        return [
            set(sources.request_tables(value, _Scope(function)))
            for _, value, function, _ in sources.items
        ]

    helpers = """
class Repo:
    def _put(self, table_name, item):
        return {"Put": {"TableName": table_name, "Item": item}}
    def _audit(self, item):
        return {**self._base(item), "TableName": self.audit_table_name}
    def _base(self, item):
        request = {"TableName": self.table_name, "Item": item}
        return request
    def write(self, item):
        self._put(self.domain_table_name, item)
        self._put(self.audit_table_name, item)
        return [{"Update": self._base(item)}, {"Put": self._audit(item)}]
"""
    assert resolve(helpers) == [{"domain", "audit"}, {"domain"}, {"audit"}]

    unknown = """
class Repo:
    def write(self, item):
        return [{"Put": {"TableName": self.leads_table_name, "Item": item}}]
"""
    try:
        resolve(unknown)
    except AssertionError as error:
        assert "unknown table expression self.leads_table_name" in str(error)
    else:  # pragma: no cover - the resolver must fail closed
        raise AssertionError("an unknown table resolved")


def test_api_tables_come_only_from_the_domain_and_audit_settings() -> None:
    sources = _PythonSources(_api_modules())
    assert sources.table_mutations == []

    settings = {
        match.group(0)
        for path in _api_modules()
        for match in re.finditer(r"\bdynamodb_\w+_table\b", path.read_text(encoding="utf-8"))
    }
    assert settings == {"dynamodb_domain_table", "dynamodb_audit_table"}
    # The audit table setting, directly or through a local it is assigned to, only ever reaches
    # an `audit_table_name` keyword, so `self.table_name` and `self.domain_table_name` always hold
    # the domain table.
    passed = 0
    for path in _api_modules():
        tree = ast.parse(path.read_text(encoding="utf-8"))
        tainted = {
            target.id
            for node in ast.walk(tree)
            if isinstance(node, ast.Assign) and "dynamodb_audit_table" in ast.unparse(node.value)
            for target in node.targets
            if isinstance(target, ast.Name)
        }

        def carries_audit(expr: ast.expr, tainted: set[str] = tainted) -> bool:
            return "dynamodb_audit_table" in ast.unparse(expr) or any(
                isinstance(node, ast.Name) and node.id in tainted for node in ast.walk(expr)
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
    table_names: set[str] = set()
    for path in _go_files():
        text = path.read_text(encoding="utf-8")
        assert re.search("audit", text, re.IGNORECASE) is None, path
        assert set(re.findall(r'"(DYNAMODB_\w*TABLE\w*)"', text)) <= {"DYNAMODB_DOMAIN_TABLE"}
        table_names |= {match.strip() for match in re.findall(r"\bTableName:\s*([^,}\n]+)", text)}

    assert table_names
    assert table_names <= _GO_TABLE_EXPRESSIONS
