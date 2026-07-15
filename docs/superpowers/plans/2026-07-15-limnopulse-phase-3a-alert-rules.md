# Limnopulse Phase 3A Alert Rules Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add tenant-scoped, audited, optimistic, and idempotently replaceable Alert Rules to the FastAPI/DynamoDB domain.

**Architecture:** A focused `AlertRuleRepository` protocol separates alert persistence from the existing domain repository. `AlertRuleService` validates tenant pond/device targets and generates IDs, while `DynamoAlertRuleRepository` owns DynamoDB Query and cross-table transactional writes for rules, audits, and replacement idempotency. FastAPI schemas reject extra input and routers reuse tenant membership authorization.

**Tech Stack:** Python 3.12, FastAPI, Pydantic 2, boto3 DynamoDB low-level client, pytest, OpenTofu with AWS Provider 6.x.

## Global Constraints

- Metrics are exactly `temp_c`, `ph`, `do_mg_l`, `turbidity_ntu`, `salinity_ppt`, `battery_v`, and `rssi`.
- Operators are exactly `<`, `<=`, `>`, and `>=`; aggregations are `min`, `max`, `mean`, and `last`.
- Severities are `warning` and `critical`; channels are `email` and `telegram`.
- `window` and `duration` are compact duration strings bounded from 60 seconds through 24 hours.
- `cooldown_seconds` is bounded from 60 through 86,400.
- Only owner/admin may mutate; all active tenant roles may list.
- DynamoDB list access uses `Query`, never `Scan`.
- Audit retention is 90 days; replacement idempotency retention is 24 hours.
- No AlertEvent, evaluator, Redis cooldown, SQS dispatch, SES, or Telegram integration is implemented in Phase 3A.

---

### Task 1: Alert Rule domain and repository contracts

**Files:**
- Create: `src/limnopulse_api/domain/alerts.py`
- Modify: `src/limnopulse_api/domain/ids.py`
- Create: `src/limnopulse_api/repositories/alerts.py`
- Test: `tests/unit/test_alert_rule_domain.py`

**Interfaces:**
- Produces: `AlertMetric`, `AlertOperator`, `AlertAggregation`, `AlertSeverity`, `AlertChannel`, `AlertRule`, `AuditContext`, `AlertRuleReplacement`, `AlertRuleRepository`, `new_alert_rule_id()`.
- `AlertRuleRepository` methods are `list_rules(tenant_id)`, `create_rule(rule, audit)`, `update_rule(tenant_id, rule_id, expected_version, updates, audit)`, `get_replacement_replay(tenant_id, idempotency_key, request_hash)`, and `replace_rule(tenant_id, rule_id, expected_version, replacement, idempotency_key, request_hash, audit)`.

- [x] **Step 1: Write failing domain tests**

```python
def test_alert_rule_rejects_duration_outside_bounds() -> None:
    with pytest.raises(ValidationError):
        make_rule(window="59s")


def test_new_alert_rule_id_has_canonical_prefix() -> None:
    assert re.fullmatch(r"rule_[0-9a-f]{32}", new_alert_rule_id())
```

- [x] **Step 2: Run the tests and verify RED**

Run: `uv run --extra dev --python 3.12 python -m pytest tests/unit/test_alert_rule_domain.py -q`

Expected: collection fails because `limnopulse_api.domain.alerts` does not exist.

- [x] **Step 3: Implement immutable domain types and protocol**

```python
class AlertRule(VersionedEntity):
    tenant_id: str
    rule_id: str
    pond_id: str
    device_id: str | None = None
    metric: AlertMetric
    name: str
    operator: AlertOperator
    threshold: float
    aggregation: AlertAggregation
    window: AlertDuration
    duration: AlertDuration
    severity: AlertSeverity
    channels: tuple[AlertChannel, ...]
    cooldown_seconds: int
    enabled: bool
    replaces_rule_id: str | None = None
    replaced_by_rule_id: str | None = None
```

- [x] **Step 4: Run domain tests and verify GREEN**

Run: `uv run --extra dev --python 3.12 python -m pytest tests/unit/test_alert_rule_domain.py -q`

Expected: all Task 1 tests pass.

- [x] **Step 5: Commit**

Run: `git add src/limnopulse_api/domain/alerts.py src/limnopulse_api/domain/ids.py src/limnopulse_api/repositories/alerts.py tests/unit/test_alert_rule_domain.py && git commit -m "feat: define alert rule domain"`

### Task 2: DynamoDB Alert Rule adapter

**Files:**
- Create: `src/limnopulse_api/adapters/alert_rules.py`
- Test: `tests/unit/test_alert_rule_repository.py`
- Modify: `tests/unit/test_no_scan_guard.py`

**Interfaces:**
- Consumes: Task 1 domain types and protocol.
- Produces: `DynamoAlertRuleRepository(domain_table_name, audit_table_name, client, clock=utc_now)`.
- Persists rule keys as `TENANT#<tenant_id>` / `ALERT_RULE#<rule_id>` and audit keys as `TENANT#<tenant_id>#MONTH#YYYY-MM` / `<timestamp>#<event_id>`.

- [x] **Step 1: Write failing Query/create transaction tests**

```python
@pytest.mark.asyncio
async def test_list_rules_queries_alert_rule_prefix_without_scan() -> None:
    rules = await repository.list_rules("tnt_1")
    assert rules == []
    assert client.query_calls[0]["KeyConditionExpression"] == "PK = :pk AND begins_with(SK, :sk_prefix)"
    assert client.scan_calls == 0


@pytest.mark.asyncio
async def test_create_rule_atomically_puts_rule_and_redacted_audit() -> None:
    created = await repository.create_rule(rule, audit_context)
    transaction = client.transact_write_items_calls[0]["TransactItems"]
    assert created.version == 1
    assert [item["Put"]["TableName"] for item in transaction] == ["LimnopulseDomain", "LimnopulseAudit"]
```

- [x] **Step 2: Run adapter tests and verify RED**

Run: `uv run --extra dev --python 3.12 python -m pytest tests/unit/test_alert_rule_repository.py -q`

Expected: collection fails because the adapter does not exist.

- [x] **Step 3: Implement Query, serialization, hashing, and atomic create**

The adapter uses `TypeSerializer`/`TypeDeserializer`, paginates `Query` with `LastEvaluatedKey`, hashes canonical JSON via SHA-256, and writes mutation audit entries in the same `TransactWriteItems` call as the rule.

- [x] **Step 4: Run focused tests and verify GREEN**

Run: `uv run --extra dev --python 3.12 python -m pytest tests/unit/test_alert_rule_repository.py -q`

Expected: list and create tests pass.

- [x] **Step 5: Write failing optimistic PATCH tests**

```python
@pytest.mark.asyncio
async def test_update_rule_conditions_on_version_and_writes_audit() -> None:
    updated = await repository.update_rule("tnt_1", "rule_1", 2, {"threshold": 4.5}, audit_context)
    assert updated.version == 3
    assert updated.threshold == 4.5
    assert len(client.transact_write_items_calls[0]["TransactItems"]) == 2
```

- [x] **Step 6: Implement conditional PATCH and conflict mapping**

The rule update condition is `attribute_exists(PK) AND attribute_exists(SK) AND #version = :expected_version AND #status = :active`; only supplied mutable fields are included in the `SET` expression.

- [x] **Step 7: Write failing replacement replay/conflict tests**

```python
@pytest.mark.asyncio
async def test_replace_rule_is_atomic_and_replays_same_request() -> None:
    first = await repository.replace_rule("tnt_1", "rule_old", 1, replacement, "request-123", request_hash, audit_context)
    replay = await repository.replace_rule("tnt_1", "rule_old", 1, replacement, "request-123", request_hash, audit_context)
    assert replay == first
    assert len(client.transact_write_items_calls) == 1


@pytest.mark.asyncio
async def test_replace_rule_rejects_idempotency_key_reuse_with_other_payload() -> None:
    await repository.replace_rule("tnt_1", "rule_old", 1, replacement, "request-123", request_hash, audit_context)
    with pytest.raises(ConflictError):
        await repository.replace_rule("tnt_1", "rule_old", 1, other_replacement, "request-123", other_hash, audit_context)
```

- [x] **Step 8: Implement atomic replacement and 24-hour idempotency**

The transaction updates the old rule, puts the replacement, puts one audit record, and puts an idempotency result conditioned on absence or expiry. Stored response snapshots allow exact replay while a mismatched request hash raises `ConflictError`.

- [x] **Step 9: Run adapter and no-scan tests and commit**

Run: `uv run --extra dev --python 3.12 python -m pytest tests/unit/test_alert_rule_repository.py tests/unit/test_no_scan_guard.py -q`

Expected: all Task 2 tests pass.

Run: `git add src/limnopulse_api/adapters/alert_rules.py tests/unit/test_alert_rule_repository.py tests/unit/test_no_scan_guard.py && git commit -m "feat: persist audited alert rules"`

### Task 3: Service, schemas, dependencies, and routes

**Files:**
- Create: `src/limnopulse_api/services/alert_rules.py`
- Create: `src/limnopulse_api/api/v1/schemas/alert_rules.py`
- Create: `src/limnopulse_api/api/v1/routers/alert_rules.py`
- Modify: `src/limnopulse_api/api/dependencies.py`
- Modify: `src/limnopulse_api/api/router.py`
- Modify: `src/limnopulse_api/main.py`
- Test: `tests/api/test_alert_rules.py`

**Interfaces:**
- Consumes: `DomainRepository`, `AlertRuleRepository`, `TenantAccess`, and Task 1 types.
- Produces: `AlertRuleService`, `AlertRuleCreate`, `AlertRuleUpdate`, `AlertRuleReplace`, `AlertRuleResponse`, `AlertRuleListResponse`, and `AlertRuleReplacementResponse`.

- [x] **Step 1: Write failing API authorization and validation tests**

```python
def test_viewer_can_list_but_cannot_create_alert_rules() -> None:
    assert client.get(path, headers=headers).status_code == 200
    assert client.post(path, json=create_payload, headers=headers).status_code == 403


def test_patch_rejects_empty_or_immutable_changes() -> None:
    assert client.patch(rule_path, json={"expected_version": 1}, headers=headers).status_code == 422
    assert client.patch(rule_path, json={"expected_version": 1, "metric": "ph"}, headers=headers).status_code == 422
```

- [x] **Step 2: Run API tests and verify RED**

Run: `uv run --extra dev --python 3.12 python -m pytest tests/api/test_alert_rules.py -q`

Expected: requests return `404` because the router is not registered.

- [x] **Step 3: Implement strict schemas, service validation, dependencies, and list/create/PATCH routes**

```python
class AlertRuleUpdate(BaseModel):
    model_config = ConfigDict(extra="forbid")
    expected_version: int = Field(ge=1)
    name: str | None = Field(default=None, min_length=1, max_length=120)
    operator: AlertOperator | None = None
    threshold: float | None = None
    aggregation: AlertAggregation | None = None
    window: AlertDuration | None = None
    duration: AlertDuration | None = None
    severity: AlertSeverity | None = None
    channels: tuple[AlertChannel, ...] | None = None
    cooldown_seconds: int | None = Field(default=None, ge=60, le=86_400)
    enabled: bool | None = None
```

`AlertRuleService.create` and `.replace` call `DomainRepository.get_pond` and, when applicable, `get_device`; any missing or pond-mismatched target raises `NotFoundError`.

- [x] **Step 4: Run focused list/create/PATCH tests and verify GREEN**

Run: `uv run --extra dev --python 3.12 python -m pytest tests/api/test_alert_rules.py -q`

Expected: list/create/PATCH tests pass.

- [x] **Step 5: Write failing replacement header and replay tests**

```python
def test_replace_requires_valid_idempotency_key() -> None:
    assert client.post(replace_path, json=replace_payload, headers=headers).status_code == 422


def test_replace_returns_old_and_new_rule_and_replays() -> None:
    first = client.post(replace_path, json=replace_payload, headers={**headers, "Idempotency-Key": "replace-123"})
    replay = client.post(replace_path, json=replace_payload, headers={**headers, "Idempotency-Key": "replace-123"})
    assert first.status_code == 201
    assert replay.json() == first.json()
```

- [x] **Step 6: Implement replace route and audit request context**

The router derives `AuditContext` from the authenticated principal, `request.client.host`, and `User-Agent`. It hashes canonical path/body identity through the service and forwards the required idempotency key to the repository.

- [x] **Step 7: Run API/full tests and commit**

Run: `uv run --extra dev --python 3.12 python -m pytest tests/api/test_alert_rules.py -q`

Expected: all Task 3 tests pass.

Run: `git add src/limnopulse_api/services/alert_rules.py src/limnopulse_api/api/v1/schemas/alert_rules.py src/limnopulse_api/api/v1/routers/alert_rules.py src/limnopulse_api/api/dependencies.py src/limnopulse_api/api/router.py src/limnopulse_api/main.py tests/api/test_alert_rules.py && git commit -m "feat: expose alert rule API"`

### Task 4: DynamoDB TTL infrastructure

**Files:**
- Modify: `infra/opentofu/dynamodb.tf`
- Modify: `scripts/dev/init_dynamodb.py`
- Modify: `tests/unit/test_opentofu_infra.py`
- Create: `tests/unit/test_init_dynamodb.py`

**Interfaces:**
- Produces: both cloud tables and local tables with DynamoDB TTL enabled on numeric `expires_at` values.

- [x] **Step 1: Write failing infrastructure tests**

```python
def test_cloud_domain_and_audit_tables_enable_expires_at_ttl() -> None:
    dynamodb = _read("dynamodb.tf")
    assert dynamodb.count('attribute_name = "expires_at"') == 2
    assert dynamodb.count("enabled        = true") >= 2


def test_existing_local_table_has_ttl_enabled() -> None:
    ensure_table(client, "LimnopulseDomain")
    assert client.update_time_to_live_calls == [{"TableName": "LimnopulseDomain", "TimeToLiveSpecification": {"Enabled": True, "AttributeName": "expires_at"}}]
```

- [x] **Step 2: Run infrastructure tests and verify RED**

Run: `uv run --extra dev --python 3.12 python -m pytest tests/unit/test_opentofu_infra.py tests/unit/test_init_dynamodb.py -q`

Expected: TTL assertions fail.

- [x] **Step 3: Enable TTL in OpenTofu and local initialization**

```hcl
ttl {
  attribute_name = "expires_at"
  enabled        = true
}
```

`ensure_table` calls `update_time_to_live` for both newly created and pre-existing tables, and treats an already-enabled TTL as success.

- [x] **Step 4: Run infrastructure tests and verify GREEN, then commit**

Run: `uv run --extra dev --python 3.12 python -m pytest tests/unit/test_opentofu_infra.py tests/unit/test_init_dynamodb.py -q`

Expected: all Task 4 tests pass.

Run: `git add infra/opentofu/dynamodb.tf scripts/dev/init_dynamodb.py tests/unit/test_opentofu_infra.py tests/unit/test_init_dynamodb.py && git commit -m "feat: enable DynamoDB record expiry"`

### Task 5: Architecture normalization and final verification

**Files:**
- Create: `docs/architecture.md`
- Modify: `README.md`

**Interfaces:**
- Documents the canonical Limnopulse naming and the boundary between delivered Phase 3A and target Phase 3B/3C behavior.

- [x] **Step 1: Normalize architecture documentation**

Document `LimnopulseDomain`, `LimnopulseAudit`, `limnopulse/v1/devices/...`, `limnopulse_raw`, current implemented components, Phase 3A Alert Rules, Phase 3B evaluator/cooldown, and Phase 3C dispatch/providers. Remove AquaFarm names while retaining the predecessor context only as migration history.

- [x] **Step 2: Update README usage examples**

Add the Alert Rules endpoint list, compact duration format, role requirements, replacement idempotency behavior, and local DynamoDB initialization command.

- [x] **Step 3: Run complete verification**

Run: `uv run --extra dev --python 3.12 python -m pytest -q`

Expected: all tests pass with no failures.

Run: `uv run --extra dev --python 3.12 python -m compileall -q src tests scripts`

Expected: exit code 0.

Run: `tofu -chdir=infra/opentofu fmt -check`

Expected: exit code 0.

Run: `git diff --check origin/main...HEAD`

Expected: exit code 0.

- [x] **Step 4: Review the diff and commit documentation**

Run: `git add README.md docs/architecture.md docs/superpowers/specs/2026-07-15-limnopulse-phase-3a-alert-rules-design.md docs/superpowers/plans/2026-07-15-limnopulse-phase-3a-alert-rules.md && git commit -m "docs: define alert rule milestone"`

Expected: the branch contains only Phase 3A code, tests, infrastructure, and documentation plus `.gitignore` worktree protection.
