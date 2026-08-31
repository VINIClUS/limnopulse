# LimnoPulse Current-State Inventory

**Execution baseline:** `main@141e108a479c983ed3a5efcbe729a30a43ab0ecb`  
**Runtime baseline:** `ce46b47fd646de762098a632b12e02d482c66485`  
**Reconciliation:** `141e108` adds the approved V4 planning documents and does not change runtime behavior.

| Surface | Status | Evidence | V4 treatment | Owning phase |
|---|---|---|---|---|
| FastAPI control plane | `implemented` | `src/limnopulse_api/main.py` | Preserve the existing API and composition boundary. | Phase 0 baseline |
| Tenant membership authorization | `implemented` | `src/limnopulse_api/api/dependencies.py` | Preserve as a core invariant; authenticated identity is not tenant authority. | Phase 0 baseline |
| Pond/Device v1 and Influx reads | `implemented` | `tests/api/test_ponds_devices.py`, `tests/api/test_telemetry.py` | Preserve `/v1`; add the generalized model behind `/v2` and retain legacy reads during telemetry migration. | Phases 1–2 |
| Alert evaluator and durable notifications | `implemented` | `internal/alertevaluator/state_machine.go`, `internal/notifications/durable_model.go` | Preserve the evaluator and ledger; generalize metric, destination, policy, and provider boundaries additively. | Phases 6–7A |
| MQTT/Telegraf/Starlark registry | `local` | `tests/unit/test_local_ingestion_config.py` | Keep for local lab and compatibility; production moves to a trusted queue and normalizer path. | Phase 3 |
| OpenTofu cloud foundation | `scaffold` | `tests/unit/test_opentofu_infra.py` | Extend incrementally with phase-owned resources; scaffold is not deployed capability. | Phase 3 onward |
| Site/Asset/Component/Deployment | `planned` | `docs/superpowers/specs/2026-08-16-limnopulse-platform-redesign-tech-spec-v4.md` | Add behind `/v2` with additive storage and `/v1` compatibility projection. | Phase 1 |
| Billing/AWS IoT/Push/SMS/commands | `planned` | `docs/superpowers/specs/2026-08-16-limnopulse-platform-redesign-tech-spec-v4.md` | Add through canonical internal contracts and replaceable provider or safety adapters. | Phases 4, 5, 7B, 7C, 8 |
| Device permanently bound to a pond | `obsolete` | `src/limnopulse_api/domain/entities.py` | Replace canonical v2 `pond_id` with temporal Deployment while projecting legacy behavior. | Phase 1 |

The execution baseline is the approved V4 planning point. The task branch starts from newer `main@e95d3eee9c813e3946d43f297071a80b62dd123b`; changes between those revisions are documentation-only, so the runtime baseline remains unchanged.
