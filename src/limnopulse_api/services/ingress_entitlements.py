"""The pre-enqueue entitlement hook (P3 D13).

Direct HTTPS ingress is on every tier (§18.3), so the default `IngressEntitlementCheck` allows
everything. Enforcement against EntitlementSnapshot is DEFERRED to P4 (#20); a P4 adapter
replaces this class behind the same port, which the ingress service already calls before the
receipt claim and the queue write.
"""


class AllowDirectIngressEntitlements:
    async def allows(
        self, tenant_id: str, integration_account_id: str, observation_count: int
    ) -> bool:
        return True
