class DynamoKeyBuilder:
    def tenant(self, tenant_id: str) -> dict[str, str]:
        return {"PK": f"TENANT#{tenant_id}", "SK": "META"}

    def pond(self, tenant_id: str, pond_id: str) -> dict[str, str]:
        return {"PK": f"TENANT#{tenant_id}", "SK": f"POND#{pond_id}"}

    def device(self, tenant_id: str, device_id: str) -> dict[str, str]:
        return {"PK": f"TENANT#{tenant_id}", "SK": f"DEVICE#{device_id}"}

    def device_lookup(self, device_id: str) -> dict[str, str]:
        return {"PK": f"DEVICE#{device_id}", "SK": "META"}

    def membership(self, cognito_sub: str, tenant_id: str) -> dict[str, str]:
        return {"PK": f"USER#{cognito_sub}", "SK": f"TENANT#{tenant_id}"}

    def tenant_member(self, tenant_id: str, cognito_sub: str) -> dict[str, str]:
        return {"PK": f"TENANT#{tenant_id}", "SK": f"MEMBER#{cognito_sub}"}
