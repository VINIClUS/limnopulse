DEVICE_REGISTRY = {
    "local-device-001": {
        "tenant_id": "tnt_local_001",
        "pond_id": "pond_local_001",
    },
}


def apply(metric):
    if "device_id" not in metric.tags:
        return None

    device_id = metric.tags["device_id"]
    if device_id not in DEVICE_REGISTRY:
        return None

    entry = DEVICE_REGISTRY[device_id]
    metric.tags["tenant_id"] = entry["tenant_id"]
    metric.tags["pond_id"] = entry["pond_id"]
    return metric
