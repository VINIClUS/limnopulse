from uuid import uuid4


def new_tenant_id() -> str:
    return f"tnt_{uuid4().hex}"


def new_pond_id() -> str:
    return f"pond_{uuid4().hex}"


def new_device_id() -> str:
    return f"dev_{uuid4().hex}"
