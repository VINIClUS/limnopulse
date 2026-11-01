"""boto3 client kwargs shared by the API (`main.py`) and the telemetry ingest worker, which
re-exports both names (#116: one helper instead of a copy in each).

A configured endpoint (DynamoDB Local, ElasticMQ) is passed through; dummy `local` credentials
are added only for such an endpoint in `local`/`test`, never in a deployed environment, where the
default credential chain applies.
"""

from limnopulse_api.core.config import Settings

_LOCAL_ENVIRONMENTS = frozenset({"local", "test"})


def _client_kwargs(settings: Settings, endpoint_url: str | None) -> dict[str, str]:
    kwargs: dict[str, str] = {"region_name": settings.aws_region}
    if endpoint_url is not None:
        kwargs["endpoint_url"] = endpoint_url
        if settings.app_env in _LOCAL_ENVIRONMENTS:
            kwargs["aws_access_key_id"] = "local"
            kwargs["aws_secret_access_key"] = "local"
    return kwargs


def dynamodb_client_kwargs(settings: Settings) -> dict[str, str]:
    return _client_kwargs(settings, settings.dynamodb_endpoint_url)


def sqs_client_kwargs(settings: Settings) -> dict[str, str]:
    return _client_kwargs(settings, settings.sqs_endpoint_url)
