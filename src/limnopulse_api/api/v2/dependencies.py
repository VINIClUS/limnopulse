from typing import Annotated, Any

from fastapi import Depends, HTTPException, Request

from limnopulse_api.repositories.v2_contracts import (
    ComponentReader,
    DeploymentRepository,
    DeviceComponentRepository,
    DeviceReader,
    IntegrationRepository,
    SiteAssetReader,
    SiteAssetRepository,
)


def _get_state_dependency(request: Request, attribute_name: str) -> Any:
    value = getattr(request.app.state, attribute_name, None)
    if value is None:
        raise HTTPException(status_code=503, detail="service unavailable")
    return value


def get_site_asset_repository(request: Request) -> SiteAssetRepository:
    return _get_state_dependency(request, "site_asset_repository")


def get_site_asset_reader(request: Request) -> SiteAssetReader:
    return _get_state_dependency(request, "site_asset_repository")


def get_device_component_repository(request: Request) -> DeviceComponentRepository:
    return _get_state_dependency(request, "device_component_repository")


def get_device_reader(request: Request) -> DeviceReader:
    return _get_state_dependency(request, "device_component_repository")


def get_component_reader(request: Request) -> ComponentReader:
    return _get_state_dependency(request, "device_component_repository")


def get_integration_repository(request: Request) -> IntegrationRepository:
    return _get_state_dependency(request, "integration_repository")


def get_deployment_repository(request: Request) -> DeploymentRepository:
    return _get_state_dependency(request, "deployment_repository")


SiteAssetRepositoryDep = Annotated[SiteAssetRepository, Depends(get_site_asset_repository)]
SiteAssetReaderDep = Annotated[SiteAssetReader, Depends(get_site_asset_reader)]
DeviceComponentRepositoryDep = Annotated[
    DeviceComponentRepository,
    Depends(get_device_component_repository),
]
DeviceReaderDep = Annotated[DeviceReader, Depends(get_device_reader)]
ComponentReaderDep = Annotated[ComponentReader, Depends(get_component_reader)]
IntegrationRepositoryDep = Annotated[IntegrationRepository, Depends(get_integration_repository)]
DeploymentRepositoryDep = Annotated[DeploymentRepository, Depends(get_deployment_repository)]
