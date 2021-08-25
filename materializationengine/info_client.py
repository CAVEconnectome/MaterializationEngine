from materializationengine.errors import (
    AlignedVolumeNotFoundException,
    DataStackNotFoundException,
)
from materializationengine.utils import get_config_param
from flask import current_app
import requests
import os
from caveclient.infoservice import InfoServiceClient
from caveclient.auth import AuthClient
import cachetools.func


@cachetools.func.ttl_cache(maxsize=2, ttl=5 * 60)
def get_aligned_volumes():
    server = get_config_param("GLOBAL_SERVER")
    api_version = int(get_config_param("INFO_API_VERSION"))
    auth = AuthClient(server_address=server)
    infoclient = InfoServiceClient(
        server_address=server,
        auth_client=auth,
        api_version=api_version,
    )
    aligned_volume_names = infoclient.get_aligned_volumes()
    return aligned_volume_names


@cachetools.func.ttl_cache(maxsize=10, ttl=5 * 60)
def get_aligned_volume(aligned_volume):
    infoservice = current_app.config["INFOSERVICE_ENDPOINT"]
    url = os.path.join(infoservice, f"api/v2/aligned_volume/{aligned_volume}")
    r = requests.get(url)
    if r.status_code != 200:
        raise AlignedVolumeNotFoundException(
            f"aligned_volume {aligned_volume} not found"
        )
    else:
        return r.json()


@cachetools.func.ttl_cache(maxsize=2, ttl=5 * 60)
def get_datastacks():
    server = current_app.config["GLOBAL_SERVER"]
    auth = AuthClient(server_address=server)
    infoclient = InfoServiceClient(
        server_address=server,
        auth_client=auth,
        api_version=current_app.config.get("INFO_API_VERSION", 2),
    )
    datastack_names = infoclient.get_datastacks()
    datastack_names = [
        ds for ds in datastack_names if ds in current_app.config["DATASTACKS"]
    ]
    return datastack_names


@cachetools.func.ttl_cache(maxsize=10, ttl=5 * 60)
def get_datastack_info(datastack_name):
    server = current_app.config["GLOBAL_SERVER"]
    auth = AuthClient(server_address=server)
    infoclient = InfoServiceClient(
        server_address=server,
        auth_client=auth,
        api_version=current_app.config.get("INFO_API_VERSION", 2),
    )
    try:
        return infoclient.get_datastack_info(datastack_name=datastack_name)
    except requests.HTTPError:
        raise DataStackNotFoundException(
            f"datastack {datastack_name} info not returned"
        )
