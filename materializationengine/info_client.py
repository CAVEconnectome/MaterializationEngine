import os

import cachetools.func
import requests
from cachetools import LRUCache, TTLCache, cached
from caveclient.auth import AuthClient
from caveclient.infoservice import InfoServiceClient
from flask import abort, current_app

from materializationengine.errors import (
    AlignedVolumeNotFoundException,
    DataStackNotFoundException,
)
from materializationengine.utils import get_config_param


@cachetools.func.ttl_cache(maxsize=2, ttl=5 * 60)
def get_aligned_volumes():
    server = get_config_param("GLOBAL_SERVER_URL")
    api_version = int(get_config_param("INFO_API_VERSION"))
    auth = AuthClient(server_address=server, token=current_app.config["AUTH_TOKEN"])
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
    server = current_app.config["GLOBAL_SERVER_URL"]
    auth = AuthClient(server_address=server, token=current_app.config["AUTH_TOKEN"])
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

    server = current_app.config["GLOBAL_SERVER_URL"]
    auth = AuthClient(server_address=server, token=current_app.config["AUTH_TOKEN"])
    info_client = InfoServiceClient(
        server_address=server,
        auth_client=auth,
        api_version=current_app.config.get("INFO_API_VERSION", 2),
    )
    try:
        datastack_info = info_client.get_datastack_info(datastack_name=datastack_name)
        datastack_info["datastack"] = datastack_name
        return datastack_info
    except requests.HTTPError as e:
        raise DataStackNotFoundException(
            f"datastack {datastack_name} info not returned {e}"
        )


@cached(cache=TTLCache(maxsize=64, ttl=600))
def get_relevant_datastack_info(datastack_name):
    ds_info = get_datastack_info(datastack_name=datastack_name)
    seg_source = ds_info["segmentation_source"]
    pcg_table_name = seg_source.split("/")[-1]
    aligned_volume_name = ds_info["aligned_volume"]["name"]
    return aligned_volume_name, pcg_table_name
