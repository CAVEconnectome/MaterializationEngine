import cloudvolume


class CloudVolumeGateway:
    """A class to manage cloudvolume clients and cache them for reuse."""

    def __init__(self):
        self._cv_clients = {}

    def get_cv(self, seg_source: str, mip_level: int = 0) -> cloudvolume.CloudVolume:
        """A function to get a cloudvolume client for a given source.

        Args:
            seg_source (str): The cloudvolume source string.
            mip_level (int, optional): The MIP level to use. Defaults to 0.

        Returns:
            cloudvolume.CloudVolume: The cloudvolume client.
        """
        seg_source_key = seg_source.split("/")[-1]
        return (
            self._get_cv_client(seg_source, seg_source_key, mip_level)
            if seg_source_key not in self._cv_clients
            else self._cv_clients[seg_source_key]
        )

    def _get_cv_client(
        self, seg_source: str, seg_source_key: str, mip_level: int = 0
    ) -> cloudvolume.CloudVolume:
        """A helper function to create a cloudvolume client and cache it for reuse.

        Args:
            seg_source (str): The cloudvolume source string.
            seg_source_key (str): The cloudvolume source key name to use for caching.
            mip_level (int, optional): The MIP level to use. Defaults to 0.

        Returns:
            cloudvolume.CloudVolume: _description_
        """
        cv_client = cloudvolume.CloudVolume(
            seg_source,
            mip=mip_level,
            use_https=True,
            bounded=False,
            fill_missing=True,
            lru_bytes=int(10e9),
        )

        self._cv_clients[seg_source_key] = cv_client
        return self._cv_clients[seg_source_key]

    def invalidate_cache(self):
        """Clear the cache of cloudvolume clients."""
        self._cv_clients = {}


cloudvolume_cache = CloudVolumeGateway()
