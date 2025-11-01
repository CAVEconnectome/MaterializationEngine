import os

import cloudvolume


class CloudVolumeGateway:
    """A class to manage cloudvolume clients and cache them for reuse."""

    def __init__(self, lru_bytes: int = 1024 * 1024 * 64 ):
        self._cv_clients = {}
        self._lru_bytes = lru_bytes


    def get_cv(self, seg_source: str, mip_level: int = 0, use_green_threads: bool = False) -> cloudvolume.CloudVolume:
        """A function to get a cloudvolume client for a given source.

        Args:
            seg_source (str): The cloudvolume source string.
            mip_level (int, optional): The MIP level to use. Defaults to 0.

        Returns:
            cloudvolume.CloudVolume: The cloudvolume client.
        """
        seg_source_key = seg_source.split("/")[-1]
        if use_green_threads:
            seg_source_key = f"{seg_source}_green_threads"
        
        
        return (
            self._get_cv_client(seg_source, seg_source_key, mip_level, use_green_threads)
            if seg_source_key not in self._cv_clients
            else self._cv_clients[seg_source_key]
        )

    def _get_cv_client(
        self, seg_source: str, seg_source_key: str, mip_level: int = 0, use_green_threads: bool = False
    ) -> cloudvolume.CloudVolume:
        """A helper function to create a cloudvolume client and cache it for reuse.

        Args:
            seg_source (str): The cloudvolume source string.
            seg_source_key (str): The cloudvolume source key name to use for caching.
            mip_level (int, optional): The MIP level to use. Defaults to 0.
            use_green_threads (bool, optional): Whether to use green threads. Defaults to False.

        Returns:
            cloudvolume.CloudVolume: _description_
        """
        if use_green_threads:
            import gevent.monkey
            gevent.monkey.patch_all(threads=False)
          
        cv_client = cloudvolume.CloudVolume(
            seg_source,
            mip=mip_level,
            use_https=True,
            bounded=False,
            fill_missing=True,
            green_threads=use_green_threads,
            lru_bytes=self._lru_bytes,
            lru_encoding='crackle',
        )

        self._cv_clients[seg_source_key] = cv_client
        return self._cv_clients[seg_source_key]

    def invalidate_cache(self):
        """Clear the cache of cloudvolume clients."""
        self._cv_clients = {}


cloudvolume_cache = CloudVolumeGateway(
    lru_bytes=int(os.environ.get("CELERY_CLOUDVOLUME_CACHE_BYTES", 1024 * 1024 * 100))
)
