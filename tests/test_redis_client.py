"""Every module shares one Redis client (one connection pool) per process per database."""

from unittest import mock

import pytest
from flask import Flask

from materializationengine import redis_client


@pytest.fixture(autouse=True)
def fresh_clients():
    redis_client.reset_redis_clients()
    yield
    redis_client.reset_redis_clients()


class TestSharedRedisClient:
    """get_redis_client / SharedRedis replace the per-module and per-call clients."""

    def test_one_client_per_database(self):
        assert redis_client.get_redis_client(0) is redis_client.get_redis_client(0)
        assert redis_client.get_redis_client(0) is not redis_client.get_redis_client(1)
        assert redis_client.get_redis_client(1).connection_pool.connection_kwargs["db"] == 1

    def test_module_clients_share_the_pool(self):
        from materializationengine import monitor, task
        from materializationengine.blueprints.upload import api, checkpoint_manager, tasks

        db0 = redis_client.get_redis_client(0)
        for module in (task, monitor, api, tasks):
            assert module.REDIS_CLIENT.connection_pool is db0.connection_pool, module.__name__
        assert checkpoint_manager.REDIS_CLIENT.connection_pool is (
            redis_client.get_redis_client(1).connection_pool
        )

    def test_settings_come_from_app_config_at_first_use(self):
        app = Flask(__name__)
        app.config.update(REDIS_HOST="10.1.2.3", REDIS_PORT="6380", REDIS_PASSWORD="pw")
        with app.app_context():
            kwargs = redis_client.get_redis_client(0).connection_pool.connection_kwargs
        assert (kwargs["host"], kwargs["port"], kwargs["password"]) == ("10.1.2.3", "6380", "pw")

    def test_fails_fast_and_checks_idle_connections(self):
        kwargs = redis_client.get_redis_client(0).connection_pool.connection_kwargs
        assert kwargs["socket_timeout"] == redis_client.SOCKET_TIMEOUT_SECONDS
        assert kwargs["socket_connect_timeout"] == redis_client.SOCKET_CONNECT_TIMEOUT_SECONDS
        assert kwargs["socket_keepalive"] is True
        assert kwargs["health_check_interval"] == redis_client.HEALTH_CHECK_INTERVAL_SECONDS

    def test_empty_password_is_not_sent(self):
        app = Flask(__name__)
        app.config.update(REDIS_HOST="h", REDIS_PORT="6379", REDIS_PASSWORD="")
        with app.app_context():
            assert redis_client.get_redis_client(0).connection_pool.connection_kwargs["password"] is None

    def test_per_call_helpers_reuse_the_shared_client(self):
        from materializationengine import throttle
        from materializationengine.workflows import deltalake_export

        fake = mock.MagicMock()
        fake.llen.return_value = 3
        fake.info.return_value = {"used_memory": 42}
        with mock.patch.object(redis_client, "_clients", {0: fake}), \
                mock.patch("redis.StrictRedis") as constructed:
            assert throttle.get_queue_length("process") == 3
            assert throttle.get_redis_memory_usage() == 42
            assert deltalake_export._get_redis_client() is fake
        constructed.assert_not_called()
