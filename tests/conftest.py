import pytest

from src.config import PipelineConfig
from src.utils.spark import create_schemas, get_spark


@pytest.fixture(scope="session")
def config(tmp_path_factory):
    return PipelineConfig.local(base_path=tmp_path_factory.mktemp("novarota"))


@pytest.fixture(scope="session")
def spark(config):
    session = get_spark(config, app_name="novarota-tests")
    create_schemas(session, config)
    yield session
    session.stop()
