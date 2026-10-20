import pytest

from src.config import PipelineConfig, detect_env


@pytest.fixture(autouse=True)
def clean_env(monkeypatch):
    for variable in ("PIPELINE_ENV", "DATABRICKS_RUNTIME_VERSION", "CATALOG", "LANDING_PATH"):
        monkeypatch.delenv(variable, raising=False)


def test_defaults_to_local_outside_databricks():
    assert detect_env() == "local"
    assert PipelineConfig.from_env().table("silver", "clientes") == "silver.clientes"


def test_detects_databricks_runtime(monkeypatch):
    monkeypatch.setenv("DATABRICKS_RUNTIME_VERSION", "client.1.13")
    config = PipelineConfig.from_env()
    assert config.env == "databricks"
    assert config.table("gold", "gold_resumo_cliente_mes") == "workspace.gold.gold_resumo_cliente_mes"
    assert config.landing_path == "/Volumes/workspace/novarota/landing"


def test_explicit_env_overrides_detection(monkeypatch):
    monkeypatch.setenv("DATABRICKS_RUNTIME_VERSION", "client.1.13")
    monkeypatch.setenv("PIPELINE_ENV", "local")
    assert PipelineConfig.from_env().env == "local"


def test_invalid_env_is_rejected(monkeypatch):
    monkeypatch.setenv("PIPELINE_ENV", "staging")
    with pytest.raises(ValueError):
        PipelineConfig.from_env()
