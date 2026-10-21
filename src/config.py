import os
from dataclasses import dataclass, field
from datetime import date
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]

SCHEMAS = {
    "bronze": "bronze",
    "silver": "silver",
    "gold": "gold",
}

ENTITIES = ("clientes", "contas", "cartoes", "transacoes_cartao", "estornos")

LANDING_SCHEMA = "novarota"
LANDING_VOLUME = "landing"


def landing_file_name(entity: str, reference_date: date) -> str:
    if entity == "transacoes_cartao":
        return f"transacoes_cartao_{reference_date:%Y%m%d}.csv"
    return f"{entity}.csv"


def detect_env() -> str:
    if os.getenv("PIPELINE_ENV"):
        return os.environ["PIPELINE_ENV"].lower()
    return "databricks" if os.getenv("DATABRICKS_RUNTIME_VERSION") else "local"


@dataclass(frozen=True)
class PipelineConfig:
    env: str
    landing_path: str
    catalog: str | None = None
    warehouse_path: str | None = None
    schemas: dict = field(default_factory=lambda: dict(SCHEMAS))

    @classmethod
    def local(cls, base_path: Path | str | None = None) -> "PipelineConfig":
        base = Path(base_path) if base_path else PROJECT_ROOT
        return cls(
            env="local",
            landing_path=os.getenv("LANDING_PATH", str(PROJECT_ROOT / "data" / "landing")),
            warehouse_path=str(base / "lakehouse"),
        )

    @classmethod
    def databricks(cls) -> "PipelineConfig":
        catalog = os.getenv("CATALOG", "workspace")
        return cls(
            env="databricks",
            landing_path=os.getenv("LANDING_PATH", f"/Volumes/{catalog}/{LANDING_SCHEMA}/{LANDING_VOLUME}"),
            catalog=catalog,
        )

    @classmethod
    def from_env(cls) -> "PipelineConfig":
        env = detect_env()
        if env == "databricks":
            return cls.databricks()
        if env == "local":
            return cls.local()
        raise ValueError(f"Invalid PIPELINE_ENV: {env}")

    def schema(self, layer: str) -> str:
        schema = self.schemas[layer]
        return f"{self.catalog}.{schema}" if self.catalog else schema

    def table(self, layer: str, name: str) -> str:
        return f"{self.schema(layer)}.{name}"

    def landing_folder(self, reference_date: date) -> str:
        return f"{self.landing_path}/{reference_date.isoformat()}"
