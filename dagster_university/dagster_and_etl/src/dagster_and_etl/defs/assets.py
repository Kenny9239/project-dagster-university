# src/dagster_and_etl/defs/assets.py
import dagster as dg
from pathlib import Path

class IngestionFileConfig(dg.Config):
    path: str

@dg.asset()
def import_file(context: dg.AssetExecutionContext, config: IngestionFileConfig) -> str:
    file_path = (
        Path(__file__).absolute().parent / f"../../../data/source/{config.path}"
    )
    return str(file_path.resolve())
