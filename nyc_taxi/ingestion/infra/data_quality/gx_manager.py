from __future__ import annotations
import sys
from pathlib import Path
if __name__ == "__main__":
    PROJECT_ROOT = Path(__file__).resolve().parents[4]
    sys.path.insert(0, str(PROJECT_ROOT))
    print("Project root added to sys.path[0]:", sys.path[0])

from dotenv import load_dotenv
from typing import Optional, Dict, Any, List
import great_expectations as gx
from great_expectations.checkpoint import Checkpoint, UpdateDataDocsAction
from nyc_taxi.ingestion.config.settings import GXS3AssetSpec, S3Config
from nyc_taxi.ingestion.infra.data_quality.gx_context import GreatExpectationsContextFactory

class GreatExpectationsManager:
    """
    Owns the "end-to-end" GX workflow:
    - Ensure datasource + asset
    - Ensure suite + expectations
    - Ensure batch definition
    - Ensure validation definition
    - Ensure checkpoint (+ actions)
    - Run checkpoint (suitable for Airflow)
    """
    def __init__(self, context_factory: GreatExpectationsContextFactory):
        self._factory = context_factory

    def context(self) -> gx.DataContext:
        return self._factory.get_or_create_context()
    
    # ---------
    # Setup
    # ---------

    def ensure_s3_datasource_and_asset(self, spec: GXS3AssetSpec):
        ds = self._factory.get_or_create_pandas_s3_datasource(
            ds_name=spec.data_source_name,
            ds_bucket=spec.bucket,
            ds_region=spec.region,
        )
        asset = self._factory.get_or_create_csv_asset(
            datasource=ds,
            asset_name=spec.asset_name,
            s3_prefix=spec.s3_prefix,
            batching_regex=spec.batching_regex,
        )
        return ds, asset
    
if __name__ == "__main__":
    load_dotenv()  # Load environment variables from .env for testing
    factory = GreatExpectationsContextFactory()
    manager = GreatExpectationsManager(factory)
    context = manager.context()
    # print("GX Context loaded:", context)
    spec_config = GXS3AssetSpec(
        data_source_name="my_s3_datasource",
        bucket="my_bucket",
        region="us-west-2",
        asset_name="my_csv_asset",
        s3_prefix="data/csv/",
        batching_regex=r"(?P<file_name>.*)\.csv",
    )
    ds, asset = manager.ensure_s3_datasource_and_asset(spec_config)
    print("Datasource and Asset ensured:", ds, asset)
