from __future__ import annotations
import sys
from pathlib import Path
if __name__ == "__main__":
    PROJECT_ROOT = Path(__file__).resolve().parents[4]
    sys.path.insert(0, str(PROJECT_ROOT))
    print("Project root added to sys.path[0]:", sys.path[0])

from typing import Any
from great_expectations.core import ExpectationSuite
from dotenv import load_dotenv
import great_expectations as gx
from great_expectations.datasource.fluent import PandasS3Datasource
from great_expectations.checkpoint import Checkpoint, UpdateDataDocsAction
from great_expectations.core.batch import Batch
from nyc_taxi.ingestion.config.settings import GXS3AssetSpec, GXValidationSpec, S3Config, GXCheckpointSpec
from nyc_taxi.ingestion.infra.data_quality.gx_context_factory import GreatExpectationsContextFactory

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
    # 1) Ensure datasource + asset
    def ensure_s3_datasource_and_assets(self, s3_config: S3Config)-> tuple[PandasS3Datasource, Any, Any]:
        try:
            # -------------------------------------------------------------------------------------------------------------------------------
            # Datasource
            # -------------------------------------------------------------------------------------------------------------------------------
            ds_name = f"s3_{s3_config.bucket_name}_{s3_config.s3_base_prefix_name}"
            ds = self._factory.get_or_create_pandas_s3_datasource(ds_name , **s3_config.to_connector_kwarg())

            #-------------------------------------------------------------------------------------------------------------------------------
            # Asset: CSV
            #-------------------------------------------------------------------------------------------------------------------------------
            dc_asset_csv = GXS3AssetSpec(
                s3conf = s3_config,
                data_source = ds,
                data_source_name = ds.name,
                asset_name = "raw_csv",
                s3_prefix_relative = "csv/",  # Relative folder (combined with base prefix)
                batching_regex = r"(?P<file_name>.*)\.csv",
                asset_type = "csv"
            )
            data_asset_csv = self._factory.get_or_create_asset( data_source = dc_asset_csv.data_source, **dc_asset_csv.to_kwarg())

            #-------------------------------------------------------------------------------------------------------------------------------
            # Asset: Parquet
            #-------------------------------------------------------------------------------------------------------------------------------
            dc_asset_parquet = GXS3AssetSpec(
                s3conf = s3_config,
                data_source = ds,
                data_source_name = ds.name,
                asset_name="raw_parquet",
                s3_prefix_relative="parquet/",  # Relative folder (combined with base prefix)
                batching_regex = r"(?P<file_name>.*)\.parquet",
                asset_type = "parquet"
            )
            data_asset_parquet = self._factory.get_or_create_asset( data_source = dc_asset_parquet.data_source, **dc_asset_parquet.to_kwarg())
            print(f"--> Ensured S3 Datasource '{ds.name}' with Assets: '{data_asset_csv.name}', '{data_asset_parquet.name}'")
            return ds
        except Exception as e:
            print("Error ensuring S3 Datasource and Asset:", e)
            raise RuntimeError() from e
    
    # 2) Ensure suite
    def ensure_s3_landing_suite(self, suite_name: str) -> ExpectationSuite:
        """
        Ensure the landing Expectation Suite exists with predefined expectations and metadata.
        Args:
            context (gx.DataContext): The Great Expectations DataContext.
            suite_name (str): The name of the landing Expectation Suite.
        Returns:
            None
        """
        try:
            expectations = self._factory.build_landing_expectations()
            meta = self._factory.build_suite_meta(layer="bronze", managed_by="code", suite_version="v1", extra={"domain": "landing"})
            suite = self._factory.get_or_create_suite(suite_name, expectations=expectations, meta=meta, overwrite_expectations=True)
            print(f"--> Ensured landing Expectation Suite '{suite_name}'.")
            return suite
        except Exception as e:
            print("Error ensuring landing Expectation Suite:", e)
            raise RuntimeError() from e

    # 3) Ensure batch definition
    def ensure_batch_definition(self, dc_asset: GXS3AssetSpec)-> list[Any]:
        try:
            batch_requests = factory.get_asset_batch_requests( datasource = ds, asset_name = dc_asset.asset_name,
                                                                asset_type = dc_asset.asset_type)
            print(f'--> Ensured batch definition: {dc_asset.asset_name}')
        except Exception as e:
            print("Error ensuring batch definitions:", e)
            raise RuntimeError() from e
        return batch_requests

    # 4) Ensure validation definition
    def ensure_validation_definition(self, validation_spec: GXValidationSpec, expectation_suite: ExpectationSuite, 
                                     bach_request_list: list[Any]):
        
        
        



        # Idempotent create: get or add
        # try:
        #     return self._context.validation_definitions.get(validation_spec.validation_definition_name)
        # except Exception:
        #     vd = gx.ValidationDefinition(
        #         name=validation_spec.validation_definition_name,
        #         data=bd,
        #         suite=suite,
        #     )
        #     self._context.validation_definitions.add(vd)
        #     return vd
    
    # 5) Final assembly method (thin orchestrator)
    def ensure_all_for_validation(self, s3_config: GXS3AssetSpec, asset_spec: GXS3AssetSpec, validation_spec: GXValidationSpec):
        """
        Ensure all GX components for a given validation spec:
        - Datasource + Asset
        - Suite (landing)
        - Validation Definition
        Returns the ensured Validation Definition.   
        """
        suite = self.ensure_s3_landing_suite(validation_spec.suite_name)
        vd = self.ensure_validation_definition(
            validation_spec=validation_spec,
            datasource_name=asset_spec.datasource_name,
            asset_name=asset_spec.asset_name,
        )
        return vd

#%%
    
if __name__ == "__main__":
    load_dotenv()
    factory = GreatExpectationsContextFactory()
    manager = GreatExpectationsManager(factory)    
    s3_config = S3Config.from_env() 

    ds = manager.ensure_s3_datasource_and_assets(s3_config)

    dc_asset_csv = GXS3AssetSpec(
        s3conf=s3_config,
        data_source=ds,
        data_source_name=ds.name,
        asset_name="raw_csv",
        s3_prefix_relative="csv/",  # Relative folder (combined with base prefix)
        batching_regex=r"(?P<file_name>.*)\.csv",
        asset_type="csv"
    )

    dc_asset_parquet = GXS3AssetSpec(
        s3conf=s3_config,
        data_source=ds,
        data_source_name=ds.name,
        asset_name="raw_parquet",
        s3_prefix_relative="parquet/",  # Relative folder (combined with base prefix)
        batching_regex=r"(?P<file_name>.*)\.parquet",
        asset_type="parquet"
    )
    
    gx_valid_land_spec = GXValidationSpec(
        suite_name="landing_gx_suite",
        validation_definition_name="landing_basic_validation_def",
        batch_definition_name="landing_basic_batch_def",
        batch_options={"file_name": "sample.csv"}
    )  

    expectatio_suite = manager.ensure_s3_landing_suite(gx_valid_land_spec.suite_name)
    asset_parquet = manager.ensure_batch_definition(dc_asset=dc_asset_parquet)
    asset_csv = manager.ensure_batch_definition(dc_asset=dc_asset_csv)
    validating_landing_csv = manager.ensure_validation_definition(validation_spec=gx_valid_land_spec)