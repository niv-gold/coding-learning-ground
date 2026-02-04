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
from great_expectations.checkpoint import Checkpoint
from great_expectations.core.batch import Batch
from nyc_taxi.ingestion.config.settings import GXS3AssetSpec, GXValidationSpec, S3Config, GXCheckpointSpec, GeneralConfig
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
        self.general_config: GeneralConfig = GeneralConfig.from_env()
        self.info_msg_prefix: str = self.general_config.info_msg_prefix
        self.error_msg_prefix: str = self.general_config.error_msg_prefix
        self._context = self._factory.get_or_create_context()
    
    
    def ensure_s3_datasource(self, s3conf: S3Config, )-> None:
        try:
            # -------------------------------------------------------------------------------------------------------------------------------
            # Datasource
            # -------------------------------------------------------------------------------------------------------------------------------
            ds_name = f"s3_{s3conf.bucket_name}.{s3conf.s3_base_prefix_name}"
            ds = self._factory.get_or_create_datasource_s3(ds_name=ds_name, s3conf=s3conf)      

            # -------------------------------------------------------------------------------------------------------------------------------
            # refetch datasource (avoid stale reference)
            # -------------------------------------------------------------------------------------------------------------------------------
            ds = self._context.data_sources.get(ds_name) 
            return ds


            # print(f"{self.info_msg_prefix} Ensured s3 Datasource '{ds.name}' with Assets: '{data_asset_csv.name}', '{data_asset_parquet.name}'")
        except Exception as e:
            print(f"{self.error_msg_prefix} ensuring s3 Datasource: {e}")
            raise RuntimeError() from e
    

    def ensure_s3_raw_asset(self, datasource: PandasS3Datasource, dc_asset_csv: GXS3AssetSpec, dc_asset_parquet: GXS3AssetSpec) -> ExpectationSuite:
        #-------------------------------------------------------------------------------------------------------------------------------
        # Asset: CSV
        #-------------------------------------------------------------------------------------------------------------------------------
        self._factory.get_or_create_asset( data_source = datasource, asset_spec = dc_asset_csv)
        
        #-------------------------------------------------------------------------------------------------------------------------------
        # Asset: Parquet
        #-------------------------------------------------------------------------------------------------------------------------------
        self._factory.get_or_create_asset( data_source = datasource, asset_spec = dc_asset_parquet)
        

    def ensure_s3_raw_assets_batch_definition(self, data_source: PandasS3Datasource, dc_asset: GXS3AssetSpec) -> None:
        #-------------------------------------------------------------------------------------------------------------------------------
        # Batch Definition: CSV
        #-------------------------------------------------------------------------------------------------------------------------------
        self._factory.get_or_create_asset_batch_definition( data_source = data_source, asset_spec = dc_asset)


    def ensure_s3_raw_suites(self, suite_name: str) -> ExpectationSuite:
        """
        Ensure the landing Expectation Suite exists with predefined expectations and metadata.
        Args:
            context (gx.DataContext): The Great Expectations DataContext.
            suite_name (str): The name of the landing Expectation Suite.
        Returns:
            None
        """
        try:
            # expectations = self._factory.build_landing_expectations()
            meta = self._factory.build_suite_meta(layer="bronze", managed_by="code", suite_version="v1", extra={"domain": "landing"})
            suite = self._factory.get_or_create_suite(suite_name, meta=meta)
            return suite
        except Exception as e:
            print(f"{self.error_msg_prefix} Suite creation failed: {e}")
            raise RuntimeError() from e


    def ensure_s3_raw_validation_Definition(self, vd_spec: GXValidationSpec, dc_asset: GXS3AssetSpec, data_source: PandasS3Datasource) -> list[dict]:        
        """"
        Ensure validation definitions exist for the given validation spec and data asset, then creating validators.
        Args:
            vd_spec (GXValidationSpec): Specification of the validation.
            dc_asset (GXS3AssetSpec): Specification of the data asset.
            datasource (PandasS3Datasource): The data source containing the asset.
        Returns:
            list[Any]: List of created validators for the validation.      
        """
        try: 
            validator = manager._factory.build_Validation_Definition(
                data_source_name=data_source.name,
                asset_name=dc_asset.asset_name,
                suite_name=vd_spec.suite_name
            )            
            # validating_br_list = manager._factory.build_validator_validation_br_list(validator=validator)      
            print(f"{self.info_msg_prefix} Ensured validation definition: {vd_spec.validation_id} for asset: {dc_asset.asset_name}")         
        except Exception as e:
            print(f"{self.error_msg_prefix} ensuring validation definition: {e}")
            raise RuntimeError() from e
        return validator # validating_br_list 
    

    def ensure_checkpoint(self, ds_name: str, checkpoint_spec: GXCheckpointSpec, vd_csv_spec: GXValidationSpec, vd_parquet_spec: GXValidationSpec) -> Checkpoint:
        """
        Ensure the checkpoint exists with specified actions.
        Args:
            checkpoint_spec (GXCheckpointSpec): Specification of the checkpoint.
            action_list (list[dict], optional): List of action dictionaries to include in the checkpoint.
        Returns:
            Checkpoint: The ensured checkpoint object.
        """
        try:
            # actions_list = self._factory._build_action_list(checkpoint_spec)     
            # checkpoint = self._factory.add_or_update_checkpoint(
            #     checkpoint_name=checkpoint_spec.checkpoint_name,
            #     validations=validations,
            #     action_list=actions_list
            # )
            manager._factory.upsert_checkpoint_for_csv_and_parquet(
                checkpoint_name=checkpoint_spec.checkpoint_name,
                data_source_name=ds_name,
                csv_asset_name=vd_csv_spec.asset_spec.asset_name,
                csv_suite_name=vd_csv_spec.suite_name,
                parquet_asset_name=vd_parquet_spec.asset_spec.asset_name,
                parquet_suite_name=vd_parquet_spec.suite_name
            )
            print(f"{self.info_msg_prefix} Ensured Checkpoint '{checkpoint_spec.checkpoint_name}'.")
            return checkpoint

        except Exception as e:
            print(f"{self.error_msg_prefix} ensuring checkpoint: {e}")
            raise RuntimeError() from e
    
    def run_checkpoint(self, checkpoint: Checkpoint) -> gx.checkpoint.checkpoint_result.CheckpointResult:
        """
        Run the specified checkpoint and return the result.
        Args:
            checkpoint (Checkpoint): The checkpoint to run. 
        Returns:
            gx.checkpoint.checkpoint_result.CheckpointResult: The result of the checkpoint run.
        """
        try:
            result = checkpoint.run()
            print(f"{self.info_msg_prefix} Checkpoint '{checkpoint.name}' run completed.")
            return result
        except Exception as e:
            print(f"{self.error_msg_prefix} running checkpoint: {e}")
            raise RuntimeError() from e

#%%
    
if __name__ == "__main__":

    # Simple local test harness
    # Initialize GX manager
    load_dotenv()
    factory = GreatExpectationsContextFactory()
    manager = GreatExpectationsManager(factory)        
    s3conf = S3Config.from_env() 

    # Define data assets:
    asset_spec_csv = GXS3AssetSpec(
        s3conf=s3conf,
        asset_name="raw_csv",
        s3_prefix_relative="csv/",  # Relative folder (combined with base prefix)
        asset_type="csv",
        batch_definition_name="raw_csv_bd"
    )
    
    asset_spec_parquet = GXS3AssetSpec(
        s3conf=s3conf,
        asset_name="raw_parquet",
        s3_prefix_relative="parquet/",  # Relative folder (combined with base prefix)
        asset_type="parquet",
        batch_definition_name="raw_parquet_bd"
    )
    
    # CSV Validation Spec
    vd_csv_spec = GXValidationSpec(
        validation_id="landing_validation_csv",
        suite_name="landing_suite_csv",
        asset_spec=asset_spec_csv,
        batch_request_options=None,
        checkpoint_name="s3_raw_checkpoint",        
    )  

    # Parquet Validation Spec
    vd_parquet_spec = GXValidationSpec(
        validation_id="landing_validation_parquet",
        suite_name="landing_suite_parquet",
        asset_spec=asset_spec_parquet,
        batch_request_options=None,
        checkpoint_name="s3_raw_checkpoint",        
    ) 

    # Checkpoint Spec
    checkp_spec_landing_data_in_s3 = GXCheckpointSpec(
        checkpoint_name="s3_raw_checkpoint",
        data_docs_site_name="local_site",
        build_data_docs=True,
        vd_spec_list = [vd_csv_spec, vd_parquet_spec]
    )

    # Ensure Datasource S3 
    ds_s3_raw = manager.ensure_s3_datasource(s3conf)

    # Ensure Assets
    manager.ensure_s3_raw_asset(datasource=ds_s3_raw, dc_asset_csv=asset_spec_csv, dc_asset_parquet=asset_spec_parquet)

    # Ensure Batch Definitions:
    # CSV
    manager.ensure_s3_raw_assets_batch_definition(data_source=ds_s3_raw, dc_asset=asset_spec_csv)
    # Parquet
    manager.ensure_s3_raw_assets_batch_definition(data_source=ds_s3_raw, dc_asset=asset_spec_parquet)

    # Ensure Suites:
    # CSV
    manager.ensure_s3_raw_suites(vd_csv_spec.suite_name) 
    # Parquet
    manager.ensure_s3_raw_suites(vd_parquet_spec.suite_name) 

    # Ensure Validations:
    # CSV
    manager.ensure_s3_raw_validation_Definition(vd_spec=vd_csv_spec, dc_asset=asset_spec_csv, data_source=ds_s3_raw)
    # Parquet
    # manager.ensure_s3_raw_validation_Definition(vd_spec=vd_parquet_spec, dc_asset=asset_spec_parquet, data_source=ds_s3_raw)