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
    
    def context(self) -> gx.DataContext:
        return self._factory.get_or_create_context()
    
    # ---------
    # Setup
    # ---------
    # 1) Ensure datasource + asset
    def ensure_s3_datasource_and_assets(self, s3_config: S3Config, dc_asset_csv: GXS3AssetSpec, dc_asset_parquet: GXS3AssetSpec)-> tuple[PandasS3Datasource, Any, Any]:
        try:
            # -------------------------------------------------------------------------------------------------------------------------------
            # Datasource
            # -------------------------------------------------------------------------------------------------------------------------------
            ds_name = f"s3_{s3_config.bucket_name}_{s3_config.s3_base_prefix_name}"
            ds = self._factory.get_or_create_pandas_s3_datasource(ds_name , **s3_config.to_connector_kwarg())

            #-------------------------------------------------------------------------------------------------------------------------------
            # Asset: CSV
            #-------------------------------------------------------------------------------------------------------------------------------
            data_asset_csv = self._factory.get_or_create_asset( data_source = ds, **dc_asset_csv.to_kwarg())

            #-------------------------------------------------------------------------------------------------------------------------------
            # Asset: Parquet
            #-------------------------------------------------------------------------------------------------------------------------------
            data_asset_parquet = self._factory.get_or_create_asset( data_source = ds, **dc_asset_parquet.to_kwarg())

            print(f"{self.info_msg_prefix} Ensured S3 Datasource '{ds.name}' with Assets: '{data_asset_csv.name}', '{data_asset_parquet.name}'")
            return ds
        except Exception as e:
            print(f"{self.error_msg_prefix}   ensuring S3 Datasource and Asset: {e}")
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
            print(f"{self.info_msg_prefix} Ensured landing Expectation Suite '{suite_name}'.")
            return suite
        except Exception as e:
            print(f"{self.error_msg_prefix} ensuring landing Expectation Suite: {e}")
            raise RuntimeError() from e

    # 3) Ensure batch definition
    def ensure_asset_batche(self, data_source_name: str, dc_asset: GXS3AssetSpec)-> list[Batch]:
        """"
        Ensure batch definitions exist for the given data asset.
        Args:
            data_source_name (str): The name of the data source containing the asset.
            dc_asset (GXS3AssetSpec): Specification of the data asset.
        Returns:
            list[Any]: List of batches for the asset.        
        """
        try:
            batches = self._factory.get_asset_batches( data_source_name = data_source_name, asset_name = dc_asset.asset_name,
                                                                asset_type = dc_asset.asset_type)
            print(f"{self.info_msg_prefix} Ensured batch definition: {dc_asset.asset_name}")
        except Exception as e:
            print(f"{self.error_msg_prefix} ensuring batch definitions: {e}")
            raise RuntimeError() from e
        return batches

    # 4) Ensure validation creation
    def ensure_validation(self, vd_spec: GXValidationSpec, dc_asset: GXS3AssetSpec, ds_name: str) -> list[dict]:        
        """"
        Ensure validation definitions exist for the given validation spec and data asset, then creating validators.
        Args:
            vd_spec (GXValidationSpec): Specification of the validation.
            dc_asset (GXS3AssetSpec): Specification of the data asset.
            ds_name (str): Name of the data source containing the asset.
        Returns:
            list[Any]: List of created validators for the validation.      
        """
        try:
            manager.ensure_s3_landing_suite(vd_spec.suite_name)
            batches = manager.ensure_asset_batche(data_source_name=ds_name, dc_asset=dc_asset)
            validating_br_list = manager._factory.build_asset_validations_br_list(batch_list=batches, suite_name=vd_spec.suite_name)            
            print(f"{self.info_msg_prefix} Ensured validation definition: {vd_spec.validation_id} for asset: {dc_asset.asset_name}")            
        except Exception as e:
            print(f"{self.error_msg_prefix} ensuring validation definition: {e}")
            raise RuntimeError() from e
        return validating_br_list 
    
    # 5) Ensure checkpoint
    def ensure_checkpoint(self, checkpoint_spec: GXCheckpointSpec, validations: list[dict]) -> Checkpoint:
        """
        Ensure the checkpoint exists with specified actions.
        Args:
            checkpoint_spec (GXCheckpointSpec): Specification of the checkpoint.
            action_list (list[dict], optional): List of action dictionaries to include in the checkpoint.
        Returns:
            Checkpoint: The ensured checkpoint object.
        """
        try:
            actions_list = self._factory._build_action_list(checkpoint_spec)     
            checkpoint = self._factory.add_or_update_checkpoint(
                checkpoint_name=checkpoint_spec.checkpoint_name,
                validations=validations,
                action_list=actions_list
            )
            print(f"{self.info_msg_prefix} Ensured Checkpoint '{checkpoint_spec.checkpoint_name}'.")
            return checkpoint
        #--------------------------------------------------------------------------------------------------------------------------------------
        #--------------------------------------------------------------------------------------------------------------------------------------
            # actions_list = self._factory._build_action_list(checkpoint_spec)
            # all_validations = []
            # for vd in checkpoint_spec.vd_spec_list:
            #     validations_list = manager._factory.get_validation_dictionary_list(
            #         data_source_name=data_source_name,
            #         asset_name=vd.asset_spec.asset_name,
            #         suite_name=vd.suite_name
            #         )   
            #     all_validations.extend(validations_list)         
            #     checkpoint = self._factory.add_or_update_checkpoint(
            #         checkpoint_name=checkpoint_spec.checkpoint_name,
            #         asset_name=vd.asset_spec.asset_name,
            #         validations=validations_list,
            #         data_source_name=data_source_name,
            #         action_list=actions_list
            #     )
            #     print(f"{self.info_msg_prefix} Ensured Checkpoint '{checkpoint_spec.checkpoint_name}__{vd.asset_spec.asset_type} files'.")
            # return checkpoint


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
    s3_config = S3Config.from_env() 

    # Define data assets:
    asset_spec_csv = GXS3AssetSpec(
        s3conf=s3_config,
        asset_name="raw_csv",
        s3_prefix_relative="csv/",  # Relative folder (combined with base prefix)
        batching_regex=r"(?P<file_name>.*)\.csv",
        asset_type="csv"
    )
    
    asset_spec_parquet = GXS3AssetSpec(
        s3conf=s3_config,
        asset_name="raw_parquet",
        s3_prefix_relative="parquet/",  # Relative folder (combined with base prefix)
        batching_regex=r"(?P<file_name>.*)\.parquet",
        asset_type="parquet"
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

    # Ensure S3 Datasource + Assets
    ds_s3_raw = manager.ensure_s3_datasource_and_assets(s3_config, asset_spec_csv, asset_spec_parquet)

    # CSV Validation Setup
    validating_landing_csv = manager.ensure_validation(vd_spec=vd_csv_spec, dc_asset=asset_spec_csv, ds_name=ds_s3_raw.name)

    # Parquet Validation Setup
    validating_landing_parquet = manager.ensure_validation(vd_spec=vd_parquet_spec, dc_asset=asset_spec_parquet, ds_name=ds_s3_raw.name)
    
    all_validations = []
    all_validations.extend(validating_landing_csv)
    all_validations.extend(validating_landing_parquet)
    print('*** length of all_validations:', len(all_validations))

    # Ensure Checkpoint
    checkpoint = manager.ensure_checkpoint(checkpoint_spec=checkp_spec_landing_data_in_s3, validations=all_validations)
    
    # Run Checkpoint
    result = manager.run_checkpoint(checkpoint)
    # print(result)