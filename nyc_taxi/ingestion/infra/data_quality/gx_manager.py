from __future__ import annotations
from typing import Dict, Any
from great_expectations.datasource.fluent import PandasS3Datasource
from great_expectations.checkpoint import Checkpoint
from nyc_taxi.ingestion.config.settings import GXS3AssetSpec, GXValidationSpec, S3Config, GXCheckpointSpec, GeneralConfig
from nyc_taxi.ingestion.infra.data_quality.gx_context_factory import GreatExpectationsContextFactory

class GreatExpectationsManager:
    def __init__(self, context_factory: GreatExpectationsContextFactory):
        self._factory = context_factory
        self.general_config: GeneralConfig = GeneralConfig.from_env()
        self.info_msg_prefix: str = self.general_config.info_msg_prefix
        self.error_msg_prefix: str = self.general_config.error_msg_prefix
        self._context = self._factory.get_or_create_context()
    
    
    def ensure_s3_datasource(self, s3conf: S3Config, )-> PandasS3Datasource:
        """
        Ensure the S3 datasource exists in the Great Expectations context.
        Args:
            s3conf (S3Config): Configuration for the S3 datasource.
        Returns:
            PandasS3Datasource: The ensured S3 datasource object.
        """
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
            print(f"{self.info_msg_prefix} Datasource '{ds.name}' ensured.")
            return ds            
        except Exception as e:
            print(f"{self.error_msg_prefix} Datasource creation failed: {e}")
            raise RuntimeError() from e
    

    def ensure_s3_raw_asset(self, datasource: PandasS3Datasource, dc_asset_csv: GXS3AssetSpec, dc_asset_parquet: GXS3AssetSpec) -> None:
        """
        Ensure the raw data assets (CSV and Parquet) exist in the specified data source.
        Args:
            datasource (PandasS3Datasource): The data source to add the assets to.
            dc_asset_csv (GXS3AssetSpec): The specification for the CSV data asset.
            dc_asset_parquet (GXS3AssetSpec): The specification for the Parquet data asset.
        Returns:
            None
        """
        try:
            #-------------------------------------------------------------------------------------------------------------------------------
            # Asset: CSV
            #-------------------------------------------------------------------------------------------------------------------------------
            self._factory.build_asset( data_source = datasource, asset_spec = dc_asset_csv)
            
            #-------------------------------------------------------------------------------------------------------------------------------
            # Asset: Parquet
            #-------------------------------------------------------------------------------------------------------------------------------
            self._factory.build_asset( data_source = datasource, asset_spec = dc_asset_parquet)
            print(f"{self.info_msg_prefix} Assets '{dc_asset_csv.asset_name}', '{dc_asset_parquet.asset_name}' in Datasource '{datasource.name}' ensured.")
        except Exception as e:
            print(f"{self.error_msg_prefix} Assets creation failed: {e}")
            raise RuntimeError() from e
        

    def ensure_s3_raw_assets_batch_definition(self, data_source: PandasS3Datasource, dc_asset: GXS3AssetSpec) -> None:
        """
        Ensure the batch definition exists for the given data asset in the specified data source.
        Args:
            data_source (PandasS3Datasource): The data source containing the asset.
            dc_asset (GXS3AssetSpec): The specification of the data asset.
        Returns:
            None
        """
        #-------------------------------------------------------------------------------------------------------------------------------
        # Batch Definition: CSV
        #-------------------------------------------------------------------------------------------------------------------------------
        try:
            self._factory.build_asset_batch_definition( data_source = data_source, asset_spec = dc_asset)
        except Exception as e:
            print(f"{self.error_msg_prefix} Batch Definition failed for asset '{dc_asset.asset_name}': {e}")
            raise RuntimeError() from e


    def ensure_s3_raw_suites(self, suite_name: str) -> None:
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
            self._factory.build_suite(suite_name, meta=meta)
            print(f"{self.info_msg_prefix} Suite '{suite_name}' ensured.")
            return None
        except Exception as e:
            print(f"{self.error_msg_prefix} Suite creation failed: {e}")
            raise RuntimeError() from e


    def ensure_s3_raw_validation_Definition(self, vd_spec: GXValidationSpec, data_source: PandasS3Datasource) -> list[dict]:        
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
            self._factory.build_Validation_Definition(
                data_source=data_source,
                asset_name=vd_spec.asset_spec.asset_name,
                suite_name=vd_spec.suite_name
            )            
            # validating_br_list = manager._factory.build_validator_validation_br_list(validator=validator)      
            print(f"{self.info_msg_prefix} Validation Definition ensured.")         
        except Exception as e:
            print(f"{self.error_msg_prefix} Validation Definition failed: {e}")
            raise RuntimeError() from e
        return None
    

    def ensure_checkpoint(self, cp_spec: GXCheckpointSpec) -> Checkpoint:
        """
        Ensure the checkpoint exists with the specified validations and actions.
        Args:
            cp_spec (GXCheckpointSpec): Specification of the checkpoint.
        Returns:
            Checkpoint: The ensured checkpoint object.
        """
        try:
            # Actions list - a default set of actions.
            # TBD: Action for GX 1.11.3 needed to be figured out.
            action = []
            
            # Checkpoint Definition list of all validations united.
            vd_names = self._factory.get_all_validation_definition_names()            

            # Checkpoint
            checkpoint = self._factory.build_or_update_checkpoint(
                cp_name=cp_spec.checkpoint_name,
                vd_names=vd_names,
                action_list=action
            )                        
            print(f"{self.info_msg_prefix} Checkpoint '{cp_spec.checkpoint_name}' ensured.")            
            return checkpoint

        except Exception as e:
            print(f"{self.error_msg_prefix} Checkpoint ensuring failed: {e}")
            raise RuntimeError() from e    


    def ensure_run_checkpoint_and_build_docs(self, checkpoint: Checkpoint) -> Dict[str, Any]:
        """
        Ensure the checkpoint runs and data docs are built.
        Args:
            checkpoint (Checkpoint): The checkpoint to run.
        Returns:
            Dict[str, Any]
        """        
        try:
            cp_results = checkpoint.run()
            self._factory._context.build_data_docs()
            site_url = self._factory._context.get_docs_sites_urls()[0]['site_url']
            print(f"{self.info_msg_prefix} Checkpoint '{checkpoint.name}' executed.")
            print(f"{self.info_msg_prefix} Data Docs site URL: {site_url}")
            return cp_results
        except Exception as e:
            print(f"{self.error_msg_prefix} Checkpoint Run or Docs build failed: {e}")
            raise RuntimeError() from e