from __future__ import annotations
import sys
from pathlib import Path
if __name__ == "__main__":
    PROJECT_ROOT = Path(__file__).resolve().parents[4]
    sys.path.insert(0, str(PROJECT_ROOT))
    print("Project root added to sys.path[0]:", sys.path[0])

import os
from traitlets import Any, List
from nyc_taxi.ingestion.config.settings import GXS3AssetSpec, GreatExpectationsConfig, GeneralConfig, S3Config
import great_expectations as gx
from great_expectations.datasource.fluent import PandasS3Datasource
from nyc_taxi.ingestion.config.settings import GreatExpectationsConfig 
from great_expectations.core import ExpectationSuite
from great_expectations.checkpoint import UpdateDataDocsAction
from typing import Callable, Optional, Dict, Any
from datetime import datetime, timezone

class GreatExpectationsContextFactory:
    def __init__(self):        
        aws_key = os.getenv('AWS_ACCESS_KEY_ID')
        aws_secret = os.getenv('AWS_SECRET_ACCESS_KEY')
        if not aws_key or not aws_secret or aws_key.strip() == '' or aws_secret.strip() == '':
            raise ValueError("AWS credentials not found. Please set AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY environment variables.")
        
        self._context: gx.DataContext | None = None
        self.gx_project_root_dir: str = GreatExpectationsConfig.from_env().ge_root_dir
        self.general_config: GeneralConfig = GeneralConfig.from_env()
        self.info_msg_prefix: str = self.general_config.info_msg_prefix
        self.error_msg_prefix: str = self.general_config.error_msg_prefix


    def _create_context(self) -> gx.DataContext:       
        """Create or load Great Expectations DataContext."""
        try:
            print(f"{self.info_msg_prefix} Great Expectations Context root dir: {self.gx_project_root_dir}")
            context = gx.get_context(project_root_dir=self.gx_project_root_dir)
            return context
        except Exception as e:
            print(f"{self.error_msg_prefix} creating/loading Great Expectations DataContext: {e}")
            raise ConnectionError(f"{self.error_msg_prefix} Failed to create/load GX DataContext.") from e
        

    def get_or_create_context(self) -> gx.DataContext | None:
        """Get the Great Expectations Data_Context."""
        if self._context is None:
            self._context = self._create_context()
        print(f"{self.info_msg_prefix} Data_Context created!")
        return self._context 
    
    
    def get_or_create_datasource_s3(self, ds_name: str, s3conf: S3Config )-> PandasS3Datasource:
        """Get or create a Pandas S3 Asset in the GX context."""      
        # Create data_source (name it once; re-running should reuse or you can guard it)        
        try:
            data_source = self._context.data_sources.add_or_update_pandas_s3(
                name=ds_name,
                bucket=s3conf.bucket_name,
                boto3_options={"region_name": s3conf.region_name} if s3conf.region_name else {},
            )
            print(f"{self.info_msg_prefix} Data_source '{ds_name}' of type PandasS3Datasource created!")
            return data_source
        except Exception as e:
            print(f"{self.error_msg_prefix} Data source creation failed for S3: {e}")
            raise ConnectionError(f"{self.error_msg_prefix} Failed to create GX Pandas S3 data_source.") from e
            
    
    def build_asset(self, data_source: PandasS3Datasource, asset_spec: GXS3AssetSpec) -> Any:
        """Ensure an asset exists in the specified data_source.
        Args:
            data_source (PandasS3Datasource): The data_source to check/add the asset to.
            asset_spec (GXS3AssetSpec): The specification of the asset.
        Returns:
            Any: The existing or newly created asset.
            """
        try:
            # Sanity checks  
            if asset_spec.asset_name is None or asset_spec.asset_name.strip() == "":
                raise ValueError(f"{self.error_msg_prefix} Asset_name must be a non-empty string.")            
            if data_source is None:
                raise ValueError(f"{self.error_msg_prefix} Data source not found.")  
            
            # Try get existing asset
            Data_Asset = data_source.get_asset(asset_spec.asset_name)
            print(f"{self.info_msg_prefix} Asset found - '{asset_spec.asset_name}'!")
            return Data_Asset
        except Exception: 
            # Asset not found - create it     
            return self._create_asset(data_source , asset_spec = asset_spec)     
    
    def _create_asset(self,  data_source: PandasS3Datasource, asset_spec: GXS3AssetSpec) -> Any:
        try:
            
            asset_type = asset_spec.asset_type.lower().strip()
            if asset_type not in {"csv", "parquet", "json"}:
                raise ValueError(f"{self.error_msg_prefix} Asset type: {asset_spec.asset_type} not supported. Supported types are: csv, parquet, json.")

            if data_source is None:
                raise ValueError(f"{self.error_msg_prefix} Datasource '{data_source.name}' not found!")

            # IF exist, get asset
            try:
                ds_exist = data_source.get_asset(asset_spec.asset_name)
                if ds_exist is not None:
                    print(f"{self.info_msg_prefix} Asset '{asset_spec.asset_name}' exists. get it from data source!.")
                    return ds_exist
            except Exception:
                print(f"{self.info_msg_prefix} Asset '{asset_spec.asset_name}' not exist. Creating new one...")
                pass

            # Create asset
            creators: Dict[str, Callable[..., Any]] = {
                "csv": data_source.add_csv_asset,
                "parquet": data_source.add_parquet_asset,
                "json": data_source.add_json_asset,
            }

            asset = creators[asset_type](
                name=asset_spec.asset_name,
                s3_prefix=asset_spec.s3_prefix,
            )

            # Commit datasource definition (typed upsert)
            self._context.data_sources.add_or_update_pandas_s3(
                name=data_source.name,
                bucket=asset_spec.s3conf.bucket_name,
                boto3_options={"region_name": asset_spec.s3conf.region_name} if asset_spec.s3conf.region_name else {},
                assets=data_source.assets
            )
            # refresh view
            ds = self._context.data_sources.get(data_source.name)
            print(f"{self.info_msg_prefix} Assets commited in ds: {[a.name for a in ds.assets]}")

            # Re-fetch asset (avoid stale reference)
            ds = self._context.data_sources.get(data_source.name)
            return asset

        except Exception as e:
            print(f"{self.error_msg_prefix} Asset creation failed: {e}")
            raise RuntimeError() from e

    def build_asset_batch_definition(self, data_source: PandasS3Datasource, asset_spec: GXS3AssetSpec) -> Any:
        try:
            bd_name = asset_spec.batch_definition_name or f"{asset_spec.asset_name}_bd"
            data_source = self._context.data_sources.get(data_source.name)
            
            # Sanity checks  
            if asset_spec.asset_name is None or asset_spec.asset_name.strip() == "":
                raise ValueError(f"{self.error_msg_prefix} Asset_name must be a non-empty string.")            
            if data_source is None:
                raise ValueError(f"{self.error_msg_prefix} Data source '{data_source.name}' not found.")
            
            # Get asset
            asset = data_source.get_asset(asset_spec.asset_name)
            if asset is None:
                raise ValueError(f"{self.error_msg_prefix} Asset '{asset_spec.asset_name}' not found in Datasource '{data_source.name}'.")

            # Get or create batch definition (idempotent by name)
            existing = None
            for bd in list(getattr(asset, "batch_definitions", []) or []):
                print(f"{self.info_msg_prefix} Checking existing BD: {getattr(bd, 'name', None)}")
                if getattr(bd, "name", None) == bd_name:
                    existing = bd
                    break      

            if existing is None:
                # Create new batch definition and persist it into the datasource
                new_bd = asset.add_batch_definition(name=bd_name)
                self._context.data_sources.add_or_update_pandas_s3(
                    name=data_source.name,
                    bucket=asset_spec.s3conf.bucket_name,
                    boto3_options={"region_name": asset_spec.s3conf.region_name} if asset_spec.s3conf.region_name else {},
                    assets=data_source.assets
                )
            print(f"{self.info_msg_prefix} Batch Definition '{bd_name}' ensured for Asset '{asset_spec.asset_name}' in Datasource '{data_source.name}'.")
            return existing

        except Exception as e:
            print(f"{self.error_msg_prefix} Batch Definition creation or retrieval failed: {e}")
            raise RuntimeError() from e
   
    def build_suite(self, suite_name: str, meta: Dict[str, Any]) -> ExpectationSuite:
        """
        Get or create an Expectation Suite for landing data with predefined expectations.
        Args:
            suite_name (str): The name of the Expectation Suite.
            meta (Dict[str, Any]): Metadata to attach to the suite.
        Returns:
            ExpectationSuite: The existing or newly created Expectation Suite.
        """
        try:
            suite = self._context.suites.get(name=suite_name)
            print(f"{self.info_msg_prefix} Suite '{suite_name}' found!")
        except gx.exceptions.DataContextError:
            suite = gx.ExpectationSuite(name=suite_name)
            print(f"{self.info_msg_prefix} Suite '{suite_name}' created!")

        #TBD: SRP - temporary hardcoded expectations for landing suite, to be replaced with 
        #           dynamic logic that reads from configs and reset the expectations as needed.
        try: 
            # Clean previous expectations if suite already exists (idempotent overwrite)
            suite.expectations = []

            # Add expectations
            suite.add_expectation(
                gx.expectations.ExpectTableRowCountToBeBetween(min_value=1)
            )

            suite.add_expectation(
                gx.expectations.ExpectColumnToExist(column="id")
            )

            suite.add_expectation(
                gx.expectations.ExpectColumnValuesToNotBeNull(column="id")
            )
            print(f"{self.info_msg_prefix} Expectations added to suite '{suite_name}'.")
        except Exception as e:
                print(f"{self.error_msg_prefix} Expectations adding to suite '{suite_name}' failed: {e}")
                raise RuntimeError() from e

        try:
            # Merge metadata (don't drop existing keys)
            suite.meta = {**(suite.meta or {}), **(meta or {})}
            # Suite Persistence
            self._context.suites.add_or_update(suite=suite)
            print(f"{self.info_msg_prefix} Suite '{suite_name}' persisted with metadata.")
        except Exception as e:
            print(f"{self.error_msg_prefix} Suite persistence failed: {e}")
            raise RuntimeError() from e
        return suite
    
    def build_suite_meta(self, *, layer: str, managed_by: str, suite_version: str, 
                            extra: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """
        Build metadata dictionary for an Expectation Suite.
        Args:
            layer (str): The data layer (e.g., "landing", "raw", "trusted").
            managed_by (str): The manager of the suite (e.g., "data_engineer").
            suite_version (str): The version of the suite.
            extra (Optional[Dict[str, Any]]): Additional metadata to include.
        Returns:
            Dict[str, Any]: A dictionary containing the metadata.
        """
        meta: Dict[str, Any] = {
            "layer": layer,
            "managed_by": managed_by,
            "suite_version": suite_version,
            "updated_at_utc": datetime.now(timezone.utc).isoformat(),
        }
        if extra:
            meta.update(extra)

        print(f"{self.info_msg_prefix} Suite metadata built: {meta}")
        return meta
    
    def build_Validation_Definition(self, data_source: PandasS3Datasource, asset_name: str, suite_name: str ) -> gx.validator.validator.Validator:
        """ Build a Validator Definition for the specified asset and expectation suite.
            Validation definition ties together the data (asset) and expectations (suite) 
            and are the foundation checkpoints run against.
        Args:
            data_source (PandasS3Datasource): The data source containing the asset.
            asset_name (str): The name of the asset to validate.
            suite_name (str): The name of the expectation suite to use.
        Returns:
            gx.validator.validator.Validator: The built Validator object.
        """
        try:            
            # Sanity checks
            if data_source is None:
                raise ValueError(f"{self.error_msg_prefix} Datasource argument is None.")
            
            # Refetch data source to avoid stale references
            datasource = self._context.data_sources.get(data_source.name)            

            # Get asset (Batch Definition is tied to asset)
            asset = datasource.get_asset(asset_name)
            if asset is None:
                raise ValueError(f" Asset '{asset_name}' not found in datasource '{data_source.name}'.")

            # Get suite (Expectation Suite)
            suite = self._context.suites.get(name=suite_name)
            if suite is None:
                raise ValueError(f" Expectation Suite '{suite_name}' not found.")

            # Pick an existing BatchDefinition from the asset (required for Validator)
            if not getattr(asset, "batch_definitions", None):
                raise RuntimeError(f"No batch_definitions found on asset '{asset_name}'.")
            
            #TBD - Multiple batch definitions require more complex logic to pick the right one:
            if len(asset.batch_definitions) > 1:
                print(f"{self.info_msg_prefix} Warning: Batch Definitions on asset '{asset_name}' has Multiple values,\
                      that not supportet at the moment. \
                      We will Use the first value.")
            
            # Use the first batch definition for simplicity
            bd = asset.batch_definitions[0]
            vd_name = f"{asset_name}__{suite_name}"
            # Build Validator
            validator_definition = gx.core.validation_definition.ValidationDefinition(
                name=vd_name,
                data=bd,
                suite=suite,
            )
            validator_definition = self._context.validation_definitions.add_or_update(validation=validator_definition)
            print(f"{self.info_msg_prefix} Validator Definition '{vd_name}' built and persisted for Asset: '{asset_name}' and Suite: '{suite_name}'.")           
            
            return validator_definition
        except Exception as e:
            print(f"{self.error_msg_prefix} Validator Definition building failed: {e}")
            raise RuntimeError() from e
    
    
    def build_or_update_checkpoint(self, cp_name: str, vd_names: Optional[List[str]] = None, action_list: Optional[List[Dict[str, Any]]] = None) -> gx.Checkpoint:
        """
        Creates/updates a Checkpoint that runs all ValidationDefinitions found in the context.
        Optionally filter by a list of VD names.
        """
        # Sanity checks
        if cp_name is None or cp_name.strip() == "":
            raise ValueError(f"{self.error_msg_prefix} Checkpoint name must be a non-empty string.")
        if vd_names is None:
            raise ValueError(f"{self.error_msg_prefix} Checkpoint Validation Definition names list must be provided.")
        if action_list is None:
            print(f"{self.info_msg_prefix} Checkpoint was Not supllied with action list. Default to empty list.")
            action_list = []

        scheckpoint = gx.Checkpoint(
            name=cp_name,
            validation_definitions=vd_names,
            actions=action_list,
            result_format={"result_format": "SUMMARY"},
        )

        scheckpoint = self._context.checkpoints.add_or_update(checkpoint=scheckpoint)
        print(f"{self.info_msg_prefix} Checkpoint '{cp_name}' created/updated.")
        return scheckpoint

    
    # TBD: Action list builder could be more complex based on needs.
    # TBD: Hard Coded should be considered as the project develop in after phase one.
    @staticmethod
    def create_action_list() -> list[dict]:
        """ Create a list of actions for a Checkpoint.
        Returns:
            list[dict]: A list of action configurations."""
        actions = [ UpdateDataDocsAction( name="update_data_docs") ]
        return actions
    

    def get_all_validation_definition_names(self) -> List[Dict[str, Any]]:
        """
        Get all Validation Definition names in the context.
        Returns:
            List[str]: A list of all Validation Definition names.
        """
        all_vds = self._context.validation_definitions.all()

        if not all_vds:
            raise ValueError("ValidationDefinitions not found in the context.")

        vd_dict = [self.vd_to_dict(vd) for vd in all_vds]
        return vd_dict
    
    #TBD: This method may need to be adjusted based on GX version and ValidationDefinition implementation.
    @staticmethod
    def vd_to_dict(vd: Any) -> Dict[str, Any]:
        """ Serialize a ValidationDefinition to a dictionary.
        Args:
            vd (Any): The ValidationDefinition to serialize.
        Returns:
            Dict[str, Any]: The serialized dictionary representation of the ValidationDefinition.
        Raises:
            TypeError: If the ValidationDefinition cannot be serialized to a dictionary.
        """
        # GX commonly supports one of these across minor releases
        if hasattr(vd, "to_json_dict"):
            return vd.to_json_dict()
        if hasattr(vd, "dict"):  # pydantic v1
            return vd.dict()
        if hasattr(vd, "to_dict"):
            return vd.to_dict()
        # last resort: try __dict__ (not ideal, but better than crashing silently)
        raise TypeError(f"Cannot serialize ValidationDefinition of type {type(vd)} to dict")