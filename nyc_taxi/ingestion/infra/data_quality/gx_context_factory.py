#%%
from __future__ import annotations
from multiprocessing import context
import sys
from pathlib import Path
import os

from traitlets import Any, List
if __name__ == "__main__":
    PROJECT_ROOT = Path(__file__).resolve().parents[4]
    sys.path.insert(0, str(PROJECT_ROOT))
    print("Project root added to sys.path[0]:", sys.path[0])

from nyc_taxi.ingestion.config.settings import GXS3AssetSpec, GreatExpectationsConfig, GXCheckpointSpec, GeneralConfig, S3Config
import great_expectations as gx
from great_expectations.datasource.fluent import PandasS3Datasource
from nyc_taxi.ingestion.config.settings import GreatExpectationsConfig 
from great_expectations.core import ExpectationSuite
from great_expectations.expectations.expectation_configuration import ExpectationConfiguration
from great_expectations.core.batch import Batch
from typing import Callable, Optional, Dict, Any
from datetime import datetime, timezone

#%%
class GreatExpectationsContextFactory:
    """
    Responsible ONLY for:
    - Creating / loading the GX File Data Context
    - Ensuring required data_sources and assets exist
    """
    def __init__(self):
        # Check AWS credentials early to provide clear error message
        aws_key = os.getenv('AWS_ACCESS_KEY_ID')
        aws_secret = os.getenv('AWS_SECRET_ACCESS_KEY')
        if not aws_key or not aws_secret or aws_key.strip() == '' or aws_secret.strip() == '':
            raise ValueError("AWS credentials not found. Please set AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY environment variables.")
        
        self._context: gx.DataContext | None = None
        self.gx_project_root_dir: str = GreatExpectationsConfig.from_env().ge_root_dir
        self.general_config: GeneralConfig = GeneralConfig.from_env()
        self.info_msg_prefix: str = self.general_config.info_msg_prefix
        self.error_msg_prefix: str = self.general_config.error_msg_prefix

    # ---------------------------------------------------------------------------------------
    # Context, data_source building blocks
    # ---------------------------------------------------------------------------------------
    
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
            
    
    def get_or_create_asset(self, data_source: PandasS3Datasource, asset_spec: GXS3AssetSpec) -> Any:
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

            # # 3) Add batch definition (in-memory)
            # asset.add_batch_definition(name=f"{asset.name}_bd")

            # 4) Commit datasource definition (typed upsert)
            self._context.data_sources.add_or_update_pandas_s3(
                name=data_source.name,
                bucket=asset_spec.s3conf.bucket_name,
                boto3_options={"region_name": asset_spec.s3conf.region_name} if asset_spec.s3conf.region_name else {},
                assets=data_source.assets
            )
            # refresh view
            ds = self._context.data_sources.get(data_source.name)
            print(f"{self.info_msg_prefix} Assets commited in ds: {[a.name for a in ds.assets]}")

            # 5) Re-fetch asset (avoid stale reference)
            ds = self._context.data_sources.get(data_source.name)
            return asset

        except Exception as e:
            print(f"{self.error_msg_prefix} Asset creation failed: {e}")
            raise RuntimeError() from e

    def get_or_create_asset_batch_definition(self, data_source: PandasS3Datasource, asset_spec: GXS3AssetSpec) -> Any:
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
   
    def get_or_create_suite(self, suite_name: str, meta: Dict[str, Any]) -> ExpectationSuite:
        """
        Get or create an Expectation Suite for landing data with predefined expectations.
        Args:
            suite_name (str): The name of the Expectation Suite.
            meta (Dict[str, Any]): Metadata to attach to the suite.
        Returns:
            ExpectationSuite: The existing or newly created Expectation Suite.
        """

        # Get or create suite
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
                raise ValueError(f" Expectation Suite '{suite_name}' not found in context.")

            # Pick an existing BatchDefinition from the asset (required for Validator)
            if not getattr(asset, "batch_definitions", None):
                raise RuntimeError(f"No batch_definitions found on asset '{asset_name}'.")
            bd = asset.batch_definitions[0]
            # Build Validator
            validator = gx.validator.validator.Validator(
                data=bd,
                suite=suite,
                data_context=self._context,
            )

            print(f"{self.info_msg_prefix} Validator Definition built for Asset '{asset_name}' in Datasource '{data_source.name}' with Suite '{suite_name}'.")           
            
            return validator
        except Exception as e:
            print(f"{self.error_msg_prefix} Validator Definition building failed: {e}")
            raise RuntimeError() from e

    # def ensure_asset_has_batch_definition(self, data_source_name: str, asset_name: str):
    #     """ Ensure the specified asset has at least one batch definition.
    #     If no batch definitions exist, add a default "whole asset" batch definition.
    #     Args:
    #         data_source_name (str): The name of the data source containing the asset.
    #         asset_name (str): The name of the asset to check.
    #     Returns:
    #         The asset with ensured batch definitions.
    #     """
    #     datasource = self._context.data_sources.get(data_source_name)
    #     if datasource is None:
    #         raise ValueError(f"datasource '{data_source_name}' not found.")

    #     asset = datasource.get_asset(asset_name)

    #     # if no batch definitions exist, add one
    #     if not asset.batch_definitions:
    #         # pick the simplest "whole asset" definition (works for many file-based assets)
    #         if hasattr(asset, "add_batch_definition_whole_asset"):
    #             asset.add_batch_definition_whole_asset(f"{asset_name}_whole_asset")
    #         else:
    #             # fallback: show what exists so you know what to call
    #             methods = [m for m in dir(asset) if m.startswith("add_batch_definition")]
    #             raise RuntimeError(
    #                 f"No batch definitions and no add_batch_definition_whole_asset() on asset '{asset_name}'. "
    #                 f"Available methods: {methods}"
    #             )

    #         # persist datasource updates
    #         self._context.data_sources.add_or_update(datasource=datasource)

    #     return asset

    # def get_asset_batches(self, data_source_name: str, asset_name: str, asset_type: str) -> List[Batch]:
    #     """ Get list of Batch objects for the given asset in the specified data source.
    #     Args:
    #         data_source_name (str): The name of the data source containing the asset.
    #         asset_name (str): The name of the asset to retrieve batches for.
    #         asset_type (str): The type of the asset (e.g., 'csv', 'parquet').
    #     Returns:
    #         List[Batch]: List of Batch objects for the asset.
    #     """ 
    #     try:
    #         datasource = self._context.data_sources.get(data_source_name)
    #         if datasource is None:
    #             raise ValueError(f"datasource '{data_source_name}' not found.")

    #         asset = datasource.get_asset(asset_name)
    #         if asset is None:
    #             raise ValueError(f"asset '{asset_name}' not found in datasource '{data_source_name}'.")

    #         # If none exist, create a path-based batch definition (1 batch per file)
    #         if not asset.batch_definitions:
    #             if hasattr(asset, "add_batch_definition_path"):
    #                 asset.add_batch_definition_path(name=f"{asset_name}_by_path", path="path",)
    #             else:
    #                 methods = [m for m in dir(asset) if m.startswith("add_batch_definition")]
    #                 raise RuntimeError(
    #                     f"Asset '{asset_name}' has no batch definitions and no add_batch_definition_path(). "
    #                     f"Available methods: {methods}"
    #                 )

    #             # Persist the datasource update (important)
    #             self._context.data_sources.add_or_update(datasource=datasource)

    #         return asset
    #     except Exception as e:
    #         print(f"{self.error_msg_prefix} retrieving batches: {e}")
    #         raise RuntimeError() from e

    # def _get_batches_request_list(self, data_source_name: str, asset_name: str) -> list[Any]:
    #     """ Get batch request list for the given asset in the specified data source.
    #     Args:
    #         data_source_name (str): The name of the data source containing the asset.
    #         asset_name (str): The name of the asset to retrieve batches for.
    #     Returns:
    #         list[Any]: List of batch requests for the asset."""
    #     datasource = self._context.data_sources.get(data_source_name)
    #     if datasource is None:
    #         raise ValueError(f"datasource '{data_source_name}' not found.")
    #     if not asset_name or not asset_name.strip():
    #         raise ValueError("asset_name must be a non-empty string.")
            
    #     try:
    #         _DataAssetT = datasource.get_asset(asset_name)
    #         batch_request_all = _DataAssetT.build_batch_request()
    #     except Exception as e:
    #         print(f"{self.error_msg_prefix} retrieving batches: {e}")
    #         raise RuntimeError() from e 
    #     return batch_request_all
    
    # def build_asset_validations_br_list(self, batch_list: list[Batch], suite_name: str ) -> list[dict]:
    #     """ Build a list of validation dictionaries from a list of Batch objects.
    #     Args:
    #         batch_list (list[Any]): List of Batch objects.
    #         suite_name (str): Name of the expectation suite to use."""
    #     print(f"{self.info_msg_prefix} Building validations for suite '{suite_name}' with {len(batch_list)} batches:")
        
    #     validations = []
    #     for i, batch in enumerate(batch_list, start=1):
    #         validation = {
    #             "batch_request": batch.batch_request,
    #             "expectation_suite_name": suite_name,
    #         }
    #         validations.append(validation)

    #         options = getattr(batch.batch_definition, "batch_identifiers", {}) or {}
    #         file_label = options.get("file_name", "<unknown>")

    #         print(
    #             f"  Validation {i}: "
    #             f"suite='{suite_name}', batch='{file_label}'"
    #         )
    #     return validations

    # def merge_validations_list(self, existing_validations: list[dict], new_validations: list[dict]) -> list[dict]:
    #     """Merge new validations into existing validations, avoiding duplicates.
    #     Args:
    #         existing_validations (list[dict]): List of existing validation dictionaries.
    #         new_validations (list[dict]): List of new validation dictionaries to merge.
    #     Returns:
    #         list[dict]: Merged list of validation dictionaries."""      
    #     all_validations = []
    #     all_validations.extend(existing_validations)
    #     all_validations.extend(new_validations)
    #     print('*** length of all_validations:', len(all_validations))
    #     return all_validations

    # def make_validations_for_asset_files(self, datasource_name: str, asset_name: str, file_names: list[str], suite: str):
    #     ds = self._context.data_sources.get(datasource_name)
    #     asset = ds.get_asset(asset_name)

    #     validations = []
    #     for fn in file_names:
    #         br = asset.build_batch_request(options={"file_name": fn})
    #         validations.append({"batch_request": br, "expectation_suite_name": suite})
    #     return validations

    @staticmethod
    def _build_action_list(spec: GXCheckpointSpec) -> list[dict]:
        actions = [
            {
                "name": "store_validation_result",
                "action": {
                    "class_name": "StoreValidationResultAction",
                    "module_name": "great_expectations.checkpoint.actions",
                },
            }
        ]

        # update data docs if specified
        if spec.build_data_docs:
            actions.append(
                {
                    "name": "update_data_docs",
                    "action": {
                        "class_name": "UpdateDataDocsAction",
                        "module_name": "great_expectations.checkpoint.actions",
                    },
                }
            )

        return actions

    def upsert_checkpoint_for_csv_and_parquet(self, *, checkpoint_name: str, data_source_name: str, csv_asset_name: str,
                csv_suite_name: str, parquet_asset_name: str, parquet_suite_name: str, ) -> gx.Checkpoint:
        """
        Create or update a Checkpoint that validates both CSV and Parquet assets.
        Args:
            checkpoint_name (str): The name of the Checkpoint.
            data_source_name (str): The name of the data source containing the assets.
            csv_asset_name (str): The name of the CSV asset.
            csv_suite_name (str): The name of the Expectation Suite for the CSV asset.
            parquet_asset_name (str): The name of the Parquet asset.
            parquet_suite_name (str): The name of the Expectation Suite for the Parquet asset.
        Returns:
            gx.Checkpoint: The created or updated Checkpoint.
        """

        # --- get assets ---
        ds = self._context.data_sources.get(data_source_name)
        if ds is None:
            raise ValueError(f"Datasource '{data_source_name}' not found.")

        csv_asset = ds.get_asset(csv_asset_name)
        pq_asset = ds.get_asset(parquet_asset_name)
        if csv_asset is None:
            raise ValueError(f"CSV asset '{csv_asset_name}' not found.")
        if pq_asset is None:
            raise ValueError(f"Parquet asset '{parquet_asset_name}' not found.")

        # --- get suites ---
        csv_suite = self._context.suites.get(name=csv_suite_name)
        pq_suite = self._context.suites.get(name=parquet_suite_name)

        # --- pick an existing BatchDefinition from each asset (required for ValidationDefinition) ---
        if not getattr(csv_asset, "batch_definitions", None):
            raise RuntimeError(f"No batch_definitions found on asset '{csv_asset_name}'.")
        if not getattr(pq_asset, "batch_definitions", None):
            raise RuntimeError(f"No batch_definitions found on asset '{parquet_asset_name}'.")

        csv_bd = csv_asset.batch_definitions[0]
        pq_bd = pq_asset.batch_definitions[0]

        # --- create / upsert validation definitions ---
        csv_vd_name = f"{csv_asset_name}__{csv_suite_name}"
        pq_vd_name = f"{parquet_asset_name}__{parquet_suite_name}"

        csv_vd = gx.ValidationDefinition(name=csv_vd_name, data=csv_bd, suite=csv_suite)
        pq_vd  = gx.ValidationDefinition(name=pq_vd_name,  data=pq_bd,  suite=pq_suite)

        csv_vd = self._context.validation_definitions.add_or_update(validation=csv_vd)
        pq_vd  = self._context.validation_definitions.add_or_update(validation=pq_vd)

        # --- create / upsert checkpoint with BOTH validation definitions ---
        checkpoint = gx.Checkpoint(
            name=checkpoint_name,
            validation_definitions=[csv_vd, pq_vd],
            actions=[],  # keep empty for now; add UpdateDataDocsAction / Slack later
        )

        checkpoint = self._context.checkpoints.add_or_update(checkpoint=checkpoint)
        return checkpoint


    # def add_or_update_checkpoint(self, checkpoint_name: str, validations: list[dict], action_list: list[dict] | None = None ) -> Any:
    #     """Get or create a checkpoint with specified actions.
    #     Args:
    #         checkpoint_name (str): Name of the checkpoint.
    #         validations (list[dict]): List of validation definitions.
    #         actions (Dict[str, Any]): Actions to associate with the checkpoint.
    #     Returns:
    #         Any: The checkpoint object."""
    #     try:
    #         checkpoint = self._context.add_or_update_checkpoint(
    #             name=checkpoint_name,
    #             validations=validations,
    #             action_list=action_list,
    #         )            
    #     except Exception as e:
    #         print(f"{self.error_msg_prefix} creating/updating Checkpoint: {e}")
    #         raise RuntimeError() from e
    #     return checkpoint
    
    # def merge_checkpoint_validations(self, checkpoint: Any, new_validations: list[dict]) -> Any:
    #     """Merge new validations into an existing checkpoint.
    #     Args:
    #         checkpoint (Any): The existing checkpoint object.
    #         new_validations (list[dict]): New validation definitions to merge.
    #     Returns:
    #         Any: The updated checkpoint object."""
    #     try:
    #         existing_validations = checkpoint.validations or []
    #         merged_validations = existing_validations.copy()
    #         merged_validations.extend(new_validations)
    #         checkpoint.validations = merged_validations
    #         updated_checkpoint = self._context.add_or_update_checkpoint(
    #             name=checkpoint.name,
    #             validations=merged_validations,
    #             action_list=checkpoint.action_list,
    #         )
    #         print(f"{self.info_msg_prefix} Merged {len(new_validations)} new validations into Checkpoint '{checkpoint.name}'. Total validations now: {len(merged_validations)}.")
    #     except Exception as e:
    #         print(f"{self.error_msg_prefix} merging validations into Checkpoint: {e}")
    #         raise RuntimeError() from e
    #     return updated_checkpoint
    
# %%
if __name__ == "__main__":
    factory = GreatExpectationsContextFactory()
    context = factory.get_or_create_context()
