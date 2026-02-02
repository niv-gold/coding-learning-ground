#%%
from __future__ import annotations
import sys
from pathlib import Path
import os

from traitlets import Any
if __name__ == "__main__":
    PROJECT_ROOT = Path(__file__).resolve().parents[4]
    sys.path.insert(0, str(PROJECT_ROOT))
    print("Project root added to sys.path[0]:", sys.path[0])

from nyc_taxi.ingestion.config.settings import GreatExpectationsConfig, GXCheckpointSpec, GeneralConfig
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
            print(f"{self.info_msg_prefix} Creating/Loading Great Expectations DataContext at root dir: {self.gx_project_root_dir}")
            context = gx.get_context(project_root_dir=self.gx_project_root_dir)
            return context
        except Exception as e:
            print(f"{self.error_msg_prefix} creating/loading Great Expectations DataContext: {e}")
            raise ConnectionError("Failed to create/load GX DataContext.") from e

    def get_or_create_context(self) -> gx.DataContext | None:
        """Get the Great Expectations DataContext."""
        if self._context is None:
            self._context = self._create_context()
        print(f"{self.info_msg_prefix} Great Expectations DataContext ready.")
        return self._context 
    
    def get_or_create_pandas_s3_datasource(self, ds_name: str, **kwarg )-> PandasS3Datasource:
        """Get or create a Pandas S3 Asset in the GX context."""      
        # Create data_source (name it once; re-running should reuse or you can guard it)
        self.get_or_create_context()
        try:
            data_source = self._context.data_sources.add_or_update_pandas_s3(
                name=ds_name,
                bucket=kwarg["bucket_name"],
                boto3_options={"region_name": kwarg["region_name"]} if kwarg["region_name"] else {},
            )
            print(f"{self.info_msg_prefix} Created/Reused Pandas S3 data_source '{ds_name}'.")
            return data_source
        except Exception as e:
            print(f"{self.error_msg_prefix} creating Pandas S3 data_source: {e}")
            raise ConnectionError("Failed to create GX Pandas S3 data_source.") from e
        
    # ---------------------------------------------------------------------------------------
    # Asset building blocks
    # ---------------------------------------------------------------------------------------
    def _create_asset(self, data_source: PandasS3Datasource, **kwarg) -> Any:
                    #    asset_name: str, s3_prefix: str, batching_regex: str, asset_type: str) -> Any:
        """Create an asset in the specified data_source.
        Args:
            data_source (PandasS3Datasource): The data_source to add the asset to.
            **kwarg: Keyword arguments containing asset specifications.
        Returns:
            Any: The created asset.
        """
        try:
            assert_type_norm = kwarg["asset_type"].lower().strip()
            if assert_type_norm not in ["csv", "parquet", "json"]:
                raise ValueError(f"Unsupported asset type: {kwarg["asset_type"]}. Supported types are: csv, parquet, json.")

            creators: Dict[str, Callable[..., Any]] = {
                "csv": data_source.add_csv_asset,
                "parquet": data_source.add_parquet_asset,
                "json": data_source.add_json_asset,
            }

            _DataAssetT = creators[assert_type_norm](
                name = kwarg["asset_name"],
                s3_prefix = kwarg["s3_prefix"]
            )

            # Sanity check
            if _DataAssetT is None:
                raise RuntimeError("Asset creation returned None.")
            print(f"{self.info_msg_prefix} Created new Asset '{kwarg["asset_name"]}'.")
            return _DataAssetT

        except Exception as e:
            print(f"{self.error_msg_prefix} creating Asset: {e}")
            raise RuntimeError() from e
    
    def get_or_create_asset(self, data_source: PandasS3Datasource, **kwarg) -> Any:
        """Ensure an asset exists in the specified data_source.
        Args:
            data_source (PandasS3Datasource): The data_source to check/add the asset to.
            **kwarg: Keyword arguments containing asset specifications.
        Returns:
            Any: The existing or newly created asset.
            """
        try:
            _DataAssetT = data_source.get_asset(kwarg["asset_name"])
            print(f"{self.info_msg_prefix} Found existing Asset '{kwarg["asset_name"]}'.")
            return _DataAssetT
        except Exception:         
            return self._create_asset( data_source = data_source, **kwarg)       

    # ---------------------------------------------------------------------------------------
    # Expectation Suite building blocks
    # ---------------------------------------------------------------------------------------
    @staticmethod
    def build_landing_expectations() -> list[ExpectationConfiguration]:
        """
        Build a list of ExpectationConfigurations for landing data.
        Returns:
            list[ExpectationConfiguration]: A list of expectation configurations.
        """

        expactations =  [
            ExpectationConfiguration(
                expectation_type="expect_table_row_count_to_be_between",
                kwargs={"min_value": 1, "max_value": None},
            ),
            ExpectationConfiguration(
                expectation_type="expect_column_to_exist",
                kwargs={"column": "id"},
            ),
            ExpectationConfiguration(
                expectation_type="expect_column_values_to_not_be_null",
                kwargs={"column": "id"},
            ),
        ]
        return expactations
    


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
        return meta

    def get_or_create_suite(self, suite_name: str, *, expectations: list[ExpectationConfiguration], 
                     meta: Dict[str, Any], overwrite_expectations: bool = True,) -> ExpectationSuite:
        """
        Create or update an Expectation Suite in the GX context.
        Args:
            suite_name (str): The name of the Expectation Suite.
            expectations (list[ExpectationConfiguration]): The list of expectations to include.
            meta (Dict[str, Any]): Metadata for the suite.
            overwrite_expectations (bool): Whether to overwrite existing expectations.
        Returns:
            None
        """
        # Validate inputs
        if not suite_name or not suite_name.strip():
            raise ValueError("suite_name must be a non-empty string")
        
        # Create or get suite
        try:            
            suite = self._context.get_expectation_suite(expectation_suite_name=suite_name)
        except gx.exceptions.DataContextError:
            suite = ExpectationSuite(expectation_suite_name=suite_name)
            print(f"{self.info_msg_prefix} Created new Expectation Suite '{suite_name}'.")

        # Update meta (merge so we don't lose other keys)
        suite.meta = {**(suite.meta or {}), **meta}

        # Decide overwrite policy centrally
        if overwrite_expectations:
            suite.expectations = expectations

        # Persist idempotently (0.18.21)
        self._context.add_or_update_expectation_suite(expectation_suite=suite)
        print(f"{self.info_msg_prefix} Upserted Expectation Suite '{suite_name}' with {len(suite.expectations)} expectations.")
        
        return suite

    def get_asset_batches(self, data_source_name: str, asset_name: str, asset_type: str) -> list[Batch]:
        
        datasource = self._context.get_datasource(data_source_name)
        if datasource is None:
            raise ValueError(f"datasource '{data_source_name}' not found.")
        if not asset_name or not asset_name.strip():
            raise ValueError("asset_name must be a non-empty string.")
            
        try:
            _DataAssetT = datasource.get_asset(asset_name)
            batch_request_all = _DataAssetT.build_batch_request()
            batches: list[Batch] = _DataAssetT.get_batch_list_from_batch_request(batch_request_all)
            print(f"{self.info_msg_prefix} Found {len(batches)} batches in {asset_type} Asset:")

            asset_batches = []
            for i, batch in enumerate(batches, start=1):
                asset_batches.append(batch)
                print(f"  Batch {i}: {batch.batch_definition.batch_identifiers}")     
        except Exception as e:
            print(f"{self.error_msg_prefix} retrieving batches: {e}")
            raise RuntimeError() from e 
        return asset_batches

    def _get_batches_request_list(self, data_source_name: str, asset_name: str) -> list[Any]:
        """ Get batch request list for the given asset in the specified data source.
        Args:
            data_source_name (str): The name of the data source containing the asset.
            asset_name (str): The name of the asset to retrieve batches for.
        Returns:
            list[Any]: List of batch requests for the asset."""
        datasource = self._context.get_datasource(data_source_name)
        if datasource is None:
            raise ValueError(f"datasource '{data_source_name}' not found.")
        if not asset_name or not asset_name.strip():
            raise ValueError("asset_name must be a non-empty string.")
            
        try:
            _DataAssetT = datasource.get_asset(asset_name)
            batch_request_all = _DataAssetT.build_batch_request()
        except Exception as e:
            print(f"{self.error_msg_prefix} retrieving batches: {e}")
            raise RuntimeError() from e 
        return batch_request_all
    
    def build_asset_validations_br_list(self, batch_list: list[Batch], suite_name: str ) -> list[dict]:
        """ Build a list of validation dictionaries from a list of Batch objects.
        Args:
            batch_list (list[Any]): List of Batch objects.
            suite_name (str): Name of the expectation suite to use."""
        print(f"{self.info_msg_prefix} Building validations for suite '{suite_name}' with {len(batch_list)} batches:")
        
        validations = []
        for i, batch in enumerate(batch_list, start=1):
            validation = {
                "batch_request": batch.batch_request,
                "expectation_suite_name": suite_name,
            }
            validations.append(validation)

            options = getattr(batch.batch_definition, "batch_identifiers", {}) or {}
            file_label = options.get("file_name", "<unknown>")

            print(
                f"  Validation {i}: "
                f"suite='{suite_name}', batch='{file_label}'"
            )
        return validations

    def merge_validations_list(self, existing_validations: list[dict], new_validations: list[dict]) -> list[dict]:
        """Merge new validations into existing validations, avoiding duplicates.
        Args:
            existing_validations (list[dict]): List of existing validation dictionaries.
            new_validations (list[dict]): List of new validation dictionaries to merge.
        Returns:
            list[dict]: Merged list of validation dictionaries."""      
        all_validations = []
        all_validations.extend(existing_validations)
        all_validations.extend(new_validations)
        print('*** length of all_validations:', len(all_validations))
        return all_validations

    def make_validations_for_asset_files(self, datasource_name: str, asset_name: str, file_names: list[str], suite: str):
        ds = self._context.get_datasource(datasource_name)
        asset = ds.get_asset(asset_name)

        validations = []
        for fn in file_names:
            br = asset.build_batch_request(options={"file_name": fn})
            validations.append({"batch_request": br, "expectation_suite_name": suite})
        return validations

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


    def add_or_update_checkpoint(self, checkpoint_name: str, validations: list[dict], action_list: list[dict] | None = None ) -> Any:
        """Get or create a checkpoint with specified actions.
        Args:
            checkpoint_name (str): Name of the checkpoint.
            validations (list[dict]): List of validation definitions.
            actions (Dict[str, Any]): Actions to associate with the checkpoint.
        Returns:
            Any: The checkpoint object."""
        try:
            checkpoint = self._context.add_or_update_checkpoint(
                name=checkpoint_name,
                validations=validations,
                action_list=action_list,
            )            
        except Exception as e:
            print(f"{self.error_msg_prefix} creating/updating Checkpoint: {e}")
            raise RuntimeError() from e
        return checkpoint
    
    def merge_checkpoint_validations(self, checkpoint: Any, new_validations: list[dict]) -> Any:
        """Merge new validations into an existing checkpoint.
        Args:
            checkpoint (Any): The existing checkpoint object.
            new_validations (list[dict]): New validation definitions to merge.
        Returns:
            Any: The updated checkpoint object."""
        try:
            existing_validations = checkpoint.validations or []
            merged_validations = existing_validations.copy()
            merged_validations.extend(new_validations)
            checkpoint.validations = merged_validations
            updated_checkpoint = self._context.add_or_update_checkpoint(
                name=checkpoint.name,
                validations=merged_validations,
                action_list=checkpoint.action_list,
            )
            print(f"{self.info_msg_prefix} Merged {len(new_validations)} new validations into Checkpoint '{checkpoint.name}'. Total validations now: {len(merged_validations)}.")
        except Exception as e:
            print(f"{self.error_msg_prefix} merging validations into Checkpoint: {e}")
            raise RuntimeError() from e
        return updated_checkpoint
# %%
if __name__ == "__main__":
    factory = GreatExpectationsContextFactory()
    context = factory.get_or_create_context()
