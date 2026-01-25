#%%
from __future__ import annotations
import sys
from pathlib import Path

from traitlets import Any
if __name__ == "__main__":
    PROJECT_ROOT = Path(__file__).resolve().parents[4]
    sys.path.insert(0, str(PROJECT_ROOT))
    print("Project root added to sys.path[0]:", sys.path[0])

from nyc_taxi.ingestion.config.settings import GreatExpectationsConfig, S3Config, GXS3AssetSpec
import great_expectations as gx
from great_expectations.datasource.fluent import PandasS3Datasource, BatchRequest
from nyc_taxi.ingestion.config.settings import GreatExpectationsConfig 
from great_expectations.core import ExpectationSuite
from great_expectations.core.expectation_configuration import ExpectationConfiguration
from great_expectations.core.batch import Batch
from typing import Callable, Optional, Dict, Any
from datetime import datetime, timezone
from dotenv import load_dotenv
import json

#%%
class GreatExpectationsContextFactory:
    """
    Responsible ONLY for:
    - Creating / loading the GX File Data Context
    - Ensuring required data_sources and assets exist
    """
    def __init__(self):
        self._context: gx.DataContext | None = None
        self.gx_project_root_dir: str = GreatExpectationsConfig.from_env().ge_root_dir

    # ---------------------------------------------------------------------------------------
    # Context, data_source building blocks
    # ---------------------------------------------------------------------------------------
    def _create_context(self) -> gx.DataContext:       
        """Create or load Great Expectations DataContext."""
        try:
            context = gx.get_context(mode="file", project_root_dir=self.gx_project_root_dir)
            return context
        except Exception as e:
            print("Error creating/loading Great Expectations DataContext:", e)
            raise ConnectionError("Failed to create/load GX DataContext.") from e

    def get_or_create_context(self) -> gx.DataContext | None:
        """Get the Great Expectations DataContext."""
        if self._context is None:
            self._context = self._create_context()
        print("--> Great Expectations DataContext ready.")
        return self._context 
    
    def get_or_create_pandas_s3_datasource(self, ds_name: str, **kwarg )-> PandasS3Datasource:
        """Get or create a Pandas S3 Asset in the GX context."""      
        # Create data_source (name it once; re-running should reuse or you can guard it)
        self.get_or_create_context()
        try:
            data_source = self._context.sources.add_or_update_pandas_s3(
                name=ds_name,
                bucket=kwarg["bucket_name"],
                boto3_options={"region_name": kwarg["region_name"]} if kwarg["region_name"] else {},
            )
            print(f"--> Created/Reused Pandas S3 data_source '{ds_name}'.")
            return data_source
        except Exception as e:
            print("Error creating Pandas S3 data_source:", e)
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
                s3_prefix = kwarg["s3_prefix"],
                batching_regex = kwarg["batching_regex"]
            )

            # Sanity check
            if _DataAssetT is None:
                raise RuntimeError("Asset creation returned None.")
            print(f"--> Created new Asset '{kwarg["asset_name"]}'.")
            return _DataAssetT

        except Exception as e:
            print("Error creating Asset:", e)
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
            print(f"--> Found existing Asset '{kwarg["asset_name"]}'.")
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
        print(f"--> Built {len(expactations)} landing expectations.")
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

        print(f"--> Built suite metadata with keys: {list(meta.keys())}.")
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
            print(f"--> Created new Expectation Suite '{suite_name}'.")

        # Update meta (merge so we don't lose other keys)
        suite.meta = {**(suite.meta or {}), **meta}

        # Decide overwrite policy centrally
        if overwrite_expectations:
            suite.expectations = expectations

        # Persist idempotently (0.18.21)
        self._context.add_or_update_expectation_suite(expectation_suite=suite)
        print(f"--> Upserted Expectation Suite '{suite_name}' with {len(suite.expectations)} expectations.")
        
        return suite

    def get_asset_batch_requests(self, datasource: PandasS3Datasource, asset_name: str, asset_type: str) -> list[Any]:
        
        if datasource is None:
            raise ValueError("datasource must be a non-empty.")
        if not asset_name or not asset_name.strip():
            raise ValueError("asset_name must be a non-empty string.")
            
        try:
            _DataAssetT = datasource.get_asset(asset_name)
            batch_request_all = _DataAssetT.build_batch_request()
            batches: list[Batch] = _DataAssetT.get_batch_list_from_batch_request(batch_request_all)
            print(f"--> Found {len(batches)} batches in {asset_type} Asset:")

            batch_requests = [Any]
            for i, batch in enumerate(batches, start=1):
                identifiers = batch.batch_definition.batch_identifiers                
                batch_request = _DataAssetT.build_batch_request(options=identifiers)
                batch_requests.append(batch_request)
                print(f"Batch {i}: {identifiers}")

            # for batch in Batches:
            #     print(json.dumps(
            #         batch.batch_definition.batch_identifiers,
            #         indent=2,
            #         default=str,
            #     ))        
        except Exception as e:
            print("Error retrieving batche request:", e)
            raise RuntimeError() from e 
        return batch_requests

    def get_or_create_validation():
        pass

    def get_or_create_validation_list():
        pass
    

#%%
if __name__ == "__main__":    
    load_dotenv()
    s3_config = S3Config.from_env() 

#-------------------------------------------------------------------------------------------------------------------------------
#-------------------------------------------------------------------------------------------------------------------------------
    factory = GreatExpectationsContextFactory()
    ds_name = f"s3_{s3_config.bucket_name}_{s3_config.s3_base_prefix_name}"
    ds = factory.get_or_create_pandas_s3_datasource(ds_name = ds_name, **s3_config.to_connector_kwarg())

#-------------------------------------------------------------------------------------------------------------------------------
#-------------------------------------------------------------------------------------------------------------------------------
    dc_asset_csv = GXS3AssetSpec(
        s3conf=s3_config,
        data_source=ds,
        data_source_name=ds.name,
        asset_name="raw_csv",
        s3_prefix_relative="csv/",  # Relative folder (combined with base prefix)
        batching_regex=r"(?P<file_name>.*)\.csv",
        asset_type="csv"
    )

    factory.get_or_create_asset( data_source = dc_asset_csv.data_source, **dc_asset_csv.to_kwarg())
#-------------------------------------------------------------------------------------------------------------------------------
#-------------------------------------------------------------------------------------------------------------------------------
    dc_asset_parquet = GXS3AssetSpec(
        s3conf=s3_config,
        data_source=ds,
        data_source_name=ds.name,
        asset_name="raw_parquet",
        s3_prefix_relative="parquet/",  # Relative folder (combined with base prefix)
        batching_regex=r"(?P<file_name>.*)\.parquet",
        asset_type="parquet"
    )
    factory.get_or_create_asset( data_source = dc_asset_parquet.data_source, **dc_asset_parquet.to_kwarg())
#-------------------------------------------------------------------------------------------------------------------------------
#-------------------------------------------------------------------------------------------------------------------------------

    batch_requests_csv = factory.get_asset_batch_requests( datasource = ds, asset_name = dc_asset_csv.asset_name,
                                                          asset_type = dc_asset_csv.asset_type)
    print(batch_requests_csv)
    batch_requests_parquet = factory.get_asset_batch_requests( datasource = ds, asset_name = dc_asset_parquet.asset_name,
                                                          asset_type = dc_asset_parquet.asset_type)
    print(batch_requests_parquet)