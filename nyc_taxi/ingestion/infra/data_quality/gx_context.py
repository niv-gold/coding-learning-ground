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
from great_expectations.datasource.fluent import PandasS3Datasource
from typing import Callable, Optional, Dict, Any, List
from dotenv import load_dotenv
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
        return self._context 
    
    def get_or_create_pandas_s3_datasource(self, ds_name: str, ds_bucket: str, ds_region: str | None )-> PandasS3Datasource:
        """Get or create a Pandas S3 Asset in the GX context."""      
        # Create data_source (name it once; re-running should reuse or you can guard it)
        self.get_or_create_context()
        try:
            data_source = self._context.sources.add_or_update_pandas_s3(
                name=ds_name,
                bucket=ds_bucket,
                boto3_options={"region_name": ds_region} if ds_region else {},
            )
            return data_source
        except Exception as e:
            print("Error creating Pandas S3 data_source:", e)
            raise ConnectionError("Failed to create GX Pandas S3 data_source.") from e

    def _create_asset(self, data_source: PandasS3Datasource, **kwarg) -> Any:
                    #    asset_name: str, s3_prefix: str, batching_regex: str, asset_type: str) -> Any:
        """Create an asset in the specified data_source.
        Args:
            data_source (PandasS3Datasource): The data_source to add the asset to.
            asset_name (str): The name of the asset.
            s3_prefix (str): The S3 prefix where the asset files are located.
            batching_regex (str): The regex pattern for batching files.
            asset_type (str): The type of the asset (e.g., "csv", "parquet", "json").
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

            asset = creators[assert_type_norm](
                name = kwarg["asset_name"],
                s3_prefix = kwarg["s3_prefix"],
                batching_regex = kwarg["batching_regex"]
            )
            if asset is None:
                raise RuntimeError("Asset creation returned None.")
            print(f"--> Created new Asset '{kwarg["asset_name"]}'.")
            return asset
        
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
            asset = data_source.get_asset(kwarg["asset_name"])
            print(f"--> Found existing Asset '{kwarg["asset_name"]}'.")
            return asset
        except Exception:
            
            return self._create_asset( data_source = data_source, **kwarg)        


#%%
if __name__ == "__main__":    
    load_dotenv()
    s3_config = S3Config.from_env() 

    factory = GreatExpectationsContextFactory()
    ds = factory.get_or_create_pandas_s3_datasource(
        ds_name = "s3_raw_data",
        ds_bucket = s3_config.bucket_name,
        ds_region = s3_config.region_name
    )
    
#-------------------------------------------------------------------------------------------------------------------------------
#-------------------------------------------------------------------------------------------------------------------------------
    dc_asset_csv = GXS3AssetSpec(
        s3conf = s3_config,
        data_source = ds,
        data_source_name = ds.name,
        asset_name = "raw_csv",
        batching_regex = r"(?P<file_name>.*)\.csv",
        asset_type = "csv"
    )
    ds_asset_csv = factory.get_or_create_asset( data_source = dc_asset_csv.data_source, **dc_asset_csv.to_kwarg())
    print("data_source and Asset ensured:\n", ds_asset_csv)

#-------------------------------------------------------------------------------------------------------------------------------
#-------------------------------------------------------------------------------------------------------------------------------
    dc_asset_parquet = GXS3AssetSpec(
        s3conf = s3_config,
        data_source = ds,
        data_source_name = ds.name,
        asset_name = "raw_parquet",
        batching_regex = r"(?P<file_name>.*)\.parquet",
        asset_type = "parquet"
    )
    ds_asset_parquet = factory.get_or_create_asset( data_source = dc_asset_csv.data_source, **dc_asset_parquet.to_kwarg())
    print("data_source and Asset ensured:\n", ds_asset_parquet)