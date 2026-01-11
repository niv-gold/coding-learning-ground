#%%
from __future__ import annotations
import sys
from pathlib import Path
if __name__ == "__main__":
    PROJECT_ROOT = Path(__file__).resolve().parents[4]
    sys.path.insert(0, str(PROJECT_ROOT))
    print("Project root added to sys.path[0]:", sys.path[0])

from nyc_taxi.ingestion.config.settings import GreatExpectationsConfig, S3Config
import great_expectations as gx
from dotenv import load_dotenv
load_dotenv()
#%%
class GreatExpectationsContextFactory:
    """
    Responsible ONLY for:
    - Creating / loading the GX File Data Context
    - Ensuring required datasources and assets exist
    """
    def __init__(self):
        self._context: gx.DataContext  = None

    @staticmethod
    def _create_context() -> gx.DataContext:       
        """Create or load Great Expectations DataContext."""
        try:
            gx_project_root_dir = GreatExpectationsConfig.from_env().ge_root_dir
            context = gx.get_context(mode="file", project_root_dir=gx_project_root_dir)
            return context
        except Exception as e:
            print("Error creating/loading Great Expectations DataContext:", e)
    
    def _get_context(self) -> gx.DataContext:
        """Get the Great Expectations DataContext."""
        if self._context is None:
            self._context = self._create_context()
        return self._context
    
    def get_or_create_pandas_s3_datasource(self, ds_name: str, ds_bucket: str, ds_region: str | None ) -> gx.datasource.data_asset.PandasS3CsvAsset:
        """Get or create a Pandas S3 CSV Asset in the GX context."""
        context = self._get_context()        
        # Create datasource (name it once; re-running should reuse or you can guard it)
        try:
            if self._context.datasources.get(ds_name):
                print(f"Pandas S3 Datasource '{ds_name}' already exists. Reusing it.")
                ds = self._context.datasources.get(ds_name)
                return ds
            else:
                print(f"Creating Pandas S3 Datasource: {ds_name}")
                ds = context.sources.add_pandas_s3(
                    name=ds_name,
                    bucket=ds_bucket,
                    boto3_options={"region_name": ds_region} if ds_region else {},
                )
                return ds
        except Exception as e:
            print("Error creating Pandas S3 Datasource:", e)
            return None

    def get_or_create_csv_asset(self, datasource_name: str, asset_name: str, s3_prefix: str, batching_regex: str) -> gx.datasource.data_asset.PandasS3CsvAsset:
        """Get or create a CSV asset in the specified datasource."""
        context = self._get_context()
        try:
            ds = context.datasources.get(datasource_name)
            if ds is None:
                print(f"Datasource '{datasource_name}' does not exist.")
                return None
        except Exception as e:
            print("Error retrieving datasource:", e)
        try:
            if ds.assets.get_by_name(asset_name):
                print(f"CSV Asset '{asset_name}' already exists in datasource '{datasource_name}'. Reusing it.")
                asset = ds.get_asset(asset_name)
                return asset
            else:
                print(f"Creating CSV Asset '{asset_name}' in datasource '{datasource_name}'.")
                asset = ds.add_csv_asset(
                    name=asset_name,
                    s3_prefix=s3_prefix,
                    batching_regex=batching_regex,
                )
                return asset
        except Exception as e:
            print("Error creating CSV Asset:", e)

#%%
if __name__ == "__main__":    
    load_dotenv()
    s3_config = S3Config.from_env()

    factory = GreatExpectationsContextFactory()
    ds = factory.get_or_create_pandas_s3_datasource(
        ds_name="s3_raw_data",
        ds_bucket=s3_config.bucket_name,
        ds_region=s3_config.region_name
    )
    print("Datasource configured:", ds.name)
    asset = factory.get_or_create_csv_asset(
        datasource_name="s3_raw_data",
        asset_name="raw_csv_data",
        s3_prefix=s3_config.s3_base_prefix_name,
        batching_regex=r"(?P<file_name>.*)\.csv",
    )
    print("Asset configured:", asset.name)
            
# %%
