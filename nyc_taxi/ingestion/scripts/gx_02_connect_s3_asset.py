#%% step 1: add project root to sys.path (for local imports)
import sys
from pathlib import Path
if __name__ == "__main__":
    PROJECT_ROOT = Path(__file__).resolve().parents[3]
    sys.path.insert(0, str(PROJECT_ROOT))
    print("Project root added to sys.path[0]:", sys.path[0])

#%% step 2: import and load context
import great_expectations as gx
from dotenv import load_dotenv
load_dotenv()
from nyc_taxi.ingestion.config.settings import GreatExpectationsConfig, S3Config

#%% step 3: connect to S3 asset
region = S3Config.from_env().region_name
bucket = S3Config.from_env().bucket_name
prefix = S3Config.from_env().s3_base_prefix_name

#%% step 4: create S3 asset
gx_project_root_dir = GreatExpectationsConfig.from_env().ge_root_dir
context = gx.get_context(mode="file", project_root_dir=gx_project_root_dir)

# Create datasource (name it once; re-running should reuse or you can guard it)
ds = context.sources.add_pandas_s3(
    name="s3_landing",
    bucket=bucket,
    boto3_options={"region_name": region} if region else {},
)

# Create an asset for CSVs under the prefix
asset = ds.add_csv_asset(
    name="landing_csv",
    s3_prefix=prefix,
    batching_regex=r"(?P<file_name>.*)\.csv",
)

print("Datasource + asset configured:", ds.name, asset.name)

# %%

C