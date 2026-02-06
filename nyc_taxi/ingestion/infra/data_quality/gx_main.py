from __future__ import annotations
import sys
from pathlib import Path

from dotenv import load_dotenv
if __name__ == "__main__":
    PROJECT_ROOT = Path(__file__).resolve().parents[4]
    sys.path.insert(0, str(PROJECT_ROOT))
    print("Project root added to sys.path[0]:", sys.path[0])

from nyc_taxi.ingestion.config.settings import S3Config
from nyc_taxi.ingestion.infra.data_quality.gx_context_factory import GreatExpectationsContextFactory
from nyc_taxi.ingestion.infra.data_quality.gx_manager import GreatExpectationsManager
from nyc_taxi.ingestion.config.settings import GXS3AssetSpec, GXValidationSpec, S3Config, GXCheckpointSpec


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
cp_spec_s3_raw_ingest = GXCheckpointSpec(
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
manager.ensure_s3_raw_validation_Definition(vd_spec=vd_csv_spec, data_source=ds_s3_raw)
# Parquet
manager.ensure_s3_raw_validation_Definition(vd_spec=vd_parquet_spec, data_source=ds_s3_raw)

# Ensure Checkpoint
checkpoint = manager.ensure_checkpoint(cp_spec=cp_spec_s3_raw_ingest)

# Ensure Run Checkpoint and docs build
cp_result = manager.ensure_run_checkpoint_and_build_docs(checkpoint=checkpoint)

# Print user-friendly checkpoint result summary
friendly_result = factory.extract_friendly_checkpoint_result(cp_result)
final_result = factory.flatten_checkpoint_summary_for_table(friendly_result)
print("\nCheckpoint Result Summary:")
for record in final_result:
    print(f'{record} : {final_result[record]}\n')