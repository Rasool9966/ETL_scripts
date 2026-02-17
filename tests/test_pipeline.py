import sys
import os

# Add project root to Python path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))

from ETL_scripts.extract import extract_data
from ETL_scripts.transform import transform_data
from ETL_scripts.load import load_to_s3
from ETL_scripts.config import S3_BUCKET, S3_KEY, AWS_REGION



def test_full_pipeline():

    # Step 1: Extract
    df = extract_data("employees")

    assert df is not None
    assert len(df) > 0

    # Step 2: Transform
    df = transform_data(df)

    assert "etl_created_at" in df.columns
    assert "source" in df.columns

    # Step 3: Load
    load_to_s3(df)

    # Step 4: Verify file exists in S3
    s3 = boto3.client("s3", region_name=AWS_REGION)

    response = s3.head_object(
        Bucket=S3_BUCKET,
        Key=S3_KEY
    )

    assert response["ResponseMetadata"]["HTTPStatusCode"] == 200

    print("Pipeline integration test passed")
