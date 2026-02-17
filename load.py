import boto3
from io import StringIO
from config import AWS_REGION, S3_BUCKET, S3_KEY


def load_to_s3(df):

    s3 = boto3.client("s3", region_name=AWS_REGION)

    buffer = StringIO()
    df.to_csv(buffer, index=False)

    s3.put_object(
        Bucket=S3_BUCKET,
        Key=S3_KEY,
        Body=buffer.getvalue()
    )

    print("Loaded to S3")
