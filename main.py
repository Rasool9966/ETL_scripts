from ETL_scripts.extract import extract_data
from ETL_scripts.transform import transform_data
from ETL_scripts.load import load_to_s3



def run_pipeline():

    df = extract_data("employees")

    df = transform_data(df)

    load_to_s3(df)


if __name__ == "__main__":
    run_pipeline()
