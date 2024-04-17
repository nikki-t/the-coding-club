"""
"""
import json
import os
import pathlib

import pandas as pd
import boto3


TMP = pathlib.Path("/tmp")


def upload_json(json_data, collection, output_s3_bucket):
    """Upload JSON data to S3 bucket which will serve as input to Lamba 2."""
    
    points_json = TMP.joinpath("points.json")
    with open(points_json, 'w') as jf:
        json.dump(json_data, jf, indent=2)
    
    session = boto3.Session(profile_name="saml-pub")
    s3 = session.resource("s3")
    
    s3.Bucket(output_s3_bucket).upload_file(points_json, f"{collection}/json/{points_json.name}")
    print(f"Uploaded: s3://{output_s3_bucket}/{collection}/json/{points_json.name}")
    
    points_json.unlink()


def lambda_handler_write(event, context):
    """Lambda event handler to write out single point"""
    # --------------------
    # Unpack event payload
    # --------------------

    input_rows = event["input_rows"]
    collection_name = event["collection_name"]

    rows_df = pd.DataFrame.from_dict(input_rows)

    # get the name of the user's output S3 bucket
    output_s3_bucket = event["output_granule_s3bucket"]

    # Set up S3 client for user output bucket.
    s3_out = boto3.client('s3')

    # ------------------------------------------------
    # Write parquet geographic points
    # ------------------------------------------------

    points_json = []
    for i, row in rows_df.iterrows():

        pid = row['point_id']
        ptime = row['time']

        output_key = collection_name + '/geo_points/' + str(pid) + '/' + str(ptime) + '.parquet'

        # create the temp path for Lambda to write results to locally
        tmp_file_path = TMP.joinpath(output_key)
        tmp_file_path.parent.mkdir(parents=True, exist_ok=True)

        # write the results to a parquet file
        try:
            print('writing file to tmp: ' + str(tmp_file_path))
            out_row = row.to_frame().T
            out_row.to_parquet(tmp_file_path)
        except Exception as e:
            print("Problem writing to tmp: " + str(e))

        s3_out.upload_file(tmp_file_path, output_s3_bucket, output_key)

        tmp_file_path.unlink(missing_ok=True)

        # save results to list
        points_json.append({
            "input_s3path": f"s3://{output_s3_bucket}/{output_key}",
            "collection_name": collection_name,
            "output_s3bucket": output_s3_bucket
        })
        
    # ------------------------------------------------
    # Upload list of points JSON to S3 bucket
    # ------------------------------------------------
    
    upload_json(points_json, collection_name, output_s3_bucket)
