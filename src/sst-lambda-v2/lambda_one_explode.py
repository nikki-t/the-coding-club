"""
"""
import json
import logging
import base64
import requests
import s3fs
import boto3
import botocore
import xarray as xr
import pandas as pd
import numpy as np
import pathlib
import os

S3_ENDPOINT_DICT = {
    'podaac': 'https://archive.podaac.earthdata.nasa.gov/s3credentials'
}
TMP = pathlib.Path("/tmp")

# Handle EDL login & S3 credentials
def get_creds(s3_endpoint, edl_username, edl_password):
    """Request and return temporary S3 credentials.

    Taken from: https://archive.podaac.earthdata.nasa.gov/s3credentialsREADME
    """

    login = requests.get(
        s3_endpoint,
        allow_redirects=False,
        timeout=30
    )
    login.raise_for_status()

    auth = f"{edl_username}:{edl_password}"
    encoded_auth = base64.b64encode(auth.encode('ascii'))

    auth_redirect = requests.post(
        login.headers['location'],
        data={"credentials": encoded_auth},
        headers={"Origin": s3_endpoint},
        allow_redirects=False,
        timeout=30
    )
    auth_redirect.raise_for_status()
    final = requests.get(
        auth_redirect.headers['location'],
        allow_redirects=False,
        timeout=30)
    results = requests.get(
        s3_endpoint,
        cookies={'accessToken': final.cookies['accessToken']},
        timeout=30)
    results.raise_for_status()
    return json.loads(results.content)


def get_temp_creds(prefix):
    """retreive EDL credentials from AWS Parameter Store
    """
    try:
        ssm_client = boto3.client('ssm', region_name="us-west-2")
        edl_username = ssm_client.get_parameter(
            Name=f"{prefix}-sst-edl-username",
            WithDecryption=True)["Parameter"]["Value"]
        edl_password = ssm_client.get_parameter(
            Name=f"{prefix}-sst-edl-password",
            WithDecryption=True)["Parameter"]["Value"]
        print("Retrieved Earthdata login credentials.")
    except botocore.exceptions.ClientError as error:
        raise error

    # use EDL creds to get AWS S3 Access Keys & Tokens
    s3_creds = get_creds(S3_ENDPOINT_DICT[prefix], edl_username, edl_password)
    print("Retrieved temporary S3 access credentials.")

    return s3_creds


def convert_to_dataframe(data_in):
    """
    Convert data from xarray Dataset to Pandas Dataframe,
    formatted to be written as parquet

    Parameters
    ==========
    data_in: xarray.Dataset()
        the input dataset

    Returns
    =======
    df: pandas.DataFrame()
        the parquet-formatted dataframe

    """

    time = str(data_in.time.values[0])

    lats = data_in.lat.values
    lons = data_in.lon.values

    sst = data_in['analysed_sst'][0, :, :].values
    sst_anom = data_in['sst_anomaly'][0, :, :].values

    data_sst = sst.reshape(-1)
    data_sst_anom = sst_anom.reshape(-1)

    Y, X = np.unravel_index(range(sst.size), sst.shape)

    num_points = len(data_sst)
    point_id_list = [i for i in range(num_points)]

    time_list = [time for _ in range(num_points)]

    lat_value_list = [lats[idx] for idx in Y]
    lon_value_list = [lons[idx] for idx in X]

    # ---------------------------------
    # create pandas dataframe structure 
    # ---------------------------------

    columns = ["time",
               "point_id",
               "Y",
               "X",
               "lats",
               "lons",
               "analysed_sst",
               "sst_anomaly"]

    df = pd.DataFrame(columns=columns)

    tuple_list = list(zip(time_list,
                          point_id_list,
                          Y,
                          X,
                          lat_value_list,
                          lon_value_list,
                          data_sst,
                          data_sst_anom))

    # Add data to the output dataframe
    new_data = pd.DataFrame(tuple_list, columns=columns)

    df = pd.concat([df, new_data])

    return df


def upload_json(json_data, collection, output_s3_bucket):
    """Upload JSON data to S3 bucket which will serve as input to Lamba 2."""
    
    explode_json = TMP.joinpath("explode.json")
    with open(explode_json, 'w') as jf:
        json.dump(json_data, jf, indent=2)
    
    session = boto3.Session(profile_name="saml-pub")
    s3 = session.resource("s3")
    
    s3.Bucket(output_s3_bucket).upload_file(explode_json, f"{collection}/json/{explode_json.name}")
    print(f"Uploaded: s3://{output_s3_bucket}/{collection}/json/{explode_json.name}")
    
    explode_json.unlink()


def lambda_handler_explode(event, context):
    """Lambda event handler to orchestrate calculation of global mean."""
    # --------------------
    # Unpack event payload
    # --------------------

    prefix = event["prefix"]

    input_granule_path = event["input_granule_s3path"]
    collection_name = event["collection_name"]

    # get the name of the user's output S3 bucket
    output_s3_bucket = event["output_granule_s3bucket"]

    # ---------------------------------------------
    # Read data from Earthdata S3 buckets using EDL
    # ---------------------------------------------

    # Get EDL credentials from AWS Parameter Store
    temp_creds_req = get_temp_creds(prefix)

    # Set up S3 client for Earthdata buckets using EDL creds
    s3_client_in = s3fs.S3FileSystem(
        anon=False,
        key=temp_creds_req['accessKeyId'],
        secret=temp_creds_req['secretAccessKey'],
        token=temp_creds_req['sessionToken']
    )

    # open the granule as an s3 obj
    s3_file_obj = s3_client_in.open(input_granule_path, mode='rb')

    # ------------------------------------------------
    # Explode the nc file to parquet geographic points
    # ------------------------------------------------

    # open data in xarray
    ds = xr.open_dataset(s3_file_obj, engine='h5netcdf')
    sst_df = convert_to_dataframe(ds)
    
    # Subset for testing and development
    if os.getenv("SUBSET_DF") == "true":
        subset = int(os.getenv("SUBSET_SIZE"))
        sst_df = sst_df[:subset]    # TEMPORARY SUBSET FOR DEVELOPMENT

    batch_size = int(os.getenv("BATCH_SIZE"))
    json_data = []
    for i in range(0, sst_df.shape[0], batch_size):
        chunk = sst_df[i: i + batch_size]        
        json_data.append({
            "input_rows": json.loads(chunk.to_json()),
            "output_granule_s3bucket": output_s3_bucket,
            "collection_name": collection_name
        })
        print(f"Next Lambda event payload: {json_data}")
    
    # ------------------------------------------------
    # Upload list of points JSON to S3 bucket
    # ------------------------------------------------
    
    upload_json(json_data, collection_name, output_s3_bucket)
