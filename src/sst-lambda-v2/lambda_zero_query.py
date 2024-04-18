import json
import boto3
import earthaccess

import pathlib

TMP = pathlib.Path("/tmp")

def lambda_handler_query(event, context):
    """Lambda handler that queries CMR and writes out granules to JSON file 
    which is uploaded to an S3 bucket."""
    
    prefix = event["prefix"]
    s3_bucket = event["s3_bucket"]
    collection_shortnames = event["collection_shortnames"].split(',')
    start_time = event["start_time"]
    end_time = event["end_time"]
    
    s3 = boto3.resource("s3")
    
    for collection in collection_shortnames:
        
        # Query for granules
        granules = earthaccess.search_data(
            short_name=collection,
            cloud_hosted=True,
            temporal=(start_time, end_time)
        )
        
        granule_paths = []
        for g in granules:
            granule_paths.append({
                "input_granule_path": g.data_links(access='direct')[0],
                "output_granule_s3bucket": s3_bucket,
                "prefix": prefix,
                "collection_name": collection
            })
        
        # Write out granules to JSON file
        granule_json = TMP.joinpath("granules.json")
        with open(granule_json, 'w') as jf:
            json.dump(granule_paths, jf, indent=2)
        
        # Upload JSON file to S3 bucket
        s3.Bucket(s3_bucket).upload_file(granule_json, f"{collection}/json/{granule_json.name}")
        print(f"Uploaded: s3://{s3_bucket}/{collection}/json/{granule_json.name}")
        
        # Delete JSON file
        granule_json.unlink()
