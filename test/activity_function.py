import azure.functions as func
import logging
import os
import jwt
from azure_blob_helper import AzureBlobHelper

def main(blobInputs: dict) -> str:
    bearer_token = blobInputs.get("bearer_token")
    container = blobInputs.get("container")
    prefix = blobInputs.get("prefix")

    # Validate Token (stub)
    try:
        decoded = jwt.decode(bearer_token, options={"verify_signature": False})
        logging.info(f"Token validated for: {decoded.get('sub')}")
    except Exception as e:
        logging.error("Invalid token")
        raise Exception("Invalid Bearer Token")

    helper = AzureBlobHelper(
        blob_conn_str=os.environ["AzureWebJobsStorage"],
        mongo_conn_str=os.environ["MONGO_CONN_STR"],
        mongo_db=os.environ["MONGO_DB"],
        mongo_collection=os.environ["MONGO_COLLECTION"]
    )

    helper.process_blobs_to_chunks_and_store(container, prefix)
    return f"Processing completed for path {prefix} in container {container}"
