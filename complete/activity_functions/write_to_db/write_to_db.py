import os
from azure_blob_helper import AzureBlobHelper
from logging_config import setup_logger

logger = setup_logger("WriteToDB")

def main(input_data: dict):
    helper = AzureBlobHelper(
        os.environ["AzureWebJobsStorage"],
        os.environ["MONGO_CONN_STR"],
        os.environ["MONGO_DB"],
        os.environ["MONGO_COLLECTION"]
    )
    helper.write_chunks_to_db(input_data["chunks"], input_data["file_path"])
    return f"Stored chunks from {input_data['file_path']}"

