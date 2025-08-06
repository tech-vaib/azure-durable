import os
from azure_blob_helper import AzureBlobHelper
from logging_config import setup_logger

logger = setup_logger("ProcessFile")

def main(input_data: dict):
    container = input_data["container"]
    prefix = input_data["prefix"]

    helper = AzureBlobHelper(
        os.environ["AzureWebJobsStorage"],
        os.environ["MONGO_CONN_STR"],
        os.environ["MONGO_DB"],
        os.environ["MONGO_COLLECTION"]
    )

    files = helper.list_files(container, prefix)
    logger.info(f"Returning {len(files)} files")
    return files

