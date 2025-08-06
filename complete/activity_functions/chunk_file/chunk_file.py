import os
from azure_blob_helper import AzureBlobHelper
from logging_config import setup_logger

logger = setup_logger("ChunkFile")

def main(input_data: dict):
    container = input_data["container"]
    file_path = input_data["file_path"]

    helper = AzureBlobHelper(
        os.environ["AzureWebJobsStorage"],
        os.environ["MONGO_CONN_STR"],
        os.environ["MONGO_DB"],
        os.environ["MONGO_COLLECTION"]
    )

    content = helper.read_blob_content(container, file_path)
    chunks = helper.chunk_content(content)
    return {"chunks": chunks, "file_path": file_path}

