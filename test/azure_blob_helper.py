import os
import logging
import uuid
from azure.storage.blob import BlobServiceClient
from pymongo import MongoClient
from typing import List

logger = logging.getLogger("azure_blob_helper")
logging.basicConfig(level=logging.INFO)

class AzureBlobHelper:
    def __init__(self, blob_conn_str: str, mongo_conn_str: str, mongo_db: str, mongo_collection: str):
        self.blob_conn_str = blob_conn_str
        self.service_client = BlobServiceClient.from_connection_string(blob_conn_str)

        self.mongo_client = MongoClient(mongo_conn_str)
        self.mongo_db = self.mongo_client[mongo_db]
        self.mongo_collection = self.mongo_db[mongo_collection]

    def list_files(self, container_name: str, prefix: str) -> List[str]:
        container_client = self.service_client.get_container_client(container_name)
        return [blob.name for blob in container_client.list_blobs(name_starts_with=prefix)]

    def read_blob_content(self, container_name: str, blob_path: str) -> str:
        blob_client = self.service_client.get_blob_client(container=container_name, blob=blob_path)
        return blob_client.download_blob().readall().decode('utf-8')

    def upload_file(self, container_name: str, blob_path: str, content: str) -> str:
        blob_client = self.service_client.get_blob_client(container_name, blob_path)
        blob_client.upload_blob(content, overwrite=True)
        return f"Uploaded to {blob_path}"

    def upload_folder(self, container_name: str, local_folder_path: str, blob_prefix: str = ""):
        if not os.path.exists(local_folder_path):
            raise ValueError(f"Local folder path does not exist: {local_folder_path}")

        for root, _, files in os.walk(local_folder_path):
            for file in files:
                local_file_path = os.path.join(root, file)
                relative_path = os.path.relpath(local_file_path, local_folder_path)
                blob_path = os.path.join(blob_prefix, relative_path).replace("\\", "/")

                logger.info(f"Uploading {local_file_path} to {blob_path}")
                with open(local_file_path, "rb") as data:
                    blob_client = self.service_client.get_blob_client(container=container_name, blob=blob_path)
                    blob_client.upload_blob(data, overwrite=True)

    def process_blobs_to_chunks_and_store(self, container: str, path_prefix: str, chunk_size: int = 1000):
        files = self.list_files(container, path_prefix)
        logger.info(f"Found {len(files)} files in path {path_prefix}")

        for file_path in files:
            logger.info(f"Reading file: {file_path}")
            try:
                content = self.read_blob_content(container, file_path)
                chunks = [content[i:i + chunk_size] for i in range(0, len(content), chunk_size)]

                for chunk in chunks:
                    doc = {"id": str(uuid.uuid4()), "chunk": chunk, "source_file": file_path}
                    self.mongo_collection.insert_one(doc)
                    logger.debug(f"Inserted chunk from {file_path}")
            except Exception as e:
                logger.error(f"Error processing file {file_path}: {e}")
