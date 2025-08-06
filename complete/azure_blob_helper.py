import os
import uuid
import logging
from typing import List
from azure.storage.blob import BlobServiceClient
from pymongo import MongoClient
from logging_config import setup_logger

logger = setup_logger("AzureBlobHelper")

class AzureBlobHelper:
    def __init__(self, blob_conn_str, mongo_conn_str, mongo_db, mongo_collection):
        self.service_client = BlobServiceClient.from_connection_string(blob_conn_str)
        self.mongo_client = MongoClient(mongo_conn_str)
        self.mongo_collection = self.mongo_client[mongo_db][mongo_collection]

    def list_files(self, container_name: str, prefix: str) -> List[str]:
        container = self.service_client.get_container_client(container_name)
        files = [blob.name for blob in container.list_blobs(name_starts_with=prefix)]
        logger.info(f"Listed {len(files)} files from {container_name}/{prefix}")
        return files

    def read_blob_content(self, container_name: str, blob_path: str) -> str:
        blob = self.service_client.get_blob_client(container=container_name, blob=blob_path)
        content = blob.download_blob().readall().decode("utf-8")
        logger.info(f"Read blob: {blob_path} ({len(content)} chars)")
        return content

    def upload_file(self, container_name: str, blob_path: str, content: str) -> str:
        blob = self.service_client.get_blob_client(container=container_name, blob=blob_path)
        blob.upload_blob(content, overwrite=True)
        logger.info(f"Uploaded blob to {blob_path}")
        return f"Uploaded {blob_path}"

    def upload_folder(self, container_name: str, local_path: str, blob_prefix=""):
        for root, _, files in os.walk(local_path):
            for file in files:
                local_file = os.path.join(root, file)
                rel_path = os.path.relpath(local_file, local_path).replace("\\", "/")
                blob_path = os.path.join(blob_prefix, rel_path).replace("\\", "/")

                with open(local_file, "rb") as data:
                    blob = self.service_client.get_blob_client(container_name, blob_path)
                    blob.upload_blob(data, overwrite=True)
                logger.info(f"Uploaded file {local_file} to {blob_path}")

    def chunk_content(self, content: str, size=1000):
        chunks = [content[i:i+size] for i in range(0, len(content), size)]
        logger.info(f"Split content into {len(chunks)} chunks")
        return chunks

    def write_chunks_to_db(self, chunks, source_file: str):
        for chunk in chunks:
            self.mongo_collection.insert_one({
                "id": str(uuid.uuid4()),
                "chunk": chunk,
                "source_file": source_file
            })
        logger.info(f"Inserted {len(chunks)} chunks to MongoDB from {source_file}")

