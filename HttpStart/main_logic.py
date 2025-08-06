import os
import uuid
import logging
from azure.storage.blob import BlobServiceClient
from pymongo import MongoClient
from typing import List

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

class AzureBlobHelper:
    def __init__(self, connection_str: str = None):
        self.connection_str = connection_str or os.getenv("AzureWebJobsStorage")
        self.service_client = BlobServiceClient.from_connection_string(self.connection_str)

    def list_files(self, container_name: str, prefix: str) -> list:
        container_client = self.service_client.get_container_client(container_name)
        return [blob.name for blob in container_client.list_blobs(name_starts_with=prefix)]

    def read_file(self, container_name: str, blob_path: str) -> str:
        blob_client = self.service_client.get_blob_client(container_name, blob_path)
        downloader = blob_client.download_blob()
        return downloader.readall().decode('utf-8')

class CosmosMongoHelper:
    def __init__(self, mongo_conn_str: str, db_name: str, collection_name: str):
        client = MongoClient(mongo_conn_str)
        self.collection = client[db_name][collection_name]

    def insert_chunks(self, chunks: List[dict]):
        if chunks:
            self.collection.insert_many(chunks)

# Utility to chunk text
def chunk_text(text: str, max_chunk_size: int = 1000) -> List[str]:
    words = text.split()
    chunks = []
    chunk = []
    size = 0
    for word in words:
        if size + len(word) + 1 > max_chunk_size:
            chunks.append(' '.join(chunk))
            chunk = [word]
            size = len(word)
        else:
            chunk.append(word)
            size += len(word) + 1
    if chunk:
        chunks.append(' '.join(chunk))
    return chunks

def process_and_store_chunks(container_name: str, prefixes: list, bearer_token: str):
    VALID_TOKEN = os.getenv("VALID_BEARER_TOKEN", "your_valid_token")

    logger.info(f"Starting chunk processing for container: {container_name} with prefixes: {prefixes}")

    if not bearer_token or bearer_token != VALID_TOKEN:
        logger.warning("Invalid bearer token received.")
        raise PermissionError("Invalid bearer token.")

    blob_helper = AzureBlobHelper()
    cosmos_helper = CosmosMongoHelper(
        mongo_conn_str=os.getenv("MONGO_CONN_STR"),
        db_name=os.getenv("MONGO_DB_NAME", "blob_chunks"),
        collection_name=os.getenv("MONGO_COLLECTION", "chunks")
    )

    for prefix in prefixes:
        logger.info(f"Processing prefix: {prefix}")
        files = blob_helper.list_files(container_name, prefix)
        logger.info(f"Found {len(files)} files in prefix '{prefix}'.")

        for file_path in files:
            logger.info(f"Reading file: {file_path}")
            content = blob_helper.read_file(container_name, file_path)
            chunks = chunk_text(content)

            logger.info(f"Generated {len(chunks)} chunks from file {file_path}")

            enriched_chunks = [{
                "_id": str(uuid.uuid4()),
                "file_path": file_path,
                "chunk_index": idx,
                "text": chunk
            } for idx, chunk in enumerate(chunks)]

            cosmos_helper.insert_chunks(enriched_chunks)
            logger.info(f"Inserted chunks for file: {file_path}")

    logger.info("All chunks processed and stored.")
    return {"status": "success", "message": "Chunks inserted successfully."}


# Example standalone usage (optional)
if __name__ == "__main__":
    container = "your-container"
    prefixes = ["path/to/files1/", "path/to/files2/"]
    token = "your_valid_token"

    try:
        result = process_and_store_chunks(container, prefixes, token)
        logger.info(result)
    except Exception as e:
        logger.error(f"Error occurred: {str(e)}")
