if __name__ == "__main__":
    os.environ["AzureWebJobsStorage"] = "<your-azure-storage-connection-string>"
    os.environ["MONGO_CONN_STR"] = "<your-cosmos-mongo-connection-string>"
    os.environ["MONGO_DB"] = "your-db"
    os.environ["MONGO_COLLECTION"] = "chunks"

    helper = AzureBlobHelper(
        blob_conn_str=os.environ["AzureWebJobsStorage"],
        mongo_conn_str=os.environ["MONGO_CONN_STR"],
        mongo_db=os.environ["MONGO_DB"],
        mongo_collection=os.environ["MONGO_COLLECTION"]
    )

    # Upload local folder (optional)
    # helper.upload_folder("my-container", "./myfolder", "raw-data/")

    helper.process_blobs_to_chunks_and_store("my-container", "raw-data/")

