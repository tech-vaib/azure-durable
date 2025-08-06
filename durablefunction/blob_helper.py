from azure.storage.blob import BlobServiceClient
import os


class AzureBlobHelper:
    def __init__(self, connection_str: str = None):
        self.connection_str = connection_str or os.getenv("AzureWebJobsStorage")
        self.service_client = BlobServiceClient.from_connection_string(self.connection_str)

    def list_files(self, container_name: str, prefix: str = "") -> list:
        """
        List all files in a container with a given prefix path.
        """
        container_client = self.service_client.get_container_client(container_name)
        return [blob.name for blob in container_client.list_blobs(name_starts_with=prefix)]

    def read_file(self, container_name: str, blob_path: str) -> str:
        """
        Read contents of a blob as UTF-8 string.
        """
        blob_client = self.service_client.get_blob_client(container_name, blob_path)
        downloader = blob_client.download_blob()
        return downloader.readall().decode('utf-8')

    def upload_file(self, container_name: str, blob_path: str, content: str) -> str:
        """
        Upload string content to a blob.
        """
        blob_client = self.service_client.get_blob_client(container_name, blob_path)
        blob_client.upload_blob(content, overwrite=True)
        return f"Uploaded to {blob_path}"

    def upload_folder(self, container_name: str, local_folder_path: str, blob_prefix: str = ""):
        """
        Upload all files from a local folder to the blob container under the given prefix.
        """
        if not os.path.exists(local_folder_path):
            raise ValueError(f"Local folder path does not exist: {local_folder_path}")

        for root, _, files in os.walk(local_folder_path):
            for file in files:
                local_file_path = os.path.join(root, file)
                relative_path = os.path.relpath(local_file_path, local_folder_path)
                blob_path = os.path.join(blob_prefix, relative_path).replace("\\", "/")

                print(f"Uploading {local_file_path} to {blob_path}")
                with open(local_file_path, "rb") as data:
                    blob_client = self.service_client.get_blob_client(container=container_name, blob=blob_path)
                    blob_client.upload_blob(data, overwrite=True)


if __name__ == "__main__":
    # Replace this with your actual connection string
    connection_str = "DefaultEndpointsProtocol=https;AccountName=...;AccountKey=...;EndpointSuffix=core.windows.net"
    
    # Instantiate the helper
    blob = AzureBlobHelper(connection_str=connection_str)
    
    # Configuration
    container_name = "your-container-name"
    local_folder = "./data/to_upload"
    blob_prefix = "uploaded_files/"  # target folder inside blob container
    prefix = "some/path/in/container/"
    
    # Upload folder
    print("Uploading folder:")
    blob.upload_folder(container_name, local_folder, blob_prefix)
    
    # List files
    print("\nListing files:")
    files = blob.list_files(container_name, prefix)
    for file in files:
        print(file)

    # Read a file
    print("\nReading a file:")
    content = blob.read_file(container_name, f"{prefix}sample.txt")
    print(content)

    # Upload a file
    print("\nUploading a file:")
    result = blob.upload_file(container_name, f"{prefix}upload_test.txt", "This is a test from local.")
    print(result)

