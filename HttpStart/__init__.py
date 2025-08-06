import azure.functions as func
import json
from blob_helper import AzureBlobHelper
import os

def validate_token(token: str) -> bool:
    # Mock validation; implement your logic or integrate with OAuth provider
    return token == "valid_token_example"

def main(req: func.HttpRequest) -> func.HttpResponse:
    try:
        auth_header = req.headers.get("Authorization", "")
        if not auth_header.startswith("Bearer "):
            return func.HttpResponse("Missing or invalid Bearer token", status_code=401)

        token = auth_header.split(" ")[1]
        if not validate_token(token):
            return func.HttpResponse("Unauthorized", status_code=403)

        # Parse JSON body
        body = req.get_json()
        container_name = body.get("container_name")
        prefix1 = body.get("prefix1")
        prefix2 = body.get("prefix2")

        if not all([container_name, prefix1, prefix2]):
            return func.HttpResponse("container_name, prefix1, and prefix2 are required.", status_code=400)

        # Connect to Azure Blob
        blob_helper = AzureBlobHelper()
        all_prefixes = [prefix1, prefix2]

        result = []

        for prefix in all_prefixes:
            files = blob_helper.list_files(container_name, prefix)
            for file in files:
                content = blob_helper.read_file(container_name, file)
                print(f"File: {file}\n---\n{content}\n---")
                result.append({"file": file, "content": content[:200]})  # Only first 200 chars

        return func.HttpResponse(json.dumps(result), mimetype="application/json", status_code=200)

    except Exception as e:
        return func.HttpResponse(f"Error: {str(e)}", status_code=500)

