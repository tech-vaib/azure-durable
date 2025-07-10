import logging
import os
import azure.functions as func
import azure.durable_functions as df
from azure.storage.blob import BlobServiceClient

# Orchestrator function
def orchestrator_function(context: df.DurableOrchestrationContext):
    input_data = context.get_input()

    if not input_data:
        return {"error": "No input provided"}

    if "filesPath" in input_data:
        # Manual trigger
        env = input_data.get("env")
        version = input_data.get("version")
        files_path = input_data.get("filesPath")

        # 1. List blobs in the container/prefix
        file_list = yield context.call_activity("read_files_activity", files_path)

        # 2. For each file, call embedding activity
        embedding_results = []
        for file in file_list:
            res = yield context.call_activity("embed_file_activity", {"file": file, "env": env, "version": version})
            embedding_results.append(res)

        return {"status": "Manual processing complete", "results": embedding_results}

    elif "trackingId" in input_data:
        # Live event trigger
        tracking_id = input_data.get("trackingId")
        session_id = input_data.get("sessionId")

        # Fetch event details from DB or other service
        event_details = yield context.call_activity("fetch_event_details_activity", tracking_id)

        # Process embedding for the event
        result = yield context.call_activity("embed_event_activity", {"event": event_details, "sessionId": session_id})

        return {"status": "Live event processing complete", "result": result}

    else:
        return {"error": "Invalid input"}

main = df.Orchestrator.create(orchestrator_function)


# HTTP starter function
async def http_start(req: func.HttpRequest, starter: str) -> func.HttpResponse:
    client = df.DurableOrchestrationClient(starter)
    try:
        payload = req.get_json()
    except Exception:
        return func.HttpResponse("Invalid JSON", status_code=400)

    instance_id = await client.start_new("orchestrator_function", None, payload)
    logging.info(f"Started orchestration with ID = '{instance_id}'.")
    return client.create_check_status_response(req, instance_id)


# Activity to list blobs from Azure Blob Storage
async def read_files_activity(files_path: str) -> list:
    # Expect files_path like "container-name/prefix"
    parts = files_path.split('/', 1)
    if len(parts) != 2:
        raise ValueError("filesPath must be in 'container-name/prefix' format")

    container_name, prefix = parts[0], parts[1]

    connect_str = os.getenv("AZURE_STORAGE_CONNECTION_STRING")
    if not connect_str:
        raise Exception("Azure Storage connection string not configured")

    blob_service_client = BlobServiceClient.from_connection_string(connect_str)
    container_client = blob_service_client.get_container_client(container_name)

    blobs = container_client.list_blobs(name_starts_with=prefix)
    files = []
    for blob in blobs:
        if blob.name.endswith(".csv") or blob.name.endswith(".xlsx"):
            files.append(blob.name)
    logging.info(f"Found {len(files)} files in {files_path}")
    return files


# Activity to download blob and simulate embedding
async def embed_file_activity(input_data: dict) -> dict:
    file = input_data.get("file")
    env = input_data.get("env")
    version = input_data.get("version")

    connect_str = os.getenv("AZURE_STORAGE_CONNECTION_STRING")
    blob_service_client = BlobServiceClient.from_connection_string(connect_str)

    # Extract container from file path
    # Since file is blob name including prefix, container is known from calling context,
    # but for simplicity, pass container in file path or adjust logic accordingly.
    # Here, we assume container name is the first path segment:
    container_name = file.split('/')[0]
    blob_name = '/'.join(file.split('/')[1:])

    container_client = blob_service_client.get_container_client(container_name)
    blob_client = container_client.get_blob_client(blob_name)

    # Download blob content (assuming text or CSV)
    stream = blob_client.download_blob()
    content = stream.readall()

    # TODO: call your LLM embedding API here with `content`
    logging.info(f"Embedding file {file} for env={env} version={version}")

    # Simulated result
    return {"file": file, "status": "embedded"}


# Activity to fetch event details (live event)
async def fetch_event_details_activity(tracking_id: str) -> dict:
    # Simulate fetching event data by trackingId
    logging.info(f"Fetching event details for trackingId {tracking_id}")
    # TODO: Replace with real DB/API call
    return {"trackingId": tracking_id, "content": "Event data for embedding"}


# Activity to embed event data (live event)
async def embed_event_activity(input_data: dict) -> dict:
    event = input_data.get("event")
    session_id = input_data.get("sessionId")

    # TODO: Call LLM embedding API with event content here
    logging.info(f"Embedding event for session {session_id}, trackingId {event.get('trackingId')}")
    return {"trackingId": event.get("trackingId"), "sessionId": session_id, "status": "embedded"}


# Azure Functions app routing
app = func.FunctionApp()

app.route(route="orchestrators/orchestrator_function")(main)
app.route(route="orchestrators/start", methods=["POST"])(http_start)

app.function_name(name="read_files_activity")(read_files_activity)
app.function_name(name="embed_file_activity")(embed_file_activity)
app.function_name(name="fetch_event_details_activity")(fetch_event_details_activity)
app.function_name(name="embed_event_activity")(embed_event_activity)
