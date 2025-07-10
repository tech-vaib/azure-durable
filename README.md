# azure-durable functions
**Prerequisites**
Install dependencies in your Azure Function environment:
pip install azure-functions
pip install azure-functions-durable
pip install azure-storage-blob

Set environment variable AZURE_STORAGE_CONNECTION_STRING in your Function App settings.
Fill in your LLM embedding API calls inside embed_file_activity and embed_event_activity.

Add your Cosmos DB logic to store embeddings.

Add proper error handling and logging as needed.

Secure your HTTP endpoints with authentication/authorization.

**How to trigger:**
**Manual:**
curl -X POST <your-function-url>/orchestrators/start -H "Content-Type: application/json" -d '{
  "env": "prod",
  "version": "v1",
  "filesPath": "mycontainer/uploads"
}'

**Live event:**
curl -X POST <your-function-url>/orchestrators/start -H "Content-Type: application/json" -d '{
  "trackingId": "abc123",
  "sessionId": "sess789"
}'

