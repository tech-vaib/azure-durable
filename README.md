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

## HTTX Connection Pool ##

This comprehensive solution provides a highly scalable HTTP client for microservices with the following key features:
Key Features:

Connection Pooling: Configurable connection limits and keep-alive settings for optimal performance
Service Registration: Register multiple microservices with individual configurations
Retry Logic: Exponential backoff retry mechanism for failed requests
Async/Await: Full async support for high concurrency
Batch Requests: Execute multiple requests concurrently
Type Safety: Uses dataclasses and type hints for better code quality
Error Handling: Comprehensive error handling with logging
Convenience Methods: Simple methods for common HTTP operations

Usage Benefits:

Reusable: Register once, use everywhere
Scalable: Handles hundreds of concurrent connections
Configurable: Per-service timeouts, retries, and headers
Maintainable: Clean separation of concerns
Production-Ready: Includes logging, error handling, and proper resource cleanup

Example Service Configurations:
The code shows how to configure different microservices with their own:

Base URLs
Authentication tokens
Timeout settings
Retry policies
Custom headers

The UserServiceClient class demonstrates how you can create service-specific wrapper clients for even cleaner code organization.
To use this in production, you would typically:

Create a singleton instance of HTTPXClient
Register all your microservices at startup
Use the client throughout your application
Properly close it during shutdown

This design scales well and provides excellent performance for microservice architectures.
