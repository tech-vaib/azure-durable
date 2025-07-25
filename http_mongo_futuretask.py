import httpx
import asyncio
from typing import Dict, Any, Optional, Union, List, Callable, Awaitable
from dataclasses import dataclass, field
import logging
from contextlib import asynccontextmanager
import json
from enum import Enum
import time
from datetime import datetime, timedelta
import weakref
from motor.motor_asyncio import AsyncIOMotorClient, AsyncIOMotorDatabase, AsyncIOMotorCollection
from pymongo.errors import PyMongoError
import concurrent.futures
from functools import wraps
import traceback

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class HTTPMethod(Enum):
    GET = "GET"
    POST = "POST"
    PUT = "PUT"
    DELETE = "DELETE"
    PATCH = "PATCH"

class TaskStatus(Enum):
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"

@dataclass
class ServiceConfig:
    """Configuration for individual microservice"""
    base_url: str
    timeout: float = 30.0
    retries: int = 3
    headers: Dict[str, str] = field(default_factory=dict)
    auth: Optional[tuple] = None
    verify_ssl: bool = True

@dataclass
class RequestConfig:
    """Configuration for individual request"""
    endpoint: str
    method: HTTPMethod = HTTPMethod.GET
    params: Optional[Dict[str, Any]] = None
    data: Optional[Union[Dict, str, bytes]] = None
    json_data: Optional[Dict[str, Any]] = None
    headers: Optional[Dict[str, str]] = None
    timeout: Optional[float] = None

@dataclass
class MongoConfig:
    """Configuration for MongoDB connection"""
    connection_string: str
    database_name: str
    max_pool_size: int = 100
    min_pool_size: int = 10
    max_idle_time_ms: int = 30000
    connect_timeout_ms: int = 20000
    server_selection_timeout_ms: int = 30000

@dataclass
class TaskResult:
    """Result of an async task"""
    task_id: str
    status: TaskStatus
    result: Any = None
    error: Optional[str] = None
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None
    duration: Optional[float] = None

class FutureTaskManager:
    """Manager for handling future tasks and background operations"""
    
    def __init__(self, max_workers: int = 50):
        self.max_workers = max_workers
        self.executor = concurrent.futures.ThreadPoolExecutor(max_workers=max_workers)
        self.tasks: Dict[str, asyncio.Task] = {}
        self.task_results: Dict[str, TaskResult] = {}
        self._task_counter = 0
        self._cleanup_interval = 300  # 5 minutes
        self._last_cleanup = time.time()
    
    def _generate_task_id(self) -> str:
        """Generate unique task ID"""
        self._task_counter += 1
        return f"task_{int(time.time())}_{self._task_counter}"
    
    async def submit_async_task(self, 
                               coro: Awaitable, 
                               task_id: Optional[str] = None,
                               timeout: Optional[float] = None) -> str:
        """
        Submit an async coroutine as a background task
        
        Args:
            coro: Coroutine to execute
            task_id: Optional custom task ID
            timeout: Optional timeout in seconds
            
        Returns:
            Task ID for tracking
        """
        if task_id is None:
            task_id = self._generate_task_id()
        
        result = TaskResult(
            task_id=task_id,
            status=TaskStatus.PENDING,
            started_at=datetime.now()
        )
        self.task_results[task_id] = result
        
        async def _wrapped_task():
            try:
                result.status = TaskStatus.RUNNING
                start_time = time.time()
                
                if timeout:
                    task_result = await asyncio.wait_for(coro, timeout=timeout)
                else:
                    task_result = await coro
                
                end_time = time.time()
                result.result = task_result
                result.status = TaskStatus.COMPLETED
                result.completed_at = datetime.now()
                result.duration = end_time - start_time
                
            except asyncio.TimeoutError:
                result.status = TaskStatus.FAILED
                result.error = "Task timed out"
                result.completed_at = datetime.now()
            except Exception as e:
                result.status = TaskStatus.FAILED
                result.error = str(e)
                result.completed_at = datetime.now()
                logger.error(f"Task {task_id} failed: {e}\n{traceback.format_exc()}")
        
        task = asyncio.create_task(_wrapped_task())
        self.tasks[task_id] = task
        
        # Auto-cleanup old tasks
        await self._cleanup_old_tasks()
        
        return task_id
    
    def submit_sync_task(self, 
                        func: Callable, 
                        *args, 
                        task_id: Optional[str] = None,
                        **kwargs) -> str:
        """
        Submit a synchronous function as a background task
        
        Args:
            func: Function to execute
            *args: Function arguments
            task_id: Optional custom task ID
            **kwargs: Function keyword arguments
            
        Returns:
            Task ID for tracking
        """
        if task_id is None:
            task_id = self._generate_task_id()
        
        async def _async_wrapper():
            loop = asyncio.get_event_loop()
            return await loop.run_in_executor(self.executor, func, *args, **kwargs)
        
        return asyncio.create_task(
            self.submit_async_task(_async_wrapper(), task_id)
        )
    
    def get_task_status(self, task_id: str) -> Optional[TaskResult]:
        """Get status of a task"""
        return self.task_results.get(task_id)
    
    async def wait_for_task(self, task_id: str, timeout: Optional[float] = None) -> TaskResult:
        """
        Wait for a task to complete
        
        Args:
            task_id: Task ID to wait for
            timeout: Optional timeout in seconds
            
        Returns:
            TaskResult with the final status and result
        """
        if task_id not in self.tasks:
            raise ValueError(f"Task {task_id} not found")
        
        try:
            if timeout:
                await asyncio.wait_for(self.tasks[task_id], timeout=timeout)
            else:
                await self.tasks[task_id]
        except asyncio.TimeoutError:
            pass  # Task result will show timeout status
        
        return self.task_results[task_id]
    
    async def cancel_task(self, task_id: str) -> bool:
        """Cancel a running task"""
        if task_id in self.tasks:
            cancelled = self.tasks[task_id].cancel()
            if cancelled and task_id in self.task_results:
                self.task_results[task_id].status = TaskStatus.CANCELLED
                self.task_results[task_id].completed_at = datetime.now()
            return cancelled
        return False
    
    def get_all_tasks(self) -> Dict[str, TaskResult]:
        """Get status of all tasks"""
        return self.task_results.copy()
    
    async def _cleanup_old_tasks(self):
        """Clean up completed tasks older than cleanup interval"""
        current_time = time.time()
        if current_time - self._last_cleanup < self._cleanup_interval:
            return
        
        cutoff_time = datetime.now() - timedelta(seconds=self._cleanup_interval * 2)
        tasks_to_remove = []
        
        for task_id, result in self.task_results.items():
            if (result.status in [TaskStatus.COMPLETED, TaskStatus.FAILED, TaskStatus.CANCELLED] 
                and result.completed_at and result.completed_at < cutoff_time):
                tasks_to_remove.append(task_id)
        
        for task_id in tasks_to_remove:
            self.task_results.pop(task_id, None)
            task = self.tasks.pop(task_id, None)
            if task and not task.done():
                task.cancel()
        
        self._last_cleanup = current_time
        if tasks_to_remove:
            logger.info(f"Cleaned up {len(tasks_to_remove)} old tasks")
    
    async def shutdown(self):
        """Shutdown task manager and cleanup resources"""
        # Cancel all running tasks
        for task_id, task in self.tasks.items():
            if not task.done():
                task.cancel()
                if task_id in self.task_results:
                    self.task_results[task_id].status = TaskStatus.CANCELLED
        
        # Wait for tasks to complete cancellation
        if self.tasks:
            await asyncio.gather(*self.tasks.values(), return_exceptions=True)
        
        # Shutdown thread pool executor
        self.executor.shutdown(wait=True)

class MongoDBClient:
    """MongoDB client with connection pooling and common operations"""
    
    def __init__(self, config: MongoConfig):
        self.config = config
        self._client: Optional[AsyncIOMotorClient] = None
        self._db: Optional[AsyncIOMotorDatabase] = None
    
    async def connect(self):
        """Establish MongoDB connection"""
        if self._client is None:
            self._client = AsyncIOMotorClient(
                self.config.connection_string,
                maxPoolSize=self.config.max_pool_size,
                minPoolSize=self.config.min_pool_size,
                maxIdleTimeMS=self.config.max_idle_time_ms,
                connectTimeoutMS=self.config.connect_timeout_ms,
                serverSelectionTimeoutMS=self.config.server_selection_timeout_ms
            )
            self._db = self._client[self.config.database_name]
            logger.info(f"Connected to MongoDB database: {self.config.database_name}")
    
    async def disconnect(self):
        """Close MongoDB connection"""
        if self._client:
            self._client.close()
            self._client = None
            self._db = None
            logger.info("Disconnected from MongoDB")
    
    def get_collection(self, collection_name: str) -> AsyncIOMotorCollection:
        """Get a collection instance"""
        if self._db is None:
            raise RuntimeError("MongoDB not connected. Call connect() first.")
        return self._db[collection_name]
    
    async def insert_one(self, collection_name: str, document: Dict[str, Any]) -> str:
        """Insert a single document"""
        try:
            collection = self.get_collection(collection_name)
            result = await collection.insert_one(document)
            return str(result.inserted_id)
        except PyMongoError as e:
            logger.error(f"Error inserting document: {e}")
            raise
    
    async def insert_many(self, collection_name: str, documents: List[Dict[str, Any]]) -> List[str]:
        """Insert multiple documents"""
        try:
            collection = self.get_collection(collection_name)
            result = await collection.insert_many(documents)
            return [str(id) for id in result.inserted_ids]
        except PyMongoError as e:
            logger.error(f"Error inserting documents: {e}")
            raise
    
    async def find_one(self, collection_name: str, filter_dict: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Find a single document"""
        try:
            collection = self.get_collection(collection_name)
            return await collection.find_one(filter_dict)
        except PyMongoError as e:
            logger.error(f"Error finding document: {e}")
            raise
    
    async def find_many(self, 
                       collection_name: str, 
                       filter_dict: Dict[str, Any] = None,
                       limit: int = None,
                       skip: int = None,
                       sort: List[tuple] = None) -> List[Dict[str, Any]]:
        """Find multiple documents"""
        try:
            collection = self.get_collection(collection_name)
            cursor = collection.find(filter_dict or {})
            
            if skip:
                cursor = cursor.skip(skip)
            if limit:
                cursor = cursor.limit(limit)
            if sort:
                cursor = cursor.sort(sort)
            
            return await cursor.to_list(length=limit)
        except PyMongoError as e:
            logger.error(f"Error finding documents: {e}")
            raise
    
    async def update_one(self, 
                        collection_name: str, 
                        filter_dict: Dict[str, Any],
                        update_dict: Dict[str, Any]) -> int:
        """Update a single document"""
        try:
            collection = self.get_collection(collection_name)
            result = await collection.update_one(filter_dict, {"$set": update_dict})
            return result.modified_count
        except PyMongoError as e:
            logger.error(f"Error updating document: {e}")
            raise
    
    async def update_many(self, 
                         collection_name: str, 
                         filter_dict: Dict[str, Any],
                         update_dict: Dict[str, Any]) -> int:
        """Update multiple documents"""
        try:
            collection = self.get_collection(collection_name)
            result = await collection.update_many(filter_dict, {"$set": update_dict})
            return result.modified_count
        except PyMongoError as e:
            logger.error(f"Error updating documents: {e}")
            raise
    
    async def delete_one(self, collection_name: str, filter_dict: Dict[str, Any]) -> int:
        """Delete a single document"""
        try:
            collection = self.get_collection(collection_name)
            result = await collection.delete_one(filter_dict)
            return result.deleted_count
        except PyMongoError as e:
            logger.error(f"Error deleting document: {e}")
            raise
    
    async def delete_many(self, collection_name: str, filter_dict: Dict[str, Any]) -> int:
        """Delete multiple documents"""
        try:
            collection = self.get_collection(collection_name)
            result = await collection.delete_many(filter_dict)
            return result.deleted_count
        except PyMongoError as e:
            logger.error(f"Error deleting documents: {e}")
            raise
    
    async def aggregate(self, collection_name: str, pipeline: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Perform aggregation"""
        try:
            collection = self.get_collection(collection_name)
            cursor = collection.aggregate(pipeline)
            return await cursor.to_list(length=None)
        except PyMongoError as e:
            logger.error(f"Error in aggregation: {e}")
            raise
    
    async def count_documents(self, collection_name: str, filter_dict: Dict[str, Any] = None) -> int:
        """Count documents matching filter"""
        try:
            collection = self.get_collection(collection_name)
            return await collection.count_documents(filter_dict or {})
        except PyMongoError as e:
            logger.error(f"Error counting documents: {e}")
            raise

class EnhancedHTTPXClient:
    """Enhanced HTTP client with task management and MongoDB integration"""
    
    def __init__(self, 
                 max_connections: int = 100,
                 max_keepalive_connections: int = 20,
                 keepalive_expiry: float = 5.0,
                 task_manager_workers: int = 50):
        """
        Initialize enhanced HTTP client
        
        Args:
            max_connections: Maximum number of HTTP connections in pool
            max_keepalive_connections: Maximum keepalive connections
            keepalive_expiry: How long to keep connections alive (seconds)
            task_manager_workers: Maximum workers for background tasks
        """
        self.services: Dict[str, ServiceConfig] = {}
        self.limits = httpx.Limits(
            max_connections=max_connections,
            max_keepalive_connections=max_keepalive_connections,
            keepalive_expiry=keepalive_expiry
        )
        self._client: Optional[httpx.AsyncClient] = None
        self.task_manager = FutureTaskManager(max_workers=task_manager_workers)
        self.mongo_client: Optional[MongoDBClient] = None
    
    def register_service(self, name: str, config: ServiceConfig):
        """Register a microservice with its configuration"""
        self.services[name] = config
        logger.info(f"Registered service: {name} at {config.base_url}")
    
    def register_mongodb(self, config: MongoConfig):
        """Register MongoDB client"""
        self.mongo_client = MongoDBClient(config)
        logger.info(f"Registered MongoDB: {config.database_name}")
    
    @asynccontextmanager
    async def get_client(self):
        """Context manager for HTTP client with connection pooling"""
        if self._client is None:
            self._client = httpx.AsyncClient(limits=self.limits)
        try:
            yield self._client
        finally:
            pass  # Keep client alive for reuse
    
    async def connect_mongodb(self):
        """Connect to MongoDB"""
        if self.mongo_client:
            await self.mongo_client.connect()
    
    async def close(self):
        """Close all clients and cleanup connections"""
        if self._client:
            await self._client.aclose()
            self._client = None
        
        if self.mongo_client:
            await self.mongo_client.disconnect()
        
        await self.task_manager.shutdown()
    
    # HTTP Methods (same as before)
    async def make_request(self, 
                          service_name: str, 
                          request_config: RequestConfig) -> httpx.Response:
        """Make HTTP request to a registered service"""
        if service_name not in self.services:
            raise ValueError(f"Service '{service_name}' not registered")
        
        service = self.services[service_name]
        url = f"{service.base_url.rstrip('/')}/{request_config.endpoint.lstrip('/')}"
        
        headers = {**service.headers}
        if request_config.headers:
            headers.update(request_config.headers)
        
        request_params = {
            "url": url,
            "method": request_config.method.value,
            "headers": headers,
            "timeout": request_config.timeout or service.timeout,
            "params": request_config.params,
        }
        
        if service.auth:
            request_params["auth"] = service.auth
        
        if request_config.json_data:
            request_params["json"] = request_config.json_data
        elif request_config.data:
            request_params["data"] = request_config.data
        
        last_exception = None
        for attempt in range(service.retries + 1):
            try:
                async with self.get_client() as client:
                    response = await client.request(**request_params)
                    response.raise_for_status()
                    return response
                    
            except httpx.HTTPError as e:
                last_exception = e
                if attempt < service.retries:
                    wait_time = 2 ** attempt
                    logger.warning(f"Request failed (attempt {attempt + 1}/{service.retries + 1}): {e}. Retrying in {wait_time}s...")
                    await asyncio.sleep(wait_time)
                else:
                    logger.error(f"Request failed after {service.retries + 1} attempts: {e}")
                    raise
        
        raise last_exception

    async def get(self, service_name: str, endpoint: str, **kwargs) -> httpx.Response:
        """Convenience method for GET requests"""
        config = RequestConfig(endpoint=endpoint, method=HTTPMethod.GET, **kwargs)
        return await self.make_request(service_name, config)
    
    async def post(self, service_name: str, endpoint: str, **kwargs) -> httpx.Response:
        """Convenience method for POST requests"""
        config = RequestConfig(endpoint=endpoint, method=HTTPMethod.POST, **kwargs)
        return await self.make_request(service_name, config)
    
    async def put(self, service_name: str, endpoint: str, **kwargs) -> httpx.Response:
        """Convenience method for PUT requests"""
        config = RequestConfig(endpoint=endpoint, method=HTTPMethod.PUT, **kwargs)
        return await self.make_request(service_name, config)
    
    async def delete(self, service_name: str, endpoint: str, **kwargs) -> httpx.Response:
        """Convenience method for DELETE requests"""
        config = RequestConfig(endpoint=endpoint, method=HTTPMethod.DELETE, **kwargs)
        return await self.make_request(service_name, config)

    async def batch_requests(self, requests: List[tuple]) -> List[httpx.Response]:
        """Execute multiple requests concurrently"""
        tasks = []
        for service_name, request_config in requests:
            task = self.make_request(service_name, request_config)
            tasks.append(task)
        
        return await asyncio.gather(*tasks, return_exceptions=True)
    
    # Task Management Methods
    async def submit_background_task(self, 
                                   coro: Awaitable, 
                                   task_id: Optional[str] = None,
                                   timeout: Optional[float] = None) -> str:
        """Submit a background task"""
        return await self.task_manager.submit_async_task(coro, task_id, timeout)
    
    def submit_sync_background_task(self, 
                                  func: Callable, 
                                  *args,
                                  task_id: Optional[str] = None,
                                  **kwargs) -> str:
        """Submit a synchronous function as background task"""
        return self.task_manager.submit_sync_task(func, *args, task_id=task_id, **kwargs)
    
    def get_task_status(self, task_id: str) -> Optional[TaskResult]:
        """Get task status"""
        return self.task_manager.get_task_status(task_id)
    
    async def wait_for_task(self, task_id: str, timeout: Optional[float] = None) -> TaskResult:
        """Wait for task completion"""
        return await self.task_manager.wait_for_task(task_id, timeout)
    
    async def cancel_task(self, task_id: str) -> bool:
        """Cancel a task"""
        return await self.task_manager.cancel_task(task_id)
    
    def get_all_tasks(self) -> Dict[str, TaskResult]:
        """Get all task statuses"""
        return self.task_manager.get_all_tasks()
    
    # MongoDB Methods
    async def mongo_insert_one(self, collection: str, document: Dict[str, Any]) -> str:
        """Insert document to MongoDB"""
        if not self.mongo_client:
            raise RuntimeError("MongoDB not configured")
        return await self.mongo_client.insert_one(collection, document)
    
    async def mongo_find_one(self, collection: str, filter_dict: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Find one document from MongoDB"""
        if not self.mongo_client:
            raise RuntimeError("MongoDB not configured")
        return await self.mongo_client.find_one(collection, filter_dict)
    
    async def mongo_find_many(self, 
                             collection: str, 
                             filter_dict: Dict[str, Any] = None,
                             **kwargs) -> List[Dict[str, Any]]:
        """Find multiple documents from MongoDB"""
        if not self.mongo_client:
            raise RuntimeError("MongoDB not configured")
        return await self.mongo_client.find_many(collection, filter_dict, **kwargs)
    
    async def mongo_update_one(self, 
                              collection: str, 
                              filter_dict: Dict[str, Any],
                              update_dict: Dict[str, Any]) -> int:
        """Update one document in MongoDB"""
        if not self.mongo_client:
            raise RuntimeError("MongoDB not configured")
        return await self.mongo_client.update_one(collection, filter_dict, update_dict)

# Enhanced Service-Specific Client Example
class EnhancedUserServiceClient:
    """Enhanced user service client with background tasks and MongoDB caching"""
    
    def __init__(self, http_client: EnhancedHTTPXClient):
        self.client = http_client
        self.service_name = "user_service"
        self.cache_collection = "user_cache"
    
    async def get_user_cached(self, user_id: str) -> Dict[str, Any]:
        """Get user with MongoDB caching"""
        # Try cache first
        try:
            cached = await self.client.mongo_find_one(
                self.cache_collection, 
                {"user_id": user_id}
            )
            if cached and cached.get("expires_at", 0) > time.time():
                return cached["data"]
        except Exception:
            pass  # Cache miss or error, continue to API
        
        # Fetch from API
        response = await self.client.get(self.service_name, f"/users/{user_id}")
        user_data = response.json()
        
        # Cache result asynchronously
        cache_doc = {
            "user_id": user_id,
            "data": user_data,
            "expires_at": time.time() + 300,  # 5 minutes
            "cached_at": datetime.now()
        }
        
        await self.client.submit_background_task(
            self.client.mongo_insert_one(self.cache_collection, cache_doc)
        )
        
        return user_data
    
    async def batch_user_processing(self, user_ids: List[str]) -> str:
        """Process multiple users in background"""
        async def process_users():
            results = []
            for user_id in user_ids:
                try:
                    user_data = await self.get_user_cached(user_id)
                    # Process user data here
                    results.append({"user_id": user_id, "processed": True, "data": user_data})
                except Exception as e:
                    results.append({"user_id": user_id, "processed": False, "error": str(e)})
            return results
        
        return await self.client.submit_background_task(process_users())

# Example usage
async def main():
    """Enhanced example with all features"""
    
    # Initialize enhanced client
    client = EnhancedHTTPXClient(
        max_connections=50,
        max_keepalive_connections=10,
        task_manager_workers=20
    )
    
    try:
        # Register services
        client.register_service("user_service", ServiceConfig(
            base_url="https://api.users.example.com",
            headers={"Authorization": "Bearer user-token"},
            timeout=15.0
        ))
        
        # Register MongoDB
        client.register_mongodb(MongoConfig(
            connection_string="mongodb://localhost:27017",
            database_name="microservices_cache"
        ))
        
        # Connect to MongoDB
        await client.connect_mongodb()
        
        # Example 1: Background task processing
        print("=== Example 1: Background Tasks ===")
        
        async def long_running_task():
            await asyncio.sleep(2)
            return {"result": "Task completed successfully!"}
        
        task_id = await client.submit_background_task(long_running_task())
        print(f"Submitted task: {task_id}")
        
        # Check task status
        status = client.get_task_status(task_id)
        print(f"Task status: {status.status}")
        
        # Wait for completion
        final_result = await client.wait_for_task(task_id, timeout=10)
        print(f"Task result: {final_result.result}")
        
        # Example 2: MongoDB operations
        print("\n=== Example 2: MongoDB Operations ===")
        
        # Insert user data
        user_doc = {
            "user_id": "123",
            "name": "John Doe",
            "email": "john@example.com",
            "created_at": datetime.now()
        }
        
        doc_id = await client.mongo_insert_one("users", user_doc)
        print(f"Inserted user document: {doc_id}")
        
        # Find user
        found_user = await client.mongo_find_one("users", {"user_id": "123"})
        print(f"Found user: {found_user}")
        
        # Example 3: Enhanced service client
        print("\n=== Example 3: Enhanced Service Client ===")
        
        user_client = EnhancedUserServiceClient(client)
        
        # Process users in background
        task_id = await user_client.batch_user_processing(["123", "456", "789"])
        print(f"Started batch processing: {task_id}")
        
        # Monitor all tasks
        all_tasks = client.get_all_tasks()
        print(f"Total active tasks: {len(all_tasks)}")
        
        # Example 4: Sync function as background task
        print("\n=== Example 4: Sync Background Task ===")
        
        def cpu_intensive_task(n: int) -> int:
            """Simulate CPU-intensive work"""
            result = 0
            for i in range(n):
                result += i * i
            return result
        
        sync_task_id = client.submit_sync_background_task(
            cpu_intensive_task, 1000000, task_id="cpu_task"
        )
        print(f"Submitted CPU task: {sync_task_id}")
        
    except Exception as e:
        logger.error(f"Error in main: {e}")
        traceback.print_exc()
    
    finally:
        # Cleanup
        await client.close()

if __name__ == "__main__":
    # Install required packages:
    # pip install httpx motor pymongo
    asyncio.run(main())
