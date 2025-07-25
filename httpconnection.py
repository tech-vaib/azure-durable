import httpx
import asyncio
from typing import Dict, Any, Optional, Union, List
from dataclasses import dataclass, field
import logging
from contextlib import asynccontextmanager
import json
from enum import Enum
import time

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class HTTPMethod(Enum):
    GET = "GET"
    POST = "POST"
    PUT = "PUT"
    DELETE = "DELETE"
    PATCH = "PATCH"

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

class HTTPXClient:
    """Highly scalable HTTP client for microservices"""
    
    def __init__(self, 
                 max_connections: int = 100,
                 max_keepalive_connections: int = 20,
                 keepalive_expiry: float = 5.0):
        """
        Initialize HTTP client with connection pooling
        
        Args:
            max_connections: Maximum number of connections in pool
            max_keepalive_connections: Maximum keepalive connections
            keepalive_expiry: How long to keep connections alive (seconds)
        """
        self.services: Dict[str, ServiceConfig] = {}
        self.limits = httpx.Limits(
            max_connections=max_connections,
            max_keepalive_connections=max_keepalive_connections,
            keepalive_expiry=keepalive_expiry
        )
        self._client: Optional[httpx.AsyncClient] = None
    
    def register_service(self, name: str, config: ServiceConfig):
        """Register a microservice with its configuration"""
        self.services[name] = config
        logger.info(f"Registered service: {name} at {config.base_url}")
    
    @asynccontextmanager
    async def get_client(self):
        """Context manager for HTTP client with connection pooling"""
        if self._client is None:
            self._client = httpx.AsyncClient(limits=self.limits)
        try:
            yield self._client
        finally:
            pass  # Keep client alive for reuse
    
    async def close(self):
        """Close the HTTP client and cleanup connections"""
        if self._client:
            await self._client.aclose()
            self._client = None
    
    async def make_request(self, 
                          service_name: str, 
                          request_config: RequestConfig) -> httpx.Response:
        """
        Make HTTP request to a registered service
        
        Args:
            service_name: Name of the registered service
            request_config: Request configuration
            
        Returns:
            httpx.Response object
            
        Raises:
            ValueError: If service is not registered
            httpx.HTTPError: For HTTP-related errors
        """
        if service_name not in self.services:
            raise ValueError(f"Service '{service_name}' not registered")
        
        service = self.services[service_name]
        url = f"{service.base_url.rstrip('/')}/{request_config.endpoint.lstrip('/')}"
        
        # Merge headers (service defaults + request specific)
        headers = {**service.headers}
        if request_config.headers:
            headers.update(request_config.headers)
        
        # Prepare request parameters
        request_params = {
            "url": url,
            "method": request_config.method.value,
            "headers": headers,
            "timeout": request_config.timeout or service.timeout,
            "params": request_config.params,
        }
        
        # Add authentication if configured
        if service.auth:
            request_params["auth"] = service.auth
        
        # Add data/json payload
        if request_config.json_data:
            request_params["json"] = request_config.json_data
        elif request_config.data:
            request_params["data"] = request_config.data
        
        # Retry logic
        last_exception = None
        for attempt in range(service.retries + 1):
            try:
                async with self.get_client() as client:
                    response = await client.request(**request_params)
                    response.raise_for_status()  # Raise exception for 4xx/5xx status codes
                    return response
                    
            except httpx.HTTPError as e:
                last_exception = e
                if attempt < service.retries:
                    wait_time = 2 ** attempt  # Exponential backoff
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
        """
        Execute multiple requests concurrently
        
        Args:
            requests: List of tuples (service_name, request_config)
            
        Returns:
            List of responses in the same order as requests
        """
        tasks = []
        for service_name, request_config in requests:
            task = self.make_request(service_name, request_config)
            tasks.append(task)
        
        return await asyncio.gather(*tasks, return_exceptions=True)

# Example usage and microservice configurations
async def main():
    """Example usage of the HTTPXClient"""
    
    # Initialize the client
    client = HTTPXClient(max_connections=50, max_keepalive_connections=10)
    
    try:
        # Register microservices
        client.register_service("user_service", ServiceConfig(
            base_url="https://api.users.example.com",
            headers={"Authorization": "Bearer user-service-token"},
            timeout=15.0,
            retries=2
        ))
        
        client.register_service("order_service", ServiceConfig(
            base_url="https://api.orders.example.com",
            headers={"Authorization": "Bearer order-service-token"},
            timeout=20.0,
            retries=3
        ))
        
        client.register_service("inventory_service", ServiceConfig(
            base_url="https://api.inventory.example.com",
            headers={
                "Authorization": "Bearer inventory-service-token",
                "Content-Type": "application/json"
            },
            timeout=10.0
        ))
        
        # Example 1: Simple GET request
        print("=== Example 1: Simple GET request ===")
        try:
            response = await client.get("user_service", "/users/123")
            print(f"User data: {response.json()}")
        except Exception as e:
            print(f"Error fetching user: {e}")
        
        # Example 2: POST request with JSON data
        print("\n=== Example 2: POST request with JSON ===")
        user_data = {
            "name": "John Doe",
            "email": "john@example.com",
            "age": 30
        }
        try:
            response = await client.post("user_service", "/users", json_data=user_data)
            print(f"Created user: {response.json()}")
        except Exception as e:
            print(f"Error creating user: {e}")
        
        # Example 3: Complex request with custom configuration
        print("\n=== Example 3: Complex request ===")
        request_config = RequestConfig(
            endpoint="/orders/search",
            method=HTTPMethod.GET,
            params={"status": "pending", "limit": 10},
            headers={"X-Custom-Header": "custom-value"},
            timeout=25.0
        )
        try:
            response = await client.make_request("order_service", request_config)
            print(f"Orders: {response.json()}")
        except Exception as e:
            print(f"Error fetching orders: {e}")
        
        # Example 4: Batch requests (concurrent)
        print("\n=== Example 4: Batch requests ===")
        batch_requests = [
            ("user_service", RequestConfig("/users/123", HTTPMethod.GET)),
            ("order_service", RequestConfig("/orders/456", HTTPMethod.GET)),
            ("inventory_service", RequestConfig("/products/789", HTTPMethod.GET))
        ]
        
        start_time = time.time()
        responses = await client.batch_requests(batch_requests)
        end_time = time.time()
        
        print(f"Batch completed in {end_time - start_time:.2f} seconds")
        for i, response in enumerate(responses):
            if isinstance(response, Exception):
                print(f"Request {i+1} failed: {response}")
            else:
                print(f"Request {i+1} status: {response.status_code}")
        
        # Example 5: Update inventory with PUT
        print("\n=== Example 5: PUT request ===")
        inventory_update = {
            "product_id": "789",
            "quantity": 100,
            "price": 29.99
        }
        try:
            response = await client.put("inventory_service", "/products/789", json_data=inventory_update)
            print(f"Updated inventory: {response.json()}")
        except Exception as e:
            print(f"Error updating inventory: {e}")
    
    finally:
        # Always close the client to cleanup connections
        await client.close()

# Helper class for service-specific clients
class UserServiceClient:
    """Example of a service-specific client wrapper"""
    
    def __init__(self, http_client: HTTPXClient):
        self.client = http_client
        self.service_name = "user_service"
    
    async def get_user(self, user_id: str) -> Dict[str, Any]:
        """Get user by ID"""
        response = await self.client.get(self.service_name, f"/users/{user_id}")
        return response.json()
    
    async def create_user(self, user_data: Dict[str, Any]) -> Dict[str, Any]:
        """Create new user"""
        response = await self.client.post(self.service_name, "/users", json_data=user_data)
        return response.json()
    
    async def update_user(self, user_id: str, user_data: Dict[str, Any]) -> Dict[str, Any]:
        """Update existing user"""
        response = await self.client.put(self.service_name, f"/users/{user_id}", json_data=user_data)
        return response.json()
    
    async def delete_user(self, user_id: str) -> bool:
        """Delete user"""
        response = await self.client.delete(self.service_name, f"/users/{user_id}")
        return response.status_code == 204

# Run the example
if __name__ == "__main__":
    asyncio.run(main())
