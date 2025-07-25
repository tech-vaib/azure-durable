# E-commerce order processing system
class OrderProcessor:
    def __init__(self, client: EnhancedHTTPXClient):
        self.client = client
    
    async def process_order(self, order_id: str):
        # 1. Validate order (HTTP call)
        order = await self.client.get("order_service", f"/orders/{order_id}")
        
        # 2. Check inventory in background
        inventory_task = await self.client.submit_background_task(
            self.client.get("inventory_service", f"/check/{order['product_id']}")
        )
        
        # 3. Process payment while checking inventory
        payment_response = await self.client.post(
            "payment_service", 
            "/charge", 
            json_data={"amount": order["total"], "customer_id": order["customer_id"]}
        )
        
        # 4. Wait for inventory check
        inventory_result = await self.client.wait_for_task(inventory_task, timeout=10)
        
        # 5. Update order status in MongoDB
        await self.client.mongo_update_one(
            "orders", 
            {"order_id": order_id},
            {"status": "processed", "processed_at": datetime.now()}
        )
        
        # 6. Send confirmation email in background
        await self.client.submit_background_task(
            self.send_confirmation_email(order["customer_email"])
        )
