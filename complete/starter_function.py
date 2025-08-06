import azure.functions as func
import azure.durable_functions as df
import json
import logging
from logging_config import setup_logger

logger = setup_logger("Starter")

def main(req: func.HttpRequest, starter: str) -> func.HttpResponse:
    client = df.DurableOrchestrationClient(starter)

    try:
        body = req.get_json()
    except ValueError:
        return func.HttpResponse("Invalid JSON", status_code=400)

    instance_id = client.start_new("orchestrator_function", None, body)
    logger.info(f"Started instance: {instance_id}")
    return client.create_check_status_response(req, instance_id)

