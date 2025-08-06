import os
import logging
from logging_config import setup_logger

logger = setup_logger("ValidateToken")

def main(input_data: dict):
    token = input_data.get("bearer_token")
    expected_token = os.environ["BEARER_TOKEN"]

    if token != expected_token:
        logger.error("Invalid Bearer Token")
        raise Exception("Invalid Token")

    logger.info("Token validated successfully")
    return True

