"""DynamoDB-based dedup checker for param_hash uniqueness.

Uses conditional put to atomically check-and-register param hashes,
ensuring no two samples with the same generator + param_hash are uploaded.
"""

import logging
import time

import boto3
from botocore.exceptions import ClientError

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

MAX_THROTTLE_RETRIES = 3


class DedupChecker:
    """Checks and registers param_hash uniqueness via DynamoDB conditional put."""

    def __init__(self, table_name: str, region: str = "us-east-2"):
        self.table_name = table_name
        self.dynamodb = boto3.resource("dynamodb", region_name=region)
        self.table = self.dynamodb.Table(table_name)

    def check_and_register(self, generator_name: str, param_hash: str, sample_id: str) -> bool:
        """
        Atomically check if a param_hash is unique and register it.

        Returns:
            True if unique (newly registered or previously registered by same sample_id).
            False if a different sample already owns this hash.

        Raises:
            ClientError: On unrecoverable DDB errors or throttle after max retries.
        """
        last_error = None

        for attempt in range(MAX_THROTTLE_RETRIES):
            try:
                self.table.put_item(
                    Item={
                        "generator_name": generator_name,
                        "param_hash": param_hash,
                        "sample_id": sample_id,
                    },
                    ConditionExpression="attribute_not_exists(generator_name) AND attribute_not_exists(param_hash)",
                )
                return True
            except ClientError as e:
                code = e.response["Error"]["Code"]

                if code == "ConditionalCheckFailedException":
                    # Hash exists — check if it was registered by the same sample (Lambda retry)
                    return self._is_owned_by(generator_name, param_hash, sample_id)

                if code in ("ProvisionedThroughputExceededException", "ThrottlingException"):
                    last_error = e
                    wait = 2**attempt
                    logger.warning(f"DDB throttled, retrying in {wait}s (attempt {attempt + 1}/{MAX_THROTTLE_RETRIES})")
                    time.sleep(wait)
                    continue

                raise

        # Throttle retries exhausted — let Lambda fail so SQS can retry
        raise last_error

    def _is_owned_by(self, generator_name: str, param_hash: str, sample_id: str) -> bool:
        """Check if an existing DDB record was registered by the same sample_id."""
        resp = self.table.get_item(
            Key={"generator_name": generator_name, "param_hash": param_hash},
            ProjectionExpression="sample_id",
        )
        existing = resp.get("Item", {}).get("sample_id")
        if existing == sample_id:
            logger.info(f"Hash {param_hash} already registered by same sample {sample_id} (Lambda retry)")
            return True
        logger.info(f"Hash {param_hash} owned by {existing}, duplicate for {sample_id}")
        return False
