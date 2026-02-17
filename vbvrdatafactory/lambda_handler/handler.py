"""AWS Lambda handler.

NO try-catch blocks - let Lambda/SQS handle retries on failure.
Pydantic validates messages automatically - invalid messages go to DLQ.
"""

import gc
import json
import logging
import os
import random
import shutil
from pathlib import Path

from vbvrdatafactory.core.config import config
from vbvrdatafactory.core.generator import GeneratorRunner
from vbvrdatafactory.core.metrics import MetricsClient
from vbvrdatafactory.core.models import TaskMessage, TaskResult
from vbvrdatafactory.core.uploader import S3Uploader
from vbvrdatafactory.core.validator import find_task_directories, rename_samples

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

MAX_DEDUP_RETRIES = 3


def handler(event: dict, context) -> dict:
    """
    Lambda entry point.

    Pydantic validates automatically - invalid messages raise ValidationError → DLQ.

    Args:
        event: Lambda event (SQS message or direct invocation)
        context: Lambda context

    Returns:
        Result dictionary with status and processed count

    Raises:
        ValidationError: If message is invalid (goes to DLQ)
        Any other exception: Lambda will retry, then DLQ
    """
    records = event.get("Records", [event])
    results = []

    for record in records:
        # Parse with Pydantic - raises ValidationError if invalid
        if "body" in record:
            task = TaskMessage.model_validate_json(record["body"])
        else:
            task = TaskMessage.model_validate(record)

        result = process_task(task)
        results.append(result.model_dump())

    return {"status": "ok", "processed": len(records), "results": results}


def process_task(task: TaskMessage) -> TaskResult:
    """
    Process a single generation task.

    No try-catch - let exceptions bubble up for Lambda/SQS retry mechanism.

    Args:
        task: Task message (Pydantic validated)

    Returns:
        TaskResult with upload information

    Raises:
        Various exceptions that trigger Lambda retry → DLQ after max retries
    """
    # Generate random seed if not provided
    if task.seed is None:
        task.seed = random.randint(1, 2**31 - 1)
        logger.info(f"No seed provided, using random seed: {task.seed}")

    metrics = MetricsClient()

    # Track duration and execute
    with metrics.track_duration(task.type):
        result = _process_samples(task, metrics)

    # Success metrics (only reached if no exception)
    metrics.put_metric("TaskSuccess", 1, "Count", task.type)
    metrics.put_metric("SamplesUploaded", len(result.sample_ids), "Count", task.type)

    return result


# ---------------------------------------------------------------------------
# Dedup helpers
# ---------------------------------------------------------------------------

def _read_param_hash(sample_dir: Path) -> str | None:
    """Read param_hash from a sample's metadata.json. Returns None if unavailable."""
    metadata_path = sample_dir / "metadata.json"
    if not metadata_path.exists():
        return None
    with open(metadata_path) as f:
        return json.load(f).get("param_hash")


def _batch_regenerate(
    duplicate_ids: list[str],
    domain_task_dir: Path,
    task: TaskMessage,
    runner: GeneratorRunner,
) -> None:
    """
    Regenerate multiple samples in one subprocess call and move them into place.

    Replaces the sample directories under domain_task_dir for each duplicate_id.
    Retries with different seeds if the generator subprocess crashes.
    On final failure the old directories are cleaned up so the caller can skip them.
    """
    count = len(duplicate_ids)

    for attempt in range(MAX_DEDUP_RETRIES):
        new_seed = random.randint(1, 2**31 - 1)
        retry_output_dir = Path(f"/tmp/dedup_retry_{task.type}_{os.getpid()}_{new_seed}")

        logger.info(f"Batch regenerating {count} samples with seed={new_seed} (attempt {attempt + 1}/{MAX_DEDUP_RETRIES})")

        try:
            retry_task = TaskMessage(
                type=task.type,
                num_samples=count,
                start_index=0,
                seed=new_seed,
                output_format=task.output_format,
                output_bucket=task.output_bucket,
            )
            runner.run(retry_task, retry_output_dir)

            # Collect generated sample dirs in order
            retry_questions = find_task_directories(retry_output_dir)
            if not retry_questions:
                logger.warning("Batch regenerate produced no output, retrying with new seed")
                continue

            new_sample_dirs: list[Path] = []
            for item in retry_questions.rglob("*"):
                if item.is_dir() and item.name.endswith("_task"):
                    for sub in sorted(item.iterdir()):
                        if sub.is_dir():
                            new_sample_dirs.append(sub)

            # Map regenerated samples back to duplicate_ids (1:1 in order)
            for i, sample_id in enumerate(duplicate_ids):
                target_dir = domain_task_dir / sample_id
                if target_dir.exists():
                    shutil.rmtree(target_dir)

                if i < len(new_sample_dirs):
                    new_sample_dirs[i].rename(target_dir)
                else:
                    logger.warning(f"Not enough regenerated samples for {sample_id}")

            return  # Success

        except Exception as e:
            logger.warning(f"Batch regeneration attempt {attempt + 1} failed: {e}")
        finally:
            if retry_output_dir.exists():
                shutil.rmtree(retry_output_dir, ignore_errors=True)

    # All attempts exhausted — clean up so caller can skip these samples
    logger.warning(f"Batch regeneration failed after {MAX_DEDUP_RETRIES} attempts, dropping {count} samples")
    for sample_id in duplicate_ids:
        target = domain_task_dir / sample_id
        if target.exists():
            shutil.rmtree(target, ignore_errors=True)


def _dedup_samples(
    domain_task_dir: Path,
    renamed_samples: list[str],
    task: TaskMessage,
    runner: GeneratorRunner,
    metrics: MetricsClient,
) -> list[str]:
    """
    Dedup renamed samples against DynamoDB. Duplicates are batch-regenerated.

    Args:
        domain_task_dir: Path to the domain task directory
        renamed_samples: List of sample IDs already renamed
        task: Task message
        runner: GeneratorRunner instance for regeneration
        metrics: MetricsClient for dedup metrics

    Returns:
        List of sample IDs that passed dedup (unique samples)
    """
    from vbvrdatafactory.core.dedup import DedupChecker

    table_name = config.dedup_table_name
    if not table_name:
        logger.warning("DEDUP_TABLE_NAME not set, skipping dedup")
        return renamed_samples

    checker = DedupChecker(table_name, config.aws_region)

    total_duplicates = 0
    total_retries = 0

    # Iterative dedup: check → collect duplicates → batch regenerate → re-check
    pending = list(renamed_samples)
    unique_samples: list[str] = []

    for round_num in range(MAX_DEDUP_RETRIES + 1):
        duplicates: list[str] = []

        for sample_id in pending:
            sample_dir = domain_task_dir / sample_id

            # Sample dir was cleaned up (e.g. regeneration failed) — skip it
            if not sample_dir.exists():
                logger.warning(f"Sample dir missing for {sample_id}, skipping")
                total_duplicates += 1
                continue

            param_hash = _read_param_hash(sample_dir)

            if not param_hash:
                logger.warning(f"No param_hash for {sample_id}, skipping dedup check")
                unique_samples.append(sample_id)
                continue

            if checker.check_and_register(task.type, param_hash, sample_id):
                logger.info(f"Dedup OK: {sample_id} (hash={param_hash})")
                unique_samples.append(sample_id)
            else:
                logger.warning(f"Duplicate: {sample_id} (hash={param_hash})")
                duplicates.append(sample_id)
                total_duplicates += 1

        if not duplicates:
            break

        # Last round was the final check — no more retries
        if round_num == MAX_DEDUP_RETRIES:
            for sample_id in duplicates:
                logger.warning(f"Skipping {sample_id} after {MAX_DEDUP_RETRIES} dedup retries")
                sample_dir = domain_task_dir / sample_id
                if sample_dir.exists():
                    shutil.rmtree(sample_dir, ignore_errors=True)
            break

        # Batch regenerate all duplicates in one subprocess call
        total_retries += 1
        logger.info(f"Dedup round {round_num + 1}: {len(duplicates)} duplicates, batch regenerating")
        _batch_regenerate(duplicates, domain_task_dir, task, runner)
        pending = duplicates  # Re-check only the regenerated ones

    # Emit dedup metrics
    metrics.put_metric("DedupDuplicatesFound", total_duplicates, "Count", task.type)
    metrics.put_metric("DedupRetryRounds", total_retries, "Count", task.type)
    metrics.put_metric("DedupSkipped", len(renamed_samples) - len(unique_samples), "Count", task.type)

    return unique_samples


# ---------------------------------------------------------------------------
# Main processing
# ---------------------------------------------------------------------------

def _process_samples(task: TaskMessage, metrics: MetricsClient) -> TaskResult:
    """
    Process samples for a generator task.

    Args:
        task: Task message
        metrics: MetricsClient instance

    Returns:
        TaskResult with upload information

    Raises:
        FileNotFoundError: If generator not found
        ValueError: If no task files found
        CalledProcessError: If generator fails
        ClientError: If S3 upload fails
        OSError: If file operations fail
    """
    output_bucket = task.output_bucket or config.output_bucket
    output_dir = Path(f"/tmp/output_{task.type}_{os.getpid()}")

    # 1. Run generator (raises on failure)
    runner = GeneratorRunner(config.generators_path)
    runner.run(task, output_dir)

    # 2. Find task directories
    logger.info(f"Checking output directory: {output_dir}")
    questions_dir = find_task_directories(output_dir)

    if not questions_dir:
        error_msg = f"No task files found in output directory: {output_dir}"
        logger.error(error_msg)
        raise ValueError(error_msg)

    logger.info(f"Using questions directory: {questions_dir}")

    # 3. Rename, dedup, and upload samples
    uploader = S3Uploader(output_bucket, config.aws_region)
    uploaded_samples = []
    tar_files = []
    found_any_task_dir = False

    for item in questions_dir.rglob("*"):
        if item.is_dir() and item.name.endswith("_task"):
            domain_task_dir = item
            logger.info(f"Found domain_task directory: {domain_task_dir}")

            renamed = rename_samples(domain_task_dir, task.start_index)
            if not renamed:
                continue

            found_any_task_dir = True

            # Dedup if enabled
            if task.dedup:
                before_count = len(renamed)
                logger.info(f"Dedup enabled, checking {before_count} samples")
                renamed = _dedup_samples(domain_task_dir, renamed, task, runner, metrics)
                logger.info(f"After dedup: {len(renamed)}/{before_count} unique samples")

                if not renamed:
                    logger.warning("All samples were duplicates, skipping upload")
                    continue

            batch_uploaded, batch_tar = uploader.upload_samples(
                domain_task_dir=domain_task_dir,
                renamed_samples=renamed,
                task_type=task.type,
                start_index=task.start_index,
                output_format=task.output_format,
            )

            uploaded_samples.extend(batch_uploaded)
            if batch_tar:
                tar_files.append(batch_tar)

            gc.collect()

    if not found_any_task_dir:
        error_msg = f"No task directories with files found in {questions_dir}"
        logger.warning(error_msg)
        raise ValueError(error_msg)

    # 4. Cleanup
    if output_dir.exists():
        shutil.rmtree(output_dir, ignore_errors=True)
        logger.info(f"Cleaned up output directory: {output_dir}")

    gc.collect()
    logger.info(f"Task complete: uploaded {len(uploaded_samples)} samples")

    return TaskResult(
        generator=task.type,
        samples_uploaded=len(uploaded_samples),
        sample_ids=[s["sample_id"] for s in uploaded_samples],
        tar_files=tar_files,
    )
