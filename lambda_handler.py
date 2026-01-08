import os
import sys
import json
import shutil
import subprocess
import re
import boto3
from pathlib import Path

# Initialize S3 client with default region
s3 = boto3.client('s3', region_name=os.environ.get('AWS_REGION', 'us-east-2'))

OUTPUT_BUCKET = os.environ.get('OUTPUT_BUCKET', 'vm-dataset-test')
GENERATORS_PATH = '/opt/generators'


def handler(event, context):
    """
    Lambda handler for generating dataset samples.

    Expected event format (from SQS):
    {
        "type": "chess-task-data-generator",
        "start_index": 0,
        "num_samples": 1000,
        "seed": 42
    }
    """
    # Handle SQS batch
    records = event.get('Records', [event])
    results = []

    for record in records:
        try:
            if 'body' in record:
                task = json.loads(record['body'])
            else:
                task = record

            result = process_task(task)
            results.append(result)
        except Exception as e:
            print(f"Error processing record: {e}")
            raise

    return {'status': 'ok', 'processed': len(records), 'results': results}


def process_task(task):
    """Process a single generation task."""
    task_type = task['type']
    num_samples = task['num_samples']
    start_index = task.get('start_index', 0)  # Starting index for task IDs
    seed = task.get('seed')  # Optional seed parameter
    
    # Find generator directory
    generator_path = os.path.join(GENERATORS_PATH, task_type)
    if not os.path.exists(generator_path):
        raise ValueError(f"Generator not found: {task_type} at {generator_path}")
    
    # Use temporary output directory
    output_dir = f'/tmp/output_{task_type}_{os.getpid()}'
    
    # Build command: python examples/generate.py --num-samples {num_samples}
    cmd = [sys.executable, 'examples/generate.py', '--num-samples', str(num_samples)]
    
    # Add seed if provided
    if seed is not None:
        cmd.extend(['--seed', str(seed)])
    
    # Add output directory
    cmd.extend(['--output', output_dir])
    
    print(f"Running command: {' '.join(cmd)}")
    print(f"Working directory: {generator_path}")
    
    # Run generator
    try:
        result = subprocess.run(
            cmd,
            cwd=generator_path,
            check=True,
            capture_output=True,
            text=True
        )
        print(f"Generator completed successfully")
        print(f"Generator stdout: {result.stdout}")
        if result.stderr:
            print(f"Generator stderr: {result.stderr}")
    except subprocess.CalledProcessError as e:
        print(f"Generator failed with return code {e.returncode}")
        print(f"stdout: {e.stdout}")
        print(f"stderr: {e.stderr}")
        print(f"Command that failed: {' '.join(cmd)}")
        print(f"Working directory: {generator_path}")
        raise
    
    # Debug: Check what was actually created
    output_path = Path(output_dir)
    print(f"Checking output directory: {output_dir}")
    print(f"Output directory exists: {output_path.exists()}")
    
    if output_path.exists():
        print(f"Output directory contents:")
        for item in output_path.rglob('*'):
            if item.is_dir():
                print(f"  DIR:  {item}")
            elif item.is_file():
                print(f"  FILE: {item}")
    
    # Find generated task directories
    # Based on OutputWriter, files are created at: output_dir/{domain}_task/{task_id}/
    # We need to search recursively for _task directories
    
    questions_dir = None
    
    # First, try to find _task directories directly under output_path
    print(f"Searching for _task directories in: {output_path}")
    if output_path.exists():
        # Look for _task directories recursively
        for item in output_path.rglob('*_task'):
            if item.is_dir():
                print(f"Found _task directory at: {item}")
                # Use the parent directory as base (which should contain the _task dirs)
                questions_dir = item.parent
                print(f"Using base directory: {questions_dir}")
                break
        
        # If not found by _task pattern, try finding by task files
        if not questions_dir:
            print(f"Searching for task files (png/txt/mp4) in: {output_path}")
            for item in output_path.rglob('*'):
                if item.is_file() and item.suffix in ['.png', '.txt', '.mp4']:
                    # Found a task file, find the _task directory in its path
                    current = item.parent
                    while current != output_path.parent and current != output_path:
                        if current.name.endswith('_task'):
                            questions_dir = current.parent
                            print(f"Found _task via file {item}, using base: {questions_dir}")
                            break
                        current = current.parent
                    if questions_dir:
                        break
    
    # Fallback: use output_path directly
    if not questions_dir:
        print(f"Using output_path as fallback: {output_path}")
        questions_dir = output_path
    
    uploaded_samples = []
    
    if questions_dir and questions_dir.exists():
        print(f"Using questions directory: {questions_dir}")
        # Find all domain_task directories (recursively if needed)
        # Process files one by one to minimize memory usage
        found_any = False
        
        # Use rglob iterator to avoid loading all paths into memory
        for item in questions_dir.rglob('*'):
            if item.is_dir() and item.name.endswith('_task'):
                domain_task_dir = item
                print(f"Found domain_task directory: {domain_task_dir}")
                
                # Process each task_id directory immediately
                try:
                    task_dirs = list(domain_task_dir.iterdir())
                    # Sort task directories to ensure consistent ordering
                    # Extract numeric part for sorting if possible
                    def get_sort_key(path):
                        name = path.name
                        # Try to extract number from task ID (e.g., "task_0" -> 0, "0" -> 0)
                        try:
                            # Try to extract last number from the name
                            numbers = re.findall(r'\d+', name)
                            if numbers:
                                return int(numbers[-1])
                        except:
                            pass
                        return name  # Fallback to string sort
                    
                    task_dirs.sort(key=get_sort_key)
                except Exception as e:
                    print(f"Error listing task dirs in {domain_task_dir}: {e}")
                    continue
                
                # Process tasks in order, mapping to global IDs starting from start_index
                local_index = 0
                for task_id_dir in task_dirs:
                    if not task_id_dir.is_dir():
                        continue
                    
                    original_task_id = task_id_dir.name
                    
                    # Quick check if directory has task files (without loading all)
                    has_files = False
                    try:
                        for _ in task_id_dir.glob('*.png'):
                            has_files = True
                            break
                        if not has_files:
                            for _ in task_id_dir.glob('*.txt'):
                                has_files = True
                                break
                        if not has_files:
                            for _ in task_id_dir.glob('*.mp4'):
                                has_files = True
                                break
                    except Exception as e:
                        print(f"Error checking files in {task_id_dir}: {e}")
                        continue
                    
                    if not has_files:
                        print(f"Skipping empty directory: {task_id_dir}")
                        # Clean up empty directory
                        try:
                            task_id_dir.rmdir()
                        except:
                            pass
                        continue
                    
                    # Only map to global ID if directory has files
                    # Map to global ID: start_index + local_index
                    global_task_id = str(start_index + local_index)
                    sample_id = global_task_id
                    local_index += 1
                    
                    print(f"Mapping local task {original_task_id} to global ID {sample_id} (start_index={start_index})")
                    
                    found_any = True
                    print(f"Processing task: {sample_id}")
                    
                    # Upload all files in this task_id directory to S3
                    # Files will be deleted during upload
                    s3_prefix = f"data/v1/{task_type}/{sample_id}/"
                    try:
                        upload_count = upload_directory_to_s3(task_id_dir, OUTPUT_BUCKET, s3_prefix)
                        
                        uploaded_samples.append({
                            'sample_id': sample_id,
                            'files_uploaded': upload_count
                        })
                        
                        print(f"Uploaded {upload_count} files for sample {sample_id}")
                        
                        # Delete the now-empty directory
                        try:
                            task_id_dir.rmdir()
                            print(f"Deleted local directory: {task_id_dir}")
                        except Exception as e:
                            # Directory might not be empty or already deleted
                            pass
                    except Exception as e:
                        print(f"Error processing {sample_id}: {e}")
                        raise
        
        if not found_any:
            print(f"Warning: No task directories with files found in {questions_dir}")
            raise ValueError(f"No task files found in output directory: {questions_dir}")
    else:
        # More detailed error message
        error_msg = f"Cannot process output: questions_dir={questions_dir}\n"
        error_msg += f"Output directory exists: {output_path.exists()}\n"
        if output_path.exists():
            error_msg += f"Output directory contents:\n"
            try:
                items = list(output_path.rglob('*'))
                if items:
                    for item in items[:50]:  # Limit to first 50 items
                        if item.is_dir():
                            error_msg += f"  DIR:  {item}\n"
                        elif item.is_file():
                            error_msg += f"  FILE: {item}\n"
                    if len(items) > 50:
                        error_msg += f"  ... and {len(items) - 50} more items\n"
                else:
                    error_msg += f"  (directory is empty)\n"
            except Exception as e:
                error_msg += f"  Error listing contents: {e}\n"
        else:
            error_msg += f"Output directory {output_dir} does not exist.\n"
        error_msg += f"This usually means the generator did not produce any output files.\n"
        print(error_msg)
        raise ValueError(error_msg)
    
    # Cleanup
    if os.path.exists(output_dir):
        shutil.rmtree(output_dir, ignore_errors=True)
    
    print(f"Completed: {task_type} - uploaded {len(uploaded_samples)} samples")
    
    return {
        'generator': task_type,
        'samples_uploaded': len(uploaded_samples),
        'sample_ids': [s['sample_id'] for s in uploaded_samples]
    }


def upload_directory_to_s3(local_dir, bucket, s3_prefix):
    """
    Upload all files in a directory to S3, deleting each file after successful upload.
    This helps reduce memory usage by freeing up disk space immediately.
    
    Args:
        local_dir: Path to local directory
        bucket: S3 bucket name
        s3_prefix: S3 key prefix (e.g., "data/v1/generator_name/sample_id/")
    
    Returns:
        Number of files uploaded
    """
    local_path = Path(local_dir)
    upload_count = 0
    files_to_delete = []
    
    # First pass: collect all files and upload them
    for file_path in local_path.rglob('*'):
        if file_path.is_file():
            # Get relative path from local_dir
            relative_path = file_path.relative_to(local_path)
            s3_key = s3_prefix + str(relative_path).replace('\\', '/')
            
            # Upload file
            try:
                s3.upload_file(str(file_path), bucket, s3_key)
                upload_count += 1
                print(f"Uploaded: s3://{bucket}/{s3_key}")
                # Mark file for deletion after successful upload
                files_to_delete.append(file_path)
            except Exception as e:
                print(f"Error uploading {file_path} to s3://{bucket}/{s3_key}: {e}")
                raise
    
    # Second pass: delete uploaded files to free memory/disk space
    for file_path in files_to_delete:
        try:
            file_path.unlink()
            print(f"Deleted local file: {file_path}")
        except Exception as e:
            print(f"Warning: Failed to delete {file_path}: {e}")
    
    return upload_count
