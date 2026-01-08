import os
import sys
import json
import shutil
import subprocess
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
        print(f"Generator stdout: {result.stdout}")
        if result.stderr:
            print(f"Generator stderr: {result.stderr}")
    except subprocess.CalledProcessError as e:
        print(f"Generator failed with return code {e.returncode}")
        print(f"stdout: {e.stdout}")
        print(f"stderr: {e.stderr}")
        raise
    
    # Find generated task directories
    # Expected structure: output_dir/data/questions/{domain}_task/{task_id}/
    output_path = Path(output_dir)
    questions_dir = output_path / 'data' / 'questions'
    
    uploaded_samples = []
    
    if questions_dir.exists():
        # Find all domain_task directories
        for domain_task_dir in questions_dir.iterdir():
            if domain_task_dir.is_dir() and domain_task_dir.name.endswith('_task'):
                # Find all task_id directories
                for task_id_dir in domain_task_dir.iterdir():
                    if task_id_dir.is_dir():
                        task_id = task_id_dir.name
                        sample_id = task_id
                        
                        # Upload all files in this task_id directory to S3
                        s3_prefix = f"data/v1/{task_type}/{sample_id}/"
                        upload_count = upload_directory_to_s3(task_id_dir, OUTPUT_BUCKET, s3_prefix)
                        
                        uploaded_samples.append({
                            'sample_id': sample_id,
                            'files_uploaded': upload_count
                        })
                        
                        print(f"Uploaded {upload_count} files for sample {sample_id}")
    else:
        raise ValueError(f"Expected output directory not found: {questions_dir}")
    
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
    Upload all files in a directory to S3.
    
    Args:
        local_dir: Path to local directory
        bucket: S3 bucket name
        s3_prefix: S3 key prefix (e.g., "data/v1/generator_name/sample_id/")
    
    Returns:
        Number of files uploaded
    """
    local_path = Path(local_dir)
    upload_count = 0
    
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
            except Exception as e:
                print(f"Error uploading {file_path} to s3://{bucket}/{s3_key}: {e}")
                raise
    
    return upload_count
