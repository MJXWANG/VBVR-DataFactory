#!/usr/bin/env python3
"""
Update seed values in DLQ message files to random integers between 1 and 100000.
Split large messages into smaller ones based on num_samples.
"""

import os
import sys
import json
import random
import argparse
from pathlib import Path
from typing import Optional, List, Dict, Any
from datetime import datetime

def update_seeds_in_dlq(dlq_dir: str, seed_min: int = 1, seed_max: int = 100000, 
                        backup: bool = True, dry_run: bool = False):
    """
    Update seed values in all DLQ message files.
    
    Args:
        dlq_dir: Directory containing DLQ message files
        seed_min: Minimum seed value (default: 1)
        seed_max: Maximum seed value (default: 100000)
        backup: If True, create backup files before modifying
        dry_run: If True, only show what would be changed without actually modifying
    """
    dlq_path = Path(dlq_dir)
    
    if not dlq_path.exists():
        print(f"Error: Directory not found: {dlq_dir}")
        return 0
    
    # Find all JSON files
    json_files = list(dlq_path.glob('*.json'))
    
    if not json_files:
        print(f"No JSON files found in {dlq_dir}")
        return 0
    
    print(f"Found {len(json_files)} message files")
    print(f"Seed range: {seed_min} to {seed_max}")
    print(f"Dry run: {dry_run}")
    print(f"Backup: {backup}")
    print()
    
    updated_count = 0
    skipped_count = 0
    
    for json_file in json_files:
        try:
            # Read the message file
            with open(json_file, 'r', encoding='utf-8') as f:
                message_data = json.load(f)
            
            # Check if message has a body with seed
            if 'body' not in message_data:
                print(f"Skipping {json_file.name}: no 'body' field")
                skipped_count += 1
                continue
            
            body = message_data['body']
            
            # Handle nested JSON string case
            if isinstance(body, str):
                try:
                    body = json.loads(body)
                except json.JSONDecodeError:
                    print(f"Skipping {json_file.name}: body is not valid JSON")
                    skipped_count += 1
                    continue
            
            # Check if seed exists
            if 'seed' not in body:
                print(f"Skipping {json_file.name}: no 'seed' field in body")
                skipped_count += 1
                continue
            
            old_seed = body['seed']
            
            # Generate new random seed
            new_seed = random.randint(seed_min, seed_max)
            
            if dry_run:
                print(f"Would update {json_file.name}: seed {old_seed} -> {new_seed}")
            else:
                # Create backup if requested
                if backup:
                    backup_file = json_file.with_suffix('.json.bak')
                    with open(json_file, 'r', encoding='utf-8') as src:
                        with open(backup_file, 'w', encoding='utf-8') as dst:
                            dst.write(src.read())
                
                # Update seed
                body['seed'] = new_seed
                
                # Handle nested JSON string case - update the string
                if isinstance(message_data['body'], str):
                    message_data['body'] = json.dumps(body)
                else:
                    message_data['body'] = body
                
                # Write back to file
                with open(json_file, 'w', encoding='utf-8') as f:
                    json.dump(message_data, f, indent=2, ensure_ascii=False)
                
                print(f"Updated {json_file.name}: seed {old_seed} -> {new_seed}")
            
            updated_count += 1
            
            if updated_count % 50 == 0:
                print(f"Processed {updated_count} files...")
                
        except json.JSONDecodeError as e:
            print(f"Error parsing {json_file.name}: {e}")
            skipped_count += 1
        except Exception as e:
            print(f"Error processing {json_file.name}: {e}")
            skipped_count += 1
    
    print()
    print(f"Total files processed: {len(json_files)}")
    print(f"Files updated: {updated_count}")
    print(f"Files skipped: {skipped_count}")
    
    if dry_run:
        print("\n(No files were actually modified - dry run mode)")
    elif backup:
        print(f"\nBackup files created with .bak extension")
    
    return updated_count


def split_messages_in_dlq(dlq_dir: str, split_size: int, 
                          seed_min: int = 1, seed_max: int = 100000,
                          update_seed: bool = True,
                          backup: bool = True, dry_run: bool = False):
    """
    Split DLQ messages into smaller messages based on num_samples.
    
    Args:
        dlq_dir: Directory containing DLQ message files
        split_size: Number of samples per split message
        seed_min: Minimum seed value (default: 1)
        seed_max: Maximum seed value (default: 100000)
        update_seed: If True, generate new random seed for each split message
        backup: If True, create backup files before modifying
        dry_run: If True, only show what would be changed without actually modifying
    """
    dlq_path = Path(dlq_dir)
    
    if not dlq_path.exists():
        print(f"Error: Directory not found: {dlq_dir}")
        return 0, 0
    
    # Find all JSON files (excluding backups)
    json_files = [f for f in dlq_path.glob('*.json') if not f.name.endswith('.bak')]
    
    if not json_files:
        print(f"No JSON files found in {dlq_dir}")
        return 0, 0
    
    print(f"Found {len(json_files)} message files")
    print(f"Split size: {split_size} samples per message")
    print(f"Seed range: {seed_min} to {seed_max} (update_seed={update_seed})")
    print(f"Dry run: {dry_run}")
    print(f"Backup: {backup}")
    print()
    
    split_count = 0
    skipped_count = 0
    total_new_messages = 0
    
    for json_file in json_files:
        try:
            # Read the message file
            with open(json_file, 'r', encoding='utf-8') as f:
                message_data = json.load(f)
            
            # Check if message has a body
            if 'body' not in message_data:
                print(f"Skipping {json_file.name}: no 'body' field")
                skipped_count += 1
                continue
            
            body = message_data['body']
            
            # Handle nested JSON string case
            if isinstance(body, str):
                try:
                    body = json.loads(body)
                except json.JSONDecodeError:
                    print(f"Skipping {json_file.name}: body is not valid JSON")
                    skipped_count += 1
                    continue
            
            # Check required fields
            if 'start_index' not in body or 'num_samples' not in body:
                print(f"Skipping {json_file.name}: missing 'start_index' or 'num_samples'")
                skipped_count += 1
                continue
            
            start_index = int(body['start_index'])
            num_samples = int(body['num_samples'])
            
            # Skip if num_samples <= split_size
            if num_samples <= split_size:
                print(f"Skipping {json_file.name}: num_samples ({num_samples}) <= split_size ({split_size})")
                skipped_count += 1
                continue
            
            # Calculate number of splits needed
            num_splits = (num_samples + split_size - 1) // split_size  # Ceiling division
            
            if dry_run:
                print(f"Would split {json_file.name}: {num_samples} samples into {num_splits} messages")
            else:
                # Create backup if requested
                if backup:
                    backup_file = json_file.with_suffix('.json.bak')
                    with open(json_file, 'r', encoding='utf-8') as src:
                        with open(backup_file, 'w', encoding='utf-8') as dst:
                            dst.write(src.read())
                
                # Generate split messages
                split_messages: List[Dict[str, Any]] = []
                
                for i in range(num_splits):
                    split_start_index = start_index + (i * split_size)
                    split_num_samples = min(split_size, num_samples - (i * split_size))
                    
                    # Create new message body
                    new_body = body.copy()
                    new_body['start_index'] = split_start_index
                    new_body['num_samples'] = split_num_samples
                    
                    # Update seed if requested
                    if update_seed:
                        new_body['seed'] = random.randint(seed_min, seed_max)
                    # Otherwise keep original seed
                    
                    # Create new message data
                    new_message = message_data.copy()
                    # Update message_id and timestamp for uniqueness
                    original_message_id = message_data.get('message_id', '')
                    new_message['message_id'] = f"{original_message_id}_split_{i+1}"
                    new_message['timestamp'] = str(int(datetime.now().timestamp() * 1000))
                    new_message['body'] = new_body
                    
                    split_messages.append(new_message)
                
                # Save split messages
                base_name = json_file.stem  # Without .json extension
                
                for i, split_msg in enumerate(split_messages):
                    # Generate new filename with split index
                    timestamp = datetime.now().strftime("%Y%m%d%H%M%S%f")
                    new_message_id = split_msg['message_id']
                    new_filename = f"{timestamp}_{new_message_id}.json"
                    new_file = json_file.parent / new_filename
                    
                    # Write split message
                    with open(new_file, 'w', encoding='utf-8') as f:
                        json.dump(split_msg, f, indent=2, ensure_ascii=False)
                    
                    print(f"Created {new_filename}: start_index={split_msg['body']['start_index']}, num_samples={split_msg['body']['num_samples']}, seed={split_msg['body']['seed']}")
                
                # Delete or rename original file
                original_backup = json_file.with_suffix('.json.original')
                json_file.rename(original_backup)
                print(f"Renamed original file to {original_backup.name}")
                
                split_count += 1
                total_new_messages += len(split_messages)
            
            if split_count % 10 == 0 and split_count > 0:
                print(f"Processed {split_count} files...")
                
        except json.JSONDecodeError as e:
            print(f"Error parsing {json_file.name}: {e}")
            skipped_count += 1
        except Exception as e:
            print(f"Error processing {json_file.name}: {e}")
            import traceback
            traceback.print_exc()
            skipped_count += 1
    
    print()
    print(f"Total files processed: {len(json_files)}")
    print(f"Files split: {split_count}")
    print(f"Total new messages created: {total_new_messages}")
    print(f"Files skipped: {skipped_count}")
    
    if dry_run:
        print("\n(No files were actually modified - dry run mode)")
    elif backup:
        print(f"\nBackup files created with .bak extension")
        print(f"Original files renamed to .original extension")
    
    return split_count, total_new_messages


def main():
    parser = argparse.ArgumentParser(
        description='Update seed values in DLQ message files or split messages into smaller ones',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Update seeds in all DLQ messages (with backup)
  python update_dlq_seeds.py --dlq-dir DLQ

  # Split messages into smaller ones (50 samples each)
  python update_dlq_seeds.py --dlq-dir DLQ --split --split-size 50

  # Split messages and update seeds for each split
  python update_dlq_seeds.py --dlq-dir DLQ --split --split-size 50 --seed-min 1 --seed-max 100000

  # Split messages but keep original seeds
  python update_dlq_seeds.py --dlq-dir DLQ --split --split-size 50 --no-update-seed

  # Dry run to preview changes
  python update_dlq_seeds.py --dlq-dir DLQ --split --split-size 50 --dry-run

  # Custom seed range
  python update_dlq_seeds.py --dlq-dir DLQ --seed-min 1000 --seed-max 50000

  # Update without creating backups
  python update_dlq_seeds.py --dlq-dir DLQ --no-backup
        """
    )
    
    parser.add_argument(
        '--dlq-dir',
        type=str,
        default='DLQ',
        help='Directory containing DLQ message files (default: ./DLQ)'
    )
    parser.add_argument(
        '--seed-min',
        type=int,
        default=1,
        help='Minimum seed value (default: 1)'
    )
    parser.add_argument(
        '--seed-max',
        type=int,
        default=100000,
        help='Maximum seed value (default: 100000)'
    )
    parser.add_argument(
        '--no-backup',
        action='store_true',
        help='Do not create backup files before modifying'
    )
    parser.add_argument(
        '--dry-run',
        action='store_true',
        help='Preview changes without actually modifying files'
    )
    parser.add_argument(
        '--split',
        action='store_true',
        help='Split messages into smaller ones based on num_samples'
    )
    parser.add_argument(
        '--split-size',
        type=int,
        default=50,
        help='Number of samples per split message (default: 50). Only used with --split'
    )
    parser.add_argument(
        '--no-update-seed',
        action='store_true',
        help='When splitting, keep original seed instead of generating new ones'
    )
    
    args = parser.parse_args()
    
    # Resolve path relative to project root
    if not os.path.isabs(args.dlq_dir):
        project_root = Path(__file__).parent.parent
        dlq_path = project_root / args.dlq_dir
    else:
        dlq_path = Path(args.dlq_dir)
    
    if args.seed_min >= args.seed_max:
        print("Error: seed-min must be less than seed-max")
        sys.exit(1)
    
    if args.split_size <= 0:
        print("Error: split-size must be greater than 0")
        sys.exit(1)
    
    print(f"DLQ directory: {dlq_path}")
    print()
    
    if args.split:
        # Split messages
        print(f"Splitting messages in DLQ...")
        print()
        split_count, new_messages = split_messages_in_dlq(
            dlq_dir=str(dlq_path),
            split_size=args.split_size,
            seed_min=args.seed_min,
            seed_max=args.seed_max,
            update_seed=not args.no_update_seed,
            backup=not args.no_backup,
            dry_run=args.dry_run
        )
        
        if split_count > 0:
            print(f"\n✓ Successfully split {split_count} messages into {new_messages} new messages")
        else:
            print(f"\nNo messages were split")
    else:
        # Update seeds only
        print(f"Updating seeds in DLQ messages...")
        print()
        updated = update_seeds_in_dlq(
            dlq_dir=str(dlq_path),
            seed_min=args.seed_min,
            seed_max=args.seed_max,
            backup=not args.no_backup,
            dry_run=args.dry_run
        )
        
        if updated > 0:
            print(f"\n✓ Successfully updated {updated} messages")
        else:
            print(f"\nNo messages were updated")


if __name__ == '__main__':
    main()

