#!/usr/bin/env python3
"""
Update seed values in DLQ message files to random integers between 1 and 100000.
"""

import os
import sys
import json
import random
import argparse
from pathlib import Path
from typing import Optional

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


def main():
    parser = argparse.ArgumentParser(
        description='Update seed values in DLQ message files',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Update seeds in all DLQ messages (with backup)
  python update_dlq_seeds.py --dlq-dir DLQ

  # Dry run to preview changes
  python update_dlq_seeds.py --dlq-dir DLQ --dry-run

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
    
    print(f"Updating seeds in DLQ messages...")
    print(f"DLQ directory: {dlq_path}")
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

