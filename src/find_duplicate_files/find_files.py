import os
import hashlib
import sys
from collections import defaultdict
import concurrent.futures
import json
from datetime import datetime
import win32security  # For Windows systems
import time

def calculate_hash(file_path, block_size=65536):
    hasher = hashlib.sha256()
    with open(file_path, 'rb') as f:
        buffer = f.read(block_size)
        while buffer:
            hasher.update(buffer)
            buffer = f.read(block_size)
    return hasher.hexdigest()


def find_duplicates(directories):
    for directory in directories:
        for dirpath, dirnames, filenames in os.walk(directory):
            for filename in filenames:
                full_path = os.path.join(dirpath, filename)
                file_hash = calculate_hash(full_path)
                yield file_hash, full_path

def get_file_metadata(file_path):
    """
    Collects required metadata for a given file, adapted for Windows.
    """
    # Getting file size in MB
    file_size = os.path.getsize(file_path) / (1024 * 1024)
    
    # Getting last modification and creation times
    last_write_time = datetime.fromtimestamp(os.path.getmtime(file_path)).strftime('%Y-%m-%d %H:%M')
    creation_time = datetime.fromtimestamp(os.path.getctime(file_path)).strftime('%Y-%m-%d %H:%M')
    
    # Getting file owner (Windows systems)
    try:
        sd = win32security.GetFileSecurity(file_path, win32security.OWNER_SECURITY_INFORMATION)
        owner_sid = sd.GetSecurityDescriptorOwner()
        name, domain, _ = win32security.LookupAccountSid(None, owner_sid)
        file_owner = f"{domain}\\{name}"
    except Exception as e:
        file_owner = f"Owner Info Not Available: {str(e)}"
    
    return {
        "Length_MB": round(file_size, 4),
        "LastWriteTime": last_write_time,
        "CreationTime": creation_time,
        "FileOwner": file_owner
    }


def export_duplicates_to_json(directories, output_file='duplicates.json'):

    # Check if the output file already exists and delete it if it does
    if os.path.exists(output_file):
        os.remove(output_file)

    hashes = defaultdict(list)
    for file_hash, full_path in find_duplicates(directories):
        hashes[file_hash].append(full_path)
    
    # Collecting only the groups of files that have duplicates
    duplicates = []
    for files in hashes.values():
        if len(files) > 1:
            # For each file in the group, get its metadata
            files_with_metadata = []
            for file in files:
                metadata = get_file_metadata(file)
                files_with_metadata.append({"path": file, "metadata": metadata})
            duplicates.append(files_with_metadata)
    
    # Modifying the structure to include an index number for each group
    indexed_duplicates = {f"Duplicate Group {index + 1}": files for index, files in enumerate(duplicates)}
    
    # Writing the modified list of duplicates with metadata to a JSON file
    with open(output_file, 'w') as f:
        json.dump(indexed_duplicates, f, indent=4)
    
    print(f'Duplicates exported to {output_file}')


def main():
    start_time = time.time()  # Capture the start time

    if len(sys.argv) > 1:
        directories = sys.argv[1:]
        output_file = 'duplicates.json'  # You can specify another file name/path if needed
        export_duplicates_to_json(directories, output_file)
    else:
        print("Please provide at least one directory as an argument.")

    end_time = time.time()  # Capture the end time
    duration = end_time - start_time  # Calculate the duration
    print(f"Runtime duration: {duration} seconds")  # Print the duration

if __name__ == "__main__":
    main()