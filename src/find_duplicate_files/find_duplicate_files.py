import os
import sys
import hashlib
import concurrent.futures
from collections import defaultdict
import json
import time
from datetime import datetime
import win32security  # For Windows systems
import logging
from logging.handlers import TimedRotatingFileHandler


def setup_logger():
    """
    logger function object for logging information to a file.
    """
    logging_directory = 'logging'
    os.makedirs(logging_directory, exist_ok=True)

    logger = logging.getLogger("Find Duplicate Files Logger")
    logger.setLevel(logging.INFO)

    handler = TimedRotatingFileHandler("logging/log.log", when="midnight", interval=1, backupCount=7)
    handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))

    logger.addHandler(handler)

    return logger


logger = setup_logger()

def calculate_hash(file_path, block_size=65536):
    hasher = hashlib.sha256()
    try:
        with open(file_path, 'rb') as f:
            buffer = f.read(block_size)
            while buffer:
                hasher.update(buffer)
                buffer = f.read(block_size)
    except Exception as e:
        logger.error(f"Error reading file {file_path}: {e}")
        return None  # Return None to indicate failure
    return hasher.hexdigest()


def find_duplicates_parallel(directories):
    with concurrent.futures.ProcessPoolExecutor() as executor:
        # Create a list to hold all future objects
        future_to_file = {executor.submit(calculate_hash, os.path.join(dirpath, filename)): (filename, dirpath)
                          for directory in directories
                          for dirpath, dirnames, filenames in os.walk(directory)
                          if "Cache" not in dirpath  # Skip directories containing 'Cache' in their path
                          for filename in filenames}
        for future in concurrent.futures.as_completed(future_to_file):
            file_hash = future.result()
            if file_hash is None:  # Skip files that couldn't be read
                continue
            filename, dirpath = future_to_file[future]
            full_path = os.path.join(dirpath, filename)
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
    for file_hash, full_path in find_duplicates_parallel(directories):
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
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(indexed_duplicates, f, indent=4, ensure_ascii=False)
    
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
    logger.info((f"Runtime duration searching in directories {directories}: {duration} seconds") ) # Log duration
    print(f"Runtime duration: {duration} seconds")  # Print the duration

if __name__ == "__main__":
    main()