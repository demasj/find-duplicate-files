import os
import hashlib
import sys
from collections import defaultdict
import concurrent.futures

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

def print_duplicates(directories):
    hashes = defaultdict(list)
    for file_hash, full_path in find_duplicates(directories):
        hashes[file_hash].append(full_path)
    index = 1
    for files in hashes.values():
        if len(files) > 1:
            print(f'Duplicate Group {index}:')
            for file in files:
                print(f'\t{file}')
            index += 1

def main():
    if len(sys.argv) > 1:
        directories = sys.argv[1:]
        print_duplicates(directories)
    else:
        print("Please provide at least one directory as an argument.")

if __name__ == "__main__":
    main()