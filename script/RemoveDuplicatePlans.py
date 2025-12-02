import os
import hashlib
from collections import defaultdict

def removeDuplicatePlans(plans_dir, simulate=False):
    hash_map = defaultdict(list)

    # Compute the SHA-256 hash of a file to detect duplicates
    def get_file_hash(file_path):
        hasher = hashlib.sha256()
        with open(file_path, "rb") as f:
            for chunk in iter(lambda: f.read(4096), b""):
                hasher.update(chunk)
        return hasher.hexdigest()

    print(f"\nScanning plans folder: {plans_dir}\n")

    # Compute hashes for each file
    for root, _, files in os.walk(plans_dir):
        for filename in files:
            if not filename.startswith('.'):  # ignore hidden files
                file_path = os.path.join(root, filename)
                try:
                    file_hash = get_file_hash(file_path)
                    hash_map[file_hash].append(file_path)
                except Exception as e:
                    print(f" Error reading {file_path}: {e}")

    # Keep only hashes with more than one file associated  (It means that are duplicates)
    duplicates = {h: files for h, files in hash_map.items() if len(files) > 1}

    if not duplicates:
        print(" No duplicates found.")
    else:
        total_deleted = 0
        print(f" Found {len(duplicates)} duplicate groups.\n")

        for h, files in duplicates.items():
            print(f" Duplicate group (hash {h[:12]}...):")
            files.sort()  
            keep = files[0]
            duplicates_to_delete = files[1:]

            print(f"  Keeping: {keep}")
            for f in duplicates_to_delete:
                print(f"  Deleting: {f}")
                if not simulate:
                    os.remove(f)
                    total_deleted += 1
            print()

        if simulate:
            print("\n SIMULATION MODE active — no files were deleted.")
            print(" Set `simulate = False` to actually remove duplicates.\n")
        else:
            print(f" Deleted {total_deleted} duplicate files.\n")
