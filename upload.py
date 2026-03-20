import os
import time
import boto3
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler

# ==============================
# CONFIGURATION
# ==============================
WATCH_FOLDER = "/home/ubuntu/ComfyUI/output"
BUCKET_NAME = "video-display-bucket"
S3_FOLDER = "videos"
REGION = "ap-south-1"

# ==============================
# S3 CLIENT
# ==============================
s3 = boto3.client("s3", region_name=REGION)

# ==============================
# HELPER: GET ALL EXISTING S3 KEYS
# ==============================
def get_existing_s3_keys():
    existing_keys = set()
    paginator = s3.get_paginator("list_objects_v2")
    pages = paginator.paginate(Bucket=BUCKET_NAME, Prefix=f"{S3_FOLDER}/")
    for page in pages:
        for obj in page.get("Contents", []):
            existing_keys.add(obj["Key"])
    print(f"📦 Found {len(existing_keys)} existing file(s) on S3.")
    return existing_keys

# ==============================
# HELPER: UPLOAD A SINGLE FILE (FLAT — NO SUBFOLDERS IN S3)
# ==============================
def upload_file(file_path):
    # ✅ Must be a real file
    if not os.path.isfile(file_path):
        return

    # ✅ Must be an mp4
    if not file_path.lower().endswith(".mp4"):
        return

    # ✅ Only the filename — no subfolder structure in S3
    file_name = os.path.basename(file_path)
    s3_key = f"{S3_FOLDER}/{file_name}"

    print(f"⬆  Uploading: {file_name}")
    try:
        s3.upload_file(file_path, BUCKET_NAME, s3_key)
        print(f"✅ Uploaded → s3://{BUCKET_NAME}/{s3_key}")
    except Exception as e:
        print(f"❌ Failed: {e}")

# ==============================
# STARTUP SYNC: UPLOAD MISSING FILES
# ==============================
def sync_existing_files():
    print("\n🔍 Scanning for unsynced .mp4 files...\n")
    existing_keys = get_existing_s3_keys()
    uploaded = 0

    for root, dirs, files in os.walk(WATCH_FOLDER):
        for filename in files:
            if not filename.lower().endswith(".mp4"):
                continue

            file_path = os.path.join(root, filename)

            if not os.path.isfile(file_path):
                continue

            s3_key = f"{S3_FOLDER}/{filename}"   # ✅ flat — just filename, no subfolders

            if s3_key not in existing_keys:
                print(f"⬆  Missing on S3: {filename}")
                upload_file(file_path)
                uploaded += 1
            else:
                print(f"⏭  Already on S3, skipping: {filename}")

    print(f"\n✅ Sync complete. {uploaded} new file(s) uploaded.\n")

# ==============================
# EVENT HANDLER (LIVE WATCHER)
# ==============================
class MP4UploadHandler(FileSystemEventHandler):
    def on_created(self, event):
        upload_file(event.src_path)  # upload_file() filters out everything that isn't a real .mp4 file

# ==============================
# MAIN
# ==============================
if __name__ == "__main__":
    sync_existing_files()

    event_handler = MP4UploadHandler()
    observer = Observer()
    observer.schedule(event_handler, WATCH_FOLDER, recursive=True)
    observer.start()
    print(f"👀 Watching: {WATCH_FOLDER}")
    print("Press CTRL+C to stop.\n")

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        observer.stop()
    observer.join()