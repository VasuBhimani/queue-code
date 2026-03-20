import os
import boto3
import time
from botocore.exceptions import ClientError
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler

# --- CONFIG ---
WATCH_FOLDER = "/content/Videos"   # Local folder to watch
S3_BUCKET = "tvideo-gen-pune"
S3_PREFIX = "video/"           
AWS_REGION = "us-east-1"


STABLE_CHECK_INTERVAL = 2   # seconds between each size check
STABLE_REQUIRED_COUNT  = 3  # how many consecutive stable checks before uploading

# Optionally set credentials here or use ~/.aws/credentials / env vars
AWS_ACCESS_KEY = None
AWS_SECRET_KEY = None
# --------------

s3 = boto3.client(
    "s3",
    region_name=AWS_REGION,
    aws_access_key_id=AWS_ACCESS_KEY,
    aws_secret_access_key=AWS_SECRET_KEY,
)


def wait_until_file_stable(path: str) -> bool:
    """
    Poll the file size every STABLE_CHECK_INTERVAL seconds.
    Returns True once the size has been unchanged for STABLE_REQUIRED_COUNT
    consecutive checks, meaning the write is complete.
    Returns False if the file disappears while waiting.
    """
    stable_count = 0
    last_size = -1

    print(f"[WAIT]  Waiting for {os.path.basename(path)} to finish writing...")

    while stable_count < STABLE_REQUIRED_COUNT:
        if not os.path.exists(path):
            print(f"[WARN]  File disappeared while waiting: {path}")
            return False

        current_size = os.path.getsize(path)

        if current_size == last_size and current_size > 0:
            stable_count += 1
        else:
            stable_count = 0  # reset if size changed or file is still empty

        last_size = current_size
        time.sleep(STABLE_CHECK_INTERVAL)

    return True


def file_exists_on_s3(s3_key: str) -> bool:
    try:
        s3.head_object(Bucket=S3_BUCKET, Key=s3_key)
        return True
    except ClientError as e:
        if e.response["Error"]["Code"] == "404":
            return False
        raise


def upload_to_s3(local_path: str):
    if not wait_until_file_stable(local_path):
        return  # file vanished or failed stability check

    filename = os.path.basename(local_path)
    s3_key = S3_PREFIX + filename

    print(f"[CHECK] {filename} -> s3://{S3_BUCKET}/{s3_key}")

    if file_exists_on_s3(s3_key):
        print(f"[SKIP]  Already exists on S3: {s3_key}")
        return

    print(f"[UPLOAD] Uploading {filename} ...")
    try:
        s3.upload_file(local_path, S3_BUCKET, s3_key)
        print(f"[DONE]  Uploaded: s3://{S3_BUCKET}/{s3_key}")
    except Exception as e:
        print(f"[ERROR] Failed to upload {filename}: {e}")


class MP4Handler(FileSystemEventHandler):
    def on_created(self, event):
        # Ignore directories — only handle files
        if event.is_directory:
            return

        if event.src_path.lower().endswith(".mp4"):
            upload_to_s3(event.src_path)

    def on_moved(self, event):
        # Handles files moved/renamed into the watch folder
        if event.is_directory:
            return

        if event.dest_path.lower().endswith(".mp4"):
            upload_to_s3(event.dest_path)


if __name__ == "__main__":
    os.makedirs(WATCH_FOLDER, exist_ok=True)

    print(f"Watching folder: {os.path.abspath(WATCH_FOLDER)}")
    print(f"Uploading to:    s3://{S3_BUCKET}/{S3_PREFIX}")
    print("Press Ctrl+C to stop.\n")

    handler = MP4Handler()
    observer = Observer()
    observer.schedule(handler, WATCH_FOLDER, recursive=True)
    observer.start()

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        observer.stop()
        print("\nWatcher stopped.")

    observer.join()