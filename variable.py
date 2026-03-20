WATCH_FOLDER = os.getenv("WATCH_FOLDER", "/home/ubuntu/ComfyUI/output")
BUCKET_NAME = os.getenv("BUCKET_NAME")
S3_FOLDER = os.getenv("S3_FOLDER", "videos")
AWS_REGION = os.getenv("AWS_REGION")


QUEUE_URL = "https://sqs.ap-south-1.amazonaws.com/448049811369/image-queue"
REGION = "ap-south-1"
COMFY_API_URL = "http://100.49.30.221:3000" 




export BUCKET_NAME=ai-event-processing-bucket-vasu
export S3_FOLDER=videos
export AWS_REGION=ap-south-1
export WATCH_FOLDER=/home/ubuntu/ComfyUI/output

export VIDEO_QUEUE_URL=https://sqs.ap-south-1.amazonaws.com/448049811369/video-upload-queue
export COMFY_API_URL=http://localhost:3000

export IMAGE_QUEUE_URL=https://sqs.ap-south-1.amazonaws.com/448049811369/image-upload-queue
export STATIC_KEY=matt.jpeg



nano ~/.bashrc
source ~/.bashrc