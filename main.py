import boto3
import json
import base64
import os
import time
import uuid
import requests

QUEUE_URL = "https://sqs.us-east-1.amazonaws.com/448049811369/image-upload-queue"
REGION = "us-east-1"
BUCKET_NAME = "video-gen-pune"
STATIC_KEY = "matt.jpeg"

COMFY_API_URL = "http://54.172.178.65:3000"

# Validate required variables
if not QUEUE_URL:
    raise ValueError("❌ QUEUE_URL is required")

if not BUCKET_NAME:
    raise ValueError("❌ BUCKET_NAME is required")
# ----------------------------
# AWS CLIENTS
# ----------------------------
sqs = boto3.client("sqs", region_name=REGION)
s3 = boto3.client("s3", region_name=REGION)

# ----------------------------
# DOWNLOAD IMAGE FROM S3
# ----------------------------
def download_from_s3(bucket, key):
    response = s3.get_object(Bucket=bucket, Key=key)
    return response["Body"].read()

# ----------------------------
# UPLOAD IMAGE TO S3
# ----------------------------
def upload_to_s3(bucket, key, image_bytes):
    s3.put_object(
        Bucket=bucket,
        Key=key,
        Body=image_bytes,
        ContentType="image/png"
    )

# ----------------------------
# CONVERT S3 IMAGE TO BASE64
# ----------------------------
def get_base64_from_s3(bucket, key):
    image_bytes = download_from_s3(bucket, key)
    return base64.b64encode(image_bytes).decode("utf-8")

# ----------------------------
# BUILD COMFY PROMPT PAYLOAD
# ----------------------------
def build_prompt(base64_queue_image, base64_matt_image):
    prompt_id = str(uuid.uuid4())

    payload = {
        "prompt": {
            "60": {
                "inputs": {
                    "filename_prefix": f"{prompt_id}_ComfyUI",
                    "images": ["433:8", 0]
                },
                "class_type": "SaveImage",
                "_meta": {"title": "Save Image"}
            },
            "78": {
                "inputs": {
                    "image": base64_queue_image  # Dynamic user image (base64)
                },
                "class_type": "LoadImage",
                "_meta": {"title": "Load Image"}
            },
            "435": {
                "inputs": {
                    "value": (
                        "Transform this simple portrait into a professional AWS-themed conference photo booth image featuring two professionals. "
                        "Capture a natural business handshake moment, then finalize the pose with both standing beside each other, slightly angled 20 to 30 degrees toward the camera in a confident, composed stance. "
                        "Posture should be upright and relaxed, executive and event-ready, not stiff. Arms should rest naturally by their sides or near a badge, blazer cuff, or smartwatch. "
                        "Maintain calm, confident, approachable expressions and keep facial features unchanged.\n\n"
                        "Place them in front of a modern tech event backdrop with AWS-inspired orange and dark blue tones, abstract cloud icons, subtle server and network graphics, glowing data streams, and LED panels. "
                        "Include a clean, accurate \"AWS\" logo that is perfectly rendered, correctly spelled, proportionally aligned, not distorted, not warped, not cropped, and not altered in any way. "
                        "Keep the environment realistic and corporate without adding extra people.\n\n"
                        "Use soft professional event lighting, natural skin tones, cinematic depth of field, slight background blur, subtle rim lighting, realistic shadows, sharp focus, ultra-detailed high resolution, and premium tech expo photography style."
                    )
                },
                "class_type": "PrimitiveStringMultiline",
                "_meta": {"title": "Prompt"}
            },
            "439": {
                "inputs": {
                    "image": base64_matt_image  # Static matt image (base64)
                },
                "class_type": "LoadImage",
                "_meta": {"title": "Load Image"}
            },
            "433:75": {
                "inputs": {
                    "strength": 1,
                    "model": ["433:66", 0]
                },
                "class_type": "CFGNorm",
                "_meta": {"title": "CFGNorm"}
            },
            "433:110": {
                "inputs": {
                    "prompt": "",
                    "clip": ["433:437", 0],
                    "vae": ["433:39", 0],
                    "image1": ["78", 0],
                    "image2": ["439", 0]
                },
                "class_type": "TextEncodeQwenImageEditPlus",
                "_meta": {"title": "TextEncodeQwenImageEditPlus"}
            },
            "433:66": {
                "inputs": {
                    "shift": 3,
                    "model": ["433:89", 0]
                },
                "class_type": "ModelSamplingAuraFlow",
                "_meta": {"title": "ModelSamplingAuraFlow"}
            },
            "433:111": {
                "inputs": {
                    "prompt": ["435", 0],
                    "clip": ["433:437", 0],
                    "vae": ["433:39", 0],
                    "image1": ["78", 0],
                    "image2": ["439", 0]
                },
                "class_type": "TextEncodeQwenImageEditPlus",
                "_meta": {"title": "TextEncodeQwenImageEditPlus"}
            },
            "433:88": {
                "inputs": {
                    "pixels": ["78", 0],
                    "vae": ["433:39", 0]
                },
                "class_type": "VAEEncode",
                "_meta": {"title": "VAE Encode"}
            },
            "433:8": {
                "inputs": {
                    "samples": ["433:3", 0],
                    "vae": ["433:39", 0]
                },
                "class_type": "VAEDecode",
                "_meta": {"title": "VAE Decode"}
            },
            "433:38": {
                "inputs": {
                    "clip_name": "qwen_2.5_vl_7b_fp8_scaled.safetensors",
                    "type": "qwen_image",
                    "device": "default"
                },
                "class_type": "CLIPLoader",
                "_meta": {"title": "Load CLIP"}
            },
            "433:37": {
                "inputs": {
                    "unet_name": "qwen_image_edit_2509_fp8_e4m3fn.safetensors",
                    "weight_dtype": "default"
                },
                "class_type": "UNETLoader",
                "_meta": {"title": "Load Diffusion Model"}
            },
            "433:436": {
                "inputs": {
                    "unet_name": "Qwen-Image-Edit-2509-Q4_K_M.gguf"
                },
                "class_type": "UnetLoaderGGUF",
                "_meta": {"title": "Unet Loader (GGUF)"}
            },
            "433:117": {
                "inputs": {
                    "image": ["78", 0]
                },
                "class_type": "FluxKontextImageScale",
                "_meta": {"title": "FluxKontextImageScale"}
            },
            "433:3": {
                "inputs": {
                    "seed": int(uuid.uuid4().int % (2**53)),  # Random seed each run
                    "steps": 4,
                    "cfg": 1,
                    "sampler_name": "euler",
                    "scheduler": "simple",
                    "denoise": 1,
                    "model": ["433:75", 0],
                    "positive": ["433:111", 0],
                    "negative": ["433:110", 0],
                    "latent_image": ["433:88", 0]
                },
                "class_type": "KSampler",
                "_meta": {"title": "KSampler"}
            },
            "433:440": {
                "inputs": {
                    "unet_name": "Qwen-Image-Edit-2509-Q4_K_M.gguf"
                },
                "class_type": "UnetLoaderGGUF",
                "_meta": {"title": "Unet Loader (GGUF)"}
            },
            "433:89": {
                "inputs": {
                    "lora_name": "Qwen-Image-Edit-2509-Lightning-4steps-V1.0-bf16.safetensors",
                    "strength_model": 1,
                    "model": ["433:440", 0]
                },
                "class_type": "LoraLoaderModelOnly",
                "_meta": {"title": "Load LoRA"}
            },
            "433:437": {
                "inputs": {
                    "clip_name": "Qwen2.5-VL-7B-Instruct-Q4_K_M.gguf",
                    "type": "qwen_image"
                },
                "class_type": "CLIPLoaderGGUF",
                "_meta": {"title": "CLIPLoader (GGUF)"}
            },
            "433:39": {
                "inputs": {
                    "vae_name": "qwen_image_vae.safetensors"
                },
                "class_type": "VAELoader",
                "_meta": {"title": "Load VAE"}
            }
        }
    }

    return payload

# ----------------------------
# CALL COMFY API
# ----------------------------
def call_comfy_api(base64_queue_image, base64_matt_image):
    print("Calling ComfyUI /prompt API...")

    payload = build_prompt(base64_queue_image, base64_matt_image)

    response = requests.post(
        f"{COMFY_API_URL}/prompt",
        json=payload,
        headers={"Content-Type": "application/json"},
        timeout=300
    )

    if response.status_code not in (200, 202):
        raise Exception(f"ComfyUI API error {response.status_code}: {response.text}")

    result = response.json()

    # Extract base64 image from response
    images = result.get("images", [])
    if not images:
        raise Exception(f"No images returned from ComfyUI. Response: {result}")

    print(f"ComfyUI job completed. Stats: total_time={result.get('stats', {}).get('total_time')}ms")

    return images[0]  # base64 string of generated image

# ----------------------------
# WORKER LOOP
# ----------------------------
print("Worker started...")

while True:
    response = sqs.receive_message(
        QueueUrl=QUEUE_URL,
        MaxNumberOfMessages=1,
        WaitTimeSeconds=20,
        VisibilityTimeout=300
    )

    if "Messages" not in response:
        print("No job found...")
        continue

    message = response["Messages"][0]
    receipt_handle = message["ReceiptHandle"]
    body = json.loads(message["Body"])

    try:
        if "Records" in body:
            record = body["Records"][0]
            bucket = record["s3"]["bucket"]["name"]
            key = record["s3"]["object"]["key"]
        else:
            bucket = body["bucket"]
            key = body["key"]

        print(f"\nProcessing: {bucket}/{key}")

        # Extract original filename
        original_filename = os.path.basename(key)

        # -----------------------------------
        # 1️⃣ Dynamic Image (from queue)
        # -----------------------------------
        base64_queue_image = get_base64_from_s3(bucket, key)

        # -----------------------------------
        # 2️⃣ Static Image (matt.jpeg)
        # -----------------------------------
        base64_matt_image = get_base64_from_s3(BUCKET_NAME, STATIC_KEY)

        # -----------------------------------
        # 3️⃣ Call ComfyUI /prompt API
        # -----------------------------------
        generated_base64 = call_comfy_api(base64_queue_image, base64_matt_image)

        # -----------------------------------
        # 4️⃣ Convert back to bytes
        # -----------------------------------
        generated_bytes = base64.b64decode(generated_base64)

        # -----------------------------------
        # 5️⃣ Upload to genimage folder
        # -----------------------------------
        output_key = f"genimage/{original_filename}"
        upload_to_s3(bucket, output_key, generated_bytes)
        print(f"Uploaded generated image to: {output_key}")

        # -----------------------------------
        # 6️⃣ Delete SQS message
        # -----------------------------------
        sqs.delete_message(
            QueueUrl=QUEUE_URL,
            ReceiptHandle=receipt_handle
        )

        print("Job completed successfully ✅")

    except Exception as e:
        print("Job failed ❌:", e)
        print("Message will retry automatically")