import boto3
import json
import base64
import os
import time
import requests
import copy

QUEUE_URL = "https://sqs.us-east-1.amazonaws.com/448049811369/processed-image-queue"
AWS_REGION = "us-east-1"
COMFY_API_URL = "http://54.172.178.65:3000/prompt"

# Validate required ones
if not QUEUE_URL:
    raise ValueError("❌ QUEUE_URL is required")

if not COMFY_API_URL:
    raise ValueError("❌ COMFY_API_URL is required")


sqs = boto3.client("sqs", region_name=AWS_REGION)
s3 = boto3.client("s3", region_name=AWS_REGION)

# ✅ Base ComfyUI Prompt Payload
COMFY_PROMPT_PAYLOAD = {
    "prompt": {
        "6": {
            "inputs": {
                "text": "Create a short, high-end cinematic conference-style intro animation using the provided simple portrait photo as the first frame and the AWS conference handshake image as the final frame reference.\n\nThe first frame is a clean, neutral portrait of a single person against a plain background. The final frame shows the same person standing at a modern AWS-themed tech conference booth, professionally dressed, shaking hands with another corporate professional in front of a large AWS backdrop with orange and dark blue lighting, cloud icons, and network graphics.\n\nAnimation Structure\n\nStart exactly from the original portrait photo with no movement for the first 0.5 seconds.\n\nIntroduce very subtle, natural breathing motion in the chest and shoulders.\n\nAround 1.5\u20132 seconds, begin a smooth cinematic morph transition where:\n\t\u2022\tThe plain background gradually transforms into a modern AWS conference booth environment.\n\t\u2022\tProfessional attire subtly transitions into a blazer and event-ready look.\n\t\u2022\tAmbient orange and blue lighting slowly appears around the subject.\n\t\u2022\tA second professional person gradually materializes into frame from the side in a natural way.\n\t\u2022\tThe main subject slightly shifts posture as if preparing for a greeting.\n\nMid Transition (Very Natural & Corporate)\n\t\u2022\tThe subject subtly turns their upper body a few degrees.\n\t\u2022\tA gentle shoulder correction and micro chin lift.\n\t\u2022\tOne natural blink.\n\t\u2022\tVery slight confident micro-smile.\n\t\u2022\tThe main subject raises their hand in a natural wave to acknowledge the arriving professional before moving into the handshake.\n\t\u2022\tThe second person responds with a subtle wave and steps into final position.\n\t\u2022\tOne smooth, confident step forward to initiate the handshake.\n\t\u2022\tThe handshake begins forming naturally during the morph.\n\nNo sudden cuts. No teleporting effect. Everything must feel like a premium cinematic transformation.\n\nFinal Frame (Handshake Scene)\n\nThe animation completes with both individuals standing beside each other, shaking hands confidently.\n\nBoth are facing slightly toward the camera in a professional conference pose.\n\nThe AWS logo must remain clean, sharp, proportionally correct, and not distorted in any way.\n\nBackground includes:\n\t\u2022\tGlowing orange and blue LED ambiance.\n\t\u2022\tSubtle animated cloud and server graphics.\n\t\u2022\tSoft light sweeps across panels.\n\t\u2022\tGentle pulsing network lines.\n\nCinematic Camera & Effects\n\t\u2022\tVery slow smooth camera push-in throughout the animation.\n\t\u2022\tShallow depth of field (professional DSLR look).\n\t\u2022\tSoft cinematic motion blur at 24fps.\n\t\u2022\tSubtle rack focus moment during transition.\n\t\u2022\tRealistic event lighting and natural skin tones.\n\t\u2022\tNo CGI or artificial plastic look.\n\nEnding\n\nThe final frame must match the provided handshake image composition.\n\nHold the final handshake pose for 0.5\u20131 second.\n\nEnd with a clean cinematic hold or subtle fade.\n\nStyle Keywords\n\nUltra-realistic, premium corporate event, AWS conference booth, smooth morph transition, professional wave and handshake, cinematic lighting, realistic human motion, high-end brand promo, seamless transformation, shallow depth of field.\n\nTransition style must be smooth morph-based blending from first frame to last frame.",
                "clip": ["38", 0]
            },
            "class_type": "CLIPTextEncode",
            "_meta": {"title": "CLIP Text Encode (Positive Prompt)"}
        },
        "7": {
            "inputs": {
                "text": "cartoon, over-animated, exaggerated gestures, large facial movement, dramatic expression, head snapping, jitter, morphing face distortion, identity change, extra fingers, extra limbs, body warping, background melting, heavy motion blur, oversharpen, oversaturated, artificial lighting, plastic skin, CGI look, unstable frame, camera shake, glitch, flicker",
                "clip": ["38", 0]
            },
            "class_type": "CLIPTextEncode",
            "_meta": {"title": "CLIP Text Encode (Negative Prompt)"}
        },
        "8": {
            "inputs": {
                "samples": ["58", 0],
                "vae": ["39", 0]
            },
            "class_type": "VAEDecode",
            "_meta": {"title": "VAE Decode"}
        },
        "37": {
            "inputs": {
                "unet_name": "wan2.2_i2v_high_noise_14B_fp8_scaled.safetensors",
                "weight_dtype": "default"
            },
            "class_type": "UNETLoader",
            "_meta": {"title": "Load Diffusion Model"}
        },
        "38": {
            "inputs": {
                "clip_name": "umt5_xxl_fp8_e4m3fn_scaled.safetensors",
                "type": "wan",
                "device": "default"
            },
            "class_type": "CLIPLoader",
            "_meta": {"title": "Load CLIP"}
        },
        "39": {
            "inputs": {"vae_name": "wan_2.1_vae.safetensors"},
            "class_type": "VAELoader",
            "_meta": {"title": "Load VAE"}
        },
        "54": {
            "inputs": {
                "shift": 5,
                "model": ["91", 0]
            },
            "class_type": "ModelSamplingSD3",
            "_meta": {"title": "ModelSamplingSD3"}
        },
        "55": {
            "inputs": {
                "shift": 5,
                "model": ["92", 0]
            },
            "class_type": "ModelSamplingSD3",
            "_meta": {"title": "ModelSamplingSD3"}
        },
        "56": {
            "inputs": {
                "unet_name": "wan2.2_i2v_low_noise_14B_fp8_scaled.safetensors",
                "weight_dtype": "default"
            },
            "class_type": "UNETLoader",
            "_meta": {"title": "Load Diffusion Model"}
        },
        "57": {
            "inputs": {
                "add_noise": "enable",
                "noise_seed": 1028868256083889,
                "steps": 4,
                "cfg": 1,
                "sampler_name": "euler",
                "scheduler": "simple",
                "start_at_step": 0,
                "end_at_step": 2,
                "return_with_leftover_noise": "enable",
                "model": ["54", 0],
                "positive": ["67", 0],
                "negative": ["67", 1],
                "latent_image": ["67", 2]
            },
            "class_type": "KSamplerAdvanced",
            "_meta": {"title": "KSampler (Advanced)"}
        },
        "58": {
            "inputs": {
                "add_noise": "disable",
                "noise_seed": 0,
                "steps": 4,
                "cfg": 1,
                "sampler_name": "euler",
                "scheduler": "simple",
                "start_at_step": 2,
                "end_at_step": 10000,
                "return_with_leftover_noise": "disable",
                "model": ["55", 0],
                "positive": ["67", 0],
                "negative": ["67", 1],
                "latent_image": ["57", 0]
            },
            "class_type": "KSamplerAdvanced",
            "_meta": {"title": "KSampler (Advanced)"}
        },
        "60": {
            "inputs": {
                "fps": 16,
                "images": ["8", 0]
            },
            "class_type": "CreateVideo",
            "_meta": {"title": "Create Video"}
        },
        "61": {
            "inputs": {
                "filename_prefix": "video/{filename prefix}__",
                "format": "auto",
                "codec": "auto",
                "video": ["60", 0]
            },
            "class_type": "SaveVideo",
            "_meta": {"title": "Save Video"}
        },
        "62": {
            "inputs": {
                "image": "{generated image}"   # Placeholder — replaced at runtime with GEN image
            },
            "class_type": "LoadImage",
            "_meta": {"title": "Load Image"}
        },
        "67": {
            "inputs": {
                "width": 1024,
                "height": 1536,
                "length": 97,
                "batch_size": 1,
                "positive": ["6", 0],
                "negative": ["7", 0],
                "vae": ["39", 0],
                "start_image": ["68", 0],
                "end_image": ["62", 0]
            },
            "class_type": "WanFirstLastFrameToVideo",
            "_meta": {"title": "WanFirstLastFrameToVideo"}
        },
        "68": {
            "inputs": {
                "image": "{captured image}"    # Placeholder — replaced at runtime with RAW image
            },
            "class_type": "LoadImage",
            "_meta": {"title": "Load Image"}
        },
        "91": {
            "inputs": {
                "lora_name": "wan2.2_i2v_lightx2v_4steps_lora_v1_high_noise.safetensors",
                "strength_model": 1,
                "model": ["37", 0]
            },
            "class_type": "LoraLoaderModelOnly",
            "_meta": {"title": "Load LoRA"}
        },
        "92": {
            "inputs": {
                "lora_name": "wan2.2_i2v_lightx2v_4steps_lora_v1_low_noise.safetensors",
                "strength_model": 1,
                "model": ["56", 0]
            },
            "class_type": "LoraLoaderModelOnly",
            "_meta": {"title": "Load LoRA"}
        }
    }
}


# ✅ Download from S3
def download_from_s3(bucket, key):
    response = s3.get_object(Bucket=bucket, Key=key)
    return response["Body"].read()


# ✅ Upload video to S3  [COMMENTED OUT — video is saved server-side by ComfyUI]
# def upload_video_to_s3(bucket, key, video_bytes):
#     s3.put_object(
#         Bucket=bucket,
#         Key=key,
#         Body=video_bytes,
#         ContentType="video/mp4"
#     )


# ✅ API Function — calls ComfyUI /prompt with both images injected into the payload
def generate_video_from_api(base64_gen_image, base64_raw_image, filename_prefix):
    """
    Submits a ComfyUI /prompt job.
    - Node 68 (start_image / captured image) = RAW image (first frame)
    - Node 62 (end_image   / generated image) = GEN image (last frame)
    Video is saved server-side by ComfyUI's SaveVideo node — no video bytes returned.
    """
    print("Calling ComfyUI /prompt API with 2 images...")

    # Deep copy payload so the base template is never mutated
    payload = copy.deepcopy(COMFY_PROMPT_PAYLOAD)

    # ✅ Inject RAW image as the start frame (node 68)
    payload["prompt"]["68"]["inputs"]["image"] = base64_raw_image

    # ✅ Inject GEN image as the end frame (node 62)
    payload["prompt"]["62"]["inputs"]["image"] = base64_gen_image

    # ✅ Set output filename prefix using the original file's name
    payload["prompt"]["61"]["inputs"]["filename_prefix"] = f"video/{filename_prefix}"

    response = requests.post(
        f"{COMFY_API_URL}/prompt",
        json=payload,
        headers={"Content-Type": "application/json"},
        timeout=600
    )

    response.raise_for_status()
    result = response.json()

    print("ComfyUI job submitted. Response:", json.dumps(result, indent=2))
    return result


# 🔥 Worker Loop
while True:
    try:
        response = sqs.receive_message(
            QueueUrl=QUEUE_URL,
            MaxNumberOfMessages=1,
            WaitTimeSeconds=20,
            VisibilityTimeout=600
        )

        if "Messages" not in response:
            print("No job found...")
            continue

        message = response["Messages"][0]
        receipt_handle = message["ReceiptHandle"]
        print("RAW MESSAGE:", message["Body"])

        body = json.loads(message["Body"])

        # ✅ Handle both S3 event format and custom format
        if "Records" in body:
            record = body["Records"][0]
            bucket = record["s3"]["bucket"]["name"]
            key = record["s3"]["object"]["key"]
        else:
            bucket = body["bucket"]
            key = body["key"]

        print(f"Processing: {bucket}/{key}")

        # ✅ Extract filename
        original_filename = os.path.basename(key)
        filename_without_ext = os.path.splitext(original_filename)[0]

        # ----------------------------
        # ✅ 1️⃣ Download GEN Image
        # ----------------------------
        print("Downloading GEN image...")
        gen_image_bytes = download_from_s3(bucket, key)
        base64_gen_image = base64.b64encode(gen_image_bytes).decode("utf-8")

        # ----------------------------
        # ✅ 2️⃣ Download RAW Image
        # ----------------------------
        raw_key = f"raw/{original_filename}"
        print(f"Downloading RAW image: {bucket}/{raw_key}")

        raw_image_bytes = download_from_s3(bucket, raw_key)
        base64_raw_image = base64.b64encode(raw_image_bytes).decode("utf-8")

        # ----------------------------
        # ✅ 3️⃣ Call ComfyUI /prompt API
        # ----------------------------
        api_result = generate_video_from_api(
            base64_gen_image,
            base64_raw_image,
            filename_prefix=filename_without_ext
        )

        # ----------------------------
        # ✅ 4️⃣ Video saving is handled server-side by ComfyUI's SaveVideo node
        #        — no video bytes are returned in the API response
        # ----------------------------

        # ----------------------------
        # ✅ 5️⃣  [COMMENTED OUT] — Video upload to S3 not needed;
        #         ComfyUI saves directly to its output folder
        # ----------------------------
        # generated_video_bytes = base64.b64decode(generated_video_base64)
        # output_key = f"videos/{filename_without_ext}.mp4"
        # upload_video_to_s3(bucket, output_key, generated_video_bytes)
        # print(f"Uploaded video to: {output_key}")

        # ----------------------------
        # ✅ 6️⃣ Delete SQS Message
        # ----------------------------
        sqs.delete_message(
            QueueUrl=QUEUE_URL,
            ReceiptHandle=receipt_handle
        )

        print("Job completed successfully\n")

    except Exception as e:
        print("Job failed:", str(e))
        print("Message will retry automatically\n")
        time.sleep(5)