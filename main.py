import os
import tempfile
import logging
from pathlib import Path
import requests
from google.cloud import storage
from flask import Request, jsonify
import functions_framework
import shutil

# --- NEW IMPORTS FOR THE AI MODEL ---
from realesrgan import RealESRGANer
from basicsr.archs.rrdbnet_arch import RRDBNet

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Constants
RAW_BUCKET_NAME = "vflow-pipeline-to-upscale" # Change to your bucket name
UPSCALED_BUCKET_NAME = "vflow-pipeline-upscaled" # Change to your bucket name
DOWNLOAD_TIMEOUT = 300
CHUNK_SIZE = 8192
MAX_FILE_SIZE = 500 * 1024 * 1024


class VideoProcessingError(Exception):
    """Custom exception for video processing errors"""
    pass


def validate_request(request: Request) -> dict:
    # ... (this function remains the same)
    if not request.is_json:
        raise ValueError("Request must be JSON")
    request_json = request.get_json()
    if not request_json or "sourceUrl" not in request_json:
        raise ValueError("Missing required 'sourceUrl' in request body")
    source_url = request_json["sourceUrl"]
    if not source_url.startswith(("http://", "https://")):
        raise ValueError("Invalid URL format")
    return request_json


def download_video(source_url: str, temp_file_path: str) -> None:
    # ... (this function remains the same)
    try:
        logger.info(f"Starting download from: {source_url}")
        with requests.get(source_url, stream=True, timeout=DOWNLOAD_TIMEOUT, headers={'User-Agent': 'Video-Pipeline/1.0'}) as response:
            response.raise_for_status()
            content_length = response.headers.get('content-length')
            if content_length and int(content_length) > MAX_FILE_SIZE:
                raise VideoProcessingError(f"File too large: {content_length} bytes")
            content_type = response.headers.get('content-type', '')
            if not content_type.startswith('video/'):
                logger.warning(f"Unexpected content type: {content_type}")
            downloaded_size = 0
            with open(temp_file_path, 'wb') as f:
                for chunk in response.iter_content(chunk_size=CHUNK_SIZE):
                    if chunk:
                        f.write(chunk)
                        downloaded_size += len(chunk)
                        if downloaded_size > MAX_FILE_SIZE:
                            raise VideoProcessingError("File size exceeds limit during download")
        logger.info(f"Successfully downloaded {downloaded_size} bytes to {temp_file_path}")
    except requests.exceptions.RequestException as e:
        raise VideoProcessingError(f"Failed to download video: {str(e)}")


def upload_to_gcs(file_path: str, bucket_name: str, blob_name: str) -> str:
    # ... (this function remains the same)
    try:
        storage_client = storage.Client()
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(blob_name)
        blob.upload_from_filename(file_path)
        logger.info(f"Successfully uploaded {blob_name} to {bucket_name}")
        return f"gs://{bucket_name}/{blob_name}"
    except Exception as e:
        raise VideoProcessingError(f"Failed to upload to GCS: {str(e)}")


def upscale_video_ai(input_path: str, output_path: str) -> None:
    """
    Performs AI upscaling using Real-ESRGAN.
    """
    logger.info("Initializing AI upscaling model...")
    
    # Define the Real-ESRGAN model.
    # The pre-trained weights will be downloaded automatically on first run.
    model = RRDBNet(num_in_ch=3, num_out_ch=3, num_feat=64, num_block=23, num_grow_ch=32, scale=4)
    upsampler = RealESRGANer(
        scale=4,
        model_path='https://github.com/xinntao/Real-ESRGAN/releases/download/v0.1.0/RealESRGAN_x4plus.pth',
        model=model,
        tile=0,
        tile_pad=10,
        pre_pad=0,
        half=True, # Use half-precision for better performance on GPUs
        gpu_id=None # Set to 0 if you have a dedicated GPU, None for CPU
    )

    logger.info(f"Starting AI upscaling for {Path(input_path).name}...")
    
    # Enhance() function performs the upscaling on the video file
    # Note: This is a computationally intensive step.
    # The 'outscale' parameter can be adjusted if needed, but 4 matches the model.
    upsampler.enhance(input_path, outscale=4, output_path=output_path)
    
    logger.info(f"Upscaling completed. Output saved to: {output_path}")


def trigger_next_function(video_url: str) -> None:
    # ... (this placeholder function remains the same for now)
    logger.info(f"Would trigger crop-video function with: {video_url}")


@functions_framework.http
def upscale_video(request: Request):
    # ... (the main function's structure remains the same)
    temp_file_path, upscaled_file_path = None, None
    try:
        request_json = validate_request(request)
        source_url = request_json["sourceUrl"]
        
        original_filename = Path(source_url).name or "video.mp4"
        base_name, extension = Path(original_filename).stem, Path(original_filename).suffix
        upscaled_filename = f"{base_name}_upscaled{extension}"
        
        logger.info(f"Processing video: {original_filename}")
        
        with tempfile.NamedTemporaryFile(delete=False, suffix=extension) as temp_file:
            temp_file_path = temp_file.name
        with tempfile.NamedTemporaryFile(delete=False, suffix=extension) as upscaled_file:
            upscaled_file_path = upscaled_file.name
        
        download_video(source_url, temp_file_path)
        
        raw_gcs_url = upload_to_gcs(temp_file_path, RAW_BUCKET_NAME, original_filename)
        
        # This now calls the REAL AI function
        upscale_video_ai(temp_file_path, upscaled_file_path)
        
        upscaled_gcs_url = upload_to_gcs(upscaled_file_path, UPSCALED_BUCKET_NAME, upscaled_filename)
        
        trigger_next_function(upscaled_gcs_url)
        
        return jsonify({
            "status": "success",
            "message": "Video upscaling completed successfully",
            "original_file": raw_gcs_url,
            "upscaled_file": upscaled_gcs_url,
            "filename": upscaled_filename
        }), 200
        
    except (ValueError, VideoProcessingError) as e:
        logger.error(f"Processing error: {str(e)}")
        return jsonify({"status": "error", "message": f"{str(e)}"}), 400
        
    except Exception as e:
        logger.error(f"Unexpected error: {str(e)}")
        return jsonify({"status": "error", "message": "Internal server error"}), 500
        
    finally:
        for file_path in [temp_file_path, upscaled_file_path]:
            if file_path and os.path.exists(file_path):
                try:
                    os.unlink(file_path)
                    logger.info(f"Cleaned up temporary file: {file_path}")
                except OSError as e:
                    logger.warning(f"Failed to clean up {file_path}: {str(e)}")