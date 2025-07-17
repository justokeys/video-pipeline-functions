import os
import tempfile
import logging
from pathlib import Path
from typing import Tuple, Optional
import requests
from google.cloud import storage
from flask import Request, jsonify
import functions_framework

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Constants
RAW_BUCKET_NAME = "vflow-pipeline-to-upscale"
UPSCALED_BUCKET_NAME = "vflow-pipeline-upscaled"
DOWNLOAD_TIMEOUT = 300  # 5 minutes
CHUNK_SIZE = 8192
MAX_FILE_SIZE = 500 * 1024 * 1024  # 500MB limit


class VideoProcessingError(Exception):
    """Custom exception for video processing errors"""
    pass


def validate_request(request: Request) -> dict:
    """Validate and parse the incoming request"""
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
    """Download video from URL with proper error handling and validation"""
    try:
        logger.info(f"Starting download from: {source_url}")
        
        with requests.get(
            source_url, 
            stream=True, 
            timeout=DOWNLOAD_TIMEOUT,
            headers={'User-Agent': 'Video-Pipeline/1.0'}
        ) as response:
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
    """Upload file to Google Cloud Storage"""
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
    Placeholder for AI upscaling logic using Real-ESRGAN
    TODO: Implement actual upscaling logic
    """
    import shutil
    shutil.copy2(input_path, output_path)
    logger.info(f"Upscaling completed (placeholder): {input_path} -> {output_path}")


def trigger_next_function(video_url: str) -> None:
    """
    Trigger the next function in the pipeline (crop-video)
    TODO: Implement HTTP call to next function
    """
    logger.info(f"Would trigger crop-video function with: {video_url}")


@functions_framework.http
def upscale_video(request: Request):
    """
    HTTP-triggered function that downloads a video from a URL,
    upscales it using AI, and saves it to Google Cloud Storage.
    """
    temp_file_path = None
    upscaled_file_path = None
    
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
        
    except ValueError as e:
        logger.error(f"Validation error: {str(e)}")
        return jsonify({"status": "error", "message": f"Invalid request: {str(e)}"}), 400
        
    except VideoProcessingError as e:
        logger.error(f"Processing error: {str(e)}")
        return jsonify({"status": "error", "message": f"Processing failed: {str(e)}"}), 500
        
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