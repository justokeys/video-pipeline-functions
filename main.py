import functions_framework
from google.cloud import storage
import os
import logging
import mimetypes
from pathlib import Path

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Supported video file extensions
SUPPORTED_VIDEO_EXTENSIONS = {'.mp4', '.avi', '.mov', '.mkv', '.wmv', '.flv', '.webm', '.m4v'}
MAX_FILE_SIZE_MB = 2000 # Maximum file size in MB

# The decorator that registers this function to be triggered by a cloud event
@functions_framework.cloud_event
def upscale_video(cloud_event): # FIX: Changed function name from upscale-video to upscale_video
    """
    This function is triggered when a file is uploaded to a Cloud Storage bucket.
    It downloads the file to a temporary location for processing.
    """
    temp_file_path = None
    
    try:
        # Get the file details from the event payload
        data = cloud_event.data
        bucket_name = data["bucket"]
        file_name = data["name"]

        logger.info(f"Processing file: {file_name} from bucket: {bucket_name}")

        # Validate file extension
        file_extension = Path(file_name).suffix.lower()
        if file_extension not in SUPPORTED_VIDEO_EXTENSIONS:
            logger.warning(f"Unsupported file type: {file_extension}. Skipping processing.")
            return f"Skipped: Unsupported file type {file_extension}"

        # Initialize the Storage client to interact with Google Cloud Storage
        storage_client = storage.Client()
        
        # Get the specific bucket that triggered the event
        source_bucket = storage_client.bucket(bucket_name)
        
        # Get the specific file (blob) from the bucket
        source_blob = source_bucket.blob(file_name)
        
        # Check file size before downloading for efficiency
        file_size_mb = source_blob.size / (1024 * 1024)
        if file_size_mb > MAX_FILE_SIZE_MB:
            logger.warning(f"File too large: {file_size_mb:.2f}MB. Maximum allowed: {MAX_FILE_SIZE_MB}MB")
            return f"Error: File too large ({file_size_mb:.2f}MB). Maximum allowed: {MAX_FILE_SIZE_MB}MB"

        # Validate MIME type to ensure it's a video
        mime_type, _ = mimetypes.guess_type(file_name)
        if mime_type and not mime_type.startswith('video/'):
            logger.warning(f"Invalid MIME type: {mime_type}. Expected video file.")
            return f"Error: Invalid file type. Expected video file, got {mime_type}"
        
        # Define a temporary path inside the Cloud Function's writable directory
        temp_file_path = f"/tmp/{file_name}"

        # Download the file from the bucket to our temporary path
        logger.info(f"Downloading {file_name} ({file_size_mb:.2f}MB) to {temp_file_path}")
        source_blob.download_to_filename(temp_file_path)

        logger.info(f"Successfully downloaded {file_name}")

        # Verify the downloaded file exists and has content
        if not os.path.exists(temp_file_path):
            raise FileNotFoundError(f"Downloaded file not found at {temp_file_path}")
        
        downloaded_size = os.path.getsize(temp_file_path)
        if downloaded_size == 0:
            raise ValueError(f"Downloaded file is empty: {temp_file_path}")

        logger.info(f"File validation successful. Size: {downloaded_size} bytes")

        # --- AI upscaling logic will go here ---

        logger.info("Processing completed successfully")
        return "Function finished successfully."

    except Exception as e:
        logger.error(f"Error processing file {file_name if 'file_name' in locals() else 'unknown'}: {str(e)}")
        return f"Error: {str(e)}"
    
    finally:
        # Clean up the temporary file to free up memory
        if temp_file_path and os.path.exists(temp_file_path):
            try:
                os.remove(temp_file_path)
                logger.info(f"Cleaned up temporary file: {temp_file_path}")
            except Exception as cleanup_error:
                logger.warning(f"Failed to clean up temporary file {temp_file_path}: {str(cleanup_error)}")