from flask import Blueprint, request, jsonify
import yt_dlp
import os
from config import get_storage_provider
import time
from retrying import retry
from typing import Dict, Any, Optional
import logging
import json
import requests

# Configure logging
logging.basicConfig(
    level=logging.INFO, 
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

v1_media_download_ytdlp_bp = Blueprint('v1_media_download_ytdlp', __name__)

# Constants with environment variable fallback and validation
DEFAULT_DOWNLOAD_PATH = os.path.join(
    os.getenv('DOWNLOAD_PATH', '/tmp/downloads'), 
    'yt-dlp'
)
DEFAULT_TIMEOUT = int(os.getenv('DOWNLOAD_TIMEOUT', 300))
DEFAULT_REQUEST_TIMEOUT = int(os.getenv('REQUEST_TIMEOUT', 60))
MAX_RETRIES = int(os.getenv('WEBHOOK_MAX_RETRIES', 3))

# Ensure download directory exists
os.makedirs(DEFAULT_DOWNLOAD_PATH, exist_ok=True)

# Allowed YT-DLP options with strict validation
ALLOWED_OPTIONS = {
    'format', 'format_sort', 'format_sort_force', 'video_multistreams', 
    'audio_multistreams', 'prefer_free_formats', 'check_formats',
    'check_all_formats', 'prefer_insecure', 'playlist_items', 
    'min_filesize', 'max_filesize', 'date', 'datebefore', 'dateafter', 
    'match_filter', 'no_playlist', 'yes_playlist', 'age_limit', 
    'download_archive', 'break_on_existing', 'break_on_reject',
    'skip_playlist_after_errors', 'limit_rate', 'retries', 
    'fragment_retries', 'skip_unavailable_fragments', 'keep_fragments', 
    'buffer_size', 'resize_buffer', 'http_chunk_size', 'playlist_reverse', 
    'playlist_random', 'xattr_set_filesize', 'hls_use_mpegts', 
    'download_sections', 'username', 'password', 'videopassword', 
    'ap_mso', 'ap_username', 'ap_password', 'client_certificate', 
    'client_certificate_key', 'client_certificate_password', 
    'extract_audio', 'audio_format', 'audio_quality', 'remux_video',
    'recode_video', 'postprocessor_args', 'keep_video', 'no_keep_video',
    'post_overwrites', 'embed_subs', 'embed_thumbnail', 'embed_metadata',
    'embed_chapters', 'embed_info_json', 'parse_metadata',
    'write_description', 'write_info_json', 'write_thumbnail',
    'write_all_thumbnails', 'sponsorblock_mark', 'sponsorblock_remove', 
    'sponsorblock_chapter_title', 'sponsorblock_api', 
    'output_na_placeholder', 'restrict_filenames', 'no_overwrites',
    'continue', 'part', 'mtime', 'write_comments'
}

def validate_options(opts: Dict[str, Any]) -> None:
    """Validate provided options against allowed settings"""
    invalid_opts = set(opts.keys()) - ALLOWED_OPTIONS
    if invalid_opts:
        raise ValueError(f"Invalid options provided: {invalid_opts}")

def send_webhook_with_retry(webhook_url: str, payload: Dict[str, Any]) -> None:
    """Send webhook with exponential backoff and retry"""
    for attempt in range(MAX_RETRIES):
        try:
            response = requests.post(webhook_url, json=payload, timeout=10)
            response.raise_for_status()
            return
        except Exception as e:
            logger.warning(f"Webhook attempt {attempt+1} failed: {str(e)}")
            if attempt < MAX_RETRIES - 1:
                time.sleep(2 ** attempt)  # Exponential backoff
            else:
                logger.error(f"Final webhook attempt failed for {webhook_url}")

class ProgressHook:
    """Track download progress with comprehensive metadata"""
    def __init__(self):
        self.start_time = time.time()
        self.downloaded_bytes = 0
        self.status = {}

    def __call__(self, d: Dict[str, Any]) -> None:
        """Update progress status based on download state"""
        if d['status'] == 'downloading':
            self.downloaded_bytes = d.get('downloaded_bytes', 0)
            self.status = {
                'status': 'downloading',
                'downloaded_bytes': self.downloaded_bytes,
                'total_bytes': d.get('total_bytes_estimate', 0),
                'speed': d.get('speed', 0),
                'eta': d.get('eta', 0),
                'elapsed': time.time() - self.start_time
            }
        elif d['status'] == 'finished':
            self.status = {
                'status': 'finished',
                'elapsed': time.time() - self.start_time
            }
        else:
            self.status = {
                'status': d.get('status', 'unknown'),
                'error': str(d.get('error', 'Unspecified error'))
            }

@v1_media_download_ytdlp_bp.route('/', methods=['POST'])
def download_video():
    """Download video with comprehensive error handling and logging"""
    start_time = time.time()
    
    # Validate request
    data = request.json
    if not data or 'url' not in data:
        logger.error("Invalid request: Missing URL")
        return jsonify({
            "error": "URL is required in request body",
            "status": "error"
        }), 400

    url = data['url']
    options = data.get('options', {})
    webhook_url = data.get('webhook_url')
    referer = data.get('referer', '')

    # Validate options
    try:
        validate_options(options)
    except ValueError as e:
        logger.error(f"Option validation failed: {str(e)}")
        return jsonify({
            "error": str(e),
            "status": "error"
        }), 400

    # Check request timeout
    if time.time() - start_time > DEFAULT_REQUEST_TIMEOUT:
        logger.warning("Request timeout occurred")
        return jsonify({
            "error": "Request timeout",
            "status": "error"
        }), 408

    # Progress tracking
    progress_hook = ProgressHook()

    # Configure yt-dlp options
    ydl_opts = {
        'format': options.get('format', 'bestvideo[ext=mp4]+bestaudio[ext=m4a]/best[ext=mp4]/best'),
        'outtmpl': os.path.join(DEFAULT_DOWNLOAD_PATH, '%(title)s.%(ext)s'),
        'quiet': options.get('quiet', False),
        'no_warnings': options.get('no_warnings', False),
        'progress_hooks': [progress_hook],
        'http_headers': {'Referer': referer},
        'ffmpeg_location': '/usr/bin',
        'socket_timeout': DEFAULT_TIMEOUT,
        'retries': options.get('retries', 3),
        **{k: v for k, v in options.items() if k in ALLOWED_OPTIONS}
    }

    try:
        # Initialize storage provider
        storage_provider = get_storage_provider()
    except Exception as e:
        logger.error(f"Storage provider initialization failed: {str(e)}")
        return jsonify({
            "error": f"Storage provider initialization failed: {str(e)}",
            "status": "error"
        }), 500

    video_path = None
    try:
        with yt_dlp.YoutubeDL(ydl_opts) as ydl:
            # Extract info and download
            info = ydl.extract_info(url, download=True)
            video_path = os.path.join(DEFAULT_DOWNLOAD_PATH, f"{info['title']}.{info['ext']}")
            
            # Upload to cloud storage
            video_url = storage_provider.upload_file(video_path)

            # Prepare response data
            response_data = {
                "title": info.get('title', 'Unknown'),
                "url": video_url,
                "format": info.get('format', ''),
                "status": "success",
                "timestamp": time.time(),
                "processing_time": time.time() - start_time,
                "download_stats": progress_hook.status,
                "extractor": info.get('extractor', ''),
                "duration": info.get('duration'),
                "view_count": info.get('view_count'),
                "like_count": info.get('like_count'),
                "upload_date": info.get('upload_date')
            }

            # Send webhook if provided
            if webhook_url:
                send_webhook_with_retry(webhook_url, response_data)

            return jsonify(response_data), 200

    except Exception as e:
        logger.error(f"Download/processing failed: {str(e)}")
        return jsonify({
            "error": f"Processing failed: {str(e)}",
            "status": "error"
        }), 500
    
    finally:
        # Cleanup downloaded file
        if video_path and os.path.exists(video_path):
            try:
                os.remove(video_path)
            except Exception as cleanup_error:
                logger.error(f"Cleanup failed: {str(cleanup_error)}")
