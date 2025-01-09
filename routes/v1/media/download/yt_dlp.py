from flask import Blueprint, request, jsonify
import yt_dlp
import os
from config import get_storage_provider
import time
from retrying import retry
from typing import Dict, Any, Optional
import logging
import json

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

v1_media_download_ytdlp_bp = Blueprint('v1_media_download_ytdlp', __name__)

# Constants
DEFAULT_DOWNLOAD_PATH = os.getenv('DOWNLOAD_PATH', '/tmp/downloads')
DEFAULT_TIMEOUT = int(os.getenv('DOWNLOAD_TIMEOUT', 300))
DEFAULT_REQUEST_TIMEOUT = int(os.getenv('REQUEST_TIMEOUT', 60))

# Allowed YT-DLP options that can be passed through the API
ALLOWED_OPTIONS = {
    # Format Selection
    'format', 'format_sort', 'format_sort_force', 'video_multistreams', 
    'audio_multistreams', 'prefer_free_formats', 'check_formats',
    'check_all_formats', 'prefer_insecure',
    
    # Video Selection
    'playlist_items', 'min_filesize', 'max_filesize', 'date', 'datebefore',
    'dateafter', 'match_filter', 'no_playlist', 'yes_playlist', 'age_limit',
    'download_archive', 'break_on_existing', 'break_on_reject',
    'skip_playlist_after_errors',
    
    # Download Options
    'limit_rate', 'retries', 'fragment_retries', 'skip_unavailable_fragments',
    'keep_fragments', 'buffer_size', 'resize_buffer', 'http_chunk_size',
    'playlist_reverse', 'playlist_random', 'xattr_set_filesize',
    'hls_use_mpegts', 'download_sections',
    
    # Authentication
    'username', 'password', 'videopassword', 'ap_mso', 'ap_username',
    'ap_password', 'client_certificate', 'client_certificate_key',
    'client_certificate_password',
    
    # Post-Processing
    'extract_audio', 'audio_format', 'audio_quality', 'remux_video',
    'recode_video', 'postprocessor_args', 'keep_video', 'no_keep_video',
    'post_overwrites', 'embed_subs', 'embed_thumbnail', 'embed_metadata',
    'embed_chapters', 'embed_info_json', 'parse_metadata',
    'write_description', 'write_info_json', 'write_thumbnail',
    'write_all_thumbnails',
    
    # SponsorBlock
    'sponsorblock_mark', 'sponsorblock_remove', 'sponsorblock_chapter_title',
    'sponsorblock_api',
    
    # Output Template
    'output_na_placeholder', 'restrict_filenames', 'no_overwrites',
    'continue', 'part', 'mtime', 'write_comments'
}

def validate_options(opts: Dict[str, Any]) -> None:
    """Validate the provided options against allowed settings."""
    invalid_opts = set(opts.keys()) - ALLOWED_OPTIONS
    if invalid_opts:
        raise ValueError(f"Invalid options provided: {invalid_opts}")

def retry_if_io_error(exception: Exception) -> bool:
    """Determine if the error is worth retrying."""
    return isinstance(exception, (IOError, OSError))

@retry(retry_on_exception=retry_if_io_error, stop_max_attempt_number=3, wait_fixed=2000)
def upload_to_storage(storage_provider: Any, video_path: str) -> str:
    """Upload file to storage with retry mechanism."""
    return storage_provider.upload_file(video_path)

class ProgressHook:
    def __init__(self):
        self.start_time = time.time()
        self.downloaded_bytes = 0
        self.status = {}

    def __call__(self, d: Dict[str, Any]) -> None:
        if d['status'] == 'downloading':
            self.downloaded_bytes = d.get('downloaded_bytes', 0)
            self.status = {
                'status': 'downloading',
                'downloaded_bytes': self.downloaded_bytes,
                'total_bytes': d.get('total_bytes'),
                'speed': d.get('speed'),
                'eta': d.get('eta'),
                'elapsed': time.time() - self.start_time
            }
        elif d['status'] == 'finished':
            self.status = {
                'status': 'finished',
                'elapsed': time.time() - self.start_time
            }
        elif d['status'] == 'error':
            self.status = {
                'status': 'error',
                'error': str(d.get('error', 'Unknown error'))
            }

@v1_media_download_ytdlp_bp.route('/v1/media/download/yt-dlp', methods=['POST'])
def download_video():
    """
    Download video using yt-dlp with customizable options.
    
    Expected JSON payload:
    {
        "url": "required_video_url",
        "options": {
            "format": "bestvideo+bestaudio/best",
            ... other yt-dlp options ...
        },
        "webhook_url": "optional_webhook_url",
        "referer": "optional_referer"
    }
    """
    start_time = time.time()
    
    try:
        # Initialize storage provider
        storage_provider = get_storage_provider()
    except Exception as e:
        logger.error(f"Storage provider initialization failed: {str(e)}")
        return jsonify({
            "error": f"Storage provider initialization failed: {str(e)}",
            "status": "error",
            "timestamp": time.time()
        }), 500

    # Validate request
    data = request.json
    if not data or 'url' not in data:
        return jsonify({
            "error": "URL is required in request body",
            "status": "error",
            "timestamp": time.time()
        }), 400

    url = data['url']
    options = data.get('options', {})
    webhook_url = data.get('webhook_url')
    referer = data.get('referer', '')

    # Validate options
    try:
        validate_options(options)
    except ValueError as e:
        return jsonify({
            "error": str(e),
            "status": "error",
            "timestamp": time.time()
        }), 400

    # Check request timeout
    if time.time() - start_time > DEFAULT_REQUEST_TIMEOUT:
        return jsonify({
            "error": "Request timeout",
            "status": "error",
            "timestamp": time.time()
        }), 408

    # Set up progress tracking
    progress_hook = ProgressHook()

    # Configure yt-dlp options
    ydl_opts = {
        'format': options.get('format', 'bestvideo[ext=mp4]+bestaudio[ext=m4a]/best[ext=mp4]/best'),
        'outtmpl': os.path.join(DEFAULT_DOWNLOAD_PATH, '%(title)s.%(ext)s'),
        'quiet': options.get('quiet', False),
        'no_warnings': options.get('no_warnings', False),
        'progress_hooks': [progress_hook],
        'http_headers': {
            'Referer': referer,
        },
        'ffmpeg_location': '/usr/bin',
        'socket_timeout': DEFAULT_TIMEOUT,
        'retries': options.get('retries', 3),
        **{k: v for k, v in options.items() if k in ALLOWED_OPTIONS}
    }

    # Ensure download directory exists
    os.makedirs(DEFAULT_DOWNLOAD_PATH, exist_ok=True)

    video_path = None
    try:
        with yt_dlp.YoutubeDL(ydl_opts) as ydl:
            # Extract info and download
            info = ydl.extract_info(url, download=True)
            video_path = os.path.join(DEFAULT_DOWNLOAD_PATH, f"{info['title']}.{info['ext']}")
            
            # Upload to cloud storage with retry mechanism
            video_url = upload_to_storage(storage_provider, video_path)

            response_data = {
                "title": info['title'],
                "url": video_url,
                "format": info.get('format', ''),
                "status": "success",
                "timestamp": time.time(),
                "processing_time": time.time() - start_time,
                "download_stats": progress_hook.status,
                "extractor": info.get('extractor', ''),
                "duration": info.get('duration', None),
                "view_count": info.get('view_count', None),
                "like_count": info.get('like_count', None),
                "upload_date": info.get('upload_date', None)
            }

            # Send webhook if provided
            if webhook_url:
                try:
                    import requests
                    requests.post(webhook_url, json=response_data, timeout=10)
                except Exception as e:
                    logger.error(f"Webhook delivery failed: {str(e)}")

            return jsonify(response_data), 200

    except yt_dlp.utils.DownloadError as e:
        error_msg = f"Download failed: {str(e)}"
        logger.error(error_msg)
        return jsonify({
            "error": error_msg,
            "status": "error",
            "timestamp": time.time()
        }), 400
        
    except Exception as e:
        error_msg = f"Processing failed: {str(e)}"
        logger.error(error_msg)
        return jsonify({
            "error": error_msg,
            "status": "error",
            "timestamp": time.time()
        }), 500
        
    finally:
        # Cleanup downloaded file
        if video_path and os.path.exists(video_path):
            try:
                os.remove(video_path)
            except Exception as e:
                logger.error(f"Cleanup failed: {str(e)}")