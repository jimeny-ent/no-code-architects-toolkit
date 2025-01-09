from flask import Blueprint, request, jsonify
import yt_dlp
import os
from config import get_storage_provider
import time
from retrying import retry

v1_media_download_ytdlp_bp = Blueprint('v1_media_download_ytdlp', __name__)

def retry_if_io_error(exception):
    return isinstance(exception, (IOError, OSError))

@retry(retry_on_exception=retry_if_io_error, stop_max_attempt_number=3, wait_fixed=2000)
def upload_to_storage(storage_provider, video_path):
    return storage_provider.upload_file(video_path)

@v1_media_download_ytdlp_bp.route('/v1/media/download/yt-dlp', methods=['POST'])
def download_video():
    DOWNLOAD_PATH = os.getenv('DOWNLOAD_PATH', '/tmp/downloads')
    TIMEOUT = int(os.getenv('DOWNLOAD_TIMEOUT', 300))
    REQUEST_TIMEOUT = int(os.getenv('REQUEST_TIMEOUT', 60))
    
    start_time = time.time()
    
    # Ensure download directory exists
    os.makedirs(DOWNLOAD_PATH, exist_ok=True)
    
    try:
        storage_provider = get_storage_provider()
    except Exception as e:
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
    referer = data.get('referer', '')
    
    # Check if processing time exceeds timeout
    if time.time() - start_time > REQUEST_TIMEOUT:
        return jsonify({
            "error": "Request timeout",
            "status": "error",
            "timestamp": time.time()
        }), 408
    
    ydl_opts = {
        'format': 'bestvideo[ext=mp4]+bestaudio[ext=m4a]/best[ext=mp4]/best',
        'outtmpl': os.path.join(DOWNLOAD_PATH, '%(title)s.%(ext)s'),
        'quiet': False,
        'no_warnings': False,
        'http_headers': {
            'Referer': referer,
        },
        'ffmpeg_location': '/usr/bin',  # Cloud Run ffmpeg location
        'socket_timeout': TIMEOUT,
        'retries': 3
    }

    video_path = None
    try:
        with yt_dlp.YoutubeDL(ydl_opts) as ydl:
            # Extract info and download
            info = ydl.extract_info(url, download=True)
            video_path = os.path.join(DOWNLOAD_PATH, f"{info['title']}.{info['ext']}")
            
            # Upload to cloud storage with retry mechanism
            video_url = upload_to_storage(storage_provider, video_path)

            response_data = {
                "title": info['title'],
                "url": video_url,
                "format": info['format'],
                "status": "success",
                "timestamp": time.time(),
                "processing_time": time.time() - start_time
            }

            return jsonify(response_data), 200

    except yt_dlp.utils.DownloadError as e:
        return jsonify({
            "error": f"Download failed: {str(e)}",
            "status": "error",
            "timestamp": time.time()
        }), 400
        
    except Exception as e:
        return jsonify({
            "error": f"Processing failed: {str(e)}",
            "status": "error",
            "timestamp": time.time()
        }), 500
        
    finally:
        # Cleanup downloaded file
        if video_path and os.path.exists(video_path):
            try:
                os.remove(video_path)
            except Exception as e:
                print(f"Cleanup failed: {str(e)}")  # Log but don't fail request