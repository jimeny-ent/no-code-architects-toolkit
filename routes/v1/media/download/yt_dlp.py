from flask import Blueprint, request, jsonify
import yt_dlp
import os
from config import get_storage_provider

v1_media_download_ytdlp_bp = Blueprint('v1_media_download_ytdlp', __name__)

@v1_media_download_ytdlp_bp.route('/v1/media/download/yt-dlp', methods=['POST'])
def download_video():
    data = request.json
    if not data or 'url' not in data:
        return jsonify({"error": "URL is required"}), 400

    url = data['url']
    referer = data.get('referer', '')
    download_path = '/tmp'  # Use /tmp in Cloud Run
    
    ydl_opts = {
        'format': 'bestvideo[ext=mp4]+bestaudio[ext=m4a]/best[ext=mp4]/best',
        'outtmpl': os.path.join(download_path, '%(title)s.%(ext)s'),
        'quiet': False,
        'no_warnings': False,
        'http_headers': {
            'Referer': referer,
        },
        'ffmpeg_location': '/usr/bin'  # Cloud Run ffmpeg location
    }

    try:
        with yt_dlp.YoutubeDL(ydl_opts) as ydl:
            info = ydl.extract_info(url, download=True)
            video_path = os.path.join(download_path, f"{info['title']}.{info['ext']}")

            # Upload to cloud storage
            storage_provider = get_storage_provider()
            video_url = storage_provider.upload_file(video_path)

            # Clean up
            if os.path.exists(video_path):
                os.remove(video_path)

            return jsonify({
                "title": info['title'],
                "url": video_url,  # Return cloud storage URL instead of local path
                "format": info['format']
            }), 200

    except Exception as e:
        # Clean up on error
        if 'video_path' in locals() and os.path.exists(video_path):
            os.remove(video_path)
        return jsonify({"error": str(e)}), 500