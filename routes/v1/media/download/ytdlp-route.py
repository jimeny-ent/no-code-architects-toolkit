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
    format = data.get('format', 'best')  # Default to best quality
    download_path = '/app/downloads'
    
    ydl_opts = {
        'format': format,
        'outtmpl': os.path.join(download_path, '%(title)s.%(ext)s'),
        'quiet': True,
        'no_warnings': True,
    }

    try:
        with yt_dlp.YoutubeDL(ydl_opts) as ydl:
            # Download the video
            info = ydl.extract_info(url, download=True)
            video_path = os.path.join(download_path, f"{info['title']}.{info['ext']}")

            # Upload to cloud storage
            storage_provider = get_storage_provider()
            video_url = storage_provider.upload_file(video_path)

            # Clean up the local file
            os.remove(video_path)

            return jsonify({
                "title": info['title'],
                "url": video_url,
                "format": info['format'],
                "duration": info.get('duration'),
                "view_count": info.get('view_count'),
                "description": info.get('description')
            }), 200

    except Exception as e:
        return jsonify({"error": str(e)}), 500
