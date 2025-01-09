from flask import Flask, request, jsonify
from queue import Queue
from services.webhook import send_webhook
import threading
import uuid
import os
import time
import logging
from concurrent.futures import ThreadPoolExecutor
from version import BUILD_NUMBER

# Configure logging
logging.basicConfig(
    level=logging.INFO, 
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Safely parse MAX_QUEUE_LENGTH with error handling
def parse_queue_length():
    try:
        length = int(os.environ.get('MAX_QUEUE_LENGTH', 50))
        return max(0, length)
    except ValueError:
        logger.warning("Invalid MAX_QUEUE_LENGTH. Defaulting to 50.")
        return 50

MAX_QUEUE_LENGTH = parse_queue_length()
MAX_WORKERS = int(os.environ.get('MAX_WORKERS', 10))

@app.route('/debug/routes')
def list_routes():
    routes = []
    for rule in app.url_map.iter_rules():
        routes.append({
            'endpoint': str(rule.endpoint),
            'methods': list(rule.methods),
            'path': str(rule)
        })
    return jsonify(routes)

def create_app():
    app = Flask(__name__)

    # Create a thread-safe queue and executor
    task_queue = Queue(maxsize=MAX_QUEUE_LENGTH)
    queue_id = id(task_queue)
    thread_pool = ThreadPoolExecutor(max_workers=MAX_WORKERS)

    # Enhanced task processing with robust error handling
    def process_queue():
        while True:
            try:
                job_id, data, task_func, queue_start_time = task_queue.get()
                try:
                    queue_time = time.time() - queue_start_time
                    run_start_time = time.time()
                    pid = os.getpid()

                    # Execute task with timeout and error tracking
                    try:
                        response = task_func()
                    except Exception as e:
                        logger.error(f"Task {job_id} failed: {str(e)}")
                        response = (str(e), 'error', 500)

                    run_time = time.time() - run_start_time
                    total_time = time.time() - queue_start_time

                    response_data = {
                        "endpoint": response[1],
                        "code": response[2],
                        "id": data.get("id"),
                        "job_id": job_id,
                        "response": response[0] if response[2] == 200 else None,
                        "message": "success" if response[2] == 200 else response[0],
                        "pid": pid,
                        "queue_id": queue_id,
                        "run_time": round(run_time, 3),
                        "queue_time": round(queue_time, 3),
                        "total_time": round(total_time, 3),
                        "queue_length": task_queue.qsize(),
                        "build_number": BUILD_NUMBER
                    }

                    # Retry webhook with exponential backoff
                    def send_webhook_with_retry(url, payload, max_retries=3):
                        for attempt in range(max_retries):
                            try:
                                send_webhook(url, payload)
                                return
                            except Exception as e:
                                logger.warning(f"Webhook attempt {attempt+1} failed: {str(e)}")
                                time.sleep(2 ** attempt)  # Exponential backoff

                    # Send webhook if URL provided
                    if data.get("webhook_url"):
                        thread_pool.submit(send_webhook_with_retry, data["webhook_url"], response_data)

                except Exception as e:
                    logger.error(f"Unexpected error in task processing: {str(e)}")
                
                finally:
                    task_queue.task_done()

            except Exception as e:
                logger.critical(f"Critical error in queue processing: {str(e)}")

    # Start queue processing in a daemon thread
    threading.Thread(target=process_queue, daemon=True).start()

    # Advanced task queuing decorator
    def queue_task(bypass_queue=False):
        def decorator(f):
            def wrapper(*args, **kwargs):
                job_id = str(uuid.uuid4())
                data = request.json if request.is_json else {}
                pid = os.getpid()
                start_time = time.time()
                
                # Bypass queue for non-webhook tasks or when specified
                if bypass_queue or 'webhook_url' not in data:
                    response = f(job_id=job_id, data=data, *args, **kwargs)
                    run_time = time.time() - start_time
                    return {
                        "code": response[2],
                        "id": data.get("id"),
                        "job_id": job_id,
                        "response": response[0] if response[2] == 200 else None,
                        "message": "success" if response[2] == 200 else response[0],
                        "run_time": round(run_time, 3),
                        "queue_time": 0,
                        "total_time": round(run_time, 3),
                        "pid": pid,
                        "queue_id": queue_id,
                        "queue_length": task_queue.qsize(),
                        "build_number": BUILD_NUMBER
                    }, response[2]
                
                # Queue management with explicit overflow handling
                if MAX_QUEUE_LENGTH > 0 and task_queue.qsize() >= MAX_QUEUE_LENGTH:
                    return {
                        "code": 429,
                        "id": data.get("id"),
                        "job_id": job_id,
                        "message": f"MAX_QUEUE_LENGTH ({MAX_QUEUE_LENGTH}) reached",
                        "pid": pid,
                        "queue_id": queue_id,
                        "queue_length": task_queue.qsize(),
                        "build_number": BUILD_NUMBER
                    }, 429
                
                # Enqueue task
                task_queue.put((job_id, data, lambda: f(job_id=job_id, data=data, *args, **kwargs), start_time))
                
                return {
                    "code": 202,
                    "id": data.get("id"),
                    "job_id": job_id,
                    "message": "processing",
                    "pid": pid,
                    "queue_id": queue_id,
                    "max_queue_length": MAX_QUEUE_LENGTH,
                    "queue_length": task_queue.qsize(),
                    "build_number": BUILD_NUMBER
                }, 202
            return wrapper
        return decorator

    app.queue_task = queue_task

    # Import blueprints (consolidated to avoid duplicates)
    from routes.media_to_mp3 import convert_bp
    from routes.transcribe_media import transcribe_bp
    from routes.combine_videos import combine_bp
    from routes.audio_mixing import audio_mixing_bp
    from routes.gdrive_upload import gdrive_upload_bp
    from routes.authenticate import auth_bp
    from routes.caption_video import caption_bp 
    from routes.extract_keyframes import extract_keyframes_bp
    from routes.image_to_video import image_to_video_bp
    from routes.v1.media.download.yt_dlp import v1_media_download_ytdlp_bp

    # V1 Blueprints
    from routes.v1.ffmpeg.ffmpeg_compose import v1_ffmpeg_compose_bp
    from routes.v1.media.media_transcribe import v1_media_transcribe_bp
    from routes.v1.media.transform.media_to_mp3 import v1_media_transform_mp3_bp
    from routes.v1.video.concatenate import v1_video_concatenate_bp
    from routes.v1.video.caption_video import v1_video_caption_bp
    from routes.v1.image.transform.image_to_video import v1_image_transform_video_bp
    from routes.v1.toolkit.test import v1_toolkit_test_bp
    from routes.v1.toolkit.authenticate import v1_toolkit_auth_bp
    from routes.v1.code.execute.execute_python import v1_code_execute_bp

    # Register blueprints
    blueprints = [
        convert_bp, transcribe_bp, combine_bp, audio_mixing_bp, 
        gdrive_upload_bp, auth_bp, caption_bp, extract_keyframes_bp, 
        image_to_video_bp, v1_media_download_ytdlp_bp,
        v1_ffmpeg_compose_bp, v1_media_transcribe_bp, 
        v1_media_transform_mp3_bp, v1_video_concatenate_bp, 
        v1_video_caption_bp, v1_image_transform_video_bp, 
        v1_toolkit_test_bp, v1_toolkit_auth_bp, v1_code_execute_bp
    ]

    for blueprint in blueprints:
        app.register_blueprint(blueprint)

    return app

app = create_app()

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8080)
