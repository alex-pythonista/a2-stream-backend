from fastapi import FastAPI, File, Depends, UploadFile
from fastapi.responses import FileResponse
import os
import uuid
import subprocess
from fastapi.staticfiles import StaticFiles


UPLOAD_DIR = 'uploads'
PROCESSED_DIR = 'processed_hls'

if not os.path.exists(UPLOAD_DIR):
    os.makedirs(UPLOAD_DIR)

if not os.path.exists(PROCESSED_DIR):
    os.makedirs(UPLOAD_DIR)

app = FastAPI()
app.mount("/processd_hls", StaticFiles(directory="processed_hls"), name="processed_hls")
app.mount("/static", StaticFiles(directory="static"), name="static")


@app.post('/video-upload/')
async def video_upload(file: UploadFile = File(...)):
    inputpath = UPLOAD_DIR + '/' + str(file.filename)
    lessonId = uuid.uuid4()
    outputpath = f'{PROCESSED_DIR}/course/{lessonId}'
    hlspath = f'{outputpath}/index.m3u8'

    os.makedirs(outputpath, exist_ok=True)
    with open(inputpath, 'wb') as buffer:
        buffer.write(file.file.read())

    ffmpeg_cmd = [
        'ffmpeg',
        '-i', inputpath,
        '-c:v', 'libx264',
        '-c:a', 'aac',
        '-hls_time', '10',
        '-hls_playlist_type', 'vod',
        '-hls_segment_filename', f'{outputpath}/%03d.ts',
        '-start_number', '0',
        hlspath
    ]
    subprocess.run(ffmpeg_cmd, check=True)
    
    return {
        'message': 'Video uploaded and converted successfully!',
        'lesson_id': lessonId
    }

@app.get("/stream/{lesson_id}")
async def get_hls_playlist(lesson_id: str):
    # Construct the path to the index.m3u8 file based on lesson_id
    hlspath = f'{PROCESSED_DIR}/course/{lesson_id}/index.m3u8'
    
    # Check if the file exists and serve it
    if os.path.exists(hlspath):
        return FileResponse(hlspath, media_type="application/vnd.apple.mpegurl")
    else:
        return {"error": "File not found"}