#!/usr/bin/env python3
"""
Uɴɪᴠᴇʀsᴀʟ Fɪʟᴇ Cᴏɴᴠᴇʀᴛᴇʀ Eɴɢɪɴᴇ - Bᴀᴄᴋ4Aᴘᴘ Eᴅɪᴛɪᴏɴ 🦚✨
Pʀᴏᴄᴇssᴇs ғɪʟᴇs ᴀɴᴅ sʏɴᴄs sᴛᴀᴛᴜs ᴡɪᴛʜ Bᴀᴄᴋ4Aᴘᴘ ᴅᴀᴛᴀʙᴀsᴇ 🔥
"""

import os
import tempfile
import asyncio
import requests
import shutil
import re
import time
from pathlib import Path
from urllib.parse import urlparse

# --- ENVIRONMENT VARIABLES (From GitHub Actions) ---
JOB_ID = os.environ.get('JOB_ID')
FILE_URL = os.environ.get('FILE_URL')
TARGET_FORMAT = os.environ.get('FORMAT')
ORIGINAL_NAME = os.environ.get('FILE_NAME', 'input_file')
B4A_APP_ID = os.environ.get('B4A_APP_ID')
B4A_REST_KEY = os.environ.get('B4A_REST_KEY')

# Back4App API Headers
B4A_HEADERS = {
    "X-Parse-Application-Id": B4A_APP_ID,
    "X-Parse-REST-API-Key": B4A_REST_KEY,
    "Content-Type": "application/json"
}

def update_b4a_job(payload):
    """Sᴇɴᴅ ᴘʀᴏɢʀᴇss ᴜᴘᴅᴀᴛᴇs ᴛᴏ Bᴀᴄᴋ4Aᴘᴘ Dᴀᴛᴀʙᴀsᴇ 🪔"""
    url = f"https://parseapi.back4app.com/classes/ConversionJob/{JOB_ID}"
    try:
        requests.put(url, headers=B4A_HEADERS, json=payload)
    except Exception as e:
        print(f"Failed to update B4A: {e}")

class ProgressTracker:
    """Rᴇᴀʟ-ᴛɪᴍᴇ Pʀᴏɢʀᴇss Tʀᴀᴄᴋɪɴɢ 🦋"""
    def __init__(self):
        self.progress_patterns = {
            'ffmpeg': re.compile(r'time=(\d{2}):(\d{2}):(\d{2})\.(\d{2})'),
            'duration': re.compile(r'Duration: (\d{2}):(\d{2}):(\d{2})\.(\d{2})'),
            'libreoffice': re.compile(r'(\d+)%'),
            'imagemagick': re.compile(r'(\d+)%')
        }
    
    def create_progress_bar(self, percentage: int, width: int = 15) -> str:
        """Cʀᴇᴀᴛᴇ Aɴɪᴍᴀᴛᴇᴅ Pʀᴏɢʀᴇss Bᴀʀ 🌀💫"""
        filled = int(width * percentage / 100)
        bar = "🔥" * filled + "❄️" * (width - filled)
        if percentage < 100:
            animation = ["🦋", "🌸", "🥀", "🍂", "🍁", "🌀", "⚡", "💫", "✨", "🔥"][int(time.time() * 2) % 10]
        else:
            animation = "🦚"
        return f"{animation} [{bar}] {percentage}% 💗"
    
    def parse_ffmpeg_progress(self, line: str, duration: float = 0):
        time_match = self.progress_patterns['ffmpeg'].search(line)
        if time_match and duration > 0:
            h, m, s, c = map(int, time_match.groups())
            current_time = h * 3600 + m * 60 + s + c / 100
            return min(100, int((current_time / duration) * 100))
        return None
    
    def parse_duration(self, line: str):
        duration_match = self.progress_patterns['duration'].search(line)
        if duration_match:
            h, m, s, c = map(int, duration_match.groups())
            return h * 3600 + m * 60 + s + c / 100
        return None

class ProcessManager:
    """Aᴅᴠᴀɴᴄᴇᴅ Pʀᴏᴄᴇss Mᴀɴᴀɢᴇᴍᴇɴᴛ 🥂✨"""
    def __init__(self):
        self.progress_tracker = ProgressTracker()
    
    async def run_with_progress(self, cmd, timeout, update_callback=None):
        process = None
        duration = 0
        last_update = 0
        try:
            process = await asyncio.create_subprocess_exec(
                *cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE, preexec_fn=os.setsid
            )
            
            async def monitor_progress():
                nonlocal duration, last_update
                buffer = ""
                while process.returncode is None:
                    try:
                        data = await asyncio.wait_for(process.stderr.read(1024), timeout=1.0)
                        if not data: break
                        buffer += data.decode('utf-8', errors='ignore')
                        lines = buffer.split('\n')
                        buffer = lines[-1]
                        
                        for line in lines[:-1]:
                            if duration == 0:
                                parsed_duration = self.progress_tracker.parse_duration(line)
                                if parsed_duration: duration = parsed_duration
                            progress = self.progress_tracker.parse_ffmpeg_progress(line, duration)
                            if progress is not None and update_callback:
                                current_time = time.time()
                                if current_time - last_update >= 3: # Send to B4A every 3 seconds max
                                    await update_callback(progress)
                                    last_update = current_time
                    except asyncio.TimeoutError:
                        continue
            
            progress_task = asyncio.create_task(monitor_progress())
            try:
                await asyncio.wait_for(process.wait(), timeout=timeout)
                progress_task.cancel()
                if update_callback: await update_callback(100)
                return process.returncode == 0, ""
            except asyncio.TimeoutError:
                progress_task.cancel()
                return False, "Timeout"
        except Exception as e:
            return False, str(e)

class UniversalConverter:
    """Eɴʜᴀɴᴄᴇᴅ Cᴏɴᴠᴇʀᴛᴇʀ 🦋✨"""
    def __init__(self):
        self.temp_dir = tempfile.mkdtemp()
        self.process_manager = ProcessManager()
        self.progress_tracker = ProgressTracker()
    
    def get_format_info(self, filename: str):
        ext = Path(filename).suffix.lower()
        if ext in ['.jpg', '.png', '.webp', '.jpeg']: return 'image'
        if ext in ['.mp4', '.avi', '.mkv', '.mov']: return 'video'
        if ext in ['.mp3', '.wav', '.ogg', '.flac']: return 'audio'
        if ext in ['.pdf', '.doc', '.docx', '.txt']: return 'document'
        return 'other'
    
    async def convert_with_progress(self, input_path, output_path, progress_callback):
        category = self.get_format_info(input_path)
        output_ext = Path(output_path).suffix.lower()

        if category == 'image':
            await progress_callback(20)
            try:
                from PIL import Image
                with Image.open(input_path) as img:
                    if img.mode != 'RGB' and output_ext in ['.jpg', '.jpeg', '.pdf']:
                        img = img.convert('RGB')
                    img.save(output_path)
                await progress_callback(100)
                return True
            except:
                cmd = ['ffmpeg', '-i', input_path, '-y', output_path]
                success, _ = await self.process_manager.run_with_progress(cmd, 60, progress_callback)
                return success

        elif category == 'video' or category == 'audio':
            await progress_callback(10)
            cmd = ['ffmpeg', '-i', input_path, '-y', output_path]
            success, _ = await self.process_manager.run_with_progress(cmd, 300, progress_callback)
            return success

        elif category == 'document':
            await progress_callback(20)
            cmd = ['libreoffice', '--headless', '--convert-to', output_ext[1:], '--outdir', str(Path(output_path).parent), input_path]
            success, _ = await self.process_manager.run_with_progress(cmd, 120, progress_callback)
            if success:
                expected_file = Path(output_path).parent / f"{Path(input_path).stem}{output_ext}"
                if expected_file.exists():
                    shutil.move(str(expected_file), output_path)
                    await progress_callback(100)
                    return True
            return False
            
        else:
            await progress_callback(50)
            shutil.copy2(input_path, output_path)
            await progress_callback(100)
            return True

async def main():
    print(f"🦚 Sᴛᴀʀᴛɪɴɢ ᴊᴏʙ {JOB_ID}... ✨")
    converter = UniversalConverter()
    
    input_path = os.path.join(converter.temp_dir, ORIGINAL_NAME)
    output_filename = f"converted.{TARGET_FORMAT}"
    output_path = os.path.join(converter.temp_dir, output_filename)

    # 1. DOWNLOAD FILE
    print("📥 Downloading file from Telegram...")
    response = requests.get(FILE_URL)
    with open(input_path, 'wb') as f:
        f.write(response.content)

    file_size_mb = os.path.getsize(input_path) / (1024 * 1024)

    # 2. PROGRESS CALLBACK (Sends UI updates to Back4App)
    async def update_progress(percentage: int):
        progress_bar = converter.progress_tracker.create_progress_bar(percentage)
        status_msg = "🌀 Cᴏɴᴠᴇʀᴛɪɴɢ ғᴏʀᴍᴀᴛ..." if percentage < 90 else "✨ Oᴘᴛɪᴍɪᴢɪɴɢ ᴏᴜᴛᴘᴜᴛ..."
        
        progress_text = (
            f"🔄 **Cᴏɴᴠᴇʀᴛɪɴɢ Fɪʟᴇ** 🔄\n\n"
            f"📄 **Fʀᴏᴍ**: `{Path(ORIGINAL_NAME).suffix[1:].upper()}` 🍂\n"
            f"🎯 **Tᴏ**: `{TARGET_FORMAT.upper()}` 🍁\n"
            f"📊 **Sɪᴢᴇ**: {file_size_mb:.2f} MB 💗\n\n"
            f"{progress_bar}\n\n"
            f"⚡ **Sᴛᴀᴛᴜs**: {status_msg} 🧿\n"
        )
        update_b4a_job({"status": "Processing", "progressText": progress_text})

    # 3. CONVERT FILE
    print("⚙️ Converting file...")
    success = await converter.convert_with_progress(input_path, output_path, update_progress)

    # 4. UPLOAD TO BACK4APP
    if success and os.path.exists(output_path):
        print("📤 Uploading converted file to Back4App...")
        
        upload_headers = B4A_HEADERS.copy()
        upload_headers["Content-Type"] = "application/octet-stream"
        
        with open(output_path, 'rb') as f:
            upload_res = requests.post(
                f"https://parseapi.back4app.com/files/{output_filename}", 
                headers=upload_headers, 
                data=f
            )
        
        if upload_res.status_code == 201:
            saved_file = upload_res.json()
            
            # Update DB to trigger the Telegram send!
            update_b4a_job({
                "status": "Complete",
                "convertedFile": {
                    "__type": "File",
                    "name": saved_file["name"]
                }
            })
            print("🎉 Cᴏɴᴠᴇʀsɪᴏɴ Cᴏᴍᴘʟᴇᴛᴇ ᴀɴᴅ Sʏɴᴄᴇᴅ! 🦚")
        else:
            update_b4a_job({"status": "Error"})
            print(f"❌ Upload failed: {upload_res.text}")
    else:
        update_b4a_job({"status": "Error"})
        print("❌ Conversion failed!")

    # Cleanup
    shutil.rmtree(converter.temp_dir, ignore_errors=True)

if __name__ == '__main__':
    asyncio.run(main())
