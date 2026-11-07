import os
import shutil
import subprocess
import tempfile

from station import logger
from station.errors import ExternalError

VOICE_REPLY_MODES = frozenset({"voice_only", "all", "off"})

def parse_voice_reply_mode(value, *, default="voice_only"):
    mode = value.strip().lower() or default
    if mode not in VOICE_REPLY_MODES:
        raise ExternalError("voice.reply_mode must be voice_only, all, or off")
    return mode

def should_send_voice_reply(*, reply_mode, incoming_had_audio):
    if reply_mode == "all":
        return True
    if reply_mode == "off":
        return False
    return bool(incoming_had_audio)

def ffmpeg_path():
    return shutil.which("ffmpeg") or ""


def audio_file_suffix(content_type):
    mime = (content_type or "").split(";")[0].strip().lower()
    return {
        "audio/mpeg": ".mp3",
        "audio/mp3": ".mp3",
        "audio/wav": ".wav",
        "audio/x-wav": ".wav",
        "audio/wave": ".wav",
        "audio/mp4": ".m4a",
        "audio/aac": ".aac",
        "audio/flac": ".flac",
        "audio/webm": ".webm",
        "audio/ogg": ".ogg",
        "audio/opus": ".ogg",
    }.get(mime, ".mp3")


def transcode_audio_to_ogg_opus(audio_bytes):
    if not audio_bytes:
        return b""
    ffmpeg = ffmpeg_path()
    if not ffmpeg:
        raise ExternalError("ffmpeg is required to transcode voice audio")
    src = ""
    dst = ""
    try:
        fd, src = tempfile.mkstemp(prefix="voice_src_", suffix=".bin")
        os.close(fd)
        with open(src, "wb") as f:
            f.write(audio_bytes)
        fd, dst = tempfile.mkstemp(prefix="voice_transcode_", suffix=".ogg")
        os.close(fd)
        result = subprocess.run(
            [
                ffmpeg,
                "-v",
                "error",
                "-y",
                "-i",
                src,
                "-acodec",
                "libopus",
                "-ac",
                "1",
                "-b:a",
                "32k",
                "-vbr",
                "on",
                "-application",
                "voip",
                "-compression_level",
                "10",
                dst,
            ],
            capture_output=True,
            timeout=60,
            stdin=subprocess.DEVNULL,
        )
        if result.returncode != 0 or os.path.getsize(dst) <= 0:
            err = (result.stderr or b"").decode("utf-8", "replace")[:500]
            raise ExternalError("ffmpeg voice transcode failed code=%s error=%s" % (result.returncode, err))
        with open(dst, "rb") as f:
            return f.read()
    except (OSError, subprocess.SubprocessError, ValueError) as exc:
        logger.error("voice transcode failed error=%s", exc)
        raise ExternalError("voice transcode failed error=%s" % exc) from exc
    finally:
        for path in (src, dst):
            if path and os.path.exists(path):
                try:
                    os.unlink(path)
                except OSError as exc:
                    logger.error("voice transcode cleanup failed path=%s error=%s", path, exc)

def is_ogg_opus_audio(audio_bytes, content_type=""):
    mime = (content_type or "").split(";")[0].strip().lower()
    if "ogg" in mime or mime == "audio/opus":
        return True
    return bool(audio_bytes) and audio_bytes[:4] == b"OggS"
