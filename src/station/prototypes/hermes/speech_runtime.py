import json
import sys


def _payload():
    raw = sys.stdin.read()
    if not raw.strip():
        raise SystemExit("speech payload is empty")
    payload = json.loads(raw)
    if type(payload) is not dict:
        raise SystemExit("speech payload must be an object")
    return payload


def _transcribe(payload):
    file_path = payload.get("file_path")
    if type(file_path) is not str or not file_path.strip():
        return {"success": False, "transcript": "", "error": "file_path is required"}
    from tools.transcription_tools import transcribe_audio
    result = transcribe_audio(file_path)
    if type(result) is not dict:
        raise SystemExit("transcribe result must be an object")
    return result


def _synthesize(payload):
    text = payload.get("text")
    output_path = payload.get("output_path")
    if type(text) is not str or not text.strip():
        return {"success": False, "error": "text is required"}
    if type(output_path) is not str or not output_path.strip():
        return {"success": False, "error": "output_path is required"}
    from tools.tts_tool import text_to_speech_tool
    raw = text_to_speech_tool(text, output_path=output_path)
    result = json.loads(raw)
    if type(result) is not dict:
        raise SystemExit("synthesize result must be an object")
    return result


def main():
    if len(sys.argv) != 2:
        raise SystemExit("usage: speech_runtime transcribe|synthesize")
    command = sys.argv[1]
    payload = _payload()
    if command == "transcribe":
        result = _transcribe(payload)
    elif command == "synthesize":
        result = _synthesize(payload)
    else:
        raise SystemExit("unknown speech command")
    sys.stdout.write(json.dumps(result))
    sys.stdout.write("\n")


if __name__ == "__main__":
    main()
