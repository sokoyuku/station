import asyncio
import audioop
import base64
import json

import aiohttp

from station import logger
from station.prototypes.grok.tools import VOICE_SERVER_TOOLS, live_function_tools
from station.timeouts import IO_TIMEOUT_S, PROCESS_STOP_TIMEOUT_S, BoundedLock, wait, ws_timeout

def pcm_mono_resample(pcm_s16le, src_rate, dst_rate):
    if not pcm_s16le or src_rate == dst_rate:
        return pcm_s16le
    converted, _ = audioop.ratecv(pcm_s16le, 2, 1, src_rate, dst_rate, None)
    return converted

class GrokRealtimeSession:
    def __init__(
        self,
        *,
        access_token,
        base_url="https://api.x.ai/v1",
        model="grok-voice-latest",
        voice="eve",
        instructions="",
        sample_rate=24000,
        push_audio_pcm_48k=None,
        on_transcript=None,
        on_tool=None,
        on_fail=None,
        function_tools=None,
    ):
        self._token = access_token
        self._base_url = base_url.rstrip("/")
        self._model = model
        self._voice = voice
        self._instructions = instructions
        self._sample_rate = sample_rate
        self._push_audio_pcm_48k = push_audio_pcm_48k
        self._on_transcript = on_transcript
        self._on_tool = on_tool
        self._on_fail = on_fail
        self._function_tools = function_tools
        self._turn_state = None
        self._response_closed = False
        self._pending_tool_outputs = 0
        self._ws = None
        self._session = None
        self._reader_task = None
        self._session_ack = asyncio.Event()
        self._buffer_cleared = asyncio.Event()
        self._buffer_committed = asyncio.Event()
        self._send_lock = BoundedLock(where="grok realtime send")

    def _ws_url(self):
        http = self._base_url
        if http.startswith("https://"):
            ws = "wss://" + http[len("https://") :]
        elif http.startswith("http://"):
            ws = "ws://" + http[len("http://") :]
        else:
            ws = http
        return "%s/realtime?model=%s" % (ws, self._model)

    async def start(self):
        if not self._token:
            raise RuntimeError("missing access token for Grok realtime")
        self._session = aiohttp.ClientSession(
            timeout=ws_timeout(),
            trust_env=True,
            connector=aiohttp.TCPConnector(ssl=True, force_close=True, limit=4),
        )
        headers = {"Authorization": "Bearer %s" % self._token}
        self._ws = await self._session.ws_connect(
            self._ws_url(),
            headers=headers,
            heartbeat=20,
            timeout=ws_timeout(),
        )
        self._reader_task = asyncio.create_task(self._read_loop())
        instructions = self._instructions or "You are a concise voice assistant."
        function_tools = self._function_tools if self._function_tools is not None else live_function_tools()
        await self._update_session(
            {
                "voice": self._voice,
                "instructions": instructions,
                "turn_detection": {
                    "type": "server_vad",
                    "threshold": 0.5,
                    "silence_duration_ms": 700,
                },
                "audio": {
                    "input": {
                        "format": {"type": "audio/pcm", "rate": self._sample_rate},
                        "transcription": {"model": "grok-transcribe"},
                    },
                    "output": {"format": {"type": "audio/pcm", "rate": self._sample_rate}},
                },
                "tools": [dict(item) for item in VOICE_SERVER_TOOLS] + [dict(item) for item in function_tools],
            },
            timeout=20,
        )

    async def _send(self, payload):
        if self._ws is None or self._ws.closed:
            raise RuntimeError("Grok realtime websocket is closed")
        async with self._send_lock:
            await wait(self._ws.send_json(payload), IO_TIMEOUT_S, "grok realtime send")

    async def _update_session(self, session_payload, *, timeout=5):
        self._session_ack.clear()
        await self._send({"type": "session.update", "session": session_payload})
        await wait(self._session_ack.wait(), timeout, "grok realtime session.update ack")

    async def begin_manual_audio(self):
        self._buffer_cleared.clear()
        await self._send({"type": "input_audio_buffer.clear"})
        await wait(self._buffer_cleared.wait(), 5, "grok realtime input_audio_buffer.clear ack")
        await self._update_session({"turn_detection": None})

    async def append_pcm_48k_mono(self, pcm_s16le_mono_48k):
        if not pcm_s16le_mono_48k:
            return
        pcm = pcm_mono_resample(pcm_s16le_mono_48k, 48000, self._sample_rate)
        if not pcm:
            return
        step = self._sample_rate * 2 // 5
        for i in range(0, len(pcm), step):
            chunk = pcm[i : i + step]
            await self._send(
                {
                    "type": "input_audio_buffer.append",
                    "audio": base64.b64encode(chunk).decode("ascii"),
                }
            )

    async def commit_audio(self):
        self._buffer_committed.clear()
        await self._send({"type": "input_audio_buffer.commit"})
        await wait(self._buffer_committed.wait(), 5, "grok realtime input_audio_buffer.commit ack")
        await self._send({"type": "response.create"})
        await self._update_session(
            {
                "turn_detection": {
                    "type": "server_vad",
                    "threshold": 0.5,
                    "silence_duration_ms": 700,
                }
            }
        )

    def begin_turn(self, state):
        self._turn_state = state
        self._response_closed = False
        self._pending_tool_outputs = 0

    def clear_turn_state(self):
        self._turn_state = None

    def _user_parts(self, text, images=None):
        parts = []
        prompt = (text or "").strip()
        if prompt:
            parts.append({"type": "input_text", "text": prompt})
        for image in images or []:
            file_id = ""
            url = ""
            if type(image) is str:
                url = image.strip()
            elif type(image) is dict:
                file_id = image.get("file_id") or ""
                url = image.get("data_url") or image.get("image_url") or ""
                if type(url) is not str:
                    url = ""
                url = url.strip()
                if type(file_id) is not str:
                    file_id = ""
            if file_id:
                parts.append({"type": "input_text", "text": "Attachment file_id=%s" % file_id})
            if url:
                parts.append({"type": "input_image", "image_url": url})
        return parts

    async def add_user_content(self, text, *, images=None):
        parts = self._user_parts(text, images)
        if not parts:
            return False
        await self._send(
            {
                "type": "conversation.item.create",
                "item": {
                    "type": "message",
                    "role": "user",
                    "content": parts,
                },
            }
        )
        return True

    async def send_text(self, text, *, images=None):
        if not await self.add_user_content(text, images=images):
            return
        await self._send({"type": "response.create"})

    async def _function_call(self, event):
        from station.prototypes.boundary import ext_str

        name = ext_str("function name", event.get("name"))
        call_id = ext_str("function call_id", event.get("call_id"))
        raw = event.get("arguments")
        if raw is None or raw == "":
            arguments = {}
        elif type(raw) is dict:
            arguments = raw
        elif type(raw) is str:
            try:
                arguments = json.loads(raw)
            except json.JSONDecodeError:
                arguments = {}
        else:
            arguments = {}
        if type(arguments) is not dict:
            arguments = {}
        view = None
        if self._on_tool is None:
            output = json.dumps({"ok": False, "error": "unknown tool"})
        else:
            try:
                output = await self._on_tool(name, arguments, self._turn_state)
            except Exception as exc:
                logger.error("grok realtime tool failed name=%s error=%s", name, exc)
                output = json.dumps({"ok": False, "error": "tool failed"})
        if type(output) is dict and "output" in output:
            view = output.get("view")
            output = output.get("output")
        if type(output) is not str:
            output = json.dumps(output)
        await self._send({
            "type": "conversation.item.create",
            "item": {
                "type": "function_call_output",
                "call_id": call_id,
                "output": output,
            },
        })
        if type(view) is dict:
            data_url = view.get("data_url") or ""
            file_id = view.get("file_id") or ""
            if type(data_url) is str and data_url:
                await self.add_user_content("", images=[{"file_id": file_id, "data_url": data_url}])
        self._pending_tool_outputs += 1
        if self._response_closed:
            await self._continue_after_tools()

    async def _continue_after_tools(self):
        if not self._pending_tool_outputs:
            return
        self._pending_tool_outputs = 0
        self._response_closed = False
        await self._send({"type": "response.create"})

    async def _fail_turn(self):
        state = self._turn_state
        self._turn_state = None
        if state is None or self._on_fail is None:
            return
        await self._on_fail(state)

    async def _emit_transcript(self, kind, text):
        cleaned = text.strip()
        if not cleaned or self._on_transcript is None:
            return
        await self._on_transcript(kind, cleaned)

    async def _handle_event(self, event):

        from station.prototypes.boundary import ext_dict, ext_str

        event = ext_dict("realtime event", event)
        etype = event.get("type")
        if etype is not None:
            etype = ext_str("realtime event type", etype)
        if etype in {"session.created", "session.updated"}:
            self._session_ack.set()
            return
        if etype == "input_audio_buffer.cleared":
            self._buffer_cleared.set()
            return
        if etype == "input_audio_buffer.committed":
            self._buffer_committed.set()
            return
        if etype == "conversation.item.input_audio_transcription.completed":
            text = ext_str("transcript", event.get("transcript"))
            if text.strip():
                await self._emit_transcript("input", text)
            return
        if etype == "response.output_audio_transcript.done":
            text = ext_str("transcript", event.get("transcript"))
            if text.strip():
                await self._emit_transcript("output", text)
            return
        if etype == "response.output_audio.delta":
            audio_b64 = event.get("delta")
            if audio_b64 is None:
                audio_b64 = event.get("audio")
            if audio_b64 is None:
                return
            audio_b64 = ext_str("audio delta", audio_b64)
            if not audio_b64 or self._push_audio_pcm_48k is None:
                return
            pcm_24k = base64.b64decode(audio_b64)
            pcm_48k = pcm_mono_resample(pcm_24k, self._sample_rate, 48000)
            if pcm_48k:
                await self._push_audio_pcm_48k(pcm_48k)
            return
        if etype == "response.created":
            self._response_closed = False
            self._pending_tool_outputs = 0
            return
        if etype == "response.function_call_arguments.done":
            await self._function_call(event)
            return
        if etype == "response.done":
            self._response_closed = True
            await self._continue_after_tools()
            return
        if etype == "error":
            await self._fail_turn()
            logger.error("Grok realtime error event: %s", event)
            raise RuntimeError("Grok realtime error event: %s" % event)

    async def _read_loop(self):
        try:
            async for msg in self._ws:
                if msg.type == aiohttp.WSMsgType.TEXT:
                    await self._handle_event(json.loads(msg.data))
                elif msg.type in {
                    aiohttp.WSMsgType.CLOSE,
                    aiohttp.WSMsgType.CLOSING,
                    aiohttp.WSMsgType.CLOSED,
                    aiohttp.WSMsgType.ERROR,
                }:
                    break
        except asyncio.CancelledError:
            raise
        except Exception as e:
            logger.error("unexpected where=grok_realtime_read error=%s", e, exc_info=e)

    async def close(self):
        if self._reader_task is not None:
            self._reader_task.cancel()
            try:
                await wait(self._reader_task, PROCESS_STOP_TIMEOUT_S, "grok realtime reader")
            except asyncio.CancelledError:
                logger.info("grok realtime reader cancelled")
            self._reader_task = None
        if self._ws is not None and not self._ws.closed:
            await wait(self._ws.close(), PROCESS_STOP_TIMEOUT_S, "grok realtime websocket close")
        self._ws = None
        if self._session is not None and not self._session.closed:
            await self._session.close()
        self._session = None
