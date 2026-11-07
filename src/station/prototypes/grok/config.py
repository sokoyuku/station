from dataclasses import dataclass

from station.errors import ExternalError
from station.prototypes.boundary import ext_mapping_get
from station.prototypes.chat_history import DEFAULT_CONTEXT_TOKENS, DEFAULT_FETCH_N, DEFAULT_PROTECT_LAST_N
from station.prototypes.launch_settings import launch_sections, merge_launch_settings, voice_settings_from_mapping

@dataclass(slots=True)
class GrokLaunchSettings:
    model: str
    base_url: str
    api_key: str
    system_prompt: str
    reuse_grok_cli_auth: bool
    temperature: float | None
    max_tokens: int | None
    max_messages: int
    fetch_n: int
    round_timeout_s: float
    context_window: int
    voice_reply_mode: str
    tts_voice_id: str
    tts_language: str
    realtime_model: str
    realtime_voice: str
    realtime_sample_rate: int
    realtime_chat_transcripts: bool
    image_model: str
    image_aspect_ratio: str
    image_resolution: str
    video_model: str
    video_duration: int
    video_aspect_ratio: str
    video_resolution: str
    provider: str
    extra_env: dict
    voice: dict
    local_llm_base_url: str
    local_llm_api_key: str

    @classmethod
    def from_model_settings(cls, model_settings, *, config_file=None, secret_file=None, overlay=None):
        raw = merge_launch_settings(
            model_settings,
            config_file=config_file,
            secret_file=secret_file,
            overlay=overlay,
        )

        sections = launch_sections(raw, "model", "oauth", "sampling", "history", "voice", "realtime", "image", "video", "round")
        model = sections["model"]
        oauth = sections["oauth"]
        sampling = sections["sampling"]
        history = sections["history"]
        voice = sections["voice"]
        realtime = sections["realtime"]
        image = sections["image"]
        video = sections["video"]
        round_cfg = sections["round"]

        max_messages = ext_mapping_get(history, "max_messages", (int,), DEFAULT_PROTECT_LAST_N)
        if max_messages <= 0:
            raise ExternalError("history.max_messages must be positive")
        context_window = ext_mapping_get(history, "context_window", (int,), DEFAULT_CONTEXT_TOKENS)
        if context_window <= 0:
            raise ExternalError("history.context_window must be positive")
        fetch_n = ext_mapping_get(history, "fetch_n", (int,), DEFAULT_FETCH_N)
        if fetch_n <= 0:
            raise ExternalError("history.fetch_n must be positive")
        round_timeout_s = ext_mapping_get(round_cfg, "timeout_s", (int, float), 60)
        if round_timeout_s <= 0:
            raise ExternalError("round.timeout_s must be positive")

        voice_reply_mode = voice_settings_from_mapping(voice, reply_mode_default="voice_only")

        sample_rate = ext_mapping_get(realtime, "sample_rate", (int,), 24000)
        if sample_rate not in {8000, 16000, 22050, 24000, 32000, 44100, 48000}:
            raise ExternalError("realtime.sample_rate must be a supported PCM rate")

        tts_voice_id = ext_mapping_get(voice, "tts_voice_id", (str,), "eve")
        realtime_voice = ext_mapping_get(realtime, "voice", (str,), tts_voice_id)

        return cls(
            model=ext_mapping_get(model, "model", (str,), "grok-4.5"),
            base_url=ext_mapping_get(model, "base_url", (str,), "https://api.x.ai/v1").rstrip("/"),
            api_key=ext_mapping_get(model, "api_key", (str,), ""),
            system_prompt=ext_mapping_get(model, "system_prompt", (str,), ""),
            reuse_grok_cli_auth=ext_mapping_get(oauth, "reuse_grok_cli_auth", (bool,), False),
            temperature=ext_mapping_get(sampling, "temperature", (int, float), None, allow_none=True),
            max_tokens=ext_mapping_get(sampling, "max_tokens", (int,), None, allow_none=True),
            max_messages=max_messages,
            fetch_n=fetch_n,
            round_timeout_s=float(round_timeout_s),
            context_window=context_window,
            voice_reply_mode=voice_reply_mode,
            tts_voice_id=tts_voice_id,
            tts_language=ext_mapping_get(voice, "tts_language", (str,), "en"),
            realtime_model=ext_mapping_get(realtime, "model", (str,), "grok-voice-latest"),
            realtime_voice=realtime_voice,
            realtime_sample_rate=sample_rate,
            realtime_chat_transcripts=ext_mapping_get(realtime, "chat_transcripts", (bool,), False),
            image_model=ext_mapping_get(image, "model", (str,), "grok-imagine-image"),
            image_aspect_ratio=ext_mapping_get(image, "aspect_ratio", (str,), ""),
            image_resolution=ext_mapping_get(image, "resolution", (str,), ""),
            video_model=ext_mapping_get(video, "model", (str,), "grok-imagine-video"),
            video_duration=ext_mapping_get(video, "duration", (int,), 8),
            video_aspect_ratio=ext_mapping_get(video, "aspect_ratio", (str,), "16:9"),
            video_resolution=ext_mapping_get(video, "resolution", (str,), "720p"),
            provider="",
            extra_env={},
            voice=dict(voice),
            local_llm_base_url="",
            local_llm_api_key="",
        )
