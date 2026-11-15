import os

from station import logger
from station.prototypes.gateway_lifecycle import PrototypeGateway
from station.prototypes.model_config import PrototypeModelConfig, load_overlay
from station.prototypes.prototype import Prototype
from station.prototypes.speech import PrototypeSpeech
from station.prototypes.workspace import PrototypeWorkspace
from station.timeouts import LONG_IO_TIMEOUT_S, wait

from .attachments import CursorAttachments
from .client import list_cursor_models, start_cursor_gateway
from .config import CursorLaunchSettings
from .worker import CursorWorker

# Telegram callback_data is 64 bytes. "/model " plus the id must fit so a tap
# still reaches the existing /model handler intact.
_MODEL_CALLBACK_MAX = 64
_MODEL_BUTTONS_PER_PAGE = 10
_MODEL_BUTTONS_PER_ROW = 2
_MODEL_LIST_ERRORS = (OSError, RuntimeError, TimeoutError, TypeError, ValueError)


def _model_list_errors():
    try:
        from cursor_sdk import CursorAgentError
    except ImportError:
        return _MODEL_LIST_ERRORS
    return _MODEL_LIST_ERRORS + (CursorAgentError,)


def cursor_model_pages(models, *, current):
    buttons = []
    too_long = []
    seen = set()
    for model in models:
        model_id = (getattr(model, "id", "") or "").strip()
        if not model_id or model_id in seen:
            continue
        seen.add(model_id)
        callback = "/model %s" % model_id
        if len(callback.encode("utf-8")) > _MODEL_CALLBACK_MAX:
            too_long.append(model_id)
            continue
        label = (getattr(model, "display_name", "") or "").strip() or model_id
        if model_id == current:
            label = "%s · current" % label
        buttons.append({"text": label, "id": callback})
    pages = []
    for start in range(0, len(buttons), _MODEL_BUTTONS_PER_PAGE):
        chunk = buttons[start:start + _MODEL_BUTTONS_PER_PAGE]
        rows = []
        row = []
        for button in chunk:
            row.append(button)
            if len(row) == _MODEL_BUTTONS_PER_ROW:
                rows.append(row)
                row = []
        if row:
            rows.append(row)
        pages.append(rows)
    return pages, too_long


def cursor_model_list_text(*, current, page, page_count, too_long, last_page):
    lines = ["Current model: %s" % (current or "(not set)")]
    if page_count == 0 and not too_long:
        lines.append("No Cursor models are available for this API key.")
        lines.append("Set one with /model <name>.")
        return "\n".join(lines)
    if page_count == 0:
        lines.append("These models do not fit on a button.")
    elif page_count == 1:
        lines.append("Tap a model to set it.")
    else:
        lines.append("Tap a model to set it (%s/%s)." % (page, page_count))
    if last_page and too_long:
        lines.append("Too long for a button: %s" % ", ".join(too_long))
        lines.append("Set those with /model <name>.")
    return "\n".join(lines)


class CursorPrototype(PrototypeModelConfig, PrototypeWorkspace, CursorWorker, CursorAttachments, PrototypeSpeech, PrototypeGateway, Prototype):
    WORKSPACE_OVERRIDE_NAME = "cursor_workspace"

    def __init__(self, app, prototype_id, model_id, model_settings=None, *, config_file=None, secret_file=None):
        super().__init__(
            app,
            prototype_id,
            model_id,
            model_settings=model_settings,
            config_file=config_file,
            secret_file=secret_file,
        )
        self._init_gateway_lifecycle()
        self._init_bridge_worker(worker_name="cursor-worker")

    def _gateway_label(self):
        return "Cursor"

    def _build_settings(self, *, workspace_override=None):
        if workspace_override is None:
            workspace_override = self._loaded_workspace_override()
        return CursorLaunchSettings.from_model_settings(
            self.model_settings,
            default_workspace=self.storage_dir,
            default_state_root=os.path.join(self.storage_dir, "cursor_bridge"),
            config_file=self.config_file,
            secret_file=self.secret_file,
            workspace_override=workspace_override,
            overlay=load_overlay(self.storage_dir),
        )

    def _config_api_key_path(self):
        return "keys.cursor_api_key"

    def _config_requires_key(self, settings):
        return not settings.api_key

    async def _start_gateway_locked(self):
        settings = self._build_settings()
        if not settings.model:
            raise RuntimeError("Cursor model is required")
        os.makedirs(settings.workspace_dir, exist_ok=True)
        gateway = await start_cursor_gateway(
            workspace=settings.workspace_dir,
            state_root=settings.state_root,
        )
        # Chat can save keys.cursor_api_key while launch_bridge is in flight.
        # Rebuild so the cached snapshot includes that overlay.
        settings = self._build_settings()
        self._settings = settings
        logger.info(
            "Cursor bridge started model_id=%s workspace=%s model=%s",
            self.model_id,
            settings.workspace_dir,
            settings.model,
        )
        return gateway

    async def _after_overlay_change(self):
        self._settings = None
        gateway = self._gateway
        if gateway is None:
            return
        for key, agent in list(gateway.agents.items()):
            gateway.agents.pop(key, None)
            try:
                await agent.close()
            except Exception as e:
                logger.error("unexpected where=cursor_agent_overlay_drop key=%s error=%s", key, e, exc_info=e)

    def _kind_help_commands(self):
        return [
            ("/models", "List models"),
            ("/workspace", "Show or set workspace"),
            ("/cwd", "Show or set workspace"),
        ]

    async def _handle_model_config_command(
        self,
        *,
        cmd,
        args,
        chat_id,
        acct_id,
        reply_to,
        model_settings,
        platform="",
        chat_type="",
    ):
        if cmd == "/models" or (cmd == "/model" and not (args or "").strip()):
            await self._send_model_list(
                chat_id=chat_id,
                acct_id=acct_id,
                reply_to=reply_to,
                platform=platform,
                chat_type=chat_type,
            )
            return True
        return await super()._handle_model_config_command(
            cmd=cmd,
            args=args,
            chat_id=chat_id,
            acct_id=acct_id,
            reply_to=reply_to,
            model_settings=model_settings,
            platform=platform,
            chat_type=chat_type,
        )

    async def _send_model_list(self, *, chat_id, acct_id, reply_to, platform, chat_type):
        settings = self._build_settings()
        if not settings.api_key:
            await self.send_outbound(
                text=self._secret_prompt(),
                chat_id=chat_id,
                acct_id=acct_id,
                reply_to=reply_to,
                platform=platform,
                chat_type=chat_type,
            )
            return
        try:
            gateway = await self._ensure_gateway()
            models = await wait(
                list_cursor_models(gateway=gateway, api_key=settings.api_key),
                LONG_IO_TIMEOUT_S,
                "cursor list models",
            )
        except _model_list_errors() as e:
            logger.error("Cursor list models failed model_id=%s error=%s", self.model_id, e)
            await self.send_outbound(
                text="Cursor models could not be listed: %s" % e,
                chat_id=chat_id,
                acct_id=acct_id,
                reply_to=reply_to,
                platform=platform,
                chat_type=chat_type,
            )
            return
        pages, too_long = cursor_model_pages(models, current=settings.model)
        page_count = len(pages)
        if page_count == 0:
            await self.send_outbound(
                text=cursor_model_list_text(
                    current=settings.model,
                    page=0,
                    page_count=0,
                    too_long=too_long,
                    last_page=True,
                ),
                chat_id=chat_id,
                acct_id=acct_id,
                reply_to=reply_to,
                platform=platform,
                chat_type=chat_type,
            )
            return
        for index, rows in enumerate(pages, start=1):
            await self.send_outbound(
                text=cursor_model_list_text(
                    current=settings.model,
                    page=index,
                    page_count=page_count,
                    too_long=too_long,
                    last_page=index == page_count,
                ),
                chat_id=chat_id,
                acct_id=acct_id,
                reply_to=reply_to,
                platform=platform,
                chat_type=chat_type,
                keyboard=rows,
            )

    async def _handle_kind_command(self, *, cmd, args, chat_id, acct_id, reply_to, model_settings, platform="", chat_type=""):
        if cmd == "/start":
            return True
        if cmd not in {"/workspace", "/cwd"}:
            return False
        await self._handle_workspace_command(
            args=args,
            chat_id=chat_id,
            acct_id=acct_id,
            platform=platform,
            chat_type=chat_type,
        )
        return True

    async def _handle_inbound_context(self, ctx):
        remaining, transcript, had_audio = await self._prepare_hosted_voice_input(ctx.attachments)
        combined = self._combine_user_text(text=ctx.text, transcript_text=transcript)
        message, image_paths = await self._build_prompt_payload(
            text=combined,
            attachments=remaining,
            acct_id=ctx.acct_id,
            chat_id=ctx.chat_id,
        )
        if not message and not image_paths:
            return
        await self._enqueue_turn(
            message=message,
            image_paths=image_paths,
            chat_id=ctx.chat_id,
            acct_id=ctx.acct_id,
            platform=ctx.platform,
            chat_type=ctx.chat_type,
            reply_with_voice=self._should_send_voice_reply(
                incoming_had_audio=had_audio,
                reply_mode=self._voice_reply_mode(),
            ),
        )
