import asyncio
import base64
import collections
import contextvars
import json
import mimetypes
import os
import time

from station import logger
from station.errors import ExternalError
from station.prototypes.attachments import is_audio_attachment, is_image_attachment
from station.prototypes.chat_history import (
    apply_compact_summary,
    format_transcript,
    history_sender,
    list_message_chats,
    list_unread_messages,
    load_messages,
    needs_compact,
    newest_unread_starter,
    new_message_id,
    persisted_attachment_records,
    read_message_keys,
    remember_chat_route,
    save_messages,
    set_messages_read,
    split_for_compact,
    take_unread_messages,
    with_attachment_notes,
)
from station.prototypes.cli_tools import CLI_TOOL_NAMES
from station.prototypes.grok.hermes_supervisor import HERMES_TOOL_NAMES, hermes_chat_context
from station.prototypes.grok.tools import (
    LIVE_FUNCTION_NAMES,
    SKILL_TOOL_NAMES,
    chat_tools,
    check_unread_page,
    format_fetched_messages,
    format_unread_notice,
    record_outbound,
    skill_view,
    tool_result,
)
from station.prototypes.voice import PrototypeVoice
from station.timeouts import PROCESS_STOP_TIMEOUT_S, BoundedLock, DeadlineExceeded, wait

from . import auth_store
from .client import (
    PROVIDER_STOP_TEXT,
    complete_chat,
    fetch_usage,
    generate_image,
    generate_video,
    grok_round_deadline,
    synthesize_speech,
    transcribe_audio,
)

BUSY_REPLY_TEXT = "I'm still processing your previous message. Please wait."
_DELEGATE_QUEUE_MAX = 20
_COMPACT_PASSES = 4
_COMPACT_REQUEST = (
    "Summarize the earlier conversation below so it can replace those messages. "
    "Keep names, decisions, and attachment notes such as [photo file_id=...]. "
    "Write only the summary.\n\n%s"
)
turn_read_state = contextvars.ContextVar("grok_turn_read", default=None)


class TurnReadState:
    def __init__(self):
        self.read_before = set()
        self.fetched = []
        self.checked = []


class GrokTurn(PrototypeVoice):
    def _queue_lock(self):
        lock = getattr(self, "_message_queue_lock", None)
        if lock is None:
            lock = BoundedLock(where="grok message queue")
            self._message_queue_lock = lock
        return lock

    def _turn_is_scheduled(self):
        return bool(getattr(self, "_turn_scheduled", False))

    def _turn_is_busy(self):
        return self._turn_is_active() or self._turn_is_scheduled()

    def _media_from_fetched(self, fetched, chat_id, kind):
        for item in reversed(fetched or []):
            if item.get("chat_id") != chat_id:
                continue
            for record in item.get("attachments") or []:
                attachment = {
                    "file_id": record.get("file_id") or "",
                    "type": record.get("type") or "",
                    "content_type": record.get("mime") or "",
                }
                if kind == "image" and is_image_attachment(attachment):
                    return attachment
                if kind == "audio" and is_audio_attachment(attachment):
                    return attachment
        return None

    def _revert_turn_reads(self, state):
        refs = list(state.fetched)
        for item in state.checked:
            if item["was_read_before"]:
                continue
            refs.append(item["ref"])
        if not refs:
            return
        set_messages_read(self.storage_dir, refs, read=False)

    def _turn_is_active(self):
        return bool(getattr(self, "_turn_active", False))

    async def _claim_turn(self, *, fetch_n):
        async with self._queue_lock():
            if self._turn_is_active():
                return None
            self._turn_active = True
            self._turn_scheduled = False
            try:
                state = TurnReadState()
                state.read_before = read_message_keys(self.storage_dir)
                fetched = take_unread_messages(self.storage_dir, fetch_n)
                set_messages_read(self.storage_dir, fetched, read=True)
                state.fetched = fetched
                return state, fetched
            except BaseException:
                self._turn_active = False
                raise

    def _delegate_tasks(self):
        tasks = getattr(self, "_delegate_task_queue", None)
        if tasks is None:
            tasks = collections.deque()
            self._delegate_task_queue = tasks
        return tasks

    def _enqueue_delegate_task(self, item):
        tasks = self._delegate_tasks()
        while len(tasks) >= _DELEGATE_QUEUE_MAX:
            dropped = tasks.popleft()
            logger.warning(
                "grok delegate queue full, dropped task chat_id=%s",
                dropped.get("chat_id") or "",
            )
        tasks.append(item)

    def _next_starter(self, *, after_seq=None):
        for row in list_unread_messages(self.storage_dir):
            if not row.get("starts_turn"):
                continue
            if after_seq is not None and (row.get("seq") or 0) <= after_seq:
                continue
            return row
        return None

    async def _kick_queued_turn(self, *, after_seq=None):
        async with self._queue_lock():
            if self._turn_is_busy():
                return
            item = self._next_starter(after_seq=after_seq)
            if item is None:
                return
            item = dict(item)
            self._turn_scheduled = True
        asyncio.create_task(self._run_queued_turn(item), name="grok-queued-turn")

    async def _kick_delegate_turn(self, *, after_seq=None):
        async with self._queue_lock():
            if self._turn_is_busy():
                return
            if not self._delegate_tasks():
                return
            if self._next_starter(after_seq=after_seq) is not None:
                return
            self._turn_scheduled = True
        asyncio.create_task(self._run_delegate_turn(), name="grok-delegate-turn")

    async def _run_delegate_turn(self):
        async with self._queue_lock():
            if self._turn_is_active():
                self._turn_scheduled = False
                return
            if newest_unread_starter(self.storage_dir) is not None:
                self._turn_scheduled = False
                handoff = True
            else:
                handoff = False
                tasks = self._delegate_tasks()
                if not tasks:
                    self._turn_scheduled = False
                    return
                item = tasks.popleft()
        if handoff:
            await self._kick_queued_turn()
            return
        try:
            settings = self._build_settings()
        except Exception:
            logger.exception("grok delegate turn failed chat_id=%s", item.get("chat_id") or "")
            async with self._queue_lock():
                self._turn_scheduled = False
                tasks = self._delegate_tasks()
                tasks.appendleft(item)
            return
        try:
            started = await self._run_chat_turn(
                chat_id=item["chat_id"],
                acct_id=item["acct_id"],
                settings=settings,
                text=item["task"],
                photo_attachment=None,
                voice_attachment=None,
                platform=item.get("platform") or "",
                chat_type=item.get("chat_type") or "",
                user_recorded=True,
                quiet_if_busy=True,
                to_live=True,
            )
        except Exception:
            logger.exception("grok delegate turn failed chat_id=%s", item.get("chat_id") or "")
            return
        if started is False:
            async with self._queue_lock():
                self._turn_scheduled = False
                tasks = self._delegate_tasks()
                tasks.appendleft(item)
                while len(tasks) > _DELEGATE_QUEUE_MAX:
                    tasks.pop()
            await self._kick_delegate_turn()

    async def _run_queued_turn(self, item):
        try:
            settings = self._build_settings()
        except Exception:
            logger.exception("grok queued turn failed chat_id=%s", item.get("chat_id") or "")
            async with self._queue_lock():
                self._turn_scheduled = False
            return
        started = await self._run_chat_turn(
            chat_id=item["chat_id"],
            acct_id=item["acct_id"],
            settings=settings,
            text="",
            photo_attachment=None,
            voice_attachment=None,
            platform=item.get("platform") or "",
            chat_type=item.get("chat_type") or "",
            user_id=item.get("user_id") or "",
            sender=item.get("sender"),
            user_recorded=True,
            from_queue=True,
            quiet_if_busy=True,
        )
        if started is False:
            async with self._queue_lock():
                self._turn_scheduled = False
            await self._kick_queued_turn()

    async def _delegate_task(self, arguments):
        if type(arguments) is not dict:
            arguments = {}
        task = arguments.get("task")
        if type(task) is not str or not task.strip():
            return json.dumps({"ok": False, "error": "task is required"})
        chat = hermes_chat_context.get() or {}
        chat_id = chat.get("chat_id") or ""
        acct_id = chat.get("acct_id") or ""
        if not chat_id or not acct_id:
            return json.dumps({"ok": False, "error": "no live session"})
        async with self._queue_lock():
            self._enqueue_delegate_task({
                "task": task.strip(),
                "acct_id": acct_id,
                "chat_id": chat_id,
                "platform": chat.get("platform") or "",
                "chat_type": chat.get("chat_type") or "",
            })
            busy = self._turn_is_busy() or newest_unread_starter(self.storage_dir) is not None
        if not busy:
            asyncio.create_task(self._run_delegate_turn(), name="grok-delegate-turn")
        return json.dumps({"ok": True})

    async def _realtime_tool(self, name, arguments, state):
        if name not in LIVE_FUNCTION_NAMES:
            return json.dumps({"ok": False, "error": "unknown tool"})
        if name == "delegate_task":
            return await self._delegate_task(arguments)
        token = turn_read_state.set(state) if state is not None else None
        try:
            result = await self._run_grok_tool(name, arguments)
            if type(result) is dict:
                result = dict(result)
                view = result.pop("_view", None)
                output = json.dumps(result)
                if view:
                    return {"output": output, "view": view}
                return output
            if type(result) is str:
                return result
            return json.dumps(result)
        finally:
            if token is not None:
                turn_read_state.reset(token)

    async def _fail_realtime_turn(self, state):
        async with self._queue_lock():
            self._revert_turn_reads(state)

    def _turn_text(
        self,
        *,
        acct_id,
        chat_id,
        user_id,
        fetched,
        source_text,
        transcript,
        voice_attachment,
        photo_data_url,
        attachment_records,
        from_queue,
    ):
        prompt_parts = []
        if source_text:
            prompt_parts.append(source_text)
        if transcript:
            if prompt_parts:
                prompt_parts.append("")
            prompt_parts.append(
                "The following transcript is authoritative evidence of what was spoken in the audio attachment."
            )
            prompt_parts.append(transcript)
        elif voice_attachment:
            prompt_parts.append("[NO SPEECH]")
        if photo_data_url and not prompt_parts and not fetched:
            prompt_parts.append("Please analyze the attached image and respond to the user.")
        prompt_text = "\n".join(prompt_parts).strip()
        parts = []
        if prompt_text:
            parts.append(with_attachment_notes(prompt_text, None if from_queue else attachment_records))
        fetched_text = format_fetched_messages(fetched)
        if fetched_text:
            parts.append(
                "Messages fetched this turn, newest first:\n%s\nThis turn is shared by all chats. Reply in the current chat. Use send_message for other chats."
                % fetched_text
            )
        model_body = "\n\n".join(parts).strip()
        if not model_body:
            return ""
        if acct_id or chat_id or user_id:
            return (
                "Current chat acct_id=%s chat_id=%s user_id=%s. This turn is shared by all chats. "
                "Prior messages are not included. Call get_chat_history to read one.\n%s"
                % (acct_id, chat_id, user_id, model_body)
            )
        return model_body

    async def _send_live_text(self, text):
        chat = hermes_chat_context.get() or {}
        state = await self._active_realtime_for_chat(chat.get("chat_id") or "")
        if state is None:
            return {"ok": False, "error": "no live session"}
        await state.session.send_text(text)
        return {"ok": True, "sent": True}

    async def _check_unread_messages(self):
        state = turn_read_state.get()
        if state is None:
            return {"ok": False, "error": "no active turn"}
        async with self._queue_lock():
            page = check_unread_page(self.storage_dir)
            for ref in page["refs"]:
                key = (ref["acct_id"], ref["chat_id"], ref["id"])
                state.checked.append({
                    "ref": ref,
                    "was_read_before": key in state.read_before,
                })
            set_messages_read(self.storage_dir, page["refs"], read=True)
        return {
            "ok": True,
            "messages": page["messages"],
            "remaining": page["remaining"],
        }

    def _pending_hermes_keys(self):
        tasks = getattr(self, "_hermes_tasks", None)
        if not tasks:
            return []
        keys = []
        seen = set()
        for task in tasks.values():
            if not task.wake_pending or not task.acct_id or not task.chat_id:
                continue
            key = (task.acct_id, task.chat_id)
            if key in seen:
                continue
            seen.add(key)
            keys.append(key)
        return keys

    async def _release_turn(self, *, after_seq=None):
        self._turn_active = False
        if getattr(self, "_hermes_ready", False):
            for acct_id, chat_id in self._pending_hermes_keys():
                await self._flush_hermes_wakes(acct_id, chat_id)
        await self._kick_queued_turn(after_seq=after_seq)
        await self._kick_delegate_turn(after_seq=after_seq)

    async def _send_photo(self, *, chat_id, acct_id, image_bytes, mime="image/png", caption="", platform="", chat_type=""):
        if not image_bytes:
            return
        content_type = (mime or "image/png").split(";")[0].strip().lower() or "image/png"
        suffix = ".jpg" if content_type in {"image/jpeg", "image/jpg"} else ".webp" if content_type == "image/webp" else ".png"
        name = "grok_img%s" % suffix
        meta = self.save_chat(
            data=image_bytes,
            original_name=name,
            mime_type=content_type,
            chat_id=chat_id,
            acct_id=acct_id,
        )
        await self.send_outbound(
            text=caption or "",
            attachments=[{"type": "photo", "file_id": meta["file_id"]}],
            chat_id=chat_id,
            acct_id=acct_id,
            platform=platform,
            chat_type=chat_type)
        return {
            "file_id": meta["file_id"],
            "type": "photo",
            "mime": content_type,
            "name": meta.get("original_name") or name,
        }

    async def _send_video(self, *, chat_id, acct_id, video_bytes, mime="video/mp4", caption="", platform="", chat_type=""):
        if not video_bytes:
            return
        content_type = (mime or "video/mp4").split(";")[0].strip().lower() or "video/mp4"
        suffix = ".webm" if content_type == "video/webm" else ".mov" if content_type == "video/quicktime" else ".mp4"
        name = "grok_vid%s" % suffix
        meta = self.save_chat(
            data=video_bytes,
            original_name=name,
            mime_type=content_type,
            chat_id=chat_id,
            acct_id=acct_id,
        )
        await self.send_outbound(
            text=caption or "",
            attachments=[{"type": "video", "file_id": meta["file_id"]}],
            chat_id=chat_id,
            acct_id=acct_id,
            platform=platform,
            chat_type=chat_type)
        return {
            "file_id": meta["file_id"],
            "type": "video",
            "mime": content_type,
            "name": meta.get("original_name") or name,
        }

    async def _photo_attachment_to_data_url(self, *, attachment):
        local_path = self._attachment_str(attachment, "local_path")
        if not local_path:
            local_path = await self._ensure_attachment_ready(attachment)
        if not local_path:
            return ""
        with open(local_path, "rb") as f:
            image_b64 = base64.b64encode(f.read()).decode("ascii")
        content_type = (attachment.get("content_type") or "").strip().lower()
        if content_type.startswith("image/"):
            mime_type = content_type
        else:
            guessed, _ = mimetypes.guess_type(local_path)
            mime_type = guessed if guessed and guessed.startswith("image/") else "image/jpeg"
        return "data:%s;base64,%s" % (mime_type, image_b64)

    async def _transcribe_audio_attachment(self, *, attachment, settings, access_token):
        local_path = self._attachment_str(attachment, "local_path")
        if not local_path:
            local_path = await self._ensure_attachment_ready(attachment)
        if not local_path:
            return None
        with open(local_path, "rb") as f:
            audio_data = f.read()
        filename = (
            (attachment.get("file_name") or "").strip()
            or (attachment.get("filename") or "").strip()
            or os.path.basename(local_path)
            or "audio.wav"
        )
        return await transcribe_audio(
            session=None,
            settings=settings,
            access_token=access_token,
            audio_bytes=audio_data,
            filename=filename,
        )

    async def _send_voice_reply(self, *, chat_id, acct_id, text, settings, access_token, platform="", chat_type=""):
        audio, content_type = await synthesize_speech(
            session=None,
            settings=settings,
            access_token=access_token,
            text=text,
        )
        if not audio:
            logger.warning("Grok TTS produced no audio chat_id=%s", chat_id)
            return
        await self._send_voice_bytes(
            chat_id=chat_id,
            acct_id=acct_id,
            audio_bytes=audio,
            content_type=content_type,
            name="grok_tts",
            platform=platform,
            chat_type=chat_type,
        )

    async def _handle_usage_command(self, *, chat_id, acct_id, settings, reply_to=None, platform="", chat_type=""):
        creds = await self._load_oauth_credentials(acct_id=acct_id, settings=settings)
        if creds is None:
            reply = await self._start_login(
                acct_id=acct_id,
                chat_id=chat_id,
                reply_to=reply_to,
                platform=platform,
                chat_type=chat_type,
            )
            await self.send_outbound(
                text=reply,
                chat_id=chat_id,
                acct_id=acct_id,
                reply_to=reply_to,
                platform=platform,
                chat_type=chat_type)
            return
        auth_store.save_credentials(self.storage_dir, acct_id, creds)
        reply = await fetch_usage(session=None, access_token=creds.access)
        await self.send_outbound(
            text=reply,
            chat_id=chat_id,
            acct_id=acct_id,
            platform=platform,
            chat_type=chat_type)

    async def _handle_generate_command(self, *, prompt, chat_id, acct_id, settings, reply_to=None, platform="", chat_type=""):
        if not prompt:
            await self.send_outbound(
                text="Usage: /image <prompt>",
                chat_id=chat_id,
                acct_id=acct_id,
                platform=platform,
                chat_type=chat_type)
            return

        access_token = await self._resolve_access_token(acct_id=acct_id, settings=settings)
        if not access_token:
            reply = await self._start_login(
                acct_id=acct_id,
                chat_id=chat_id,
                reply_to=reply_to,
                platform=platform,
                chat_type=chat_type,
            )
            await self.send_outbound(
                text=reply,
                chat_id=chat_id,
                acct_id=acct_id,
                reply_to=reply_to,
                platform=platform,
                chat_type=chat_type)
            return

        image_bytes, mime, err = await generate_image(
            session=None,
            settings=settings,
            access_token=access_token,
            prompt=prompt,
        )
        if err or not image_bytes:
            await self.send_outbound(
                text=err or "Grok image generation failed.",
                chat_id=chat_id,
                acct_id=acct_id,
                platform=platform,
                chat_type=chat_type)
            return

        record = await self._send_photo(
            chat_id=chat_id,
            acct_id=acct_id,
            image_bytes=image_bytes,
            mime=mime,
            caption=prompt,
            platform=platform,
            chat_type=chat_type,
        )
        if record:
            self._append_assistant_turn(
                acct_id=acct_id,
                chat_id=chat_id,
                reply_text=prompt,
                attachments=[record],
                platform=platform,
                chat_type=chat_type,
            )

    async def _handle_generate_video_command(self, *, prompt, chat_id, acct_id, settings, reply_to=None, platform="", chat_type=""):
        if not prompt:
            await self.send_outbound(
                text="Usage: /video <prompt>",
                chat_id=chat_id,
                acct_id=acct_id,
                platform=platform,
                chat_type=chat_type)
            return

        access_token = await self._resolve_access_token(acct_id=acct_id, settings=settings)
        if not access_token:
            reply = await self._start_login(
                acct_id=acct_id,
                chat_id=chat_id,
                reply_to=reply_to,
                platform=platform,
                chat_type=chat_type,
            )
            await self.send_outbound(
                text=reply,
                chat_id=chat_id,
                acct_id=acct_id,
                reply_to=reply_to,
                platform=platform,
                chat_type=chat_type)
            return

        video_bytes, mime, err = await generate_video(
            session=None,
            settings=settings,
            access_token=access_token,
            prompt=prompt,
        )
        if err or not video_bytes:
            await self.send_outbound(
                text=err or "Grok video generation failed.",
                chat_id=chat_id,
                acct_id=acct_id,
                platform=platform,
                chat_type=chat_type)
            return

        record = await self._send_video(
            chat_id=chat_id,
            acct_id=acct_id,
            video_bytes=video_bytes,
            mime=mime,
            caption=prompt,
            platform=platform,
            chat_type=chat_type,
        )
        if record:
            self._append_assistant_turn(
                acct_id=acct_id,
                chat_id=chat_id,
                reply_text=prompt,
                attachments=[record],
                platform=platform,
                chat_type=chat_type,
            )

    def _compact_head_keys(self, messages):
        keys = []
        for message in messages:
            keys.append((
                message.get("role") or "",
                message.get("id") or "",
                message.get("content") or "",
            ))
        return tuple(keys)

    def _compact_head_attachments(self, messages):
        records = []
        seen = set()
        for message in messages:
            for record in message.get("attachments") or []:
                file_id = record.get("file_id") or ""
                if not file_id or file_id in seen:
                    continue
                seen.add(file_id)
                records.append(dict(record))
        return records

    async def _compact_chat_once(self, acct_id, chat_id, *, settings, access_token):
        async with self._queue_lock():
            messages = load_messages(self.storage_dir, acct_id, chat_id)
            if not needs_compact(
                messages,
                context_tokens=settings.context_window,
                protect_last_n=settings.max_messages,
            ):
                return False
            head, _tail = split_for_compact(messages, protect_last_n=settings.max_messages)
            head_keys = self._compact_head_keys(head)
            attachments = self._compact_head_attachments(head)
            transcript = format_transcript(head)
        summary = ""
        if transcript:
            try:
                summary = await complete_chat(
                    session=None,
                    settings=settings,
                    access_token=access_token,
                    user_text=_COMPACT_REQUEST % transcript,
                )
            except Exception:
                logger.exception(
                    "grok compact failed acct_id=%s chat_id=%s",
                    acct_id,
                    chat_id,
                )
                return False
            if not self._grok_reply_ok(summary) or summary.strip() == "Grok returned no content.":
                logger.info(
                    "grok compact skipped acct_id=%s chat_id=%s",
                    acct_id,
                    chat_id,
                )
                return False
        async with self._queue_lock():
            messages = load_messages(self.storage_dir, acct_id, chat_id)
            if not needs_compact(
                messages,
                context_tokens=settings.context_window,
                protect_last_n=settings.max_messages,
            ):
                return False
            head, tail = split_for_compact(messages, protect_last_n=settings.max_messages)
            if self._compact_head_keys(head) != head_keys:
                return False
            updated = apply_compact_summary(summary, tail, attachments=attachments or None)
            save_messages(self.storage_dir, acct_id, chat_id, updated)
        logger.info(
            "grok compact acct_id=%s chat_id=%s removed=%s",
            acct_id,
            chat_id,
            len(head),
        )
        return True

    async def _compact_oversized_chats(self, *, settings, access_token):
        if not access_token:
            return
        try:
            for acct_id, chat_id in list_message_chats(self.storage_dir):
                for _ in range(_COMPACT_PASSES):
                    if not await self._compact_chat_once(
                        acct_id,
                        chat_id,
                        settings=settings,
                        access_token=access_token,
                    ):
                        break
        except Exception:
            logger.exception("grok compact failed")

    async def _busy_reply(self, *, chat_id, acct_id, platform, chat_type, quiet_if_busy):
        if quiet_if_busy:
            return False
        await self.send_outbound(
            text=BUSY_REPLY_TEXT,
            chat_id=chat_id,
            acct_id=acct_id,
            platform=platform,
            chat_type=chat_type)
        return None

    def _delegate_turn_text(self, task, *, live, deliver_chat):
        shared = (
            "The live voice session delegated this task. "
            "Do it with your tools. "
            "Call view_attachment when the task includes a file_id. "
            "Use send_message when the task is to message a person. "
        )
        if live and not deliver_chat:
            return (
                shared
                + "Call send_text with what the call should hear. "
                "The call does not see this turn unless you call send_text. "
                "While the call stays open, the final assistant message is saved in history and is not sent into the chat.\n\n"
                "%s" % task
            )
        if live:
            return (
                shared
                + "Call send_text with what the call should hear. "
                "The call does not see this turn unless you call send_text. "
                "Messages were fetched this turn, so the final assistant message is delivered to the current chat.\n\n"
                "%s" % task
            )
        return (
            shared
            + "The call has ended. Deliver the result as the final assistant message.\n\n"
            "%s" % task
        )

    async def _halt_sessions(self):
        task = getattr(self, "_turn_task", None)
        if task is not None and not task.done():
            task.cancel()
            try:
                await wait(task, PROCESS_STOP_TIMEOUT_S, "grok turn stop")
            except (asyncio.CancelledError, DeadlineExceeded):
                logger.info("grok turn stopped model_id=%s", self.model_id)
        self._turn_active = False
        calls = list(getattr(self, "_realtime_by_call", {}))
        for call_key in calls:
            try:
                await self._end_realtime(call_key=call_key)
            except Exception as e:
                logger.error(
                    "grok realtime halt failed model_id=%s error=%s",
                    self.model_id,
                    e,
                    exc_info=e,
                )
        interrupt = getattr(self, "_interrupt_all_hermes", None)
        if interrupt is not None:
            await interrupt()

    async def _resume_sessions(self):
        await self._kick_queued_turn()

    async def _stop_chat(self, *, chat_id, acct_id):
        key = (acct_id, chat_id)
        task = getattr(self, "_turn_task", None)
        turn_key = getattr(self, "_turn_chat_key", None)
        if task is not None and not task.done() and turn_key == key:
            task.cancel()
            try:
                await wait(task, PROCESS_STOP_TIMEOUT_S, "grok turn stop")
            except (asyncio.CancelledError, DeadlineExceeded):
                logger.info("grok turn stopped model_id=%s chat_id=%s", self.model_id, chat_id)
        interrupt = getattr(self, "_interrupt_hermes_chat", None)
        if interrupt is not None:
            await interrupt(chat_id=chat_id, acct_id=acct_id)
        await super()._stop_chat(chat_id=chat_id, acct_id=acct_id)

    async def _run_chat_turn(self, *, chat_id, acct_id, settings, text, photo_attachment, voice_attachment, reply_to=None, platform="", chat_type="", user_id="", sender=None, user_recorded=False, attachment_records=None, quiet_if_busy=False, from_queue=False, to_live=False):
        task = asyncio.create_task(
            self._run_chat_turn_body(
                chat_id=chat_id,
                acct_id=acct_id,
                settings=settings,
                text=text,
                photo_attachment=photo_attachment,
                voice_attachment=voice_attachment,
                reply_to=reply_to,
                platform=platform,
                chat_type=chat_type,
                user_id=user_id,
                sender=sender,
                user_recorded=user_recorded,
                attachment_records=attachment_records,
                quiet_if_busy=quiet_if_busy,
                from_queue=from_queue,
                to_live=to_live,
            ),
            name="grok-turn:%s:%s" % (acct_id, chat_id),
        )
        try:
            return await task
        except asyncio.CancelledError:
            logger.info("grok turn cancelled model_id=%s chat_id=%s", self.model_id, chat_id)
            return

    async def _run_chat_turn_body(self, *, chat_id, acct_id, settings, text, photo_attachment, voice_attachment, reply_to=None, platform="", chat_type="", user_id="", sender=None, user_recorded=False, attachment_records=None, quiet_if_busy=False, from_queue=False, to_live=False):
        claimed = await self._claim_turn(fetch_n=settings.fetch_n)
        if claimed is None:
            if quiet_if_busy:
                return False
            return
        state, fetched = claimed
        if photo_attachment is None:
            photo_attachment = self._media_from_fetched(fetched, chat_id, "image")
        if voice_attachment is None:
            voice_attachment = self._media_from_fetched(fetched, chat_id, "audio")
        ok = False
        access_token = ""
        turn_task = asyncio.current_task()
        self._turn_task = turn_task
        self._turn_chat_key = (acct_id, chat_id)
        read_token = turn_read_state.set(state)
        chat_token = hermes_chat_context.set({
            "chat_id": chat_id,
            "acct_id": acct_id,
            "platform": platform,
            "chat_type": chat_type,
        })
        try:
            access_token = await self._resolve_access_token(acct_id=acct_id, settings=settings)
            if access_token:
                await self._compact_oversized_chats(settings=settings, access_token=access_token)
            notice = format_unread_notice(self.storage_dir)
            if (
                from_queue
                and not fetched
                and not notice
                and not photo_attachment
                and not voice_attachment
                and not (text or "").strip()
            ):
                ok = True
                return

            live = await self._active_realtime_for_chat(chat_id) is not None
            if not access_token:
                reply = await self._start_login(
                    acct_id=acct_id,
                    chat_id=chat_id,
                    reply_to=reply_to,
                    platform=platform,
                    chat_type=chat_type,
                )
                await self.send_outbound(
                    text=reply,
                    chat_id=chat_id,
                    acct_id=acct_id,
                    reply_to=reply_to,
                    platform=platform,
                    chat_type=chat_type)
                if to_live and live and (reply or "").strip():
                    await self._send_live_text(reply)
                return

            photo_data_url = ""
            transcript = ""
            reply_text = ""

            if photo_attachment:
                photo_data_url = await self._photo_attachment_to_data_url(attachment=photo_attachment)
                if not photo_data_url:
                    reply_text = "Image not supported."

            if not reply_text and voice_attachment:
                transcript_or_none = await self._transcribe_audio_attachment(
                    attachment=voice_attachment,
                    settings=settings,
                    access_token=access_token,
                )
                if transcript_or_none is None:
                    reply_text = "Voice input not supported."
                else:
                    transcript = transcript_or_none
                    logger.info("Grok STT transcript_len=%d", len(transcript))

            if reply_text:
                await self.send_outbound(
                    text=reply_text,
                    chat_id=chat_id,
                    acct_id=acct_id,
                    platform=platform,
                    chat_type=chat_type)
                if to_live and live:
                    await self._send_live_text(reply_text)
            else:
                source_text = "" if from_queue else (text or "")
                if to_live and source_text:
                    source_text = self._delegate_turn_text(
                        source_text,
                        live=live,
                        deliver_chat=bool(fetched),
                    )
                turn_sender = history_sender(user_id, sender)
                model_text = self._turn_text(
                    acct_id=acct_id,
                    chat_id=chat_id,
                    user_id=user_id,
                    fetched=fetched,
                    source_text=source_text,
                    transcript=transcript,
                    voice_attachment=voice_attachment,
                    photo_data_url=photo_data_url,
                    attachment_records=attachment_records,
                    from_queue=from_queue,
                )
                if not model_text:
                    if not format_unread_notice(self.storage_dir):
                        return
                    model_text = "Unread messages remain in chat history."
                if not user_recorded and source_text:
                    async with self._queue_lock():
                        self._append_user_turn(
                            acct_id=acct_id,
                            chat_id=chat_id,
                            user_text=source_text,
                            attachments=attachment_records,
                            sender=turn_sender,
                            platform=platform,
                            chat_type=chat_type,
                        )
                content = model_text
                if photo_data_url:
                    content = [
                        {"type": "text", "text": model_text},
                        {"type": "image_url", "image_url": {"url": photo_data_url}},
                    ]
                generated = []

                async def _generated_image(image_bytes, mime):
                    record = await self._send_photo(
                        chat_id=chat_id,
                        acct_id=acct_id,
                        image_bytes=image_bytes,
                        mime=mime,
                        platform=platform,
                        chat_type=chat_type,
                    )
                    if record:
                        generated.append(record)

                async with self.typing_while(chat_id=chat_id, acct_id=acct_id):
                    reply_text = await complete_chat(
                        session=None,
                        settings=settings,
                        access_token=access_token,
                        user_content=content,
                        sender=turn_sender,
                        tools=chat_tools(local=self.is_local, live=live),
                        run_tool=self._run_grok_tool,
                        on_image=_generated_image,
                        unread_notice=self._fresh_unread_notice,
                        on_provider=self._tell_provider,
                    )
                if self._grok_reply_ok(reply_text) or generated:
                    saved_reply = reply_text if self._grok_reply_ok(reply_text) else ""
                    self._append_assistant_turn(
                        acct_id=acct_id,
                        chat_id=chat_id,
                        reply_text=saved_reply,
                        attachments=generated,
                        platform=platform,
                        chat_type=chat_type,
                    )
                    ok = True
                call_open = False
                if to_live:
                    call_open = await self._active_realtime_for_chat(chat_id) is not None
                if not (to_live and call_open and ok and not fetched):
                    await self.send_outbound(
                        text=reply_text,
                        chat_id=chat_id,
                        acct_id=acct_id,
                        platform=platform,
                        chat_type=chat_type)
                    send_voice = self._should_send_voice_reply(
                        incoming_had_audio=voice_attachment is not None,
                        reply_mode=settings.voice_reply_mode,
                    )
                    if send_voice and ok and self._grok_reply_ok(reply_text):
                        await self._send_voice_reply(
                            chat_id=chat_id,
                            acct_id=acct_id,
                            text=reply_text,
                            settings=settings,
                            access_token=access_token,
                            platform=platform,
                            chat_type=chat_type,
                        )
                    if to_live and call_open and not ok and (reply_text or "").strip():
                        await self._send_live_text(reply_text)
        finally:
            if getattr(self, "_turn_task", None) is turn_task:
                self._turn_task = None
                self._turn_chat_key = None
            if not ok:
                async with self._queue_lock():
                    self._revert_turn_reads(state)
            if access_token:
                await self._compact_oversized_chats(settings=settings, access_token=access_token)
            turn_read_state.reset(read_token)
            hermes_chat_context.reset(chat_token)
            after_seq = None
            if not ok:
                seqs = [ref.get("seq") or 0 for ref in state.fetched]
                after_seq = max(seqs) if seqs else 0
            await self._release_turn(after_seq=after_seq)

    def _grok_reply_ok(self, reply_text):
        text = (reply_text or "").strip()
        if not text:
            return False
        if text.startswith("Grok request failed:") or text.startswith("Grok not configured"):
            return False
        if text == PROVIDER_STOP_TEXT:
            return False
        if "Sign in to Grok" in text:
            return False
        return True

    async def _run_grok_tool(self, name, arguments):
        if name in CLI_TOOL_NAMES or name in HERMES_TOOL_NAMES or name in SKILL_TOOL_NAMES:
            if not self.is_local:
                return json.dumps({"ok": False, "error": "unknown tool"})
        if name == "skill_view":
            try:
                return json.dumps(skill_view(arguments))
            except ExternalError as exc:
                return json.dumps({"ok": False, "error": str(exc)})
        if name in CLI_TOOL_NAMES:
            if name in {"sleep", "wait"}:
                return await self._run_timed_cli(name, arguments)
            try:
                result = await self.cli_tools().dispatch(name, arguments)
            except ExternalError as exc:
                return json.dumps({"ok": False, "error": str(exc)})
            return json.dumps(result)
        if name in HERMES_TOOL_NAMES:
            result = await self.hermes_dispatch(name, arguments)
            return json.dumps(result)
        if name == "check_unread_messages":
            return json.dumps(await self._check_unread_messages())
        try:
            result = tool_result(self.storage_dir, name, arguments)
        except ExternalError as exc:
            return json.dumps({"ok": False, "error": str(exc)})
        if result.get("view"):
            return await self._attachment_view_result(result["view"])
        live_text = result.get("live_text")
        if live_text:
            return json.dumps(await self._send_live_text(live_text))
        send = result.get("send")
        if not send:
            return json.dumps(result)
        attachments = self._attachments_from_refs(send.get("files") or [])
        if (send.get("files") or []) and not attachments:
            return json.dumps({"ok": False, "error": "files could not be sent"})
        sent = await self.send_outbound(
            text=send["text"],
            attachments=attachments or None,
            chat_id=send["chat_id"],
            acct_id=send["acct_id"],
            platform=send["platform"],
            chat_type=send["chat_type"])
        if not sent:
            return json.dumps({"ok": False, "error": "send failed"})
        records = persisted_attachment_records(attachments or [])
        record_outbound(
            self.storage_dir,
            send["acct_id"],
            send["chat_id"],
            send["text"],
            attachments=records or None,
        )
        return json.dumps({
            "ok": True,
            "sent": True,
            "acct_id": send["acct_id"],
            "chat_id": send["chat_id"],
            "user_id": send["user_id"],
        })

    async def _attachment_view_result(self, record):
        file_id = record.get("file_id") or ""
        path = ""
        try:
            path = self.resolve_file_id(file_id)
        except ExternalError:
            path = ""
        if not path or not os.path.isfile(path):
            try:
                await self.download_chat_file(file_id=file_id)
                path = self.resolve_file_id(file_id)
            except Exception:
                return {"ok": False, "error": "attachment file is missing"}
        if not path or not os.path.isfile(path):
            return {"ok": False, "error": "attachment file is missing"}
        mime = record.get("mime") or ""
        if not mime:
            guessed, _ = mimetypes.guess_type(path)
            mime = guessed or ""
        out = {
            "ok": True,
            "file_id": file_id,
            "type": record.get("type") or "file",
        }
        if mime:
            out["mime"] = mime
        if record.get("name"):
            out["name"] = record["name"]
        if not is_image_attachment({"type": record.get("type") or "", "content_type": mime}, local_path=path):
            out["image"] = False
            return out
        with open(path, "rb") as handle:
            raw = handle.read()
        if not mime.startswith("image/"):
            mime = "image/jpeg"
            out["mime"] = mime
        out["image"] = True
        out["_view"] = {
            "file_id": file_id,
            "data_url": "data:%s;base64,%s" % (mime, base64.b64encode(raw).decode("ascii")),
        }
        return out

    async def _tell_provider(self, text):
        chat = hermes_chat_context.get() or {}
        chat_id = chat.get("chat_id") or ""
        acct_id = chat.get("acct_id") or ""
        if not chat_id or not acct_id:
            return
        await self.send_outbound(
            text=text,
            chat_id=chat_id,
            acct_id=acct_id,
            platform=chat.get("platform") or "",
            chat_type=chat.get("chat_type") or "",
        )

    async def _run_timed_cli(self, name, arguments):
        if type(arguments) is not dict:
            arguments = {}
        deadline = grok_round_deadline.get()
        limit = None
        if deadline is not None:
            limit = max(0.0, deadline - time.monotonic())
        try:
            if name == "sleep":
                result = await self.cli_tools().sleep(
                    arguments.get("seconds"),
                    run_ids=arguments.get("run_ids"),
                    limit=limit,
                )
            else:
                result = await self.cli_tools().wait(
                    arguments.get("run_ids"),
                    seconds=arguments.get("seconds"),
                    limit=limit,
                )
        except ExternalError as exc:
            return json.dumps({"ok": False, "error": str(exc)})
        return json.dumps(result)

    async def _fresh_unread_notice(self):
        async with self._queue_lock():
            return format_unread_notice(self.storage_dir)

    def _append_user_turn(self, *, acct_id, chat_id, user_text, sender=None, attachments=None, platform="", chat_type="", unread=False, starts_turn=False):
        stored_text = with_attachment_notes(user_text, attachments)
        history = load_messages(self.storage_dir, acct_id, chat_id)
        message_id = None
        if stored_text:
            user_message = {
                "role": "user",
                "content": stored_text,
                "id": new_message_id(),
                "seq": time.time_ns(),
            }
            if unread:
                user_message["read"] = False
            if starts_turn:
                user_message["starts_turn"] = True
            if sender:
                user_message["sender"] = sender
            if attachments:
                user_message["attachments"] = attachments
            history.append(user_message)
            save_messages(self.storage_dir, acct_id, chat_id, history)
            message_id = user_message["id"]
        remember_chat_route(self.storage_dir, acct_id, chat_id, platform=platform, chat_type=chat_type)
        return message_id

    def _append_assistant_turn(self, *, acct_id, chat_id, reply_text, attachments=None, platform="", chat_type=""):
        if not (reply_text or "").strip() and not attachments:
            remember_chat_route(self.storage_dir, acct_id, chat_id, platform=platform, chat_type=chat_type)
            return
        history = load_messages(self.storage_dir, acct_id, chat_id)
        message = {"role": "assistant", "content": reply_text or ""}
        if attachments:
            message["attachments"] = attachments
        history.append(message)
        save_messages(self.storage_dir, acct_id, chat_id, history)
        remember_chat_route(self.storage_dir, acct_id, chat_id, platform=platform, chat_type=chat_type)
