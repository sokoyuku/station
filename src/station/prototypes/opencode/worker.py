from dataclasses import dataclass
import asyncio

from station import logger
from station.prototypes.boundary import ext_int, ext_str
from station.prototypes.bridge_worker import PrototypeBridgeWorker
from station.prototypes.chat_history import list_started_chats, load_session_id, save_session_id
from station.timeouts import TURN_TIMEOUT_S, wait

@dataclass(slots=True)
class PendingTurnMessage:
    parts: list
    reply_with_voice: bool = False

class OpenCodeWorker(PrototypeBridgeWorker):
    def _bridge_worker_error_text(self, error):
        return "OpenCode bridge error: %s" % error

    async def _each_saved_session(self):
        for chat in list_started_chats(self.storage_dir):
            acct_id = chat.get("acct_id") or ""
            chat_id = chat.get("chat_id") or ""
            session_id = load_session_id(self.storage_dir, acct_id, chat_id)
            if acct_id and chat_id and session_id:
                yield acct_id, chat_id, session_id

    async def _halt_sessions(self):
        gateway = self._gateway
        if gateway is None:
            return
        async for acct_id, chat_id, session_id in self._each_saved_session():
            try:
                await gateway.session_abort(session_id)
            except Exception as e:
                logger.error(
                    "OpenCode halt failed model_id=%s acct_id=%s chat_id=%s error=%s",
                    self.model_id,
                    acct_id,
                    chat_id,
                    e,
                    exc_info=e,
                )

    async def _resume_sessions(self):
        gateway = self._gateway
        if gateway is None:
            return
        async for acct_id, chat_id, session_id in self._each_saved_session():
            try:
                info = await gateway.session_get(session_id)
            except Exception as e:
                logger.error(
                    "OpenCode resume on restore failed model_id=%s acct_id=%s chat_id=%s error=%s",
                    self.model_id,
                    acct_id,
                    chat_id,
                    e,
                    exc_info=e,
                )
                continue
            if info is None:
                continue
            state = await self._get_chat_state(acct_id=acct_id, chat_id=chat_id)
            state.session_id = session_id
            gateway.bind_session(session_id=session_id, acct_id=acct_id, chat_id=chat_id)

    async def _enqueue_parts(self, *, parts, chat_id, acct_id, platform="", chat_type="", reply_with_voice=False):
        state = await self._get_chat_state(acct_id=acct_id, chat_id=chat_id)
        gateway = await self._ensure_gateway()
        await self._enqueue_pending(
            state=state,
            gateway=gateway,
            chat_id=chat_id,
            acct_id=acct_id,
            item=PendingTurnMessage(parts=parts, reply_with_voice=reply_with_voice),
            platform=platform,
            chat_type=chat_type,
        )

    async def _bridge_worker_loop(self, *, gateway, state, chat_id, acct_id):
        while True:
            batched = await self._pop_batched_turn(state=state)
            if batched is None:
                return
            parts, reply_with_voice = batched

            await self._ensure_chat_session(gateway=gateway, state=state, acct_id=acct_id, chat_id=chat_id)

            state.busy = True
            prompt_task = asyncio.create_task(gateway.prompt_async(session_id=state.session_id, parts=parts))
            try:
                async with self.typing_while(chat_id=chat_id, acct_id=acct_id):
                    reply = await self._drain_session_events(gateway=gateway, state=state)
                    await gateway.maybe_compact_session(state.session_id)
            finally:
                if prompt_task.done():
                    await prompt_task
                state.busy = False

            if reply:
                await self._deliver_turn_reply(
                    text=reply,
                    include_voice=reply_with_voice,
                    chat_id=chat_id,
                    acct_id=acct_id,
                    platform=state.platform,
                    chat_type=state.chat_type,
                )

    async def _pop_batched_turn(self, *, state):
        def merge_many(messages):
            merged = []
            reply_with_voice = False
            for index, message in enumerate(messages, start=1):
                reply_with_voice = reply_with_voice or message.reply_with_voice
                for item in message.parts:
                    copied = dict(item)
                    if copied.get("type") == "text":
                        copied["text"] = self._queued_followup_label(index, copied["text"]).strip()
                    merged.append(copied)
            return merged, reply_with_voice

        return await self._pop_batched_items(
            state=state,
            merge_one=lambda message: (list(message.parts), message.reply_with_voice),
            merge_many=merge_many,
        )

    async def _interrupt_backend(self, *, session_id, chat_id, acct_id):
        gateway = self._gateway
        if gateway is None or not session_id:
            return
        try:
            await gateway.session_abort(session_id)
        except Exception as exc:
            logger.error(
                "OpenCode abort on stop failed model_id=%s acct_id=%s chat_id=%s error=%s",
                self.model_id,
                acct_id,
                chat_id,
                exc,
                exc_info=exc,
            )

    async def _drop_backend_session(self, *, session_id, chat_id, acct_id):
        gateway = self._gateway
        if gateway is None or not session_id:
            return
        await self._interrupt_backend(session_id=session_id, chat_id=chat_id, acct_id=acct_id)
        gateway.unbind_session(session_id)

    async def _ensure_chat_session(self, *, gateway, state, acct_id, chat_id):
        if state.session_id:
            gateway.bind_session(session_id=state.session_id, acct_id=acct_id, chat_id=chat_id)
            return state.session_id
        stored = load_session_id(self.storage_dir, acct_id, chat_id)
        if stored:
            info = await gateway.session_get(stored)
            if info is not None:
                state.session_id = stored
                gateway.bind_session(session_id=stored, acct_id=acct_id, chat_id=chat_id)
                return stored
        session_id = await gateway.session_create(title="station:%s:%s" % (acct_id, chat_id))
        state.session_id = session_id
        save_session_id(self.storage_dir, acct_id, chat_id, session_id)
        gateway.bind_session(session_id=session_id, acct_id=acct_id, chat_id=chat_id)
        return session_id

    async def _drain_session_events(self, *, gateway, state):
        if not state.session_id:
            return ""

        queue = gateway.session_queue(state.session_id)
        text_parts = {}
        text_order = []
        extra_lines = []
        user_message_ids = set()
        part_message_ids = {}

        while True:
            event = await wait(queue.get(), TURN_TIMEOUT_S, "opencode session events")
            fatal = self._fatal_retry_message(event)
            if fatal is not None:
                logger.error("OpenCode fatal retry session_id=%s error=%s", state.session_id, fatal)
                await gateway.session_abort(state.session_id)
                await self._discard_until_idle(queue)
                extra_lines.append("OpenCode error: %s" % fatal)
                return self._join_reply(text_parts, text_order, extra_lines, user_message_ids, part_message_ids) or "OpenCode error"
            reply = self._consume_event(
                event=event,
                text_parts=text_parts,
                text_order=text_order,
                extra_lines=extra_lines,
                user_message_ids=user_message_ids,
                part_message_ids=part_message_ids,
            )
            if reply is not None:
                return reply

    def _fatal_retry_message(self, event):
        if event.type != "session.retry.scheduled":
            return None
        error = event.payload.get("error")
        if error is None:
            return None
        message = self._format_opencode_error(error)
        lowered = message.lower()
        for marker in (
            "cannot connect",
            "unable to connect",
            "socket",
            "econn",
            "ehostunreach",
            "fetch failed",
            "timed out",
            "timeout",
            "enotfound",
            "network",
            "closed unexpectedly",
            "other side closed",
        ):
            if marker in lowered:
                return message
        return None

    async def _discard_until_idle(self, queue):
        loop = asyncio.get_running_loop()
        deadline = loop.time() + 5
        while loop.time() < deadline:
            timeout = deadline - loop.time()
            if timeout <= 0:
                return
            try:
                event = await asyncio.wait_for(queue.get(), timeout=timeout)
            except asyncio.TimeoutError:
                return
            if event.type in {"session.execution.succeeded", "session.execution.failed", "session.execution.interrupted"}:
                return

    def _consume_event(self, *, event, text_parts, text_order, extra_lines, user_message_ids, part_message_ids):

        event_type = event.type
        payload = event.payload

        if event_type == "session.text.delta":
            message_id = ext_str("session.text.delta assistantMessageID", payload.get("assistantMessageID"))
            delta = ext_str("session.text.delta delta", payload.get("delta"), strip=False)
            if message_id and delta:
                if message_id not in text_parts:
                    text_parts[message_id] = ""
                    text_order.append(message_id)
                text_parts[message_id] += delta
            return None

        if event_type == "session.text.ended":
            message_id = ext_str("session.text.ended assistantMessageID", payload.get("assistantMessageID"))
            text = ext_str("session.text.ended text", payload.get("text"), strip=False)
            if message_id and text:
                if message_id not in text_parts:
                    text_order.append(message_id)
                text_parts[message_id] = text
            return None

        if event_type == "session.execution.failed":
            extra_lines.append("OpenCode error: %s" % self._format_opencode_error(payload.get("error")))
            return self._join_reply(text_parts, text_order, extra_lines, user_message_ids, part_message_ids)

        if event_type in {"session.execution.succeeded", "session.execution.interrupted"}:
            return (
                self._join_reply(text_parts, text_order, extra_lines, user_message_ids, part_message_ids)
                or "OpenCode turn finished."
            )

        return None

    def _format_opencode_error(self, error):

        if error is None:
            return "OpenCode error"
        if type(error) is dict:
            kind = ext_str("opencode error type", error.get("type"))
            message = ext_str("opencode error message", error.get("message"))
            pieces = [piece for piece in (kind, message) if piece]
            status = error.get("status")
            if status is not None:
                status = ext_int("opencode error status", status)
                pieces.append("status=%s" % status)
            response = error.get("response")
            if type(response) is dict:
                body = response.get("body")
                if body not in (None, ""):
                    pieces.append("body=%s" % body)
            if pieces:
                return " | ".join(pieces)
            return "OpenCode error"
        return ext_str("opencode error", error) or "OpenCode error"

    def _join_reply(self, text_parts, text_order, extra_lines, user_message_ids, part_message_ids):
        parts = []
        for part_id in text_order:
            message_id = part_message_ids.get(part_id, "")
            if message_id and message_id in user_message_ids:
                continue
            text = text_parts.get(part_id, "").strip()
            if text:
                parts.append(text)
        for line in extra_lines:
            cleaned = line.strip()
            if cleaned:
                parts.append(cleaned)
        return "\n\n".join(parts).strip()
