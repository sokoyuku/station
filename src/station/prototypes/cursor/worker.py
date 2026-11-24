from dataclasses import dataclass, field

from station import logger
from station.timeouts import TURN_TIMEOUT_S, wait
from station.prototypes.boundary import ext_str
from station.prototypes.bridge_worker import PrototypeBridgeWorker
from station.prototypes.chat_history import list_started_chats, load_session_record, save_session_id

from .client import active_cursor_run, cancel_active_cursor_runs, create_cursor_agent, cursor_user_message, resume_cursor_agent


@dataclass(slots=True)
class PendingTurnMessage:
    message: str
    image_paths: list = field(default_factory=list)
    reply_with_voice: bool = False


class CursorWorker(PrototypeBridgeWorker):
    def _bridge_worker_error_text(self, error):
        return "Cursor bridge error: %s" % error

    async def _halt_sessions(self):
        gateway = self._gateway
        if gateway is None or gateway.client is None:
            return
        settings = self._build_settings()
        if not settings.api_key:
            return
        for chat in list_started_chats(self.storage_dir):
            acct_id = chat.get("acct_id") or ""
            chat_id = chat.get("chat_id") or ""
            agent_id = self._load_agent_id(acct_id=acct_id, chat_id=chat_id, workspace=settings.workspace_dir)
            if not agent_id:
                continue
            try:
                await cancel_active_cursor_runs(gateway=gateway, settings=settings, agent_id=agent_id)
            except Exception as e:
                logger.error(
                    "Cursor halt failed model_id=%s acct_id=%s chat_id=%s agent_id=%s error=%s",
                    self.model_id,
                    acct_id,
                    chat_id,
                    agent_id,
                    e,
                    exc_info=e,
                )

    async def _resume_sessions(self):
        gateway = self._gateway
        if gateway is None or gateway.client is None:
            return
        settings = self._build_settings()
        if not settings.api_key:
            return
        for chat in list_started_chats(self.storage_dir):
            acct_id = chat.get("acct_id") or ""
            chat_id = chat.get("chat_id") or ""
            agent_id = self._load_agent_id(acct_id=acct_id, chat_id=chat_id, workspace=settings.workspace_dir)
            if not agent_id:
                continue
            try:
                agent = await resume_cursor_agent(gateway=gateway, settings=settings, agent_id=agent_id)
            except Exception as e:
                logger.error(
                    "Cursor resume on restore failed model_id=%s acct_id=%s chat_id=%s agent_id=%s error=%s",
                    self.model_id,
                    acct_id,
                    chat_id,
                    agent_id,
                    e,
                    exc_info=e,
                )
                continue
            key = gateway.chat_key(acct_id, chat_id)
            gateway.agents[key] = agent
            state = await self._get_chat_state(acct_id=acct_id, chat_id=chat_id)
            state.session_id = agent.agent_id

    async def _enqueue_turn(self, *, message, image_paths, chat_id, acct_id, platform="", chat_type="", reply_with_voice=False):
        state = await self._get_chat_state(acct_id=acct_id, chat_id=chat_id)
        gateway = await self._ensure_gateway()
        await self._enqueue_pending(
            state=state,
            gateway=gateway,
            chat_id=chat_id,
            acct_id=acct_id,
            item=PendingTurnMessage(message=message, image_paths=list(image_paths or []), reply_with_voice=reply_with_voice),
            platform=platform,
            chat_type=chat_type,
        )

    async def _bridge_worker_loop(self, *, gateway, state, chat_id, acct_id):
        waited = set()
        while True:
            run = await self._find_active_run(gateway=gateway, state=state, chat_id=chat_id, acct_id=acct_id)
            if run is not None and run.id not in waited:
                waited.add(run.id)
                logger.info(
                    "Cursor run attached model_id=%s acct_id=%s chat_id=%s agent_id=%s run_id=%s",
                    self.model_id,
                    acct_id,
                    chat_id,
                    run.agent_id,
                    run.id,
                )
                state.busy = True
                try:
                    async with self.typing_while(chat_id=chat_id, acct_id=acct_id):
                        reply = await self._finish_run(run, attached=True)
                finally:
                    state.busy = False
                if reply:
                    await self._deliver_turn_reply(
                        text=reply,
                        include_voice=False,
                        chat_id=chat_id,
                        acct_id=acct_id,
                        platform=state.platform,
                        chat_type=state.chat_type,
                    )
                continue

            payload = await self._pop_batched_turn(state=state)
            if payload is None:
                return

            message, image_paths, reply_with_voice = payload
            state.busy = True
            try:
                async with self.typing_while(chat_id=chat_id, acct_id=acct_id):
                    reply = await self._run_turn(
                        gateway=gateway,
                        state=state,
                        chat_id=chat_id,
                        acct_id=acct_id,
                        message=message,
                        image_paths=image_paths,
                        reply_with_voice=reply_with_voice,
                    )
            finally:
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
            text_parts = []
            image_paths = []
            reply_with_voice = False
            for index, item in enumerate(messages, start=1):
                text_parts.append(self._queued_followup_label(index, item.message))
                image_paths.extend(item.image_paths)
                reply_with_voice = reply_with_voice or item.reply_with_voice
            return "\n\n".join(text_parts), image_paths, reply_with_voice

        return await self._pop_batched_items(
            state=state,
            merge_one=lambda message: (message.message, list(message.image_paths), message.reply_with_voice),
            merge_many=merge_many,
        )

    async def _find_active_run(self, *, gateway, state, chat_id, acct_id):
        from cursor_sdk import CursorAgentError

        settings = self._build_settings()
        if not settings.api_key:
            return None
        try:
            agent = await self._open_chat_agent(
                gateway=gateway,
                state=state,
                settings=settings,
                chat_id=chat_id,
                acct_id=acct_id,
            )
            if agent is None:
                return None
            return await active_cursor_run(
                gateway=gateway,
                settings=settings,
                agent_id=agent.agent_id,
            )
        except CursorAgentError:
            return None

    async def _restore_pending(self, *, state, message, image_paths, reply_with_voice):
        async with state.guard:
            state.pending_messages.insert(
                0,
                PendingTurnMessage(
                    message=message,
                    image_paths=list(image_paths or []),
                    reply_with_voice=reply_with_voice,
                ),
            )

    async def _finish_run(self, run, *, attached):
        async def _collect():
            from cursor_sdk.types import SDKStatusMessage

            notes = []
            stream = run.observe() if attached else run.events()
            async for event in stream:
                event_message = event.sdk_message
                if type(event_message) is not SDKStatusMessage:
                    continue
                if event_message.message or event_message.status:
                    notes.append("%s %s" % (event_message.status, event_message.message))
            result = await run.wait()
            return result, notes

        result, status_notes = await wait(_collect(), TURN_TIMEOUT_S, "cursor run")
        return await self._run_reply(run, result, status_notes)

    async def _run_reply(self, run, result, status_notes):
        status = ext_str("cursor run status", result.status)
        text = ext_str("cursor run result", result.result, strip=False).strip()
        if status == "finished":
            return text or "Cursor turn finished."
        detail = text
        if status_notes:
            detail = (detail + " " + " | ".join(status_notes)).strip()
        if not detail:
            try:
                detail = await run.conversation_json()
            except Exception as e:
                detail = str(e)
        logger.error(
            "Cursor run %s model_id=%s run_id=%s detail=%s",
            status,
            self.model_id,
            run.id,
            detail[:2000],
        )
        if text:
            return text
        if status_notes:
            return " ".join(status_notes).strip()
        if status == "error":
            return "Cursor run failed."
        return "Cursor run %s." % status

    async def _run_turn(self, *, gateway, state, chat_id, acct_id, message, image_paths, reply_with_voice=False):
        from cursor_sdk import CursorAgentError

        settings = self._build_settings()
        if not settings.api_key:
            return self._secret_prompt()
        try:
            agent = await self._ensure_chat_agent(
                gateway=gateway,
                state=state,
                settings=settings,
                chat_id=chat_id,
                acct_id=acct_id,
            )
            payload = cursor_user_message(text=message, image_paths=image_paths)
            run = await agent.send(payload)
            logger.info(
                "Cursor run started model_id=%s acct_id=%s chat_id=%s agent_id=%s run_id=%s",
                self.model_id,
                acct_id,
                chat_id,
                agent.agent_id,
                run.id,
            )
            return await self._finish_run(run, attached=False)
        except CursorAgentError as e:
            if "already has active run" in str(e):
                active = await self._find_active_run(
                    gateway=gateway,
                    state=state,
                    chat_id=chat_id,
                    acct_id=acct_id,
                )
                if active is not None:
                    await self._restore_pending(
                        state=state,
                        message=message,
                        image_paths=image_paths,
                        reply_with_voice=reply_with_voice,
                    )
                    logger.info(
                        "Cursor run attached model_id=%s acct_id=%s chat_id=%s agent_id=%s run_id=%s",
                        self.model_id,
                        acct_id,
                        chat_id,
                        active.agent_id,
                        active.id,
                    )
                    return await self._finish_run(active, attached=True)
            logger.error(
                "CursorAgentError model_id=%s acct_id=%s chat_id=%s retryable=%s status=%s request_id=%s error=%s",
                self.model_id,
                acct_id,
                chat_id,
                e.is_retryable,
                e.status,
                e.request_id,
                e,
            )
            return self._bridge_worker_error_text(e)

    async def _open_chat_agent(self, *, gateway, state, settings, chat_id, acct_id):
        from cursor_sdk import CursorAgentError

        key = gateway.chat_key(acct_id, chat_id)
        agent = gateway.agents.get(key)
        if agent is not None:
            return agent

        agent_id = state.session_id or self._load_agent_id(acct_id=acct_id, chat_id=chat_id, workspace=settings.workspace_dir)
        if not agent_id:
            return None
        try:
            agent = await resume_cursor_agent(gateway=gateway, settings=settings, agent_id=agent_id)
        except CursorAgentError as e:
            logger.error(
                "Cursor resume failed model_id=%s acct_id=%s chat_id=%s agent_id=%s error=%s",
                self.model_id,
                acct_id,
                chat_id,
                agent_id,
                e,
            )
            raise

        gateway.agents[key] = agent
        state.session_id = agent.agent_id
        self._save_agent_id(
            acct_id=acct_id,
            chat_id=chat_id,
            agent_id=agent.agent_id,
            workspace=settings.workspace_dir,
        )
        return agent

    async def _ensure_chat_agent(self, *, gateway, state, settings, chat_id, acct_id):
        agent = await self._open_chat_agent(
            gateway=gateway,
            state=state,
            settings=settings,
            chat_id=chat_id,
            acct_id=acct_id,
        )
        if agent is not None:
            return agent

        agent = await create_cursor_agent(
            gateway=gateway,
            settings=settings,
            acct_id=acct_id,
            chat_id=chat_id,
        )
        key = gateway.chat_key(acct_id, chat_id)
        gateway.agents[key] = agent
        state.session_id = agent.agent_id
        self._save_agent_id(
            acct_id=acct_id,
            chat_id=chat_id,
            agent_id=agent.agent_id,
            workspace=settings.workspace_dir,
        )
        return agent

    def _load_agent_id(self, *, acct_id, chat_id, workspace):
        record = load_session_record(self.storage_dir, acct_id, chat_id)
        agent_id = ext_str("chat session_id", record.get("session_id"))
        stored_workspace = ext_str("chat workspace", record.get("workspace"))
        if not agent_id or stored_workspace != workspace:
            return ""
        return agent_id

    def _save_agent_id(self, *, acct_id, chat_id, agent_id, workspace):
        save_session_id(self.storage_dir, acct_id, chat_id, agent_id, workspace=workspace)

    async def _interrupt_backend(self, *, session_id, chat_id, acct_id):
        gateway = self._gateway
        if gateway is None:
            return
        key = gateway.chat_key(acct_id, chat_id)
        agent = gateway.agents.pop(key, None)
        if agent is None:
            return
        try:
            await agent.close()
        except Exception as e:
            logger.error(
                "unexpected where=cursor_agent_stop model_id=%s acct_id=%s chat_id=%s error=%s",
                self.model_id,
                acct_id,
                chat_id,
                e,
                exc_info=e,
            )

    async def _drop_backend_session(self, *, session_id, chat_id, acct_id):
        await self._interrupt_backend(session_id=session_id, chat_id=chat_id, acct_id=acct_id)
