import asyncio
import os

from station import logger
from station.prototypes.launch_settings import bypass_loopback_proxy


class CursorGateway:
    def __init__(self, client):
        self.client = client
        self.proc = None
        self.agents = {}

    def chat_key(self, acct_id, chat_id):
        return "%s:%s" % (acct_id, chat_id)

    async def close(self):
        for key, agent in list(self.agents.items()):
            try:
                await agent.close()
            except Exception as e:
                logger.error("unexpected where=cursor_agent_close key=%s error=%s", key, e, exc_info=e)
        self.agents.clear()
        client = self.client
        self.client = None
        if client is not None:
            await client.aclose()


def _load_sdk():
    try:
        from cursor_sdk import AgentOptions, AsyncClient, CursorAgentError, LocalAgentOptions, SDKImage, UserMessage
    except ImportError as e:
        raise RuntimeError("cursor-sdk is required: pip install cursor-sdk") from e
    return AgentOptions, AsyncClient, CursorAgentError, LocalAgentOptions, SDKImage, UserMessage


def _retryable_cursor_error(error):
    if error.is_retryable:
        return True
    return (error.status or 0) >= 500


async def list_cursor_models(*, gateway, api_key):
    return await gateway.client.models.list(api_key=api_key)


async def start_cursor_gateway(*, workspace, state_root):
    os.makedirs(workspace, exist_ok=True)
    os.makedirs(state_root, exist_ok=True)
    bypass_loopback_proxy()
    _, AsyncClient, _, _, _, _ = _load_sdk()
    client = await AsyncClient.launch_bridge(workspace=workspace, state_root=state_root)
    return CursorGateway(client)


async def create_cursor_agent(*, gateway, settings, acct_id, chat_id):
    _, _, CursorAgentError, LocalAgentOptions, _, _ = _load_sdk()
    last_error = None
    for attempt in range(4):
        try:
            return await gateway.client.create_agent(
                model=settings.model,
                api_key=settings.api_key,
                name="station:%s:%s" % (acct_id, chat_id),
                local=LocalAgentOptions(cwd=settings.workspace_dir),
            )
        except CursorAgentError as e:
            last_error = e
            if attempt == 3 or not _retryable_cursor_error(e):
                raise
            logger.error(
                "Cursor create_agent retry attempt=%s error=%s",
                attempt + 1,
                e,
            )
            await asyncio.sleep(min(0.5 * (2 ** attempt), 4.0))
    raise last_error


_ACTIVE_RUN_STATUS = "running"
_RUN_LIST_PAGE_LIMIT = 5


async def cancel_active_cursor_runs(*, gateway, settings, agent_id):
    cancelled = []
    page = await gateway.client.agents.list_runs(
        agent_id,
        cwd=settings.workspace_dir,
        api_key=settings.api_key,
        runtime="local",
        limit=20,
    )
    for _ in range(_RUN_LIST_PAGE_LIMIT):
        for run in page.items:
            if run.status != _ACTIVE_RUN_STATUS or not run.id:
                continue
            await gateway.client.agents.cancel_run(run.id, agent_id=agent_id)
            cancelled.append(run.id)
        if not page.next_cursor:
            break
        page = await page.get_next_page()
    return cancelled


async def active_cursor_run(*, gateway, settings, agent_id):
    page = await gateway.client.agents.list_runs(
        agent_id,
        cwd=settings.workspace_dir,
        api_key=settings.api_key,
        runtime="local",
        limit=20,
    )
    for _ in range(_RUN_LIST_PAGE_LIMIT):
        for run in page.items:
            if run.status == _ACTIVE_RUN_STATUS and run.id:
                return run
        if not page.next_cursor:
            return None
        page = await page.get_next_page()
    return None


async def resume_cursor_agent(*, gateway, settings, agent_id):
    AgentOptions, _, _, LocalAgentOptions, _, _ = _load_sdk()
    return await gateway.client.resume_agent(
        agent_id,
        AgentOptions(
            api_key=settings.api_key,
            model=settings.model,
            local=LocalAgentOptions(cwd=settings.workspace_dir),
        ),
    )


def cursor_user_message(*, text, image_paths):
    _, _, _, _, SDKImage, UserMessage = _load_sdk()
    images = []
    for path in image_paths:
        images.append(SDKImage.from_file(path))
    if images:
        return UserMessage(text=text or "The user sent image attachment(s). Please inspect them.", images=images)
    return text
