import asyncio
import json
import os
import random
import re
import socket
from urllib.parse import quote

import aiohttp

from station import logger
from station.timeouts import TURN_TIMEOUT_S, http_timeout, read_line, ws_timeout
from station.errors import ExternalError
from station.prototypes.boundary import ext_dict, ext_list, ext_str
from station.prototypes.gateway_process import SessionEvent, SessionEventBus, close_subprocess, gateway_subprocess_kwargs
from station.prototypes.launch_settings import apply_local_provider_env, child_process_env

LISTENING_RE = re.compile(r"server listening on (https?://\S+)", re.IGNORECASE)
PASSWORD_RE = re.compile(r"server password:?\s*(\S+)", re.IGNORECASE)
PORT_RANGE_START = 19200
PORT_RANGE_END = 19990
AUTO_APPROVE_MODES = {"always", "allow", "auto"}

def _api_path(path):
    if path.startswith("/api/") or path == "/api":
        return path
    if not path.startswith("/"):
        path = "/" + path
    return "/api" + path

def _port_open(hostname, port):
    host = "127.0.0.1" if hostname in {"0.0.0.0", "::", ""} else hostname
    try:
        with socket.create_connection((host, port), timeout=0.15):
            return True
    except OSError:
        return False

def _iter_candidate_ports(hostname, *, preferred=0):
    if preferred > 0:
        return [preferred]

    ports = list(range(PORT_RANGE_START, PORT_RANGE_END))
    random.shuffle(ports)
    available = []
    for port in ports:
        if _port_open(hostname, port):
            continue
        available.append(port)
        if len(available) >= 24:
            break
    if not available:
        raise RuntimeError(
            "No free OpenCode serve port found in %d-%d" % (PORT_RANGE_START, PORT_RANGE_END - 1)
        )
    return available

class OpenCodeAuthError(RuntimeError):
    pass

class OpenCodeServerProcess:
    def __init__(self, *, settings):
        self.settings = settings
        self.base_url = ""
        self.proc = None
        self._session = None
        self._stdout_task = None
        self._stderr_task = None
        self._event_task = None
        self._ready = asyncio.Event()
        self._announced_password = ""
        self._events = SessionEventBus()
        self._chat_by_session = {}
        self._integrations_ready = False

    async def start(self, *, timeout_s=30.0):
        if self.proc is not None and self.proc.returncode is None and self.base_url:
            return

        workspace = os.path.expanduser(self.settings.workspace_dir)
        os.makedirs(workspace, exist_ok=True)

        env = child_process_env(self.settings.extra_env)
        if self.settings.uses_local_llm():
            env = apply_local_provider_env(env, base_url=self.settings.local_llm_base_url)
        if self.settings.server_password:
            env["OPENCODE_PASSWORD"] = self.settings.server_password

        candidates = _iter_candidate_ports(self.settings.hostname, preferred=self.settings.port)
        last_error = None
        for port in candidates:
            try:
                await self._start_on_port(port=port, workspace=workspace, env=env, timeout_s=timeout_s)
                logger.info("OpenCode serve started base_url=%s workspace=%s", self.base_url, workspace)
                return
            except OpenCodeAuthError:
                raise
            except (OSError, RuntimeError, asyncio.TimeoutError) as exc:
                last_error = exc
                logger.warning("OpenCode serve failed on port %s: %s", port, exc)
                await self._reset_process()
        raise RuntimeError("OpenCode serve failed to start: %s" % last_error)

    async def _start_on_port(self, *, port, workspace, env, timeout_s):
        command = [
            *self.settings.command,
            "serve",
            "--hostname",
            self.settings.hostname,
            "--port",
            str(port),
            *self.settings.serve_args,
        ]
        self._ready.clear()
        self._announced_password = ""
        self.base_url = "http://%s:%d" % (self.settings.hostname, port)
        self.proc = await asyncio.create_subprocess_exec(
            *command,
            cwd=workspace,
            env=env,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
            **gateway_subprocess_kwargs(),
        )
        self._stdout_task = asyncio.create_task(self._read_stdout(), name="opencode-stdout")
        self._stderr_task = asyncio.create_task(self._read_stderr(), name="opencode-stderr")
        await self._wait_listening(port=port, timeout_s=min(timeout_s, 12.0))
        await self._wait_healthy(timeout_s=min(timeout_s, 12.0))
        if self.proc.returncode is not None:
            raise RuntimeError("OpenCode serve exited early with code %s" % self.proc.returncode)
        self._ready.set()
        self._session = aiohttp.ClientSession(
            auth=self._basic_auth(),
            timeout=ws_timeout(sock_read=TURN_TIMEOUT_S),
        )
        self._event_task = asyncio.create_task(self._read_events(), name="opencode-events")

    async def _reset_process(self):
        self._integrations_ready = False
        tasks = [self._stdout_task, self._stderr_task, self._event_task]
        self._stdout_task = None
        self._stderr_task = None
        self._event_task = None
        for task in tasks:
            if task is not None:
                task.cancel()
        session = self._session
        self._session = None
        if session is not None:
            await session.close()
        proc = self.proc
        self.proc = None
        self.base_url = ""
        self._announced_password = ""
        await close_subprocess(proc, tasks, terminate_timeout_s=2.0)

    async def close(self):
        await self._reset_process()
        self._events.clear()
        self._chat_by_session.clear()

    def bind_session(self, *, session_id, acct_id, chat_id):
        self._chat_by_session[session_id] = (acct_id, chat_id)

    def unbind_session(self, session_id):
        session_id = ext_str("OpenCode session id", session_id)
        if session_id:
            self._chat_by_session.pop(session_id, None)

    def session_queue(self, session_id):
        return self._events.queue(session_id)

    async def session_create(self, *, title=""):
        body = {}
        if title:
            body["title"] = title
        if self.settings.agent:
            body["agent"] = self.settings.agent
        model = self.settings.model_payload()
        if model:
            body["model"] = {"id": model["modelID"], "providerID": model["providerID"]}
            if "variant" in model:
                body["model"]["variant"] = model["variant"]
        if self.settings.approvals_mode in AUTO_APPROVE_MODES:
            body["permissions"] = [{"action": "*", "resource": "*", "effect": "allow"}]

        result = await self._request_dict("POST", "/session", json_body=body)
        session_id = ext_str("OpenCode session id", result.get("id"))
        if not session_id:
            raise ExternalError("OpenCode session create must return non-empty str id")
        logger.info("OpenCode session created session_id=%s", session_id)
        return session_id

    async def session_get(self, session_id):
        session_id = ext_str("OpenCode session id", session_id)
        if not session_id:
            return None
        result = await self._request_json(
            "GET",
            "/session/%s" % quote(session_id, safe=""),
            allow_statuses=(404,),
        )
        if result is None:
            return None
        return ext_dict("OpenCode session", result)

    async def session_messages(self, session_id):
        session_id = ext_str("OpenCode session id", session_id)
        if not session_id:
            return None
        result = await self._request_json(
            "GET",
            "/session/%s/message" % quote(session_id, safe=""),
            allow_statuses=(404,),
        )
        if result is None:
            return None
        if type(result) is dict:
            messages = result.get("data")
            if messages is None:
                return None
            return ext_list("OpenCode session messages", messages)
        return ext_list("OpenCode session messages", result)

    async def session_summarize(self, session_id):
        session_id = ext_str("OpenCode session id", session_id)
        if not session_id:
            return False
        if self._session is None or not self.base_url:
            raise RuntimeError("OpenCode server is not running")
        path = _api_path("/session/%s/compact" % quote(session_id, safe=""))
        async with self._session.request("POST", "%s%s" % (self.base_url, path), json={}) as resp:
            if resp.status in {404, 405}:
                return False
            text = await resp.text()
            if resp.status >= 400:
                logger.info(
                    "OpenCode summarize failed session_id=%s status=%s error=%s",
                    session_id,
                    resp.status,
                    text[:300],
                )
                return False
            return True

    async def maybe_compact_session(self, session_id):
        try:
            messages = await self.session_messages(session_id)
            if messages is None:
                return
            if len(messages) <= 40:
                return
            summarized = await self.session_summarize(session_id)
            if summarized:
                logger.info("OpenCode session summarized session_id=%s messages=%s", session_id, len(messages))
        except Exception as e:
            logger.error(
                "unexpected where=opencode compact session_id=%s error=%s",
                session_id,
                e,
                exc_info=e,
            )

    async def prompt_async(self, *, session_id, parts):
        await self._request_json(
            "POST",
            "/session/%s/prompt" % quote(session_id, safe=""),
            json_body=self._prompt_body(parts),
            expect_json=False,
        )

    async def session_abort(self, session_id):
        session_id = ext_str("OpenCode session id", session_id)
        if not session_id or self._session is None or not self.base_url:
            return
        try:
            await self._request_json(
                "POST",
                "/session/%s/interrupt" % quote(session_id, safe=""),
                json_body={},
                expect_json=False,
            )
        except (aiohttp.ClientError, OSError, RuntimeError, asyncio.TimeoutError) as e:
            logger.error("OpenCode session abort failed session_id=%s error=%s", session_id, e)

    def _prompt_body(self, parts):
        texts = []
        files = []
        for part in parts:
            if part["type"] == "text":
                text = part["text"]
                if text:
                    texts.append(text)
            elif part["type"] == "file":
                item = {"uri": part["url"]}
                name = part["filename"]
                if name:
                    item["name"] = name
                files.append(item)
            else:
                raise ExternalError("OpenCode prompt part type %s" % part["type"])
        body = {"text": "\n\n".join(texts)}
        if files:
            body["files"] = files
        return body

    async def reply_permission(self, *, session_id, request_id, reply="always"):
        await self._request_json(
            "POST",
            "/session/%s/permission/%s/reply" % (quote(session_id, safe=""), quote(request_id, safe="")),
            json_body={"decision": reply},
            expect_json=False,
        )

    def _location_params(self):
        return {"directory": os.path.abspath(os.path.expanduser(self.settings.workspace_dir))}

    async def _warm_integrations(self):
        if self._integrations_ready:
            return
        await self._request_json("GET", "/integration", params=self._location_params())
        self._integrations_ready = True

    async def _integration_request(self, method, path, *, json_body=None, expect_json=True):
        params = self._location_params()
        await self._warm_integrations()
        try:
            return await self._request_json(
                method,
                path,
                json_body=json_body,
                params=params,
                expect_json=expect_json,
            )
        except RuntimeError as exc:
            if "IntegrationNotFoundError" not in str(exc):
                raise
            self._integrations_ready = False
            await self._warm_integrations()
            return await self._request_json(
                method,
                path,
                json_body=json_body,
                params=params,
                expect_json=expect_json,
            )

    async def integration_get(self, integration_id):
        payload = await self._integration_request(
            "GET",
            "/integration/%s" % quote(integration_id, safe=""),
        )
        payload = ext_dict("OpenCode integration", payload)
        data = payload.get("data")
        if data is None:
            return payload
        return ext_dict("OpenCode integration", data)

    async def integration_connected(self, integration_id):
        info = await self.integration_get(integration_id)
        connections = info.get("connections")
        if connections is None:
            return False
        connections = ext_list("OpenCode integration connections", connections)
        for item in connections:
            item = ext_dict("OpenCode integration connection", item)
            status = item.get("status")
            if status is None:
                continue
            status = ext_dict("OpenCode connection status", status)
            if ext_str("OpenCode connection status", status.get("status")) != "needs_auth":
                return True
        return False

    async def connect_key(self, *, integration_id, key):
        await self._integration_request(
            "POST",
            "/integration/%s/connect/key" % quote(integration_id, safe=""),
            json_body={"key": key},
            expect_json=False,
        )

    async def remove_integration_credentials(self, integration_id):
        payload = await self._request_json("GET", "/credential")
        if payload is None:
            return
        if type(payload) is dict:
            payload = payload.get("data")
        entries = ext_list("OpenCode credentials", payload)
        for entry in entries:
            entry = ext_dict("OpenCode credential", entry)
            if ext_str("OpenCode credential integrationID", entry.get("integrationID")) != integration_id:
                continue
            credential_id = ext_str("OpenCode credential id", entry.get("id"))
            if not credential_id:
                continue
            await self._request_json(
                "DELETE",
                "/credential/%s" % quote(credential_id, safe=""),
                expect_json=False,
            )

    async def oauth_method_id(self, integration_id):
        info = await self.integration_get(integration_id)
        methods = ext_list("OpenCode integration methods", info.get("methods"))
        chosen = ""
        for method in methods:
            method = ext_dict("OpenCode integration method", method)
            if ext_str("OpenCode integration method type", method.get("type")) != "oauth":
                continue
            method_id = ext_str("OpenCode integration method id", method.get("id"))
            if not method_id:
                continue
            if not chosen:
                chosen = method_id
            label = ext_str("OpenCode integration method label", method.get("label")).lower()
            if "supergrok" in label or "subscription" in label or "headless" in label:
                return method_id
        if chosen:
            return chosen
        raise RuntimeError("OpenCode has no OAuth method for integration %s" % integration_id)

    async def oauth_begin(self, *, integration_id, method_id):
        payload = ext_dict(
            "OpenCode oauth begin",
            await self._integration_request(
                "POST",
                "/integration/%s/connect/oauth" % quote(integration_id, safe=""),
                json_body={"methodID": method_id},
            ),
        )
        data = payload.get("data")
        if data is None:
            data = payload
        return ext_dict("OpenCode oauth attempt", data)

    async def oauth_status(self, *, integration_id, attempt_id):
        payload = ext_dict(
            "OpenCode oauth status",
            await self._integration_request(
                "GET",
                "/integration/%s/connect/oauth/%s" % (quote(integration_id, safe=""), quote(attempt_id, safe="")),
            ),
        )
        data = payload.get("data")
        if data is None:
            data = payload
        return ext_dict("OpenCode oauth status", data)

    async def oauth_complete(self, *, integration_id, attempt_id, code=None):
        body = {}
        if code is not None:
            body["code"] = code
        await self._integration_request(
            "POST",
            "/integration/%s/connect/oauth/%s/complete" % (
                quote(integration_id, safe=""),
                quote(attempt_id, safe=""),
            ),
            json_body=body,
            expect_json=False,
        )

    def _basic_auth(self):
        password = self._announced_password or self.settings.server_password
        if not password:
            return None
        return aiohttp.BasicAuth("opencode", password)

    def _note_password(self, text):
        match = PASSWORD_RE.search(text)
        if not match:
            return
        self._announced_password = match.group(1)

    async def _wait_listening(self, *, port, timeout_s):
        loop = asyncio.get_running_loop()
        deadline = loop.time() + timeout_s
        needle = ":%d" % port
        while loop.time() < deadline:
            if self.proc.returncode is not None:
                raise RuntimeError("OpenCode serve exited early with code %s" % self.proc.returncode)
            if self._ready.is_set() and needle in self.base_url:
                return
            await asyncio.sleep(0.05)
        raise RuntimeError("OpenCode serve did not publish listening URL for port %d" % port)

    async def _wait_healthy(self, *, timeout_s):
        loop = asyncio.get_running_loop()
        deadline = loop.time() + timeout_s
        last_error = None
        while loop.time() < deadline:
            if self.proc.returncode is not None:
                raise RuntimeError(
                    "OpenCode serve exited early with code %s: %s" % (self.proc.returncode, last_error)
                )
            try:
                async with aiohttp.ClientSession(auth=self._basic_auth(), timeout=http_timeout(total=10)) as session:
                    async with session.get("%s/api/info" % self.base_url, timeout=http_timeout(total=10)) as resp:
                        text = await resp.text()
                        if resp.status == 401 and self._announced_password:
                            raise OpenCodeAuthError("OpenCode info status 401")
                        if resp.status >= 400:
                            last_error = RuntimeError("OpenCode info status %s" % resp.status)
                        else:
                            payload = ext_dict("OpenCode info payload", json.loads(text))
                            version = payload.get("version")
                            if type(version) is str and version.strip():
                                return
                            last_error = RuntimeError("OpenCode info missing version")
            except (aiohttp.ClientError, asyncio.TimeoutError, OSError, json.JSONDecodeError, ExternalError) as exc:
                last_error = exc
            await asyncio.sleep(0.1)
        raise RuntimeError("OpenCode server did not become healthy: %s" % last_error)

    async def _request_json(self, method, path, *, json_body=None, params=None, expect_json=True, allow_statuses=()):
        if self._session is None or not self.base_url:
            raise RuntimeError("OpenCode server is not running")
        path = _api_path(path)
        async with self._session.request(
            method,
            "%s%s" % (self.base_url, path),
            json=json_body,
            params=params,
            timeout=http_timeout(),
        ) as resp:
            text = await resp.text()
            if resp.status >= 400:
                if resp.status in allow_statuses:
                    return None
                raise RuntimeError("OpenCode %s %s failed (%d): %s" % (method, path, resp.status, text[:500]))
            if not expect_json or not text.strip():
                return None
            try:
                payload = json.loads(text)
            except json.JSONDecodeError as exc:
                raise ExternalError("OpenCode %s %s returned non-JSON: %s" % (method, path, text[:500])) from exc
            if type(payload) is dict and set(payload.keys()) == {"data"}:
                return payload["data"]
            return payload

    async def _request_dict(self, method, path, *, json_body=None, params=None):
        return ext_dict(
            "OpenCode %s %s" % (method, path),
            await self._request_json(method, path, json_body=json_body, params=params),
        )

    def _note_listening(self, text):
        match = LISTENING_RE.search(text)
        if not match:
            return
        url = match.group(1).rstrip("/")
        if self.base_url and url != self.base_url.rstrip("/"):
            logger.warning("OpenCode listening URL mismatch expected=%s actual=%s", self.base_url, url)
            return
        self.base_url = url
        self._ready.set()

    async def _read_stdout(self):
        stream = self.proc.stdout
        while True:
            line = await read_line(stream, "opencode stdout")
            if not line:
                break
            text = line.decode("utf-8", "replace").rstrip()
            if text:
                if "server password" not in text.lower():
                    logger.info("opencode serve stdout: %s", text)
                self._note_password(text)
                self._note_listening(text)

    async def _read_stderr(self):
        stream = self.proc.stderr
        while True:
            line = await read_line(stream, "opencode stderr")
            if not line:
                break
            text = line.decode("utf-8", "replace").rstrip()
            if text:
                if "server password" not in text.lower():
                    logger.info("opencode serve stderr: %s", text)
                self._note_password(text)
                self._note_listening(text)

    async def _read_events(self):
        url = "%s%s" % (self.base_url, _api_path("/event"))
        while True:
            session = self._session
            if session is None:
                return
            try:
                async with session.get(url, headers={"Accept": "text/event-stream"}) as resp:
                    if resp.status >= 400:
                        body = await resp.text()
                        raise RuntimeError("OpenCode event stream failed (%d): %s" % (resp.status, body[:300]))
                    buffer = ""
                    async for raw in resp.content.iter_any():
                        if not raw:
                            continue
                        buffer += raw.decode("utf-8", "replace")
                        while "\n" in buffer:
                            line, buffer = buffer.split("\n", 1)
                            await self._handle_sse_line(line.rstrip("\r"))
                    if buffer.strip():
                        await self._handle_sse_line(buffer.rstrip("\r"))
            except asyncio.CancelledError:
                raise
            except Exception as e:
                if self._session is None:
                    return
                logger.error("unexpected where=opencode_event_stream url=%s error=%s", url, e, exc_info=e)
                await asyncio.sleep(1.0)

    async def _handle_sse_line(self, line):
        if not line.startswith("data:"):
            return
        raw = line[5:].strip()
        if not raw or raw == "[DONE]":
            return

        try:
            frame = json.loads(raw)
        except json.JSONDecodeError as exc:
            raise ExternalError("OpenCode SSE malformed JSON: %s" % raw[:400]) from exc
        frame = ext_dict("OpenCode SSE frame", frame)

        event_type = ext_str("OpenCode SSE type", frame.get("type"))
        payload = frame.get("data")
        if payload is None:
            payload = {}
        payload = ext_dict("OpenCode SSE data", payload)

        if event_type in {"permission.asked", "permission.updated"} or event_type.endswith("permission.asked"):
            request_id = payload.get("id")
            if request_id is None:
                request_id = payload.get("requestID")
            request_id = ext_str("OpenCode permission id", request_id)
            session_id = ext_str("OpenCode SSE sessionID", payload.get("sessionID"))
            if request_id and session_id and self.settings.approvals_mode in AUTO_APPROVE_MODES:
                logger.info("Auto-approving OpenCode permission request_id=%s type=%s", request_id, event_type)
                try:
                    await self.reply_permission(session_id=session_id, request_id=request_id, reply="always")
                except (aiohttp.ClientError, RuntimeError, TypeError) as e:
                    logger.error("OpenCode permission auto-approve failed request_id=%s error=%s", request_id, e)
                return

        session_id = ext_str("OpenCode SSE sessionID", payload.get("sessionID"))
        if not session_id:
            return
        await self._events.put(SessionEvent(type=event_type, session_id=session_id, payload=payload))
