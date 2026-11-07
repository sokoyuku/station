import json
from pathlib import Path

from station.errors import ExternalError
from station.prototypes.boundary import ext_dict, ext_list, ext_str
from station.prototypes.cli_tools import CLI_TOOL_NAMES, CLI_TOOL_SCHEMAS
from station.prototypes.hermes.config import HERMES_ROOT
from station.prototypes.grok.hermes_supervisor import HERMES_TOOL_NAMES, HERMES_TOOL_SCHEMAS
from station.prototypes.chat_history import (
    chat_history_view,
    find_persisted_attachment,
    list_started_chats,
    list_unread_messages,
    load_messages,
    save_messages,
    sender_label,
)

CHECK_UNREAD_LIMIT = 50
SKILL_VIEW_CHARS = 12000
SKILL_TOOL_NAMES = frozenset({"skill_view"})

# Server-side Responses API tools. They run on xAI and are not local functions.
RESPONSE_SERVER_TOOLS = [
    {"type": "web_search"},
    {"type": "x_search"},
    {"type": "code_interpreter"},
    {"type": "image_generation"},
]

# Voice sessions accept these server-side tools. file_search and mcp need ids this prototype does not have.
VOICE_SERVER_TOOLS = [
    {"type": "web_search"},
    {"type": "x_search"},
]

LIVE_FUNCTION_NAMES = frozenset({"list_chats", "get_chat_history", "send_message", "delegate_task"})

_LIVE_SESSION_INSTRUCTIONS = (
    "You can list started chats, read one chat with get_chat_history, and send_message into a started chat. "
    "send_message cannot start a chat. "
    "Prior chat history is not included. Call get_chat_history with acct_id and chat_id when you need it. "
    "Attachments in history are listed as [type file_id=...]. You do not receive the file. "
    "Call delegate_task to hand any task to a text turn, including understanding or acting on a file. "
    "Include the file_id in the task when it needs a file. "
    "delegate_task returns immediately. That turn calls send_text when this call should hear the result. "
    "Web search and X search run on the server. Do not call those as functions."
)

_LIVE_HISTORY_DESCRIPTION = (
    "Read one started chat and match a person by display name or first name and by how they described themselves. "
    "Senders include the user_id to pass to send_message. "
    "Messages include attachments as [type file_id=...]. "
    "You do not receive the file. Call delegate_task and include the file_id when a file needs to be understood or used."
)

DELEGATE_TASK_TOOL = {
    "type": "function",
    "function": {
        "name": "delegate_task",
        "description": "Hand a task to a text turn. The task can be anything, including a file operation. Include any file_id the task needs. Returns immediately and does not wait for the result. The text turn acts with its tools and calls send_text when this call should hear the result.",
        "parameters": {
            "type": "object",
            "properties": {
                "task": {"type": "string"},
            },
            "required": ["task"],
        },
    },
}

SERVER_SIDE_CALL_TYPES = frozenset({
    "web_search_call",
    "x_search_call",
    "code_interpreter_call",
    "file_search_call",
    "mcp_call",
    "image_generation_call",
})

_CHAT_TURN_INSTRUCTIONS = (
    "A turn can have any number of rounds. A round is either tool calls or one final assistant message with no tool calls. "
    "The final message is delivered to the user who started the turn and ends the turn. "
    "Use send_message to tell users about progress or anything worth noticing. "
    "If that user only needs one message, do not call send_message; put that reply in the final message. "
    "Text in a tool-call response is not shown to anyone. "
    "send_message cannot start a chat. "
    "Prior chat history is not included in the turn. Call get_chat_history with acct_id and chat_id when you need it. "
    "Attachments in history are listed as [type file_id=...]. "
    "Call view_attachment with that file_id to look at one. "
    "The file bytes are not in history until you call view_attachment. "
    "Web search, X search, code execution, and image generation run on the server. Do not call those as functions. "
    "Each round tells you when unread messages remain. "
    "Call check_unread_messages to read them from newest to oldest, including messages that arrive during this turn. "
    "Keep calling it until remaining is 0. Read all unread messages before the final reply. "
    "Messages fetched at the start of a turn are already read. "
    "Messages returned by check_unread_messages are marked read for this turn. "
)

def _skills_root():
    return (HERMES_ROOT / "skills").resolve()


def _frontmatter_name_desc(path):
    name = path.parent.name
    desc = ""
    try:
        text = path.read_text(encoding="utf-8", errors="replace")
    except OSError:
        return name, desc
    if not text.startswith("---"):
        return name, desc
    end = text.find("\n---", 3)
    block = text[3:end] if end != -1 else ""
    for line in block.splitlines():
        if line.startswith("name:"):
            name = line.split(":", 1)[1].strip().strip("\"'")
        elif line.startswith("description:"):
            desc = line.split(":", 1)[1].strip().strip("\"'")
    return name, desc


def skill_index_text():
    root = _skills_root()
    if not root.is_dir():
        return ""
    lines = []
    for path in sorted(root.rglob("SKILL.md")):
        rel = path.parent.relative_to(root).as_posix()
        name, desc = _frontmatter_name_desc(path)
        lines.append("- %s: %s" % (rel, desc or name))
    if not lines:
        return ""
    return "Skill index. Call skill_view with the path to load one, then follow it.\n" + "\n".join(lines)


def _skill_file(path_arg):
    root = _skills_root()
    raw = (path_arg or "").strip().strip("/")
    parts = Path(raw).parts
    if not raw or raw.startswith("/") or ".." in parts:
        raise ExternalError("skill path is invalid")
    candidate = (root / raw).resolve()
    try:
        inside = candidate == root or candidate.is_relative_to(root)
    except ValueError:
        inside = False
    if not inside:
        raise ExternalError("skill path is invalid")
    if candidate.is_dir():
        candidate = candidate / "SKILL.md"
    if not candidate.is_file():
        raise ExternalError("skill not found")
    return candidate


def skill_view(arguments):
    arguments = ext_dict("arguments", arguments if arguments is not None else {})
    path = ext_str("path", arguments.get("path"))
    file = _skill_file(path)
    try:
        text = file.read_text(encoding="utf-8", errors="replace")
    except OSError as exc:
        raise ExternalError("skill could not be read") from exc
    truncated = len(text) > SKILL_VIEW_CHARS
    if truncated:
        text = text[:SKILL_VIEW_CHARS]
    root = _skills_root()
    refs = []
    if file.name == "SKILL.md":
        for child in sorted(file.parent.rglob("*")):
            if child.is_file() and child != file:
                refs.append(child.relative_to(root).as_posix())
    return {
        "ok": True,
        "path": file.relative_to(root).as_posix(),
        "text": text,
        "truncated": truncated,
        "references": refs[:40],
    }


SKILL_TOOL_SCHEMAS = [
    {
        "type": "function",
        "function": {
            "name": "skill_view",
            "description": (
                "Read one skill document and follow it for the user's goal. "
                "path is a skill index path such as email/himalaya, or a reference file under that skill."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "path": {"type": "string"},
                },
                "required": ["path"],
            },
        },
    },
]

_SKILL_TURN_INSTRUCTIONS = (
    "A skill index is below. When the user gives a goal, pick the matching skill, call skill_view, and follow that document with your own tools. "
    "The user supplies the goal, confirmation, and credentials. Do not ask the user for the procedure. "
    "Use hermes_delegate only if the skill requires the hermes worker and that worker is up. "
)

_CLI_TURN_INSTRUCTIONS = (
    "A round has a finite timeout. cli_run starts a shell command and returns immediately. Its output is in output_path, not in the tool result. "
    "Call cli_status to read that file later. "
    "If a tool cannot finish within the round timeout, leave it running and continue. It keeps running in the background. "
    "sleep and wait stop at the round timeout. If you need longer, call sleep or wait again on the next round. "
    "When output is long, it shows the start, how many characters were omitted, and the end. The middle stays in output_path. "
)

_HERMES_TURN_INSTRUCTIONS = (
    "hermes_delegate hands a workspace task to hermes-agent and returns immediately with task_id. "
    "Supervise it with hermes_status, hermes_steer, hermes_approve, hermes_interrupt, and hermes_result. "
    "hermes_config reads the worker config. Pass model, provider, base_url, toolsets, or config to change it, including which model it uses. "
    "When a hermes task finishes or needs approval, you are told. Do not delegate that same goal again."
)

_LIVE_TURN_INSTRUCTIONS = (
    "A live voice session is open for this chat. "
    "Call send_text to feed text into that session. The session speaks it on the call. "
    "send_text does not replace the final chat reply. "
    "If this turn says the live session delegated a task, call send_text with what the call should hear. "
    "While that call stays open and this turn fetched no messages, the final assistant message is saved in history and is not sent into the chat."
)

SEND_TEXT_TOOL = {
    "type": "function",
    "function": {
        "name": "send_text",
        "description": "Feed text into the open live voice session for this chat. The session speaks it on the call. This does not send a chat message and does not replace the final reply.",
        "parameters": {
            "type": "object",
            "properties": {
                "text": {"type": "string"},
            },
            "required": ["text"],
        },
    },
}

CHAT_TOOLS = [
    {
        "type": "function",
        "function": {
            "name": "list_chats",
            "description": "List chats already started with this model. Use to find another person to message. Each row has acct_id, chat_id, senders, and unread_count.",
            "parameters": {"type": "object", "properties": {}},
        },
    },
    {
        "type": "function",
        "function": {
            "name": "get_chat_history",
            "description": "Read one started chat and match a person by display name or first name and by how they described themselves. Senders include the user_id to pass to send_message. Messages include attachments with file_id. Call view_attachment to look at one.",
            "parameters": {
                "type": "object",
                "properties": {
                    "acct_id": {"type": "string"},
                    "chat_id": {"type": "string"},
                },
                "required": ["acct_id", "chat_id"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "send_message",
            "description": "Send text and optional files into a started chat. Use it to tell a user about progress or anything worth noticing, including the user who started this turn and other users. Do not use this for that user's only or final reply; that must be a normal assistant message with no tool calls, which ends the turn. user_id must be a sender user_id from that chat. files are Station file_ids or local paths. Cannot start a chat.",
            "parameters": {
                "type": "object",
                "properties": {
                    "acct_id": {"type": "string"},
                    "chat_id": {"type": "string"},
                    "user_id": {"type": "string"},
                    "text": {"type": "string"},
                    "files": {"type": "array", "items": {"type": "string"}},
                },
                "required": ["acct_id", "chat_id", "user_id", "text"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "check_unread_messages",
            "description": "Read unread messages across all chats from newest to oldest and mark them read. This includes messages that arrive during the turn. Call this when a round says unread messages remain. Keep calling until remaining is 0. Read all of them before the final reply.",
            "parameters": {"type": "object", "properties": {}},
        },
    },
    {
        "type": "function",
        "function": {
            "name": "view_attachment",
            "description": "Look at one attachment saved in chat history. file_id comes from a history line [type file_id=...] or from get_chat_history attachments. Images are shown to you on the next step. This does not send the file to a user.",
            "parameters": {
                "type": "object",
                "properties": {
                    "file_id": {"type": "string"},
                },
                "required": ["file_id"],
            },
        },
    },
]


def live_session_instructions():
    return _LIVE_SESSION_INSTRUCTIONS


def live_function_tools():
    selected = []
    for item in CHAT_TOOLS:
        function = item.get("function")
        if type(function) is not dict:
            continue
        name = function.get("name")
        if name not in LIVE_FUNCTION_NAMES:
            continue
        if name == "get_chat_history":
            item = {
                "type": "function",
                "function": {
                    "name": "get_chat_history",
                    "description": _LIVE_HISTORY_DESCRIPTION,
                    "parameters": function.get("parameters") or {"type": "object", "properties": {}},
                },
            }
        selected.append(item)
    selected.append(DELEGATE_TASK_TOOL)
    return voice_function_tools(selected)


def voice_function_tools(local_tools):
    converted = []
    for item in local_tools or []:
        if type(item) is not dict:
            continue
        function = item.get("function")
        if type(function) is not dict:
            function = item
        name = function.get("name")
        if type(name) is not str or not name:
            continue
        parameters = function.get("parameters")
        if type(parameters) is not dict:
            parameters = {"type": "object", "properties": {}}
        entry = {
            "type": "function",
            "name": name,
            "parameters": parameters,
        }
        description = function.get("description")
        if type(description) is str and description:
            entry["description"] = description
        converted.append(entry)
    return converted


def function_tool_names(tools):
    names = set()
    for item in tools or []:
        if type(item) is not dict:
            continue
        function = item.get("function")
        if type(function) is dict:
            name = function.get("name")
        else:
            name = item.get("name")
        if type(name) is str and name:
            names.add(name)
    return frozenset(names)


def chat_tools(*, local, live=False):
    tools = list(CHAT_TOOLS)
    if live:
        tools.append(SEND_TEXT_TOOL)
    if local:
        tools = tools + CLI_TOOL_SCHEMAS + HERMES_TOOL_SCHEMAS + SKILL_TOOL_SCHEMAS
    return tools


def turn_instructions(tools):
    names = function_tool_names(tools)
    text = _CHAT_TURN_INSTRUCTIONS
    if "send_text" in names:
        text += _LIVE_TURN_INSTRUCTIONS
    if names & CLI_TOOL_NAMES:
        text += _CLI_TURN_INSTRUCTIONS
    if names & HERMES_TOOL_NAMES:
        text += _HERMES_TURN_INSTRUCTIONS
    if names & SKILL_TOOL_NAMES:
        text += _SKILL_TURN_INSTRUCTIONS
        index = skill_index_text()
        if index:
            text += "\n" + index
    return text


def _tool_arguments(label, arguments):
    if arguments is None or arguments == "":
        arguments = {}
    if type(arguments) is str:
        try:
            arguments = json.loads(arguments)
        except json.JSONDecodeError:
            raise ExternalError("%s is not JSON" % label)
    return ext_dict(label, arguments)


def response_tools(local_tools):
    tools = [dict(item) for item in RESPONSE_SERVER_TOOLS]
    if not local_tools:
        return tools
    for index, item in enumerate(local_tools):
        item = ext_dict("tools[%d]" % index, item)
        if item.get("type") != "function":
            raise ExternalError("tools[%d] must be a function" % index)
        function = item.get("function")
        if type(function) is dict:
            name = ext_str("tools[%d].function.name" % index, function.get("name"))
            description = function.get("description")
            parameters = function.get("parameters")
        else:
            name = ext_str("tools[%d].name" % index, item.get("name"))
            description = item.get("description")
            parameters = item.get("parameters")
        if not name:
            raise ExternalError("tools[%d] name is required" % index)
        converted = {"type": "function", "name": name}
        if type(description) is str and description:
            converted["description"] = description
        if parameters is None:
            parameters = {"type": "object", "properties": {}}
        parameters = ext_dict("tools[%d].parameters" % index, parameters)
        converted["parameters"] = parameters
        tools.append(converted)
    return tools


def parse_tool_calls(message):
    raw = message.get("tool_calls")
    if raw is None:
        return []
    calls = ext_list("tool_calls", raw)
    parsed = []
    for index, item in enumerate(calls):
        item = ext_dict("tool_calls[%d]" % index, item)
        call_id = ext_str("tool_calls[%d].id" % index, item.get("id"))
        if not call_id:
            raise ExternalError("tool_calls[%d].id is required" % index)
        function = item.get("function")
        function = ext_dict("tool_calls[%d].function" % index, function)
        name = ext_str("tool_calls[%d].function.name" % index, function.get("name"))
        if not name:
            raise ExternalError("tool_calls[%d].function.name is required" % index)
        arguments = _tool_arguments(
            "tool_calls[%d].function.arguments" % index,
            function.get("arguments"),
        )
        parsed.append({
            "id": call_id,
            "name": name,
            "arguments": arguments,
            "api": {
                "id": call_id,
                "type": "function",
                "function": {"name": name, "arguments": json.dumps(arguments)},
            },
        })
    return parsed


def parse_response_function_calls(output):
    items = ext_list("response output", output)
    parsed = []
    for index, item in enumerate(items):
        item = ext_dict("response output[%d]" % index, item)
        if item.get("type") != "function_call":
            continue
        call_id = item.get("call_id")
        if not call_id:
            call_id = item.get("id")
        call_id = ext_str("response output[%d].call_id" % index, call_id)
        if not call_id:
            raise ExternalError("response output[%d].call_id is required" % index)
        name = ext_str("response output[%d].name" % index, item.get("name"))
        if not name:
            raise ExternalError("response output[%d].name is required" % index)
        arguments = _tool_arguments(
            "response output[%d].arguments" % index,
            item.get("arguments"),
        )
        parsed.append({
            "id": call_id,
            "name": name,
            "arguments": arguments,
        })
    return parsed


def _required(arguments, key):
    if key not in arguments:
        raise ExternalError("%s is required" % key)
    text = ext_str(key, arguments.get(key))
    if not text:
        raise ExternalError("%s is required" % key)
    return text


def _optional_files(arguments):
    raw = arguments.get("files")
    if raw is None:
        return []
    items = ext_list("files", raw)
    files = []
    for index, item in enumerate(items):
        text = ext_str("files[%d]" % index, item)
        if text:
            files.append(text)
    return files


def tool_result(storage_dir, name, arguments):
    if name == "list_chats":
        return {"ok": True, "chats": list_started_chats(storage_dir)}
    if name == "get_chat_history":
        acct_id = _required(arguments, "acct_id")
        chat_id = _required(arguments, "chat_id")
        view = chat_history_view(storage_dir, acct_id, chat_id)
        if view is None:
            return {"ok": False, "error": "chat has not started"}
        return {"ok": True, "chat": view}
    if name == "send_message":
        acct_id = _required(arguments, "acct_id")
        chat_id = _required(arguments, "chat_id")
        user_id = _required(arguments, "user_id")
        text = _required(arguments, "text")
        files = _optional_files(arguments)
        view = chat_history_view(storage_dir, acct_id, chat_id)
        if view is None:
            return {"ok": False, "error": "chat has not started"}
        known = False
        for sender in view["senders"]:
            if sender.get("user_id") == user_id:
                known = True
                break
        if not known:
            return {"ok": False, "error": "user_id is not in this chat history"}
        return {
            "ok": True,
            "send": {
                "acct_id": acct_id,
                "chat_id": chat_id,
                "user_id": user_id,
                "text": text,
                "files": files,
                "platform": view["platform"],
                "chat_type": view["chat_type"],
            },
        }
    if name == "send_text":
        text = _required(arguments, "text")
        return {"ok": True, "live_text": text}
    if name == "view_attachment":
        file_id = _required(arguments, "file_id")
        record = find_persisted_attachment(storage_dir, file_id)
        if record is None:
            return {"ok": False, "error": "attachment is not in chat history"}
        return {"ok": True, "view": record}
    raise ExternalError("unknown tool")


def format_fetched_messages(messages):
    blocks = []
    for item in messages or []:
        header = "acct_id=%s chat_id=%s" % (item.get("acct_id") or "", item.get("chat_id") or "")
        user_id = item.get("user_id") or ""
        if user_id:
            header += " user_id=%s" % user_id
        label = sender_label(item.get("sender"))
        if label:
            header += " sender=%s" % label
        blocks.append("[%s]\n%s" % (header, item.get("content") or ""))
    return "\n\n".join(blocks)


def format_unread_notice(storage_dir):
    rows = list_unread_messages(storage_dir)
    if not rows:
        return ""
    counts = {}
    for row in rows:
        key = (row["acct_id"], row["chat_id"])
        counts[key] = counts.get(key, 0) + 1
    lines = []
    for (acct_id, chat_id), count in counts.items():
        lines.append("acct_id=%s chat_id=%s unread=%s" % (acct_id, chat_id, count))
    return (
        "Unread messages remain in chat history (%d), newest first. Call check_unread_messages and read all of them from newest to oldest before your final reply. Messages that arrive during this turn stay unread until you check them.\n%s"
        % (len(rows), "\n".join(lines))
    )


def check_unread_page(storage_dir, *, limit=CHECK_UNREAD_LIMIT):
    found = list_unread_messages(storage_dir)
    batch = found[:limit]
    rows = []
    for item in batch:
        row = {
            "id": item["id"],
            "acct_id": item["acct_id"],
            "chat_id": item["chat_id"],
            "content": item["content"],
            "platform": item.get("platform") or "",
            "chat_type": item.get("chat_type") or "",
        }
        if item.get("sender"):
            row["sender"] = item["sender"]
        if item.get("attachments"):
            row["attachments"] = item["attachments"]
        rows.append(row)
    return {
        "ok": True,
        "messages": rows,
        "remaining": len(found) - len(batch),
        "refs": batch,
    }


def record_outbound(storage_dir, acct_id, chat_id, text, attachments=None):
    history = load_messages(storage_dir, acct_id, chat_id)
    message = {"role": "assistant", "content": text}
    if attachments:
        message["attachments"] = attachments
    history.append(message)
    save_messages(storage_dir, acct_id, chat_id, history)
