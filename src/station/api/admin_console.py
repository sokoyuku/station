import time

from aiohttp import web

from station import hopmetrics, logger
from station.api.http import log_caught
from station.api.instances import isolate_instance
from station.api.token import _require_admin
from station.errors import ExternalError


def _model_id(request):
    model_id = (request.match_info.get("model_id") or "").strip()
    if not model_id:
        raise ExternalError("Missing model_id")
    return model_id


def _poller_state(task):
    if task is None:
        return "absent"
    if task.cancelled():
        return "cancelled"
    if task.done():
        return "finished"
    return "running"


def _kind_for(registry, prototype_id):
    if registry is None or prototype_id is None:
        return ""
    try:
        prototype_id = int(prototype_id)
    except (TypeError, ValueError):
        return ""
    tenant = registry.get(prototype_id)
    if tenant is None:
        return ""
    return tenant.kind or ""


def _int(value):
    try:
        return int(value or 0)
    except (TypeError, ValueError):
        return 0


async def _live_instances(app):
    async with app["instance_lock"]:
        live = {}
        for model_id, instance in app["instances"].items():
            live[str(model_id)] = {
                "started": bool(getattr(instance, "_started", False)),
                "pending": _int(getattr(instance, "_pending_count", 0)),
                "prototype_id": getattr(instance, "prototype_id", None),
            }
        runtime = {}
        for model_id, state in app["instance_runtime"].items():
            unexpected = 0
            if isinstance(state, dict):
                unexpected = _int(state.get("unexpected"))
            runtime[str(model_id)] = unexpected
    return live, runtime


def _prototypes(app):
    registry = app["tenants"]
    primary_id = registry.primary.id
    rows = []
    for prototype_id in registry.prototype_ids:
        tenant = registry.get(prototype_id)
        if tenant is None:
            continue
        ctx = tenant.client_context
        rows.append(
            {
                "id": tenant.id,
                "kind": tenant.kind,
                "is_local": bool(tenant.is_local),
                "ava": bool(tenant.ava),
                "reply_to": bool(tenant.reply_to),
                "primary": tenant.id == primary_id,
                "version": None if ctx is None else ctx.prototype_version,
                "token_set": bool((tenant.token or "").strip()),
                "config_file": tenant.config_file or "",
                "secret_file_set": bool(tenant.secret_file),
            }
        )
    return rows


def _model_row(row, live, runtime, registry):
    model_id = str(row.get("model_id") or "")
    state = live.get(model_id)
    accts = row.get("accts")
    created = row.get("created_at")
    return {
        "model_id": model_id,
        "prototype_id": row.get("prototype_id"),
        "kind": _kind_for(registry, row.get("prototype_id")),
        "version": row.get("version"),
        "chat_version": row.get("chat_version"),
        "created_at": "" if created is None else str(created),
        "loaded": state is not None,
        "started": bool(state and state["started"]),
        "pending": 0 if state is None else state["pending"],
        "unexpected": runtime.get(model_id, 0),
        "account_count": len(accts) if isinstance(accts, list) else 0,
    }


def _account_row(row, tasks):
    acct_id = str(row.get("acct_id") or "")
    return {
        "acct_id": acct_id,
        "model_id": str(row.get("model_id") or ""),
        "prototype_id": row.get("prototype_id"),
        "acct_type": str(row.get("acct_type") or ""),
        "username": str(row.get("username") or ""),
        "poller": _poller_state(tasks.get(acct_id)),
    }


async def handle_admin_overview(request):
    denied = _require_admin(request)
    if denied is not None:
        return denied

    app = request.app
    config = app["config"]
    registry = app["tenants"]
    live, runtime = await _live_instances(app)

    models_error = ""
    try:
        raw_models = await app["db"].list_models()
    except Exception as e:
        log_caught(logger, e, where="admin overview models")
        raw_models = []
        models_error = "Failed to list models"

    accounts_error = ""
    try:
        raw_accounts = await app["db"].list_local_accounts()
    except Exception as e:
        log_caught(logger, e, where="admin overview accounts")
        raw_accounts = []
        accounts_error = "Failed to list accounts"

    lc = app.get("local_conductor")
    tasks = {}
    if lc is not None:
        tasks = getattr(lc, "_poller_tasks", None) or {}

    models = [_model_row(row, live, runtime, registry) for row in raw_models if isinstance(row, dict)]
    known = {row["model_id"] for row in models}
    for model_id, state in live.items():
        if model_id in known:
            continue
        models.append(
            {
                "model_id": model_id,
                "prototype_id": state.get("prototype_id"),
                "kind": _kind_for(registry, state.get("prototype_id")),
                "version": None,
                "chat_version": None,
                "created_at": "",
                "loaded": True,
                "started": bool(state.get("started")),
                "pending": state.get("pending") or 0,
                "unexpected": runtime.get(model_id, 0),
                "account_count": 0,
            }
        )
    models.sort(key=lambda row: row["model_id"])

    accounts = [_account_row(row, tasks) for row in raw_accounts if isinstance(row, dict)]
    recorder = app.get("recorder")
    log_level = recorder.get_log_level() if recorder is not None else ""
    started_at = app.get("started_at") or time.time()
    server = config.server
    data = {
        "station": {
            "env": config.env,
            "host": config.host or "",
            "port": config.port,
            "needs_manual": bool(app.get("needs_manual")),
            "db_backend": config.database.backend,
            "primary_prototype_id": registry.primary.id,
            "local_conductor": lc is not None,
            "log_level": log_level,
            "uptime_seconds": max(0, int(time.time() - started_at)),
            "inbound_workers": server.inbound_workers,
            "inbound_queue": server.inbound_queue,
            "inbound_batch": server.inbound_batch,
            "isolate_after": server.isolate_after,
            "at_least_once": bool(server.inbound_at_least_once),
        },
        "hops": hopmetrics.snapshot(),
        "prototypes": _prototypes(app),
        "models": models,
        "models_error": models_error,
        "accounts": accounts,
        "accounts_error": accounts_error,
    }
    return web.json_response({"result": 0, "msg": "ok", "data": data})


async def _locked_instance(request):
    denied = _require_admin(request)
    if denied is not None:
        return denied, None, None, None
    app = request.app
    model_id = _model_id(request)
    if not await app["db"].ensure_model_lock(app, model_id):
        raise ExternalError("Model is active on another station", status=503)
    return None, app, model_id, app["instance_lock"]


async def handle_admin_start_model(request):
    denied, app, model_id, instance_lock = await _locked_instance(request)
    if denied is not None:
        return denied
    error = None
    instance = None
    async with instance_lock:
        instance = app["instances"].get(model_id)
        if instance is None:
            raise ExternalError("Model instance not found", status=404)
        try:
            await instance.start()
        except Exception as e:
            error = e
    if error is not None:
        if not isinstance(error, ExternalError):
            log_caught(logger, error, where="admin start_model model_id=%s" % model_id)
        await isolate_instance(app, model_id, instance)
        raise error
    return web.json_response({"result": 0, "msg": "Model started", "data": None})


async def _admin_call(request, call, *, where):
    denied, app, model_id, instance_lock = await _locked_instance(request)
    if denied is not None:
        return denied
    async with instance_lock:
        instance = app["instances"].get(model_id)
        if instance is None:
            raise ExternalError("Model instance not found", status=404)
        try:
            return await call(instance, request)
        except ExternalError:
            raise
        except Exception as e:
            log_caught(logger, e, where="%s model_id=%s" % (where, model_id))
            raise


async def handle_admin_pause_model(request):
    return await _admin_call(request, lambda instance, req: instance.handle_pause(req), where="admin pause_model")


async def handle_admin_resume_model(request):
    return await _admin_call(request, lambda instance, req: instance.handle_resume(req), where="admin resume_model")


async def handle_admin_rewind_model(request):
    return await _admin_call(request, lambda instance, req: instance.handle_rewind(req), where="admin rewind_model")


async def handle_admin_stop_model(request):
    denied, app, model_id, instance_lock = await _locked_instance(request)
    if denied is not None:
        return denied
    async with instance_lock:
        instance = app["instances"].get(model_id)
        if instance is None:
            raise ExternalError("Model instance not found", status=404)
        try:
            await instance.stop()
        except Exception as e:
            log_caught(logger, e, where="admin stop_model model_id=%s" % model_id)
            raise
    return web.json_response({"result": 0, "msg": "Model stopped", "data": None})


async def handle_admin_remove_model(request):
    denied = _require_admin(request)
    if denied is not None:
        return denied
    app = request.app
    model_id = _model_id(request)
    db = app["db"]
    if not await db.ensure_model_lock(app, model_id):
        raise ExternalError("Model is active on another station", status=503)
    await db.delete_model(model_id)
    async with app["instance_lock"]:
        instance = app["instances"].pop(model_id, None)
        app["instance_runtime"].pop(model_id, None)
    if instance is not None:
        try:
            await instance.stop()
        except Exception as e:
            log_caught(logger, e, where="admin remove_model stop model_id=%s" % model_id)
    await db.release_model_lock_from_app(app, model_id)
    return web.json_response({"result": 0, "msg": "Model removed", "data": None})
