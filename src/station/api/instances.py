import asyncio

from station import logger

MODEL_START_FAILED_REPLY = "The model had issues, please try later."
PROTOTYPE_FAILED_REPLY = "The prototype had issues. You can reach this prototype's support for support. It is shown in the prototype details."

class ModelStartError(Exception):
    pass

def prototype_start_failed(app, prototype_id):
    failed = app.get("prototype_start_failed")
    return failed is not None and prototype_id in failed

async def precheck_prototype(app, prototype_id):
    ready = app.setdefault("prototype_start_ready", set())
    failed = app.setdefault("prototype_start_failed", set())
    if prototype_id in ready:
        return True
    if prototype_id in failed:
        return False
    lock = app.setdefault("_prototype_precheck_lock", asyncio.Lock())
    async with lock:
        if prototype_id in ready:
            return True
        if prototype_id in failed:
            return False
        registry = app.get("tenants")
        tenant = None if registry is None else registry.get(prototype_id)
        if tenant is None:
            failed.add(prototype_id)
            logger.error("prototype start skipped prototype_id=%s", prototype_id)
            return False
        instance = create_tenant_instance(
            app,
            tenant,
            prototype_id=prototype_id,
            model_id="precheck-%s" % prototype_id,
            model_settings={},
        )
        try:
            await instance.start()
        except asyncio.CancelledError:
            raise
        except Exception as e:
            failed.add(prototype_id)
            logger.error(
                "prototype start failed prototype_id=%s kind=%s error=%s",
                prototype_id,
                tenant.kind,
                e,
            )
            if tenant.kind == "cursor":
                logger.error("cursor start skipped prototype_id=%s", prototype_id)
            else:
                logger.error("prototype start skipped prototype_id=%s kind=%s", prototype_id, tenant.kind)
            try:
                await instance.stop()
            except Exception as stop_error:
                logger.error(
                    "unexpected where=prototype_precheck_stop prototype_id=%s error=%s",
                    prototype_id,
                    stop_error,
                    exc_info=stop_error,
                )
            return False
        ready.add(prototype_id)
        try:
            await instance.stop()
        except Exception as stop_error:
            logger.error(
                "unexpected where=prototype_precheck_stop prototype_id=%s error=%s",
                prototype_id,
                stop_error,
                exc_info=stop_error,
            )
        return True

def instance_kind(app, instance):
    registry = app.get("tenants")
    if registry is None:
        return ""
    tenant = registry.get(instance.prototype_id)
    if tenant is None:
        return ""
    return tenant.kind

def create_tenant_instance(app, tenant, *, prototype_id, model_id, model_settings=None):
    cls = tenant.prototype_class
    instance = cls(
        app,
        prototype_id,
        model_id,
        model_settings=model_settings,
        config_file=tenant.config_file,
        secret_file=tenant.secret_file,
    )
    if tenant.client_context is not None:
        instance.client_context = tenant.client_context
    return instance

def mark_needs_manual(app, where):
    app["needs_manual"] = True
    logger.critical("needs_manual handle=administrator where=%s", where)

def note_instance_ok(app, model_id):
    state = app["instance_runtime"].get(model_id)
    if state is not None:
        state["unexpected"] = 0

def note_instance_unexpected(app, model_id):
    runtime = app["instance_runtime"]
    state = runtime.get(model_id)
    if state is None:
        state = {"unexpected": 0}
        runtime[model_id] = state
    state["unexpected"] = state["unexpected"] + 1
    return state["unexpected"]

def should_isolate(count, app):
    return count >= app["config"].server.isolate_after

async def isolate_instance(app, model_id, instance=None):
    instances = app["instances"]
    instance_lock = app["instance_lock"]
    db = app["db"]
    async with instance_lock:
        current = instances.pop(model_id, None)
        app["instance_runtime"].pop(model_id, None)
    target = current or instance
    if target is None:
        return True
    try:
        await target.stop()
    except Exception as e:
        logger.error("unexpected where=instance isolate stop model_id=%s error=%s", model_id, e, exc_info=e)
        mark_needs_manual(app, "instance model_id=%s" % model_id)
        try:
            await db.release_model_lock_from_app(app, model_id)
        except Exception as inner:
            logger.error(
                "unexpected where=instance isolate unlock model_id=%s error=%s",
                model_id,
                inner,
                exc_info=inner,
            )
        return False
    try:
        await db.release_model_lock_from_app(app, model_id)
    except Exception as e:
        logger.error(
            "unexpected where=instance isolate unlock model_id=%s error=%s",
            model_id,
            e,
            exc_info=e,
        )
    logger.error("instance isolated model_id=%s", model_id)
    return True
