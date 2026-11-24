from aiohttp import web
from .reception import handle_reception, handle_event
from .webrtc import handle_webrtc_offer
from .token import handle_admin_update_prototype_token, handle_admin_add_prototype
from .reload import handle_admin_reload_config
from .memory_admin import handle_admin_memory
from .voice_inject import handle_admin_inject_voice_pcm
from .admin_console import (
    handle_admin_overview,
    handle_admin_pause_model,
    handle_admin_remove_model,
    handle_admin_resume_model,
    handle_admin_rewind_model,
    handle_admin_start_model,
    handle_admin_stop_model,
)
from .admin_config import handle_admin_get_config, handle_admin_set_config
from .admin_commands import handle_admin_rekey, handle_admin_sync_tokens
from .admin_logs import handle_admin_logs
from station import hopmetrics
from .model import (
    handle_reload_model,
    handle_remove_model,
    handle_start_model,
    handle_stop_model,
    handle_pause_model,
    handle_resume_model,
    handle_rewind_model,
)

async def handle_metrics(request):
    body = hopmetrics.render_prometheus("station")
    return web.Response(text=body, content_type="text/plain; version=0.0.4")

async def handle_hopmetrics(request):
    return web.json_response({"hops": hopmetrics.snapshot()})

class Routes:
    def __init__(self, app):
        app.router.add_get("/metrics", handle_metrics)
        app.router.add_get("/hopmetrics", handle_hopmetrics)
        app.router.add_post("/reception/{model_id}", handle_reception)
        app.router.add_post("/event/{model_id}", handle_event)
        app.router.add_post("/webrtc/offer/{model_id}", handle_webrtc_offer)
        app.router.add_post("/station/reload_model/{model_id}", handle_reload_model)
        app.router.add_post("/station/remove_model/{model_id}", handle_remove_model)
        app.router.add_post("/station/start_model/{model_id}", handle_start_model)
        app.router.add_post("/station/stop_model/{model_id}", handle_stop_model)
        app.router.add_post("/station/pause_model/{model_id}", handle_pause_model)
        app.router.add_post("/station/resume_model/{model_id}", handle_resume_model)
        app.router.add_post("/station/rewind_model/{model_id}", handle_rewind_model)
        app.router.add_get("/admin/overview", handle_admin_overview)
        app.router.add_get("/admin/config", handle_admin_get_config)
        app.router.add_post("/admin/config", handle_admin_set_config)
        app.router.add_get("/admin/logs", handle_admin_logs)
        app.router.add_post("/admin/sync_tokens", handle_admin_sync_tokens)
        app.router.add_post("/admin/rekey", handle_admin_rekey)
        app.router.add_post("/admin/prototype_token", handle_admin_update_prototype_token)
        app.router.add_post("/admin/prototype", handle_admin_add_prototype)
        app.router.add_post("/admin/reload_config", handle_admin_reload_config)
        app.router.add_post("/admin/model/start/{model_id}", handle_admin_start_model)
        app.router.add_post("/admin/model/stop/{model_id}", handle_admin_stop_model)
        app.router.add_post("/admin/model/pause/{model_id}", handle_admin_pause_model)
        app.router.add_post("/admin/model/resume/{model_id}", handle_admin_resume_model)
        app.router.add_post("/admin/model/rewind/{model_id}", handle_admin_rewind_model)
        app.router.add_post("/admin/model/remove/{model_id}", handle_admin_remove_model)
        app.router.add_post("/admin/memory/{model_id}", handle_admin_memory)
        app.router.add_post("/admin/inject_voice_pcm", handle_admin_inject_voice_pcm)
