import os
import re
from pathlib import Path

from aiohttp import web

from station.api.token import _is_localhost_request
from station.errors import ExternalError

_PAGE = re.compile(r"^(connect|config|prototypes|platforms|logs|overview|models|prototypes/\d+/models)$")


def resolve_station_app_dist():
    return Path(__file__).resolve().parent.parent / "web"


def _document(dist):
    station = dist / "station.html"
    if station.is_file():
        return station
    return dist / "index.html"


def mount_station_app(app):
    dist = resolve_station_app_dist()
    index = _document(dist)
    if not index.is_file():
        raise ExternalError(
            "STATION_APP is enabled but the console build is missing at %s" % (dist / "index.html")
        )
    root = dist.resolve()

    async def handle(request):
        if not _is_localhost_request(request):
            return web.json_response({"result": 1, "msg": "Forbidden", "data": None}, status=403)
        rel = request.path.strip("/")
        if not rel:
            return web.FileResponse(index)
        target = (dist / rel).resolve()
        if target.is_file() and (target == root or root in target.parents):
            return web.FileResponse(target)
        if _PAGE.fullmatch(rel):
            return web.FileResponse(index)
        raise web.HTTPNotFound()

    app.router.add_get("/", handle)
    app.router.add_get("/{path:.*}", handle)
    app["station_app_dist"] = os.fspath(root)
