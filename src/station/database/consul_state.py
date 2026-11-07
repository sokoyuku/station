from station.client.consul import fetch_model
from station.client.context import update_client_context_prototype
from station import logger

def _norm_addr(value):
    if value is None:
        return ""
    return str(value).strip().rstrip("/")

def _norm_id(value):
    if value is None:
        return ""
    return str(value).replace("-", "").strip().lower()

def _acct_ids(model):
    accts = None if model is None else model.get("accts")
    if not accts:
        return set()
    out = set()
    for acct in accts:
        if isinstance(acct, dict):
            acct_id = _norm_id(acct.get("acct_id"))
            if acct_id:
                out.add(acct_id)
    return out

def _acct_rich(model):
    for acct in model.get("accts") or []:
        if isinstance(acct, dict) and (acct.get("acct_token") or acct.get("acct_type") or acct.get("acct_username")):
            return True
    return False

def model_conflicts(local, remote):
    if not local or not remote:
        return False
    if local.get("prototype_id") != remote.get("prototype_id"):
        return True
    if _norm_addr(local.get("conductor_address")) != _norm_addr(remote.get("conductor_address")):
        return True
    if _norm_id(local.get("account_id")) != _norm_id(remote.get("account_id")):
        return True
    if _acct_ids(local) != _acct_ids(remote):
        return True
    local_settings = local.get("settings") if isinstance(local.get("settings"), dict) else {}
    remote_settings = remote.get("settings") if isinstance(remote.get("settings"), dict) else {}
    if ("__enc__" in local_settings) == ("__enc__" in remote_settings) and local_settings != remote_settings:
        return True
    return False

async def apply_consul_model(db, model_id, remote):
    local = await db.get_model(model_id)
    if local and _acct_rich(local) and not _acct_rich(remote):
        updated = dict(local)
        changed = False
        if _norm_addr(local.get("conductor_address")) != _norm_addr(remote.get("conductor_address")):
            logger.error(
                "conductor address conflict model_id=%s local=%s consul=%s; using consul",
                model_id,
                local.get("conductor_address"),
                remote.get("conductor_address"),
            )
            updated["conductor_address"] = remote.get("conductor_address") or ""
            changed = True
        if remote.get("prototype_id") is not None and local.get("prototype_id") != remote.get("prototype_id"):
            logger.error(
                "model prototype conflict model_id=%s local=%s consul=%s; using consul",
                model_id,
                local.get("prototype_id"),
                remote.get("prototype_id"),
            )
            updated["prototype_id"] = remote.get("prototype_id")
            changed = True
        if changed:
            await db.put_model(model_id, updated)
            stored = await db.get_model(model_id)
            if stored is not None:
                return stored
        return updated
    if model_conflicts(local, remote):
        logger.error(
            "model conflict model_id=%s local_prototype=%s consul_prototype=%s local_conductor=%s consul_conductor=%s; consul replaces local",
            model_id,
            local.get("prototype_id"),
            remote.get("prototype_id"),
            local.get("conductor_address"),
            remote.get("conductor_address"),
        )
    await db.put_model(model_id, remote)
    remote_model_id = remote.get("model_id")
    if remote_model_id and remote_model_id != model_id:
        await db.put_model(remote_model_id, remote)
    stored = await db.get_model(model_id)
    if stored is None and remote_model_id:
        stored = await db.get_model(remote_model_id)
    if stored is None:
        return remote
    return stored

async def fetch_and_apply_model(db, model_id, session, consul_url, token):
    remote = await fetch_model(session=session, consul_url=consul_url, token=token, model_id=model_id)
    return await apply_consul_model(db, model_id, remote)

def merge_consul_prototype(local, remote):
    merged = dict(local) if local else {}
    conflicts = []
    for key, value in remote.items():
        if key in merged and merged.get(key) != value:
            conflicts.append(key)
        merged[key] = value
    if "call_support" not in remote and "call_support" in merged:
        conflicts.append("call_support")
        merged.pop("call_support", None)
    return merged, conflicts

async def apply_consul_prototype(db, remote, ctx=None):
    remote = dict(remote)
    prototype_id = remote.get("prototype_id")
    local = None
    if db is not None and prototype_id is not None:
        local = await db.get_prototype_info(prototype_id)
    merged, conflicts = merge_consul_prototype(local, remote)
    if conflicts:
        logger.error(
            "prototype conflict prototype_id=%s fields=%s; consul replaces local",
            prototype_id,
            ",".join(conflicts),
        )
    if db is not None:
        await db.put_prototype(merged)
    if ctx is not None:
        update_client_context_prototype(ctx, merged)
    return merged
