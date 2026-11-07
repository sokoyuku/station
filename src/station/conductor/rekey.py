import json
import os

import aiohttp
from cryptography.exceptions import InvalidTag

from station import logger
from station.conductor.crypto import (
    ENCRYPTED_TOKEN_PREFIX,
    decrypt_if_encrypted,
    default_private_key_path,
    encrypt_with_public_key,
    load_private_key,
    public_key_token,
)
from station.conductor.platform_types import is_qr_account_type
from station.database.database import create_database
from station.errors import ExternalError
from station.prototypes.boundary import ext_dict, ext_list, ext_str
from station.timeouts import http_timeout

_ATTEMPTS = 3


def _norm_id(value):
    return str(value or "").replace("-", "").strip().lower()


def _private_key_path(config):
    path = (config.local_conductor.private_key_path or "").strip()
    if not path:
        path = default_private_key_path(config.database.path)
    return path


def _load_key(path):
    path = (path or "").strip()
    if not path:
        raise ExternalError("private key path is required")
    if not os.path.isfile(path):
        raise ExternalError("private key not found")
    try:
        return load_private_key(path)
    except OSError as e:
        raise ExternalError("private key not readable") from e


def _open_secret(priv, token):
    if priv is None:
        return None
    try:
        plain, was = decrypt_if_encrypted(priv, token)
    except (ExternalError, ValueError, InvalidTag):
        return None
    except Exception as e:
        logger.error("unexpected where=rekey_decrypt error=%s", e, exc_info=e)
        return None
    if not was:
        return None
    return plain


def _classify_secret(value, *, new_key, old_key, allow_plain):
    value = (value or "").strip()
    if not value and allow_plain:
        return value, "plain"
    if allow_plain and not value.startswith(ENCRYPTED_TOKEN_PREFIX):
        return value, "plain"
    if not value.startswith(ENCRYPTED_TOKEN_PREFIX):
        return value, "neither"
    if _open_secret(new_key, value) is not None:
        return value, "new"
    if old_key is not None:
        plain = _open_secret(old_key, value)
        if plain is not None:
            return encrypt_with_public_key(new_key.public_key(), plain), "old"
    return value, "neither"


def _parse_settings(plain):
    try:
        decoded = json.loads(plain) if plain.strip() else {}
    except json.JSONDecodeError:
        return None
    try:
        return ext_dict("decrypted settings", decoded)
    except ExternalError:
        return None


def _prepare(models, accounts, *, new_key, old_key):
    prepared_models = []
    prepared_accounts = []
    plain_settings = {}
    unreadable_models = []
    unreadable_accounts = []
    updated_models = []
    updated_accounts = []
    changed = False
    for model in models:
        model = ext_dict("model", model)
        model_id = ext_str("model_id", model.get("model_id"))
        if not model_id:
            raise ExternalError("model_id is required")
        settings = model.get("settings")
        if settings is None:
            settings = {}
        settings = ext_dict("settings", settings)
        if not settings:
            prepared_models.append({"model_id": model_id, "settings": {}})
            continue
        enc = settings.get("__enc__")
        if type(enc) is not str or set(settings.keys()) != {"__enc__"}:
            unreadable_models.append(model_id)
            prepared_models.append({"model_id": model_id, "settings": settings})
            continue
        enc = enc.strip()
        new_plain = _open_secret(new_key, enc)
        if new_plain is not None:
            decoded = _parse_settings(new_plain)
            if decoded is not None:
                plain_settings[_norm_id(model_id)] = decoded
            prepared_models.append({"model_id": model_id, "settings": {"__enc__": enc}})
            continue
        old_plain = _open_secret(old_key, enc)
        if old_plain is not None:
            decoded = _parse_settings(old_plain)
            if decoded is None:
                unreadable_models.append(model_id)
                prepared_models.append({"model_id": model_id, "settings": {"__enc__": enc}})
                continue
            plain_settings[_norm_id(model_id)] = decoded
            prepared_models.append({
                "model_id": model_id,
                "settings": {"__enc__": encrypt_with_public_key(new_key.public_key(), old_plain)},
            })
            updated_models.append(model_id)
            changed = True
            continue
        unreadable_models.append(model_id)
        prepared_models.append({"model_id": model_id, "settings": {"__enc__": enc}})

    for account in accounts:
        account = ext_dict("account", account)
        account_id = ext_str("account_id", account.get("account_id"))
        if not account_id:
            raise ExternalError("account_id is required")
        account_type = ext_str("type", account.get("type"))
        token = ext_str("account_token", account.get("account_token"), default="", strip=False).strip()
        server = ext_str("server", account.get("server"), default="", strip=False).strip()
        allow_plain = is_qr_account_type(account_type)
        token_out, token_status = _classify_secret(
            token,
            new_key=new_key,
            old_key=old_key,
            allow_plain=allow_plain,
        )
        server_out, server_status = _classify_secret(
            server,
            new_key=new_key,
            old_key=old_key,
            allow_plain=True if allow_plain or not server else False,
        )
        if token_status == "neither" or server_status == "neither":
            unreadable_accounts.append(account_id)
        if token_status == "old" or server_status == "old":
            updated_accounts.append(account_id)
            changed = True
        prepared_accounts.append({
            "account_id": account_id,
            "account_token": token_out,
            "server": server_out,
        })
    return (
        prepared_models,
        prepared_accounts,
        plain_settings,
        unreadable_models,
        unreadable_accounts,
        updated_models,
        updated_accounts,
        changed,
    )


async def _post(session, consul_url, path, payload):
    url = consul_url.rstrip("/") + path
    try:
        async with session.post(url, json=payload) as resp:
            try:
                body = await resp.json(content_type=None)
            except (aiohttp.ContentTypeError, json.JSONDecodeError, ValueError) as e:
                raise ExternalError("consul response is not json") from e
            return resp.status, ext_dict("consul response", body)
    except aiohttp.ClientError as e:
        raise ExternalError("consul request failed") from e


async def _sign_in(session, consul_url, username, password):
    status, body = await _post(session, consul_url, "/api/sign_in", {
        "telegram_data": {},
        "username": username or "",
        "email": "",
        "password": password or "",
    })
    if status != 200 or body.get("result") != 0:
        msg = body.get("msg")
        if type(msg) is not str or not msg:
            msg = "sign_in failed"
        raise ExternalError(msg)


def _rewrite_accts(accts, secrets):
    out = []
    changed = False
    for acct in accts:
        if type(acct) is not dict:
            out.append(acct)
            continue
        spec = secrets.get(_norm_id(acct.get("acct_id")))
        if spec is None:
            out.append(acct)
            continue
        nxt = dict(acct)
        if "acct_token" in nxt:
            nxt["acct_token"] = spec["account_token"]
        if "server" in nxt:
            nxt["server"] = spec["server"]
        if nxt != acct:
            changed = True
        out.append(nxt)
    return out, changed


async def _sync_station_db(config, *, prepared_accounts, plain_settings):
    backend = (config.database.backend or "").strip().lower()
    if backend == "sqlite" and not os.path.isfile(config.database.path):
        return
    db = await create_database(
        backend=backend,
        db_path=config.database.path,
        dsn=config.database.dsn,
        enable_local_conductor=True,
        wipe_on_restart=False,
    )
    try:
        secrets = {_norm_id(item["account_id"]): item for item in prepared_accounts}
        for row in await db.list_local_accounts():
            spec = secrets.get(_norm_id(row.get("acct_id")))
            if spec is None:
                continue
            await db.update_local_account_secret(
                row["acct_id"],
                encrypted_token=spec["account_token"],
                server=spec["server"],
            )
        for row in await db.list_models():
            settings = row["settings"]
            accts = row["accts"]
            new_settings = settings
            if "__enc__" in settings:
                plain = plain_settings.get(_norm_id(row["model_id"]))
                if plain is not None:
                    new_settings = plain
            new_accts, accts_changed = _rewrite_accts(accts, secrets)
            if new_settings != settings or accts_changed:
                await db.update_model_secrets(row["model_id"], settings=new_settings, accts=new_accts)
    finally:
        await db.close()


def _print_unreadable(models, accounts):
    if models:
        print("unreadable models: %s" % ", ".join(models))
    if accounts:
        print("unreadable accounts: %s" % ", ".join(accounts))


def _row_ids(rows, key):
    out = []
    for row in rows:
        row = ext_dict(key, row)
        item_id = ext_str(key, row.get(key))
        if item_id:
            out.append(item_id)
    return out


def _split_status(ids, updated, unreadable):
    unread = set(unreadable)
    changed = set(updated)
    current = []
    old = []
    neither = []
    for item_id in ids:
        if item_id in unread:
            neither.append(item_id)
        elif item_id in changed:
            old.append(item_id)
        else:
            current.append(item_id)
    return current, old, neither


def _print_ids(label, ids):
    if ids:
        print("%s: %s" % (label, ", ".join(ids)))
    else:
        print("%s:" % label)


def _print_list(models, accounts, updated_models, updated_accounts, bad_models, bad_accounts):
    current_models, old_models, neither_models = _split_status(_row_ids(models, "model_id"), updated_models, bad_models)
    current_accounts, old_accounts, neither_accounts = _split_status(_row_ids(accounts, "account_id"), updated_accounts, bad_accounts)
    _print_ids("current models", current_models)
    _print_ids("old models", old_models)
    _print_ids("unreadable models", neither_models)
    _print_ids("current accounts", current_accounts)
    _print_ids("old accounts", old_accounts)
    _print_ids("unreadable accounts", neither_accounts)


def _print_key_status(consul_key, new_token, old_key):
    if consul_key == new_token:
        print("conductor public key: current")
        return
    if old_key is not None and consul_key == public_key_token(old_key.public_key()):
        print("conductor public key: old")
        return
    if not consul_key:
        print("conductor public key: empty")
        return
    print("conductor public key: neither")


def _consul_msg(body, fallback):
    msg = body.get("msg")
    if type(msg) is not str or not msg:
        return fallback
    return msg


async def rekey_local_secrets(*, config, username, password, old_private_key_path, list_only=False):
    consul_url = (config.consul_url or "").strip()
    if not consul_url:
        raise ExternalError("CONSUL_URL is required")
    current = _load_key(_private_key_path(config))
    new_token = public_key_token(current.public_key())
    old_path = (old_private_key_path or "").strip()
    old_key = None
    cookie_jar = aiohttp.CookieJar(unsafe=True)
    async with aiohttp.ClientSession(cookie_jar=cookie_jar, timeout=http_timeout(), trust_env=True) as session:
        await _sign_in(session, consul_url, username, password)
        for attempt in range(_ATTEMPTS):
            status, body = await _post(session, consul_url, "/api/local_conductor/list_local_secrets", {})
            if status != 200 or body.get("result") != 0:
                raise ExternalError(_consul_msg(body, "list_local_secrets failed"))
            data = ext_dict("list_local_secrets data", body.get("data"))
            consul_key = ext_str("conductor_public_key", data.get("conductor_public_key"), default="")
            models = ext_list("models", data.get("models"))
            accounts = ext_list("accounts", data.get("accounts"))
            if list_only:
                if old_path and old_key is None:
                    old_key = _load_key(old_path)
                elif consul_key != new_token and old_key is None:
                    raise ExternalError("old private key path is required")
                _print_key_status(consul_key, new_token, old_key)
                _prepared_models, _prepared_accounts, _plain_settings, bad_models, bad_accounts, updated_models, updated_accounts, _changed = _prepare(
                    models,
                    accounts,
                    new_key=current,
                    old_key=old_key,
                )
                _print_list(models, accounts, updated_models, updated_accounts, bad_models, bad_accounts)
                return
            if consul_key != new_token:
                if old_key is None:
                    if not old_path:
                        raise ExternalError("old private key path is required")
                    old_key = _load_key(old_path)
                old_token = public_key_token(old_key.public_key())
                if consul_key and consul_key != old_token:
                    raise ExternalError("consul public key does not match old private key")
            elif old_path and old_key is None:
                old_key = _load_key(old_path)
            prepared_models, prepared_accounts, plain_settings, bad_models, bad_accounts, updated_models, updated_accounts, changed = _prepare(
                models,
                accounts,
                new_key=current,
                old_key=old_key,
            )
            if consul_key == new_token and not changed:
                await _sync_station_db(
                    config,
                    prepared_accounts=prepared_accounts,
                    plain_settings=plain_settings,
                )
                print("conductor public key already matches")
                _print_unreadable(bad_models, bad_accounts)
                return
            status, body = await _post(
                session,
                consul_url,
                "/api/local_conductor/replace_local_secrets",
                {
                    "expected_conductor_public_key": consul_key,
                    "conductor_public_key": new_token,
                    "models": prepared_models,
                    "accounts": prepared_accounts,
                },
            )
            if status == 409:
                msg = _consul_msg(body, "replace_local_secrets failed")
                if "already in use" in msg or attempt + 1 == _ATTEMPTS:
                    raise ExternalError(msg)
                logger.error("rekey conflict attempt=%s msg=%s", attempt + 1, msg)
                continue
            if status != 200 or body.get("result") != 0:
                raise ExternalError(_consul_msg(body, "replace_local_secrets failed"))
            await _sync_station_db(
                config,
                prepared_accounts=prepared_accounts,
                plain_settings=plain_settings,
            )
            print("rekeyed %d models and %d accounts" % (len(updated_models), len(updated_accounts)))
            _print_unreadable(bad_models, bad_accounts)
            return
    raise ExternalError("replace_local_secrets failed")
