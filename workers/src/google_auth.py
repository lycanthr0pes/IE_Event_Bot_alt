import json
import time
import base64
from uuid import uuid4


def _b64url(data: bytes) -> str:
    return base64.urlsafe_b64encode(data).decode("ascii").rstrip("=")


def _env_text(env, key: str, default: str = "") -> str:
    value = getattr(env, key, None)
    if value is None:
        return default
    text = str(value).strip()
    return text or default


async def _get_cached_token(state):
    if not state.enabled():
        return None
    token = await state.get_text("google:access_token")
    expires_at_raw = await state.get_text("google:expires_at")
    if not token:
        return None
    try:
        expires_at = float(expires_at_raw or "0")
    except Exception:
        expires_at = 0.0
    # refresh 60s before expiry
    if expires_at > 0 and time.time() < (expires_at - 60):
        return token
    return None


async def _get_cached_token_meta(state):
    if not state.enabled():
        return {
            "present": False,
            "valid": False,
            "expires_at": None,
            "ttl_seconds": None,
        }
    token = await state.get_text("google:access_token")
    expires_at_raw = await state.get_text("google:expires_at")
    if not token:
        return {
            "present": False,
            "valid": False,
            "expires_at": None,
            "ttl_seconds": None,
        }
    try:
        expires_at = float(expires_at_raw or "0")
    except Exception:
        expires_at = 0.0
    now = time.time()
    ttl = (expires_at - now) if expires_at > 0 else None
    valid = bool(ttl is None or ttl > 60)
    return {
        "present": True,
        "valid": valid,
        "expires_at": expires_at if expires_at > 0 else None,
        "ttl_seconds": ttl,
    }


async def _save_cached_token(state, token: str, expires_at: float | None):
    if not state.enabled():
        return
    await state.put_text("google:access_token", token)
    if expires_at is not None and expires_at > 0:
        await state.put_text("google:expires_at", str(expires_at))


async def _fetch_token_from_broker(env, state):
    broker_url = _env_text(env, "GOOGLE_TOKEN_BROKER_URL", "")
    if not broker_url:
        return None

    broker_auth = _env_text(env, "GOOGLE_TOKEN_BROKER_AUTH", "")
    headers = {"Content-Type": "application/json"}
    if broker_auth:
        headers["Authorization"] = f"Bearer {broker_auth}"

    response = await fetch(
        broker_url,
        {
            "method": "POST",
            "headers": headers,
            "body": json.dumps({"scope": "https://www.googleapis.com/auth/calendar"}),
        },
    )
    if int(response.status) >= 400:
        return None
    text = await response.text()
    try:
        data = json.loads(text or "{}")
    except Exception:
        return None

    access_token = str(data.get("access_token") or "").strip()
    if not access_token:
        return None

    expires_at = None
    if data.get("expires_at") is not None:
        try:
            expires_at = float(data.get("expires_at"))
        except Exception:
            expires_at = None
    elif data.get("expires_in") is not None:
        try:
            expires_at = time.time() + float(data.get("expires_in"))
        except Exception:
            expires_at = None

    await _save_cached_token(state, access_token, expires_at)
    return access_token


def _load_service_account_info_from_env(env):
    raw = _env_text(env, "GOOGLE_SERVICE_ACCOUNT_JSON", "")
    if raw:
        try:
            return json.loads(raw)
        except Exception:
            return None
    raw_b64 = _env_text(env, "GOOGLE_SERVICE_ACCOUNT_JSON_B64", "")
    if raw_b64:
        try:
            decoded = base64.b64decode(raw_b64).decode("utf-8")
            return json.loads(decoded)
        except Exception:
            return None
    return None


def _sign_rs256(message: bytes, private_key_pem: str):
    # Try cryptography first (supports PKCS8 PRIVATE KEY commonly used by Google SA)
    try:
        from cryptography.hazmat.primitives import hashes
        from cryptography.hazmat.primitives.asymmetric import padding
        from cryptography.hazmat.primitives.serialization import load_pem_private_key

        key = load_pem_private_key(private_key_pem.encode("utf-8"), password=None)
        sig = key.sign(message, padding.PKCS1v15(), hashes.SHA256())
        return sig
    except Exception:
        pass

    # Fallback to rsa package (works with RSA PRIVATE KEY / PKCS1)
    try:
        import rsa

        key = rsa.PrivateKey.load_pkcs1(private_key_pem.encode("utf-8"))
        return rsa.sign(message, key, "SHA-256")
    except Exception:
        return None


def _build_service_account_assertion(sa_info: dict, scope: str):
    private_key = str(sa_info.get("private_key") or "").strip()
    client_email = str(sa_info.get("client_email") or "").strip()
    private_key_id = str(sa_info.get("private_key_id") or "").strip()
    if not private_key or not client_email:
        return None

    now = int(time.time())
    header = {"alg": "RS256", "typ": "JWT"}
    if private_key_id:
        header["kid"] = private_key_id
    payload = {
        "iss": client_email,
        "scope": scope,
        "aud": "https://oauth2.googleapis.com/token",
        "iat": now,
        "exp": now + 3600,
        "jti": str(uuid4()),
    }
    header_b64 = _b64url(json.dumps(header, separators=(",", ":")).encode("utf-8"))
    payload_b64 = _b64url(json.dumps(payload, separators=(",", ":")).encode("utf-8"))
    signing_input = f"{header_b64}.{payload_b64}".encode("utf-8")
    signature = _sign_rs256(signing_input, private_key)
    if not signature:
        return None
    return f"{header_b64}.{payload_b64}.{_b64url(signature)}"


async def _fetch_token_from_service_account(env, state):
    sa_info = _load_service_account_info_from_env(env)
    if not sa_info:
        return None
    assertion = _build_service_account_assertion(
        sa_info,
        "https://www.googleapis.com/auth/calendar",
    )
    if not assertion:
        return None

    body = (
        "grant_type=urn%3Aietf%3Aparams%3Aoauth%3Agrant-type%3Ajwt-bearer"
        f"&assertion={assertion}"
    )
    response = await fetch(
        "https://oauth2.googleapis.com/token",
        {
            "method": "POST",
            "headers": {"Content-Type": "application/x-www-form-urlencoded"},
            "body": body,
        },
    )
    if int(response.status) >= 400:
        return None
    text = await response.text()
    try:
        data = json.loads(text or "{}")
    except Exception:
        return None
    access_token = str(data.get("access_token") or "").strip()
    if not access_token:
        return None
    expires_in = data.get("expires_in")
    expires_at = None
    if expires_in is not None:
        try:
            expires_at = time.time() + float(expires_in)
        except Exception:
            expires_at = None
    await _save_cached_token(state, access_token, expires_at)
    return access_token


async def get_google_access_token(env, state):
    """
    Resolve Google access token in this order:
    1) GOOGLE_API_BEARER_TOKEN (direct)
    2) KV cache (google:access_token / google:expires_at)
    3) GOOGLE_TOKEN_BROKER_URL (fetch and cache)
    """
    direct = _env_text(env, "GOOGLE_API_BEARER_TOKEN", "")
    if direct:
        return direct

    cached = await _get_cached_token(state)
    if cached:
        return cached

    broker_token = await _fetch_token_from_broker(env, state)
    if broker_token:
        return broker_token
    sa_token = await _fetch_token_from_service_account(env, state)
    if sa_token:
        return sa_token
    return None


async def describe_google_auth_sources(env, state):
    direct = _env_text(env, "GOOGLE_API_BEARER_TOKEN", "")
    broker = _env_text(env, "GOOGLE_TOKEN_BROKER_URL", "")
    cache_meta = await _get_cached_token_meta(state)
    return {
        "direct_env": bool(direct),
        "broker_configured": bool(broker),
        "service_account_json_configured": bool(_load_service_account_info_from_env(env)),
        "cache": cache_meta,
    }


async def set_google_access_token(state, access_token: str, expires_in_seconds: int | None):
    if not access_token:
        return False
    expires_at = None
    if expires_in_seconds is not None:
        try:
            expires_at = time.time() + max(1, int(expires_in_seconds))
        except Exception:
            expires_at = None
    await _save_cached_token(state, access_token, expires_at)
    return True
