import json
import time
import base64
from uuid import uuid4
from typing import Any, TYPE_CHECKING

try:
    from workers import fetch as _runtime_fetch
except Exception:
    _runtime_fetch = globals().get("fetch")

if _runtime_fetch is None:
    async def fetch(*args, **kwargs):
        raise RuntimeError("fetch_not_available")
else:
    async def fetch(url, options=None):
        opts = options or {}
        try:
            return await _runtime_fetch(
                url,
                method=opts.get("method"),
                headers=opts.get("headers"),
                body=opts.get("body"),
            )
        except TypeError:
            return await _runtime_fetch(url, opts)

if TYPE_CHECKING:
    fetch: Any

"""
Google API アクセストークン解決モジュール。

解決優先順:
1) 直接 env (`GOOGLE_API_BEARER_TOKEN`)
2) KV キャッシュ (`google:access_token`)
3) 外部トークンブローカー
4) Service Account JWT assertion
"""


def _b64url(data: bytes) -> str:
    """
    JWT 用 Base64URL エンコード（パディングなし）。
    """
    return base64.urlsafe_b64encode(data).decode("ascii").rstrip("=")


def _env_text(env, key: str, default: str = "") -> str:
    """
    Worker env から文字列を安全に取得する。
    """
    value = getattr(env, key, None)
    if value is None:
        return default
    text = str(value).strip()
    return text or default


async def _get_cached_token(state):
    """
    KV からキャッシュ済みGoogleアクセストークンを取得する。
    - `expires_at - 60秒` を有効期限として扱い、失効直前の利用を避ける。
    """
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
    # 期限の60秒前までなら使う
    if expires_at > 0 and time.time() < (expires_at - 60):
        return token
    return None


async def _get_cached_token_meta(state):
    """
    キャッシュトークンの存在・有効性メタ情報(健康度)を返す。
    可観測データとして利用する。
    """
    if not state.enabled():
        return {
            "present": False, # 存在
            "valid": False, # 有効性
            "expires_at": None, # 期限
            "ttl_seconds": None, # 残り秒数
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
    """
    token と任意の有効期限(epoch)を KV に保存する。
    """
    if not state.enabled():
        return
    await state.put_text("google:access_token", token)
    if expires_at is not None and expires_at > 0:
        await state.put_text("google:expires_at", str(expires_at))


async def _fetch_token_from_broker(env, state):
    """
    外部トークンブローカーからトークンを取得する。
    - `access_token` (必須)
    - `expires_at` または `expires_in` (任意)
    """
    broker_url = _env_text(env, "GOOGLE_TOKEN_BROKER_URL", "")
    if not broker_url:
        return None

    # トークン取得用の認証情報を作成
    broker_auth = _env_text(env, "GOOGLE_TOKEN_BROKER_AUTH", "")
    headers = {"Content-Type": "application/json"}
    if broker_auth:
        headers["Authorization"] = f"Bearer {broker_auth}"

    # トークン取得リクエスト
    response = await fetch(
        broker_url,
        {
            "method": "POST",
            "headers": headers,
            "body": json.dumps({"scope": "https://www.googleapis.com/auth/calendar"}),
        },
    )
    # 読み取り
    if int(response.status) >= 400:
        return None
    text = await response.text()
    try:
        data = json.loads(text or "{}")
    except Exception:
        return None
    
    # Google API アクセストークン取得
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
    """
    Service Account JSON を env から読み込む。
    - `GOOGLE_SERVICE_ACCOUNT_JSON` (生JSON)
    - `GOOGLE_SERVICE_ACCOUNT_JSON_B64` (base64)
    """
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
    """
    RS256 署名を行う。
    - `cryptography` 優先（PKCS8 対応）
    - 失敗時 `rsa` パッケージへフォールバック（PKCS1 対応）
    """
    # cryptography
    try:
        from cryptography.hazmat.primitives import hashes
        from cryptography.hazmat.primitives.asymmetric import padding
        from cryptography.hazmat.primitives.serialization import load_pem_private_key

        key = load_pem_private_key(private_key_pem.encode("utf-8"), password=None)
        sig = key.sign(message, padding.PKCS1v15(), hashes.SHA256())
        return sig
    except Exception:
        pass

    # RSA
    try:
        import rsa

        key = rsa.PrivateKey.load_pkcs1(private_key_pem.encode("utf-8"))
        return rsa.sign(message, key, "SHA-256")
    except Exception:
        return None


def _build_service_account_assertion(sa_info: dict, scope: str):
    """
    OAuth JWT Bearer 用 JWT を生成する。
    """
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
    """
    JWT アサーションを使ってGoogle の OAuth トークンエンドポイントから
    アクセストークンを取得する。
    """
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
    # Google OAuth リクエスト
    response = await fetch(
        "https://oauth2.googleapis.com/token",
        {
            "method": "POST",
            "headers": {"Content-Type": "application/x-www-form-urlencoded"},
            "body": body,
        },
    )
    # 読み取り
    if int(response.status) >= 400:
        return None
    text = await response.text()
    try:
        data = json.loads(text or "{}")
    except Exception:
        return None
    # Google API アクセストークン取得
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
    Google API アクセストークン取得試行の順番:
    1) GOOGLE_API_BEARER_TOKEN (直接)
    2) KVキャッシュ(google:access_token / google:expires_at)
    3) GOOGLE_TOKEN_BROKER_URL
    4) サービスアカウントJWTアサーション
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
    """
    現在利用可能な認証ソース状態を返す。
    運用時の診断（/admin/migration-status）で使用する。
    """
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
    """
    管理API経由で受け取ったトークンを KV キャッシュに保存する。
    """
    if not access_token:
        return False
    expires_at = None
    if expires_in_seconds is not None:
        try:
            # Unix Timeで比較するために絶対時刻にする
            expires_at = time.time() + max(1, int(expires_in_seconds))
        except Exception:
            expires_at = None
    await _save_cached_token(state, access_token, expires_at)
    return True
