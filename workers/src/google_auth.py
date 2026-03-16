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

_last_service_account_error = None
_last_assertion_error = None
_last_sign_error = None


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
    await state.put_text_if_changed("google:access_token", token)
    if expires_at is not None and expires_at > 0:
        await state.put_text_if_changed("google:expires_at", str(expires_at))


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


def _pem_pkcs8_to_der(private_key_pem: str):
    """
    PEM 形式の PKCS8 秘密鍵を DER(bytes) に変換する。
    """
    lines = [line.strip() for line in str(private_key_pem or "").splitlines()]
    body = [
        line for line in lines
        if line and not line.startswith("-----BEGIN") and not line.startswith("-----END")
    ]
    if not body:
        return None
    try:
        return base64.b64decode("".join(body))
    except Exception:
        return None


def _js_uint8_array(data: bytes):
    """
    Python bytes を JS Uint8Array に変換する。
    """
    try:
        js = __import__("js")
        arr = js.Uint8Array.new(len(data))
        for i, b in enumerate(data):
            arr[i] = int(b)
        return arr
    except Exception:
        return None


def _uint8_array_to_bytes(js_arr):
    """
    JS Uint8Array を Python bytes に変換する。
    """
    if js_arr is None:
        return None
    try:
        return bytes(js_arr.to_py())
    except Exception:
        pass
    try:
        length = int(getattr(js_arr, "length", 0) or 0)
        out = bytearray(length)
        for i in range(length):
            out[i] = int(js_arr[i])
        return bytes(out)
    except Exception:
        return None


async def _sign_rs256(message: bytes, private_key_pem: str):
    global _last_sign_error
    _last_sign_error = None
    """
    RS256 秘密鍵署名を行う。
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
    # 例外 -> exc
    # 失敗した署名方法 : 例外の型 : メッセージ
    except Exception as exc:
        _last_sign_error = f"cryptography_failed:{type(exc).__name__}:{str(exc)[:160]}"
        pass

    # RSA
    try:
        import rsa

        key = rsa.PrivateKey.load_pkcs1(private_key_pem.encode("utf-8"))
        return rsa.sign(message, key, "SHA-256")
    except Exception as exc:
        _last_sign_error = f"rsa_failed:{type(exc).__name__}:{str(exc)[:160]}"
        pass

    # WebCrypto フォールバック
    try:
        # PEM文字列を PKCS#8 DER バイト列に変換
        der = _pem_pkcs8_to_der(private_key_pem)
        if not der:
            return None
        # Python の bytes を JS 用の Uint8Array に変換
        key_bytes = _js_uint8_array(der)
        msg_bytes = _js_uint8_array(message)
        if key_bytes is None or msg_bytes is None:
            return None
        """
        鍵の目的指定。
        Python 配列は不可なので JS 配列を渡す。
        """
        js = __import__("js")
        usages = js.Array.new()
        usages.push("sign")
        key = await js.crypto.subtle.importKey(
            "pkcs8",
            key_bytes,
            {"name": "RSASSA-PKCS1-v1_5", "hash": {"name": "SHA-256"}},
            False,
            usages,
        )
        # 署名
        sig_buffer = await js.crypto.subtle.sign("RSASSA-PKCS1-v1_5", key, msg_bytes)
        sig_arr = js.Uint8Array.new(sig_buffer)
        return _uint8_array_to_bytes(sig_arr)
    except Exception as exc:
        _last_sign_error = f"webcrypto_failed:{type(exc).__name__}:{str(exc)[:160]}"
        return None


async def _build_service_account_assertion(sa_info: dict, scope: str):
    """
    OAuth JWT Bearer 用 JWT を生成する。
    """
    global _last_assertion_error
    _last_assertion_error = None
    private_key = str(sa_info.get("private_key") or "").strip()
    client_email = str(sa_info.get("client_email") or "").strip()
    private_key_id = str(sa_info.get("private_key_id") or "").strip()
    if not private_key or not client_email:
        _last_assertion_error = "missing_private_key_or_client_email"
        return None

    # ヘッダ作成
    now = int(time.time())
    header = {"alg": "RS256", "typ": "JWT"}
    if private_key_id:
        header["kid"] = private_key_id
    # JWT ペイロード作成
    payload = {
        "iss": client_email,
        "scope": scope,
        "aud": "https://oauth2.googleapis.com/token",
        "iat": now,
        "exp": now + 3600,
        "jti": str(uuid4()),
    }
    # google-auth が使える場合は RSA signer 実装に委譲する
    try:
        from google.auth import crypt as gcrypt, jwt as gjwt

        signer = gcrypt.RSASigner.from_service_account_info(sa_info)
        token = gjwt.encode(
            signer,
            payload,
            header=header,
        )
        if isinstance(token, bytes):
            return token.decode("utf-8")
        return str(token)
    # 無理なら自前で作る
    except Exception as exc:
        _last_assertion_error = f"google_auth_signer_failed:{type(exc).__name__}:{str(exc)[:160]}"
        pass

    header_b64 = _b64url(json.dumps(header, separators=(",", ":")).encode("utf-8"))
    payload_b64 = _b64url(json.dumps(payload, separators=(",", ":")).encode("utf-8"))
    signing_input = f"{header_b64}.{payload_b64}".encode("utf-8")
    signature = await _sign_rs256(signing_input, private_key)
    # エラー箇所確認用( signer 不可 + 鍵署名失敗)
    if not signature:
        global _last_sign_error
        if _last_assertion_error:
            _last_assertion_error = (
                f"{_last_assertion_error};fallback_sign_failed"
                + (f":{_last_sign_error}" if _last_sign_error else "")
            )
        else:
            _last_assertion_error = "fallback_sign_failed" + (
                f":{_last_sign_error}" if _last_sign_error else ""
            )
        return None
    return f"{header_b64}.{payload_b64}.{_b64url(signature)}"


async def _fetch_token_from_service_account(env, state):
    """
    JWT アサーションを使ってGoogle の OAuth トークンエンドポイントから
    アクセストークンを取得する。
    """
    global _last_service_account_error
    _last_service_account_error = None
    sa_info = _load_service_account_info_from_env(env)
    if not sa_info:
        _last_service_account_error = "missing_service_account_json"
        return None
    assertion = await _build_service_account_assertion(
        sa_info,
        "https://www.googleapis.com/auth/calendar",
    )
    if not assertion:
        detail = _last_assertion_error or "unknown"
        _last_service_account_error = f"service_account_assertion_build_failed:{detail}"
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
        body = await response.text()
        _last_service_account_error = f"oauth_token_http_{int(response.status)}:{str(body or '')[:200]}"
        return None
    text = await response.text()
    try:
        data = json.loads(text or "{}")
    except Exception:
        _last_service_account_error = "oauth_token_response_json_parse_failed"
        return None
    # Google API アクセストークン取得
    access_token = str(data.get("access_token") or "").strip()
    if not access_token:
        _last_service_account_error = "oauth_token_missing_access_token"
        return None
    expires_in = data.get("expires_in")
    expires_at = None
    if expires_in is not None:
        try:
            expires_at = time.time() + float(expires_in)
        except Exception:
            expires_at = None
    await _save_cached_token(state, access_token, expires_at)
    _last_service_account_error = None
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
        "service_account_last_error": _last_service_account_error,
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
