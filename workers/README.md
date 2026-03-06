# Cloudflare Workers

`workers/` は同期機能の本体です。旧 webhook へのプロキシは廃止済みで、すべて Worker 内で処理します。

## Endpoints

- `GET /health`
- `POST /gcal/webhook`
- `GET|POST /sync/all`
- `GET|POST /gcal/sync`
- `GET|POST /sync/discord-notion`
- `GET|POST /jobs/qa-check`
- `GET|POST /jobs/reminder`
- `GET|POST /jobs/cleanup`
- `GET|POST /jobs/run-all`
- `POST /admin/google-token`
- `POST /admin/gcal/watch/register`
- `POST /admin/gcal/watch/renew`
- `GET /admin/migration-status`

## Setup

```bash
cd workers
wrangler secret put INTERNAL_API_TOKEN
wrangler secret put NOTION_TOKEN
wrangler secret put DISCORD_TOKEN
wrangler secret put GOOGLE_SERVICE_ACCOUNT_JSON
```

主要 vars:
- `WORKER_SYNC_ALL_MODE` (`native` or `hybrid`)
- `SYNC_INTERVAL_SECONDS`
- `KV_SYNC_COOLDOWN_ENABLED`
- `KV_GCAL_DEDUPE_ENABLED`
- `SYNC_DO_LOCK_ENABLED`
- `SYNC_DO_LOCK_TTL_SECONDS`
- `CRON_ENABLE_SYNC`
- `CRON_ENABLE_DISCORD_NOTION_SYNC`
- `CRON_ENABLE_GCAL_WATCH_RENEW`
- `CRON_ENABLE_GCAL_WATCH_ENSURE`
- `GCAL_WATCH_RENEW_THRESHOLD_SECONDS`
- `CRON_ENABLE_QA`
- `CRON_ENABLE_REMINDER`
- `CRON_ENABLE_AUTO_CLEAN`
- `CLEANUP_INTERVAL_SECONDS`
- `DISCORD_TO_GOOGLE_SYNC_ENABLED`

## Deploy

```bash
cd workers
wrangler deploy
```

## Watch Management

- 手動登録: `POST /admin/gcal/watch/register`
- 手動更新: `POST /admin/gcal/watch/renew`
- 自動維持: `CRON_ENABLE_GCAL_WATCH_ENSURE=true`
  - 未登録なら `register`
  - 期限が `GCAL_WATCH_RENEW_THRESHOLD_SECONDS` 以下なら `renew`

## Google Auth

Google token 解決順:
1. `GOOGLE_API_BEARER_TOKEN`
2. KV (`google:access_token`, `google:expires_at`)
3. `GOOGLE_TOKEN_BROKER_URL`
4. Service Account JWT (`GOOGLE_SERVICE_ACCOUNT_JSON` / `_B64`)
