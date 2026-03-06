# IE_Event_Bot

Discord / Google Calendar / Notion 同期ボットです。

## Architecture

- `workers/`: 本番同期基盤（Cloudflare Workers）
  - `POST /gcal/webhook`
  - `POST /sync/all` (`GET|POST /gcal/sync` 互換)
  - `POST /jobs/qa-check`
  - `POST /jobs/reminder`
  - `POST /jobs/cleanup`
  - `POST /jobs/run-all`
  - `GET /health`
  - Cron Trigger で定期実行
  - KV + Durable Object で状態管理と排他制御

旧 webhook / watcher 実装は撤去済みです。

## Operations

1. `workers/wrangler.jsonc` の vars を設定
2. 必要な secret を投入
3. `cd workers && wrangler deploy`
4. Cron Trigger を有効化
5. 初回のみ `POST /admin/gcal/watch/register` を実行（または `CRON_ENABLE_GCAL_WATCH_ENSURE=true` で自動化）

詳細は [workers/README.md](/c:/Users/starv/OneDrive/OneShare/Git_Products/IE/IE_Event_Bot_alt/workers/README.md) を参照してください。
