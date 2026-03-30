# IE Event Bot

Discord / Google カレンダー / Notion のイベント情報を同期する Cloudflare Workers (Python) プロジェクトです。  
Google Calendar webhook と Discord Bot を入口にして、差分同期、通知、watch 維持、定期メンテナンスを Worker 側で実行します。

## 1. 機能

### 1.1 コア同期
- Google カレンダー差分取得 (`updatedMin` カーソル方式)
- Google イベント -> Notion DB 反映（作成/更新/削除対応）
- Google イベント -> Discord イベント反映
- Discord イベント -> Notion 反映
- Discord イベント -> Google カレンダー反映

### 1.2 Webhook / watch 運用
- `POST /gcal/webhook` で Google 通知を受信し、同期ディスパッチを実行
- Google watch の手動登録 / 更新 API
- cron による watch 自動維持（期限しきい値で renew）

### 1.3 定期ジョブ
- Q&A 未回答更新通知（Notion Q&A DB -> Discord）
- 前日リマインド通知（24 時間後に開始するイベントを Discord 通知）
- Notion イベントページの自動アーカイブ
- まとめ実行ジョブ（`/jobs/run-all`）

## 2. アーキテクチャ概要

主要実装は `workers/` 配下です。

高頻度で更新される状態の扱い:

- `sync:last_epoch` と Google webhook 重複抑止は Durable Object (`SyncCoordinator`) 側で保持
- KV はカーソル、マッピング、キャッシュ、診断結果などの比較的低頻度な状態を保持
- KV へは「値が変わった時だけ」書き込む設計を基本とし、同一内容なら一定時間再保存しない

- エントリポイント: `workers/src/entry.py`
- 状態管理 (KV): `workers/src/state.py`
- 排他ロック (Durable Object): `workers/src/sync_lock_do.py`
- Google 差分取得: `workers/src/google_calendar_sync.py`
- Google 反映（Notion / Discord）: `workers/src/google_apply_sync.py`
- Discord-Notion 同期: `workers/src/discord_notion_sync.py`
- Google 認証: `workers/src/google_auth.py`
- watch 管理: `workers/src/google_watch.py`
- 定期ジョブ: `workers/src/jobs.py`
- 外部疎通確認: `workers/src/health_checks.py`

## 3. エンドポイント一覧

`INTERNAL_API_TOKEN` を設定している場合、`/health` と `/gcal/webhook` 以外は `Authorization: Bearer <token>` が必要です。

- `GET /health`
  - ヘルス確認
- `POST /gcal/webhook`
  - Google webhook 受信。必要に応じて重複通知を KV で抑止
- `GET|POST /sync/all` (`/gcal/sync` はエイリアス)
  - 全体同期ディスパッチ
- `GET|POST /sync/discord-notion`
  - Discord Scheduled Events -> Notion 同期
- `POST /admin/google-token`
  - Google access token を KV キャッシュへ手動登録
- `POST /admin/gcal/watch/ensure`
  - watch を ensure（必要時のみ register/renew）
- `GET /admin/migration-status`
  - 設定・状態・最終結果を診断表示
  - `?include_checks=1` で外部接続テストを追加
- `GET|POST /jobs/qa-check`
  - Q&A 未回答更新通知
- `GET|POST /jobs/reminder`
  - 前日リマインド
- `GET|POST /jobs/cleanup`
  - Notion cleanup
- `GET|POST /jobs/run-all`
  - 同期 + 全ジョブをまとめて実行

## 4. 同期

- Google 差分取得
- Google -> Notion/Discord 反映
- Discord -> Notion 同期

現行 `workers/wrangler.jsonc` では以下です。

- `SYNC_ALL_INCLUDE_DISCORD_NOTION=true`

## 5. Cron 実行

`workers/wrangler.jsonc` の既定値:

- cron: `*/5 * * * *` (5分)
- `KV_SYNC_COOLDOWN_ENABLED=true`
- `KV_GCAL_DEDUPE_ENABLED=true`
- `KV_RESULT_MIN_WRITE_SECONDS=3600`
- `GCAL_DEDUPE_TTL_SECONDS=86400`
- `SYNC_DO_LOCK_ENABLED=true`
- `SYNC_DO_LOCK_TTL_SECONDS=120`
- `GOOGLE_APPLY_MAX_EVENTS_PER_RUN=5`
- `DISCORD_NOTION_MAX_CHANGES_PER_RUN=2`
- `CRON_ENABLE_SYNC=true`
- `CRON_ENABLE_DISCORD_NOTION_SYNC=false`
- `CRON_ENABLE_GCAL_WATCH_ENSURE=true`
- `CRON_ENABLE_QA=true`
- `CRON_ENABLE_REMINDER=true`
- `CRON_ENABLE_AUTO_CLEAN=true`
- `DISCORD_TO_GOOGLE_SYNC_ENABLED=true`

## 6. シークレット 

- `INTERNAL_API_TOKEN`
- `NOTION_TOKEN`
- `DISCORD_TOKEN`

Google 認証ソースの優先順:

1. `GOOGLE_API_BEARER_TOKEN`
2. KV キャッシュ
3. `GOOGLE_TOKEN_BROKER_URL` + `GOOGLE_TOKEN_BROKER_AUTH`
4. `GOOGLE_SERVICE_ACCOUNT_JSON` または `GOOGLE_SERVICE_ACCOUNT_JSON_B64`

## 7. 主要環境変数

### 7.1 必須クラス
- `INTERNAL_API_TOKEN`: 管理系 API（`/sync/all`, `/jobs/*`, `/admin/*`）の Bearer 認証トークン
- `GOOGLE_CALENDAR_ID`: 同期対象の Google カレンダー ID
- `GCAL_WEBHOOK_URL`: Google watch 通知の送信先 URL（通常 `https://<worker>/gcal/webhook`）
- `NOTION_TOKEN`: Notion API の認証トークン
- `NOTION_EVENT_INTERNAL_ID`: 内部向けイベント DB の Notion Database ID
- `DISCORD_TOKEN`: Discord Bot トークン
- `DISCORD_GUILD_ID`: 対象 Discord サーバー（Guild）ID

### 7.2 Google 認証ソース（優先順）
1. `GOOGLE_API_BEARER_TOKEN`
2. KV キャッシュ (`google:access_token`, `google:expires_at`) ※ `/admin/google-token` で登録可能
3. `GOOGLE_TOKEN_BROKER_URL` + `GOOGLE_TOKEN_BROKER_AUTH`
4. `GOOGLE_SERVICE_ACCOUNT_JSON` または `GOOGLE_SERVICE_ACCOUNT_JSON_B64`

役割:
- `GOOGLE_API_BEARER_TOKEN`: 固定の Google Access Token を直接使用
- KV キャッシュ: 直近取得した Access Token を再利用（期限管理あり）
- `GOOGLE_TOKEN_BROKER_URL`: 外部トークンブローカーの取得エンドポイント
- `GOOGLE_TOKEN_BROKER_AUTH`: ブローカー呼び出し時の認可トークン
- `GOOGLE_SERVICE_ACCOUNT_JSON`: サービスアカウント JSON（生文字列）
- `GOOGLE_SERVICE_ACCOUNT_JSON_B64`: サービスアカウント JSON の base64 版

備考: 現時点ではサービスアカウント運用(ブローカー不使用)
  
### 7.3 同期・排他
- `SYNC_INTERVAL_SECONDS`: 連続同期の最小間隔（秒）
- `KV_SYNC_COOLDOWN_ENABLED`: クールダウンスキップを有効化（実体は `SyncCoordinator` を優先利用）
- `KV_GCAL_DEDUPE_ENABLED`: webhook 重複通知抑止を有効化（実体は `SyncCoordinator` を優先利用）
- `KV_RESULT_MIN_WRITE_SECONDS`: `result:*` に同一内容を再保存する最小間隔（秒）
- `GCAL_DEDUPE_TTL_SECONDS`: Google webhook 重複抑止レコードの保持秒数
- `SYNC_DO_LOCK_ENABLED`: Durable Object ロックによる同時実行抑止を有効化
- `SYNC_DO_LOCK_TTL_SECONDS`: ロック有効時間（秒）

### 7.4 Cron / watch / ジョブ
- `CRON_ENABLE_SYNC`: cron で `/sync/all` 相当を実行するか
- `CRON_ENABLE_DISCORD_NOTION_SYNC`: cron で Discord->Notion 同期を実行するか
- `CRON_ENABLE_GCAL_WATCH_ENSURE`: watch を「期限確認して必要時のみ register/renew」するか
- `CRON_ENABLE_QA`: Q&A 通知ジョブ実行可否
- `CRON_ENABLE_REMINDER`: 前日リマインド実行可否
- `CRON_ENABLE_AUTO_CLEAN`: Notion cleanup 実行可否
- `GCAL_WATCH_RENEW_THRESHOLD_SECONDS`: watch 残り期限がこの秒数未満なら renew
- `WATCH_CHANNEL_ID`: Google watch 登録時の channel ID（未指定時は自動生成）
- `CLEANUP_INTERVAL_SECONDS`: cleanup ジョブの最小実行間隔
  - 外部DBはイベント終了日から30日後にアーカイブ、内部DBはイベント終了時点でアーカイブ
- `REMINDER_WINDOW_MINUTES`: 「24時間後」から何分幅で通知対象にするか

### 7.5 Discord 同期制御
- `DISCORD_TO_GOOGLE_SYNC_ENABLED`: Discord 変更を Google に反映する経路を有効化
- `DISCORD_SYNC_ENABLED`: Google->Discord 反映自体の有効/無効
- `DISCORD_APPEND_GCAL_MARKER`: Discord イベント本文に gcal-id マーカーを追記するか
- `DISCORD_ORIGIN_MARKER_PREFIX`: マーカー接頭辞（例: `[gcal-id:`）
- `DISCORD_DESCRIPTION_LIMIT`: Discord イベント本文の最大文字数
- `DISCORD_NAME_LIMIT`: イベント名の最大文字数
- `DISCORD_LOCATION_LIMIT`: 場所の最大文字数
- `DISCORD_LOCATION_FALLBACK`: 場所が空のときの代替文字列

### 7.6 Notion プロパティ名上書き
- `NOTION_PROP_TITLE`
- `NOTION_PROP_CONTENT`
- `NOTION_PROP_DATE`
- `NOTION_PROP_MESSAGE_ID`
- `NOTION_PROP_CREATOR_ID`
- `NOTION_PROP_PAGE_ID`
- `NOTION_PROP_EVENT_URL`
- `NOTION_PROP_GOOGLE_EVENT_ID`
- `NOTION_PROP_LOCATION`

役割:
- Notion DB の列名が既定値（`イベント名`, `内容`, `日時` など）と違う場合に、実際の列名へマッピングするための設定群
- 例: `NOTION_PROP_DATE=開始日時` を設定すると、同期時の日付書き込み先が `開始日時` プロパティになる

### 7.7 Q&A / リマインド
- `NOTION_QA_ID`: Q&A 通知対象の Notion DB ID
- `QA_CHANNEL_ID`: 未回答更新を通知する Discord チャンネル ID
- `REMINDER_CHANNEL_ID`: 前日リマインド送信先チャンネル ID
- `REMINDER_ROLE_ID`: リマインド時にメンションするロール ID

## 8. Notion DB 想定スキーマ

既定プロパティ名（上書きしない場合）:

- イベント DB
  - `イベント名` (title)
  - `内容` (rich_text)
  - `日時` (date)
  - `メッセージID` (rich_text)
  - `作成者ID` (rich_text)
  - `ページID` (rich_text)
  - `イベントURL` (url)
  - `GoogleイベントID` (rich_text)
  - `場所` (rich_text)

- Q&A DB
  - `質問` (title)
  - `回答` (rich_text)
  - `質問番号` (number)

## 9. 初期動作確認

```bash
BASE_URL="https://<worker-domain>"
TOKEN="<INTERNAL_API_TOKEN>"
```

1. ヘルスチェック
```bash
curl -sS "$BASE_URL/health"
```

2. 設定・状態確認
```bash
curl -sS -H "Authorization: Bearer $TOKEN" \
  "$BASE_URL/admin/migration-status"
```

3. 外部疎通を含む診断
```bash
curl -sS -H "Authorization: Bearer $TOKEN" \
  "$BASE_URL/admin/migration-status?include_checks=1"
```

4. 手動同期
```bash
curl -sS -X POST -H "Authorization: Bearer $TOKEN" \
  "$BASE_URL/sync/all"
```

5. watch ensure
```bash
curl -sS -X POST -H "Authorization: Bearer $TOKEN" \
  "$BASE_URL/admin/gcal/watch/ensure"
```

## 10. トラブルシュートの入口

- `/sync/all` が `cooldown_skip`: `SYNC_INTERVAL_SECONDS` と `sync:last_epoch` を確認
- `/sync/all` が `in_progress_skip`: `sync_lock` と `SYNC_DO_LOCK_TTL_SECONDS` を確認
- Google 認証失敗: `/admin/migration-status` の `google_auth` を確認
- watch 更新失敗: `watch_state.expiration` と `CRON_ENABLE_GCAL_WATCH_ENSURE` を確認
- キュー滞留: `sync:google_apply_queue` と `sync:discord_notion_queue` を確認
- 疎通切り分け: `/admin/migration-status?include_checks=1`

## 11. 開発メモ

- Python package metadata: `pyproject.toml`
- 開発依存: `ruff`, `pytest`
- リリース補助: `.release-please-config.json`
