# IE Event Bot

Discord / Google Calendar / Notion のイベント情報を同期する Cloudflare Workers (Python) プロジェクトです。  
Google Calendar webhook を入口にして、差分同期・通知・定期メンテナンスまでを Worker で実行します。

## 1. できること

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

### 1.4 安定運用の仕組み
- KV による同期カーソル・重複通知抑止・ジョブキャッシュ
- Durable Object ロックで `/sync/all` 同時実行を抑止
- `/admin/migration-status` による状態可視化
- `?include_checks=1` で Notion / Discord / Google 接続確認

## 2. アーキテクチャ概要

主要実装は `workers/` 配下です。

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
- `POST /admin/gcal/watch/register`
  - watch 新規登録
- `POST /admin/gcal/watch/renew`
  - watch 再登録
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

## 4. 同期モード

`WORKER_SYNC_ALL_MODE` で `native` / `hybrid` を選べます。

- `native`
  - Google 差分取得
  - Google -> Notion/Discord 反映
  - Discord -> Notion 同期
- `hybrid`
  - Google 差分取得のみは常に実行
  - Google 反映は `WORKER_HYBRID_APPLY_GOOGLE_EVENTS`
  - Discord-Notion 同期は `WORKER_HYBRID_INCLUDE_DISCORD_NOTION`

## 5. Cron 実行

`workers/wrangler.jsonc` の既定値:

- cron: `*/5 * * * *` (5分)
- 代表フラグ
  - `CRON_ENABLE_SYNC=true`
  - `CRON_ENABLE_DISCORD_NOTION_SYNC=false`
  - `CRON_ENABLE_GCAL_WATCH_ENSURE=true`
  - `CRON_ENABLE_GCAL_WATCH_RENEW=false`
  - `CRON_ENABLE_QA=true`
  - `CRON_ENABLE_REMINDER=true`
  - `CRON_ENABLE_AUTO_CLEAN=true`

## 6. セットアップ

### 6.1 前提
- Cloudflare Workers (Python Workers 有効)
- Wrangler
- Notion / Discord / Google Calendar API 利用権限

### 6.2 デプロイ

```bash
cd workers
wrangler deploy
```

### 6.3 シークレット設定（最低限）

```bash
cd workers
wrangler secret put INTERNAL_API_TOKEN
wrangler secret put NOTION_TOKEN
wrangler secret put DISCORD_TOKEN
```

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

### 7.3 同期・排他
- `WORKER_SYNC_ALL_MODE` (`native` / `hybrid`): `/sync/all` の動作モード選択
- `WORKER_HYBRID_INCLUDE_DISCORD_NOTION`: `hybrid` 時に Discord->Notion 同期を含めるか
- `WORKER_HYBRID_APPLY_GOOGLE_EVENTS`: `hybrid` 時に Google 差分を Notion/Discord へ反映するか
- `SYNC_INTERVAL_SECONDS`: 連続同期の最小間隔（秒）
- `KV_SYNC_COOLDOWN_ENABLED`: KV ベースのクールダウンスキップを有効化
- `KV_GCAL_DEDUPE_ENABLED`: webhook 重複通知抑止（`gcal_msg:*`）を有効化
- `SYNC_DO_LOCK_ENABLED`: Durable Object ロックによる同時実行抑止を有効化
- `SYNC_DO_LOCK_TTL_SECONDS`: ロック有効時間（秒）

### 7.4 Cron / watch / ジョブ
- `CRON_ENABLE_SYNC`: cron で `/sync/all` 相当を実行するか
- `CRON_ENABLE_DISCORD_NOTION_SYNC`: cron で Discord->Notion 同期を実行するか
- `CRON_ENABLE_GCAL_WATCH_ENSURE`: watch を「期限確認して必要時のみ register/renew」するか
- `CRON_ENABLE_GCAL_WATCH_RENEW`: watch を毎回 renew するか（`ENSURE` が false の時に有効）
- `CRON_ENABLE_QA`: Q&A 通知ジョブ実行可否
- `CRON_ENABLE_REMINDER`: 前日リマインド実行可否
- `CRON_ENABLE_AUTO_CLEAN`: Notion cleanup 実行可否
- `GCAL_WATCH_RENEW_THRESHOLD_SECONDS`: watch 残り期限がこの秒数未満なら renew
- `WATCH_CHANNEL_ID`: Google watch 登録時の channel ID（未指定時は自動生成）
- `CLEANUP_INTERVAL_SECONDS`: cleanup ジョブの最小実行間隔
- `REMINDER_WINDOW_MINUTES`: 「24時間後」から何分幅で通知対象にするか

### 7.5 Discord 同期制御
- `DISCORD_TO_GOOGLE_SYNC_ENABLED`: Discord 変更を Google に反映する経路を有効化
- `DISCORD_SYNC_ENABLED`: Google->Discord 反映自体の有効/無効
- `DISCORD_APPEND_GCAL_MARKER`: Discord description に gcal-id マーカーを追記するか
- `DISCORD_ORIGIN_MARKER_PREFIX`: マーカー接頭辞（例: `[gcal-id:`）
- `DISCORD_DESCRIPTION_LIMIT`: Discord description の最大文字数
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
BASE_URL="https://<your-worker-domain>"
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

5. watch 登録
```bash
curl -sS -X POST -H "Authorization: Bearer $TOKEN" \
  "$BASE_URL/admin/gcal/watch/register"
```

## 10. KV と状態データ

KV のキー設計と詳細は以下を参照してください。

- `workers/README_KV.md`

主なキー:
- `sync:updated_min`, `sync:last_epoch`
- `gcal_msg:*`（webhook 重複抑止）
- `map:gcal_discord`, `map:gcal_notion`
- `discord:snapshot`
- `google:access_token`, `google:expires_at`
- `gcal_watch_state`
- `qa_cache`, `reminder_cache`, `cleanup:last_epoch`
- `result:*`

## 11. トラブルシューティング

### 11.1 401 unauthorized
症状:
- 管理 API やジョブ API が `401 unauthorized`

確認ポイント:
- `INTERNAL_API_TOKEN` が Worker 側で設定されているか
- リクエストヘッダが `Authorization: Bearer <token>` 形式か
- 前後空白や改行混入がないか

### 11.2 `/sync/all` が `cooldown_skip`
症状:
- API は 200 だが `status: cooldown_skip`

確認ポイント:
- `SYNC_INTERVAL_SECONDS`
- `KV_SYNC_COOLDOWN_ENABLED`
- `sync:last_epoch` が最近値になっていないか

対処:
- 手動検証時のみ一時的に `KV_SYNC_COOLDOWN_ENABLED=false`

### 11.3 `/sync/all` が `in_progress_skip`
症状:
- API は 200 だが `status: in_progress_skip`

確認ポイント:
- `SYNC_DO_LOCK_ENABLED`
- `SYNC_DO_LOCK_TTL_SECONDS`
- `/admin/migration-status` の `sync_lock`

対処:
- 長時間継続する場合は lock owner / expires を確認し、TTL を見直す

### 11.4 Google webhook が来ても反映されない
確認ポイント:
- `GCAL_WEBHOOK_URL` が公開 URL と一致しているか
- `GOOGLE_CALENDAR_ID` が正しいか
- `KV_GCAL_DEDUPE_ENABLED=true` の場合、同一通知が `gcal_msg:*` で抑止されていないか
- `X-Goog-Channel-ID`, `X-Goog-Message-Number` が送られているか

### 11.5 watch 登録/更新に失敗する
確認ポイント:
- `/admin/migration-status` の `google_auth`
- Google 認証ソース（直接 token / broker / service account）
- `CRON_ENABLE_GCAL_WATCH_ENSURE` と `GCAL_WATCH_RENEW_THRESHOLD_SECONDS`

対処:
- まず `POST /admin/gcal/watch/register` を実行し、`gcal_watch_state` 更新を確認

### 11.6 Google 認証が取れない
確認ポイント:
- 設定した認証ソースが優先順で正しく有効か
- broker 利用時: `GOOGLE_TOKEN_BROKER_URL` と `GOOGLE_TOKEN_BROKER_AUTH`
- service account 利用時: JSON 形式や base64 破損

対処:
- 一時的に `POST /admin/google-token` で token を投入して切り分け

### 11.7 Notion 同期が動かない
確認ポイント:
- `NOTION_TOKEN`
- `NOTION_EVENT_INTERNAL_ID`（必要に応じて `NOTION_EVENT_ID`）
- Notion DB のプロパティ名が `NOTION_PROP_*` 設定と一致しているか

対処:
- `/admin/migration-status?include_checks=1` で Notion 接続を確認

### 11.8 Discord 同期/通知が動かない
確認ポイント:
- `DISCORD_TOKEN`, `DISCORD_GUILD_ID`
- `DISCORD_SYNC_ENABLED`
- `DISCORD_TO_GOOGLE_SYNC_ENABLED`
- リマインド時: `REMINDER_CHANNEL_ID`, `REMINDER_ROLE_ID`

### 11.9 Q&A 通知が来ない
確認ポイント:
- `NOTION_QA_ID`, `QA_CHANNEL_ID`
- 初回実行は通知せず `qa_cache` 初期化のみ（仕様）
- Q&A DB の `質問` / `回答` / `質問番号` 型

### 11.10 cleanup が動かない / すぐ再実行されない
確認ポイント:
- `CLEANUP_INTERVAL_SECONDS`
- `cleanup:last_epoch`（interval guard）
- `NOTION_PROP_DATE` が実DBの日付プロパティ名と一致しているか

### 11.11 外部接続の切り分け
最短手順:
```bash
curl -sS -H "Authorization: Bearer $TOKEN" \
  "$BASE_URL/admin/migration-status?include_checks=1"
```

`connectivity_checks.notion / discord / google` の `ok` と `status` を確認してください。

## 12. 開発メモ

- Python package metadata: `pyproject.toml`
- 開発依存: `ruff`, `pytest`
- リリース補助: `.release-please-config.json`

ローカルでの簡易チェック例:

```bash
pytest
```

```bash
ruff check .
```
