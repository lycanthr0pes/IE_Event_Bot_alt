# IE_Event_Bot

Discord と Google Calendar / Notion を同期する Bot です。

## Workflow

### 1) Google Calendar -> Discord / Notion

- `services/watcher/register.py` / `services/watcher/renew.py` が Google Calendar watch を管理
  - 通知先は `GCAL_WEBHOOK_URL`（`https://<webhook-domain>/gcal/webhook`）
- `services/webhook/webhook.py` が通知受信後に Google Calendar の差分を取得して反映
  - Notion 内部DB / 外部DB
  - Discord Scheduled Events

### 2) Discord -> Google Calendar / Notion (HTTP + 差分ポーリング)

- `services/webhook/webhook.py` が Discord Scheduled Events 一覧を定期取得
- 前回スナップショットとの差分で `create/update/delete` を判定
- 差分を Google Calendar / Notion に反映

## Services

- `services/bot`: 旧Gateway運用。`ENABLE_REALTIME_SYNC=false` ならイベント同期は無効
- `services/watcher`: Google Calendar watch 登録 / 更新
- `services/webhook`: 同期本体（Google通知受信 + 5分差分同期 + 通知ジョブ）

## Auto Archive

- 外部DB: イベント日が「今日から30日以上前」ならアーカイブ
- 内部DB: イベントの終了時刻（end なければ start）が「現在時刻以下」ならアーカイブ

## Command Features

### HTTP Endpoints

- `POST /sync/all`:
  - Google差分同期 + Discord差分同期を実行
- `GET/POST /gcal/sync`:
  - `POST /sync/all` と同等（互換）
- `POST /jobs/qa-check`:
  - Q&A DB の差分確認と未回答通知
- `POST /jobs/reminder`:
  - 前日リマインド送信
- `POST /jobs/run-all`:
  - 同期 + QA + リマインドを一括実行

## Operation

1. `services/webhook` をデプロイして URL を確定
2. `services/watcher` の `GCAL_WEBHOOK_URL` に webhook URL を設定
3. `services/watcher/register.py` を実行して watch 初回登録
4. 定期的に `services/watcher/renew.py` を実行して watch 更新
5. Cloud Scheduler などで以下を定期実行
   - `POST https://<webhook-domain>/sync/all` を 5分間隔
   - `POST https://<webhook-domain>/jobs/qa-check` を 5-10分間隔
   - `POST https://<webhook-domain>/jobs/reminder` を 5分間隔

## Cloud Scheduler Runbook

### 推奨ジョブ構成（最小）

1. `sync-all`:
   - URL: `POST /sync/all`
   - Cron: `*/5 * * * *`
2. `qa-check`:
   - URL: `POST /jobs/qa-check`
   - Cron: `*/10 * * * *`（通知遅延を抑えるなら `*/5`）
3. `reminder`:
   - URL: `POST /jobs/reminder`
   - Cron: `*/5 * * * *`

### 必須環境変数（webhook）

- `NOTION_TOKEN`
- `NOTION_EVENT_INTERNAL_ID`
- `NOTION_EVENT_ID`（外部DBを使う場合）
- `GOOGLE_CALENDAR_ID`
- `GOOGLE_SERVICE_ACCOUNT_JSON` または `GOOGLE_SERVICE_ACCOUNT_JSON_PATH`
- `DISCORD_TOKEN`
- `DISCORD_GUILD_ID`
- `STATE_DIR`（永続ボリュームを推奨）
- `SYNC_INTERVAL_SECONDS=300`

### 通知ジョブ用の追加環境変数

- `NOTION_QA_ID`
- `QA_CHANNEL_ID`
- `REMINDER_CHANNEL_ID`
- `REMINDER_ROLE_ID`
- `REMINDER_WINDOW_MINUTES`（既定15）

### GCP コマンド例（Cloud Run + Cloud Scheduler）

以下は `bash` 前提の例。

```bash
# 0) 変数設定
PROJECT_ID="<your-project-id>"
REGION="asia-northeast1"
SERVICE_NAME="ie-event-webhook"
IMAGE="gcr.io/${PROJECT_ID}/${SERVICE_NAME}:latest"

WEBHOOK_URL="https://${SERVICE_NAME}-<hash>-an.a.run.app"
SCHEDULER_SA="scheduler-invoker@${PROJECT_ID}.iam.gserviceaccount.com"
```

```bash
# 1) API有効化
gcloud services enable run.googleapis.com cloudscheduler.googleapis.com iam.googleapis.com \
  --project "${PROJECT_ID}"
```

```bash
# 2) Scheduler実行用SA作成
gcloud iam service-accounts create scheduler-invoker \
  --project "${PROJECT_ID}" \
  --display-name "Scheduler Invoker"
```

```bash
# 3) Cloud Run デプロイ（環境変数は必要に応じて追加）
gcloud run deploy "${SERVICE_NAME}" \
  --project "${PROJECT_ID}" \
  --region "${REGION}" \
  --image "${IMAGE}" \
  --platform managed \
  --allow-unauthenticated \
  --set-env-vars SYNC_INTERVAL_SECONDS=300
```

```bash
# 4) Scheduler SA に Cloud Run Invoker 権限付与
gcloud run services add-iam-policy-binding "${SERVICE_NAME}" \
  --project "${PROJECT_ID}" \
  --region "${REGION}" \
  --member "serviceAccount:${SCHEDULER_SA}" \
  --role "roles/run.invoker"
```

```bash
# 5) 5分同期ジョブ
gcloud scheduler jobs create http sync-all \
  --project "${PROJECT_ID}" \
  --location "${REGION}" \
  --schedule "*/5 * * * *" \
  --time-zone "Asia/Tokyo" \
  --uri "${WEBHOOK_URL}/sync/all" \
  --http-method POST \
  --oidc-service-account-email "${SCHEDULER_SA}" \
  --oidc-token-audience "${WEBHOOK_URL}"
```

```bash
# 6) QA通知ジョブ（10分）
gcloud scheduler jobs create http qa-check \
  --project "${PROJECT_ID}" \
  --location "${REGION}" \
  --schedule "*/10 * * * *" \
  --time-zone "Asia/Tokyo" \
  --uri "${WEBHOOK_URL}/jobs/qa-check" \
  --http-method POST \
  --oidc-service-account-email "${SCHEDULER_SA}" \
  --oidc-token-audience "${WEBHOOK_URL}"
```

```bash
# 7) 前日リマインドジョブ（5分）
gcloud scheduler jobs create http reminder \
  --project "${PROJECT_ID}" \
  --location "${REGION}" \
  --schedule "*/5 * * * *" \
  --time-zone "Asia/Tokyo" \
  --uri "${WEBHOOK_URL}/jobs/reminder" \
  --http-method POST \
  --oidc-service-account-email "${SCHEDULER_SA}" \
  --oidc-token-audience "${WEBHOOK_URL}"
```

```bash
# 8) 手動実行テスト
gcloud scheduler jobs run sync-all --project "${PROJECT_ID}" --location "${REGION}"
gcloud scheduler jobs run qa-check --project "${PROJECT_ID}" --location "${REGION}"
gcloud scheduler jobs run reminder --project "${PROJECT_ID}" --location "${REGION}"
```

```bash
# 9) ログ確認
gcloud run services logs read "${SERVICE_NAME}" \
  --project "${PROJECT_ID}" \
  --region "${REGION}" \
  --limit 200
```

## Health Check

- `GET /health` -> `ok`
- `GET/POST /gcal/sync` -> 手動同期（互換）
- `POST /sync/all` -> 手動同期（推奨）
- `POST /jobs/qa-check` -> QA通知ジョブ
- `POST /jobs/reminder` -> 前日リマインドジョブ
- `POST /jobs/run-all` -> 同期+通知一括ジョブ

## Troubleshooting

### 1) webhook にリクエストが届いているか確認

- `services/webhook` ログに `/gcal/webhook` アクセスが出るか確認
- まず疎通確認:
  - `curl -i https://<webhook-domain>/health`
  - `curl -i -X POST https://<webhook-domain>/sync/all`

### 2) 手動同期で切り分ける

- `POST /sync/all` が成功して Notion / Discordが更新される場合:
  - Notion API / Google API / 認証は概ね正常
  - 問題は「watch通知」「Scheduler」「入力データ」に絞れる

### 3) `updatedMinTooLongAgo` (HTTP 410) が出る場合

- ログに `updatedMinTooLongAgo` が出たら古い同期状態
- 状態ファイルをリセットして再同期

### 4) Notion / Discord に反映されない場合

- `services/webhook` ログで以下を確認:
  - `Google events fetched: N`
  - `Sync completed`
  - Notion API エラー有無

## Notion DB Recovery Checklist

Notion DB を誤って直接編集した場合は、次の順で復旧する。

1. まずDBを複製し、復旧作業は複製側で実施する。
2. プロパティ名と型を確認し、スキーマを戻す。
3. ID系プロパティ（`メッセージID` / `GoogleイベントID`）を復元する。
4. 重複ページを整理して不要ページをアーカイブする。
5. 同期を手動実行し、ログで整合性を確認する。

### Required Properties

- 内部用DB（必須）
  - `イベント名` (title)
  - `内容` (rich_text)
  - `日時` (date)
  - `メッセージID` (rich_text)
  - `作成者ID` (rich_text)
  - `ページID` (rich_text)
  - `イベントURL` (url)
  - `GoogleイベントID` (rich_text)
  - `場所` (rich_text)
- 外部用DB（必須）
  - `イベント名` (title)
  - `内容` (rich_text)
  - `日時` (date)
  - `メッセージID` (rich_text)
  - `作成者ID` (rich_text)
  - `ページID` (rich_text)
  - `GoogleイベントID` (rich_text)

### ID復元の優先順

1. Discord同期を復旧したい場合:
   `メッセージID` に Discord Scheduled Event ID を入れる。
2. Google同期を復旧したい場合:
   内部用DBの `GoogleイベントID` に Google Event ID を入れる。
3. 外部用DBは `メッセージID` と `GoogleイベントID` のどちらでも webhook 側で照合されるが、混在させない。

### Re-sync and Verification

1. 手動同期を実行:
   - `POST /sync/all`
   - または `POST /gcal/sync`
2. `services/webhook` ログで以下を確認:
   - `Google events fetched: N`
   - `Sync completed`
3. `services/webhook` ログで以下を確認:
   - Discord poll / Google sync でエラーが増えていないこと
