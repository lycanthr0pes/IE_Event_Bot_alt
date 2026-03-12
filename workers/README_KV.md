# Workers KV State 詳細ガイド

このドキュメントは、このリポジトリの Worker が `STATE_KV` に保存する状態データの仕様をまとめたものです。
対象実装は主に [workers/src/state.py](./src/state.py) です。

## 1. 役割

`STATE_KV` は以下のために使います。

- 同期カーソル保存（Google 差分同期）
- 同期クールダウン判定
- Google webhook 重複受信の抑止
- Google/Notion/Discord 間の ID マップ保持
- ジョブの重複通知防止キャッシュ
- watch 情報の保持
- 最終実行結果（diagnostics）の保持

`StateStore.enabled()` が `false`（`STATE_KV` バインディングなし）の場合、KV 読み書きはスキップされます。

## 2. バインディング前提

この Worker では `env.STATE_KV` が必要です。
`wrangler.jsonc` に `vars` はありますが、KV バインディング自体（`kv_namespaces`）は環境側設定で与える前提です。

確認観点:

- `/admin/migration-status` の `kv_enabled` が `true` であること
- `sync_state`, `watch_state`, `last_results` が期待どおり更新されること

## 3. キー一覧（保存内容）

### 3.1 同期制御・カーソル

1. `sync:updated_min`  
用途: Google Calendar 差分取得カーソル（RFC3339 文字列）
例:
```text
2026-03-11T04:20:31Z
```
主な更新箇所:
- `run_google_delta_fetch()` 成功時
- `/sync/all` 成功時（Google fetch + apply 成功後）

2. `sync:last_epoch`  
用途: 最終同期成功時刻（Unix epoch 秒）
例:
```text
1778201220.48231
```
主な更新箇所:
- `/sync/all` 全体成功時
- クールダウン判定 `should_skip_sync_by_cooldown()` の参照元

### 3.2 Google webhook 重複抑止

1. `gcal_msg:{channel_id}:{message_number}`  
用途: Google webhook (`/gcal/webhook`) の同一通知重複抑止
値: `"1"`（固定）
備考:
- `KV_GCAL_DEDUPE_ENABLED=true` のときのみ有効
- 既存キーがあれば 204 を返して処理スキップ

### 3.3 ID マップ

1. `map:gcal_discord`  
用途: Google event ID -> Discord event ID の対応
型: JSON object
例:
```json
{
  "4f8r1v1q0d2f2v3s9h0l9k0": "1359876543210987654"
}
```

2. `map:gcal_notion`  
用途: Google event ID -> Notion page ID の対応
型: JSON object（`internal` と `external` の2系統）
例:
```json
{
  "internal": {
    "4f8r1v1q0d2f2v3s9h0l9k0": "2f6f0ab3-9ed5-4f91-aee6-2ecf8d49e991"
  },
  "external": {
    "4f8r1v1q0d2f2v3s9h0l9k0": "77db2f5e-b0db-4af7-b5b5-b74ad6a9f53e"
  }
}
```

### 3.4 Discord ポーリングスナップショット

1. `discord:snapshot`  
用途: Discord scheduled events の前回状態スナップショット
型: JSON object (`event_id` -> fingerprint string)
例:
```json
{
  "1359876543210987654": "{\"description\":\"...\",\"id\":\"1359876543210987654\",\"location\":\"...\",\"name\":\"...\",\"scheduled_end_time\":\"...\",\"scheduled_start_time\":\"...\",\"status\":\"...\"}"
}
```
備考:
- `run_discord_notion_poll_sync()` で現状態と比較し、作成/更新/削除を判定

### 3.5 Google 認証キャッシュ

1. `google:access_token`  
用途: Google API アクセストークンキャッシュ

2. `google:expires_at`  
用途: 期限 epoch 秒
備考:
- 期限 60 秒前を切ったトークンは無効扱い
- broker / service account / admin API token セット後に更新される

### 3.6 Google watch 状態

1. `gcal_watch_state`  
用途: events.watch チャンネル状態保持
型: JSON object
例:
```json
{
  "channel_id": "gcal-f4ce2c06-9d31-4c8d-9e66-d3b7db6a9f6d",
  "resource_id": "AENx2U...",
  "expiration": "1778285874000",
  "calendar_id": "example@group.calendar.google.com",
  "created_at": "2026-03-11T04:21:14.278489+00:00"
}
```

### 3.7 ジョブキャッシュ

1. `qa_cache`  
用途: Q&A 通知済み判定（初回スパイク・重複防止）
型: JSON object (`page_id` -> `last_edited_time`)

2. `reminder_cache`  
用途: 前日リマインド送信済み判定
型: JSON object (`discord_event_id` -> sent timestamp ISO8601)

3. `cleanup:last_epoch`  
用途: cleanup ジョブ最終実行時刻（epoch 秒）

### 3.8 診断結果

1. `result:{op_name}`  
用途: API/cron 実行結果の最新1件を保存
型:
```json
{
  "updated_at": "2026-03-11T04:25:00.123456+00:00",
  "payload": {
    "ok": true
  }
}
```

`op_name` 例:

- `sync_all`
- `sync_discord_notion`
- `job_qa_check`
- `job_reminder`
- `job_cleanup`
- `job_run_all`
- `gcal_watch_register`
- `gcal_watch_renew`
- `gcal_watch_ensure`

## 4. いつ更新されるか（フロー）

1. `/sync/all`
- `sync:updated_min`（成功時）
- `sync:last_epoch`（全体成功時）
- `map:gcal_discord`, `map:gcal_notion`（Google apply 成功時）
- `discord:snapshot`（Discord poll 実行時）
- `result:sync_all`

2. `/gcal/webhook`
- `gcal_msg:*`（重複判定）
- その後 `/sync/all` 相当処理により上記キーが更新されうる

3. `/admin/gcal/watch/register` / `renew`
- `gcal_watch_state`
- `result:gcal_watch_register` / `result:gcal_watch_renew`

4. `/jobs/qa-check` / `reminder` / `cleanup` / `run-all`
- `qa_cache` / `reminder_cache` / `cleanup:last_epoch`
- `result:job_*`

## 5. 参照 API（運用確認）

状態確認はまず `/admin/migration-status` を使うのが安全です。

- `sync_state.last_epoch`
- `sync_state.updated_min`
- `watch_state`
- `last_results.*`
- `kv_enabled`

接続確認も含める場合:

```bash
curl -sS -H "Authorization: Bearer $TOKEN" \
  "$BASE_URL/admin/migration-status?include_checks=1"
```

## 6. Wrangler で直接 KV を見る

環境によってコマンドオプション差があるため、`wrangler --help` で最終確認してください。
一般的には以下です。

```bash
cd workers
wrangler kv key list --binding STATE_KV --remote
wrangler kv key get --binding STATE_KV "sync:last_epoch" --remote
wrangler kv key get --binding STATE_KV "map:gcal_notion" --remote
```

## 7. 運用上の注意

1. KV は最終的整合性です。高頻度更新直後は読み取りタイミングで古い値が見える可能性があります。
2. 同期の排他は Durable Object ロックで行いますが、ロック対象外の処理や webhook 多重入力には設計上の限界があります。
3. `gcal_msg:*` は webhook イベント番号単位の重複抑止です。永続増加が気になる場合は運用でメンテナンス方針を決めてください。
4. `google:access_token` は機微情報なので、ログやスクリーンショットで露出させないでください。

## 8. トラブルシュート

1. `kv_enabled=false`
- `STATE_KV` バインディング未設定を確認

2. `/sync/all` が `cooldown_skip`
- `sync:last_epoch` と `SYNC_INTERVAL_SECONDS` を確認
- 必要なら `KV_SYNC_COOLDOWN_ENABLED=false` で一時無効化

3. webhook で処理が進まない
- `KV_GCAL_DEDUPE_ENABLED` と `gcal_msg:*` キー存在を確認
- `X-Goog-Channel-ID`, `X-Goog-Message-Number` が正しく来ているか確認

4. watch が切れる
- `gcal_watch_state.expiration` を確認
- `CRON_ENABLE_GCAL_WATCH_ENSURE` と `GCAL_WATCH_RENEW_THRESHOLD_SECONDS` を確認
