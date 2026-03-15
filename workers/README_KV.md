# Workers KV State 詳細ガイド

このドキュメントは、このリポジトリの Worker が `STATE_KV` に保存する状態データの現行仕様をまとめたものです。
主な実装は `workers/src/state.py`、`workers/src/google_apply_sync.py`、`workers/src/discord_notion_sync.py`、`workers/src/google_auth.py`、`workers/src/google_watch.py`、`workers/src/jobs.py` です。

## 1. 役割

`STATE_KV` は主に以下の用途で使われます。

- Google 差分同期カーソルの保存
- `/sync/all` のクールダウン判定
- Google webhook 重複受信の抑止
- Google / Discord / Notion 間の ID マップ保持
- Google apply / Discord-Notion poll の繰り越しキュー保持
- Discord scheduled events の前回スナップショット保持
- Google access token キャッシュ
- Google watch 状態保持
- ジョブ重複通知防止キャッシュ
- API / cron の最終実行結果保持

`StateStore.enabled()` が `false` の場合、これらの保存処理は無効化されます。

## 2. バインディング前提

この Worker は `env.STATE_KV` を利用します。現行の `workers/wrangler.jsonc` では `kv_namespaces` が定義済みです。

運用確認の観点:

- `/health` の `kv_state_enabled` が `true`
- `/admin/migration-status` の `kv_enabled` が `true`
- `sync_state`, `watch_state`, `last_results` が更新される

## 3. キー一覧

### 3.1 同期カーソル・クールダウン

1. `sync:updated_min`  
用途: Google Calendar 差分取得カーソル（RFC3339）

例:

```text
2026-03-11T04:20:31Z
```

更新タイミング:

- `run_google_delta_fetch()` 成功時
- `/sync/all` で Google fetch と apply が両方成功した時

2. `sync:last_epoch`  
用途: `/sync/all` 最終成功時刻（Unix epoch 秒）

例:

```text
1778201220.48231
```

更新タイミング:

- `/sync/all` 全体成功時
- クールダウン判定 `should_skip_sync_by_cooldown()` の参照元

### 3.2 Google webhook 重複抑止

1. `gcal_msg:{channel_id}:{message_number}`  
用途: `/gcal/webhook` で同一通知を 204 で握りつぶすための重複判定

値:

```text
1
```

備考:

- `KV_GCAL_DEDUPE_ENABLED=true` の場合のみ利用
- `X-Goog-Channel-ID` と `X-Goog-Message-Number` の組で記録

### 3.3 Google と他サービスの ID マップ

1. `map:gcal_discord`  
用途: `Google event id -> Discord scheduled event id`

例:

```json
{
  "4f8r1v1q0d2f2v3s9h0l9k0": "1359876543210987654"
}
```

2. `map:gcal_notion`  
用途: `Google event id -> Notion page id`

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

### 3.4 同期繰り越しキュー

1. `sync:google_apply_queue`  
用途: `GOOGLE_APPLY_MAX_EVENTS_PER_RUN` を超えた Google イベント、または apply 失敗イベントの再試行キュー

例:

```json
[
  {
    "id": "google-event-id",
    "summary": "Team Meeting"
  }
]
```

2. `sync:discord_notion_queue`  
用途: `DISCORD_NOTION_MAX_CHANGES_PER_RUN` を超えた Discord 差分、または同期失敗差分の再試行キュー

例:

```json
[
  {
    "op": "upsert",
    "id": "1359876543210987654"
  },
  {
    "op": "delete",
    "id": "1359876543210987655"
  }
]
```

### 3.5 Discord ポーリングスナップショット

1. `discord:snapshot`  
用途: Discord scheduled events の前回状態

例:

```json
{
  "1359876543210987654": "{\"description\":\"...\",\"id\":\"1359876543210987654\",\"location\":\"...\",\"name\":\"...\",\"scheduled_end_time\":\"...\",\"scheduled_start_time\":\"...\",\"status\":\"...\"}"
}
```

備考:

- `run_discord_notion_poll_sync()` が前回との差分比較に利用

### 3.6 Google 認証キャッシュ

1. `google:access_token`  
用途: Google API access token キャッシュ

2. `google:expires_at`  
用途: token 有効期限（epoch 秒）

備考:

- `expires_at - 60秒` までは有効扱い
- `/admin/google-token`、broker、service account 発行成功時に更新

### 3.7 Google watch 状態

1. `gcal_watch_state`  
用途: Google Calendar `events.watch` の現在状態

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

### 3.8 ジョブ系キャッシュ

1. `qa_cache`  
用途: Q&A 通知済み・初回実行判定

例:

```json
{
  "_first_qa_run": false,
  "page-id": "2026-03-11T04:21:14.278489+00:00"
}
```

2. `reminder_cache`  
用途: 前日リマインド送信済みイベント記録

例:

```json
{
  "1359876543210987654": "2026-03-11T04:21:14.278489+00:00"
}
```

3. `cleanup:last_epoch`  
用途: cleanup ジョブ最終実行時刻（epoch 秒）

### 3.9 実行結果

1. `result:{op_name}`  
用途: API / cron 実行結果の最新 1 件

例:

```json
{
  "updated_at": "2026-03-11T04:25:00.123456+00:00",
  "payload": {
    "ok": true
  }
}
```

`op_name` の主な値:

- `sync_all`
- `sync_discord_notion`
- `job_qa_check`
- `job_reminder`
- `job_cleanup`
- `job_run_all`
- `gcal_watch_register`
- `gcal_watch_renew`
- `gcal_watch_ensure`

## 4. いつ更新されるか

1. `/sync/all`
- `sync:updated_min`
- `sync:last_epoch`
- `map:gcal_discord`
- `map:gcal_notion`
- `sync:google_apply_queue`
- `discord:snapshot`
- `sync:discord_notion_queue`
- `result:sync_all`

2. `/gcal/webhook`
- `gcal_msg:*`
- その後 `/sync/all` 相当の結果に応じて上記各キーが更新

3. `/sync/discord-notion`
- `discord:snapshot`
- `sync:discord_notion_queue`
- `google:access_token`, `google:expires_at` 更新の可能性あり
- `result:sync_discord_notion`

4. `/admin/google-token`
- `google:access_token`
- `google:expires_at`

5. `/admin/gcal/watch/register` / `/admin/gcal/watch/renew`
- `gcal_watch_state`
- `result:gcal_watch_register` / `result:gcal_watch_renew`

6. cron の watch ensure
- `gcal_watch_state`
- `result:gcal_watch_ensure`

7. `/jobs/qa-check` / `/jobs/reminder` / `/jobs/cleanup` / `/jobs/run-all`
- `qa_cache`
- `reminder_cache`
- `cleanup:last_epoch`
- `result:job_*`

## 5. 参照 API

日常の確認は `/admin/migration-status` が最も安全です。

確認できる主な項目:

- `kv_enabled`
- `sync_state.last_epoch`
- `sync_state.updated_min`
- `watch_state`
- `last_results.*`
- `google_auth.cache`

外部疎通を含める場合:

```powershell
curl.exe -sS -H "Authorization: Bearer $TOKEN" `
  "$BASE_URL/admin/migration-status?include_checks=1"
```

## 6. Wrangler で直接 KV を見る

Wrangler の細かいサブコマンド差異は版によって異なるため `wrangler kv --help` で最終確認してください。

```powershell
cd workers
wrangler kv key list --binding STATE_KV --remote
wrangler kv key get --binding STATE_KV "sync:last_epoch" --remote
wrangler kv key get --binding STATE_KV "map:gcal_notion" --remote
wrangler kv key get --binding STATE_KV "sync:google_apply_queue" --remote
wrangler kv key get --binding STATE_KV "sync:discord_notion_queue" --remote
```

## 7. 運用上の注意

1. KV は最終的整合性なので、直後の読み取りで古い値が見える可能性があります。
2. `/sync/all` の同時実行抑止は Durable Object ロックで行いますが、KV 自体は厳密トランザクションではありません。
3. `gcal_msg:*` は TTL なしで蓄積されます。運用上の保守方針を決めてください。
4. `google:access_token` は機微情報です。ログや画面共有に出さないでください。
5. キュー系キーが残り続ける場合は、`max_events_per_run` / `max_changes_per_run` の設定値とエラー内容を併せて確認してください。

## 8. トラブルシュート

1. `kv_enabled=false`
- `STATE_KV` バインディング未設定または壊れている可能性があります。

2. `/sync/all` が `cooldown_skip`
- `sync:last_epoch` と `SYNC_INTERVAL_SECONDS` を確認してください。
- 必要なら一時的に `KV_SYNC_COOLDOWN_ENABLED=false` で切り分けできます。

3. `/sync/all` が `in_progress_skip`
- Durable Object ロックが残っている可能性があります。
- `/admin/migration-status` の `sync_lock` と `SYNC_DO_LOCK_TTL_SECONDS` を確認してください。

4. webhook で処理が進まない
- `KV_GCAL_DEDUPE_ENABLED` と `gcal_msg:*` の存在を確認してください。
- `X-Goog-Channel-ID` と `X-Goog-Message-Number` が期待どおり届いているか確認してください。

5. Google apply / Discord poll の処理が少しずつしか進まない
- `sync:google_apply_queue` または `sync:discord_notion_queue` に繰り越しが残っていないか確認してください。
- `GOOGLE_APPLY_MAX_EVENTS_PER_RUN` と `DISCORD_NOTION_MAX_CHANGES_PER_RUN` を確認してください。

6. watch が切れる
- `gcal_watch_state.expiration` を確認してください。
- `CRON_ENABLE_GCAL_WATCH_ENSURE` と `GCAL_WATCH_RENEW_THRESHOLD_SECONDS` を確認してください。
