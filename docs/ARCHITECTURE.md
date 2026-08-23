# Архитектура FastSync

Этот документ описывает фактическое состояние workspace версии `0.1.0`. Слова «планируется» в разделе roadmap не означают, что соответствующий код уже существует.

## Контекст и границы MVP

FastSync состоит из одинаковых агентов на каждом компьютере. Агент одновременно:

- обслуживает локальный HTTP control plane;
- хранит локальное состояние и секреты в SQLite;
- обнаруживает другие агенты через IPv4 UDP;
- устанавливает и принимает authenticated QUIC connections;
- запускает outgoing jobs и исполняет incoming protocol requests.

Отдельного controller/process/service сейчас нет. Передача всегда one-way push, инициированный на отправителе. Destination не является зеркалом: записи, отсутствующие в source manifest, не удаляются.

## Control plane и data plane

```text
                               CONTROL PLANE

 Browser ── HTTP/JSON ──> axum Router ──> JobManager ──> TransferEngine
    ^                        │    │             │               │
    └──── WebSocket events ──┘    └─────────────┴──> SQLite <───┘
                                      identity / trust / jobs / resume

 Peer discovery <──── unsigned IPv4 UDP JSON broadcast ────> Peer discovery

                                DATA PLANE

 TransferEngine A <══ QUIC + TLS 1.3 + signed handshake ══> TransferEngine B
                  <══ framed CBOR request/response RPC   ══>
                  ══> parallel raw chunk streams         ══>
```

HTTP bind по умолчанию loopback. Discovery сообщает HTTP port, но удалённого controller, который использует peer HTTP API, нет. QUIC connection переносит как protocol control messages, так и raw file data; разделение planes здесь логическое, а не отдельными процессами.

## Слои workspace

### `fastsync-core`

Не зависит от сети или SQLite. Содержит domain types и синхронные filesystem primitives:

- нормализацию relative wire paths;
- recursive scan без follow symlinks;
- снимки metadata и проверку source mutation;
- BLAKE3 полного файла и fixed-size chunks;
- low-level sibling `.fastsync-part` API и explicit-path staging primitives;
- полную проверку, metadata application и atomic replacement.

На Windows atomic replacement изолирован в `MoveFileExW`; остальные платформы используют `std::fs::rename`.

### `fastsync-protocol`

Содержит единственную поддерживаемую версию wire protocol, `PROTOCOL_VERSION = 2`, и не выполняет I/O кроме generic async framing. Crate re-export-ит необходимые core types, чтобы sender и receiver использовали одинаковую Serde schema.

### `fastsync-storage`

Одна `rusqlite::Connection` находится под `parking_lot::Mutex` и разделяется через `Arc`. Encode выполняется до lock, decode после lock, где это возможно. Модель рассчитана на один agent process, а не на несколько writers.

### `fastsync-discovery`

Один IPv4 `UdpSocket` одновременно отправляет announcement по interval и принимает datagrams. Внутренний bounded `mpsc` передаёт validated observations агенту. Discovery не устанавливает trust.

### `fastsync-transfer`

Объединяет Quinn endpoint, identity, database, global hashing semaphore и runtime state incoming jobs. Sender и receiver используют отдельный bidirectional QUIC stream на каждый RPC/chunk.

### `fastsync-agent`

Composition root: Clap CLI, data directory, database/identity/engine startup, axum router, discovery tasks, event bus и lifecycle JobManager. `web/index.html` встраивается compile-time, поэтому runtime static directory отсутствует.

## Основные структуры данных

### Domain model

| Тип | Поля и invariant |
|---|---|
| `VerificationMode` | `Fast` или default `Verified` |
| `TransferConfig` | `verification_mode`, `chunk_size`, `concurrency`, `retry_limit` |
| `TransferJob` | UUIDv4 `id`, optional `peer_id`, native `source_root/destination_root`, config, core status, progress, recoverable errors |
| `ManifestEntry` | Validated UTF-8 relative path, size, signed nanosecond mtime, `File/Directory`, read-only bit |
| `SourceMetadata` | size, mtime, file type, read-only; используется для mutation checks |
| `ChunkDescriptor` | contiguous zero-based `index`, `offset`, nonzero `size`, BLAKE3 hash |
| `HashedFile` | File-only manifest entry, full BLAKE3 и ordered chunk descriptors |
| `FileError` | optional relative path, operation category и human-readable message |
| `TransferProgress` | file/byte counters, scheduler estimates, rates и optional current file |

`HashedFile` invariant:

- indices начинаются с `0` без пропусков;
- первый offset равен `0`, следующий равен концу предыдущего chunk;
- каждый chunk не больше configured chunk size;
- коротким может быть только последний chunk;
- сумма sizes равна file size;
- empty file имеет full BLAKE3 пустого ввода и ноль chunks.

### Wire path

Relative path передаётся как UTF-8 с separator `/`. Не допускаются пустая строка, leading slash, Windows drive/UNC, backslash, `.`, `..`, пустой component, NUL/control bytes, Windows-invalid characters, trailing dot/space, reserved device names и `.fastsync-stage-*`. `safe_join` выполняет только lexical join после этой проверки. Receiver отклоняет case-only aliases одного manifest и дополнительно проверяет destination root и каждый parent через `symlink_metadata`.

Это не openat-style sandbox: локальный процесс, способный одновременно заменять уже проверенные parents symlink/reparse points, находится вне гарантии containment. Unicode NFC/NFD normalization не выполняется; канонически эквивалентные имена на normalization-insensitive filesystems не поддерживаются как distinct paths.

### Runtime sender state

Outgoing job удерживает последовательно или одновременно:

- `ScanResult.entries` для всего дерева;
- map `relative_path -> CompareAction`;
- `Vec<HashedFile>` и затем maps negotiated files/plans;
- lazy iterator `UploadWork` поверх negotiated plans; `buffer_unordered` одновременно материализует только активное окно;
- `HashSet<String>` failed paths;
- counters и errors внутри mutable `TransferJob`.

Upload work не дублируется отдельным полным vector, но сами manifest, descriptors и `missing_chunks` внутри plans остаются целиком в памяти. Поэтому bounded polling не делает общий job manifest bounded по памяти.

### Runtime receiver state

`TransferEngine.incoming_jobs` — `DashMap<IncomingJobKey, Arc<IncomingJob>>`, где key состоит из `(peer_id, job_id)`. Одинаковый UUID другого peer не получает доступ к state.

`IncomingJob` хранит:

- requested/canonical destination roots, verification mode и chunk size;
- одноразовые manifest IDs, строгую compare sequence и exact committed `ManifestEntry` map;
- `DashMap<path, Arc<IncomingFile>>` negotiated files;
- список directory manifest entries/errors под `Mutex`, применяемый deepest-first при complete;
- keys in-memory destination leases для root, files и directories.

`IncomingFile` хранит exact negotiated `HashedFile`, set inflight chunk indices, async finalize mutex и atomic `finalized`. Одновременная повторная upload одного index и finalize при активных uploads запрещены.

Этот map непостоянный. После restart он восстанавливается не deserialize-операцией, а повторными `Compare` и `Negotiate` от sender; persistent chunk rows и job-scoped staging files проходят reconciliation. Staging UUID и последний committed manifest ID сохраняются в typed settings по `(peer_id, job_id)`.

### JobManager

`JobManager` содержит:

- `DashMap<job_id, RunnerControl>` активных outgoing runners;
- child `CancellationToken` на job;
- `watch::Sender<bool>` для pause;
- `DashMap<job_id, ProgressSnapshot>` для live throughput;
- общий shutdown token.

Два runner для одного job запрещены, но глобального ограничения числа разных outgoing jobs нет. Incoming jobs не получают RunnerControl и управляются protocol sender. Receiver имеет общие semaphores на 32 connection handlers и 64 request tasks.

## Scheduler pipeline

### 1. API validation и persistence

`POST /api/jobs` проверяет absolute existing non-symlink source directory, receiver-compatible absolute destination (native, Windows drive-absolute или UNC), наличие trusted peer и config bounds. Job сначала записывается как `pending`, затем JobManager запускает task.

### 2. Scan

Status становится `scanning`. `scan_directory` выполняется одним `spawn_blocking`, целиком собирает и сортирует manifest. Root error fatal; ошибки отдельных entries сохраняются. Symlinks не следуются, special entries пропускаются.

После scan sender заменяет свои `job_files` полным списком regular files и публикует totals.

### 3. Connect и authenticate

Создаётся новая QUIC connection, первый stream выполняет application handshake. Sender требует, чтобы server key уже был pinned локально. После handshake status становится `transferring`.

### 4. Manifest compare

Manifest получает random одноразовый `manifest_id` и режется по 512 entries. Batches отправляются строго последовательно, каждый как отдельный RPC stream. Sequence 0 начинает generation; receiver требует exact следующий sequence, отклоняет duplicate/tree/case aliases и удерживает full entry map до commit. Все последующие negotiate/upload/finalize/complete requests обязаны предъявлять тот же ID.

Реально receiver выдаёт:

- `Unchanged` для directory, успешно созданного/проверенного;
- `Unchanged` для Fast file с совпавшей metadata;
- `NeedHash` для остальных regular files;
- `Conflict` при path/filesystem conflict.

`Transfer` и `Delete` существуют в protocol enum, но receiver MVP их не выдаёт; delete operation отсутствует.

До filesystem mutation receiver арендует canonical destination root и relative paths за `(peer_id, job_id, wire_path)`. Root другого job, равный/выше/ниже уже арендованного path, получает retryable conflict. Последний batch reconciles stale `job_files`, runtime descriptors, staging files и leases предыдущей attempt, но не удаляет final destination files.

### 5. Source hashing

Files с `NeedHash/Transfer` хешируются через `buffer_unordered(job.concurrency)`, но каждый blocking hashing task сначала получает общий engine semaphore. Число фактических hash readers равно `min(requested concurrency, available_parallelism clamped to 1..8)` с учётом других jobs.

Каждый worker использует buffer `256 KiB`, одним sequential read считает full и chunk BLAKE3 и проверяет metadata до/после. В `Fast` hash cache lookup выполняется раньше semaphore; cache key включает hash native path bytes, size, mtime и chunk size, а cache hit всё равно проверяет текущую metadata. `Verified` полностью обходит hash cache и физически перечитывает source/destination.

### 6. Hash negotiation

Один `HashedFile` сначала проверяется на возможность уместиться в frame. Далее files группируются максимум по 64 и дополнительно по encoded frame limit 16 MiB. Batches обрабатываются sender последовательно.

Внутри одного negotiation request receiver обрабатывает files последовательно. Negotiation одного incoming job эксклюзивна, exact metadata сверяется с committed manifest, а destination hashing использует общий semaphore `1..8`. Разные jobs с непересекающимися roots могут выполняться параллельно.

Receiver классифицирует каждый chunk:

- `resumed`: есть row для того же `(job, path, index, full source hash)` и bytes staging file повторно прошли BLAKE3;
- `reused`: chunk на том же index/offset в existing same-size destination совпал и скопирован локально;
- `missing`: требуется network upload.

Whole-file identity existing destination даёт все chunks как `reused` без копирования в staging file.

### 7. Upload work planning

Sender проверяет, что три категории plan не пересекаются, покрывают все descriptors и их byte totals точны. `queued_chunks` считается по missing indices; отдельный полный `Vec<UploadWork>` не создаётся. Work item строится лениво при polling upload stream.

### 8. Parallel upload

`buffer_unordered(job.concurrency)` держит не более N upload futures в polling. Каждый future:

1. ждёт снятия pause;
2. повторно проверяет source metadata;
3. открывает source file и делает seek к chunk offset;
4. открывает новый QUIC bidirectional stream;
5. пишет framed header и raw bytes через `256 KiB` buffer;
6. параллельно считает outgoing BLAKE3;
7. finish-ит send side и ждёт exact acknowledgement.

Receiver на stream повторно проверяет manifest ID/lease, открывает staging file, seek-ит к offset, читает ровно descriptor size через свой `256 KiB` buffer, требует EOF, проверяет BLAKE3, flush-ит file и только затем upsert-ит completed chunk row. Отсутствие raw bytes/FIN более 60 секунд завершает stream.

Все missing chunks всех файлов отправляются до начала finalize любого файла.

### 9. Finalize

Status sender становится `verifying`. Files обходятся последовательно. Sender проверяет source metadata и отправляет `FinalizeFile` с manifest ID. Receiver требует active generation/lease, отсутствие inflight chunks и наличие каждой completed row, повторно читает полный staging file для full BLAKE3, делает `sync_all`, применяет mtime/read-only и атомарно заменяет destination. Chunk rows удаляются после успешного finalize.

Если whole existing destination уже был полностью хеширован и отмечен finalized в текущем runtime state, отдельного staging materialization нет; finalize подтверждает negotiated file.

### 10. Complete

`CompleteJob` проверяет manifest ID/leases и применяет metadata directories deepest-first. Pending/running files и directory conflicts учитываются как failures. Receiver job получает `completed` или `completed_with_errors`; acknowledgement содержит `completed_with_errors` и `failed_files`. Runtime state/leases удаляются, а staging root удаляется best-effort только при чистом success.

## Bounds и backpressure

| Ресурс/этап | Реальный bound |
|---|---:|
| API chunk size | `1 MiB..=1 GiB`; default `4 MiB` |
| Chunks одного файла | максимум 131 072 |
| API job concurrency | `1..=64`; default `32` |
| API retry limit | `0..=20`; default `6` |
| Hash readers на engine | `available_parallelism`, clamp `1..=8` |
| Compare batch | 512 manifest entries |
| Hashed batch | 64 files и одновременно frame <= 16 MiB |
| CBOR frame allocation | максимум 16 MiB после проверки prefix |
| Untrusted request frame | максимум 64 KiB |
| Upload polling | `job.concurrency` на outgoing job |
| Application stream buffer | `256 KiB` на sender chunk и `256 KiB` на receiver chunk |
| Discovery packet | 65 507 bytes |
| Discovery update channel | 128 в agent composition |
| WebSocket broadcast | 256 events |
| Fatal task channel | 2 messages |
| SQLite busy timeout | 5 секунд |
| Shutdown wait for runners | 5 секунд |
| QUIC transport | keepalive 10 s, idle 10 min, 512 bidi streams |
| Incoming handlers | 32 connections, 64 requests |
| Handshake/header/chunk idle | 15 s / 30 s / 60 s |

Backpressure обеспечивается `buffer_unordered`, Quinn stream flow/congestion control, bounded discovery channel и WebSocket broadcast semantics. Отсутствуют:

- bound на полный manifest, descriptor count и missing-index vectors в plans;
- global semaphore на outgoing upload streams всех jobs;
- adaptive concurrency;
- rate limiting и memory budget.

При N активных uploads минимальная видимая application-buffer составляющая на двух peers приблизительно `N * 512 KiB`. При default N=32 это около 16 MiB, при API maximum N=64 около 32 MiB, без Quinn buffers, CBOR frames, descriptors, tasks и OS page cache. Hashing добавляет до 8 buffers по 256 KiB на engine.

Progress `active_*` вычисляется после завершения work item как `min(remaining queued, configured concurrency)` и является оценкой. Scheduler не получает фактическое число live Quinn streams.

## Состояния jobs

Core status:

```text
pending -> scanning -> transferring -> verifying -> completed
    │          │              │              └----> completed_with_errors
    │          │              ├---- pause <----> resume
    │          │              └----> failed / cancelled
    └----------┴--------------------> failed / cancelled
```

Storage добавляет `interrupted`, которого нет в core enum. На database open active statuses `scanning/transferring/verifying` переводятся в `interrupted`, при этом CBOR payload сохраняет последний core status. Manual resume разрешён для `pending`, `paused`, `interrupted`, `failed` и `completed_with_errors`; он очищает старые outgoing errors и запускает pipeline заново с тем же job ID. Jobs не стартуют автоматически после restart.

Pause кооперативный: он проверяется между этапами/work items и до RPC, но уже выполняющийся operation не прерывается только из-за pause. Cancel закрывает transfer connection и кооперативно прерывает hashing/full verification/chunk reads; уже завершившая запись всё ещё может успеть получить acknowledgement.

## SQLite

### Connection policy

При каждом open:

```text
PRAGMA busy_timeout = 5000
PRAGMA foreign_keys = ON
PRAGMA journal_mode = WAL
PRAGMA synchronous = NORMAL
```

Migration version выше поддерживаемой (`2`) отклоняется, downgrade не выполняется. Migration применяется в transaction и фиксируется одновременно в `schema_migrations` и `user_version`.

### Schema v2

#### `schema_migrations`

| Column | Type/constraint |
|---|---|
| `version` | `INTEGER PRIMARY KEY` |
| `applied_at` | `INTEGER NOT NULL`, Unix milliseconds |

#### `settings`

| Column | Type/constraint |
|---|---|
| `key` | `TEXT PRIMARY KEY` |
| `kind` | `TEXT`, только `string/bytes` |
| `value` | `BLOB NOT NULL` |

Identity keys:

```text
transfer.identity.ed25519_signing_key
transfer.identity.device_name
transfer.identity.tls_certificate_der
transfer.identity.tls_private_key_der
receiver_staging_token_v1:<peer_id>:<job_id>
receiver_manifest_id_v2:<peer_id>:<job_id>
```

#### `devices`

`device_id TEXT PRIMARY KEY`, `name`, optional `public_key BLOB`, `address`, `first_seen`, `last_seen`. Index: `last_seen DESC`. Discovery upsert не стирает уже authenticated public key, если новый beacon его не содержит.

#### `trusted_peers`

`device_id TEXT PRIMARY KEY`, `name`, required `public_key BLOB`, `address`, `trusted_at`, `last_seen`. Index: case-insensitive name. Key length/Device ID relation проверяются application layer при trust/use, а не SQL CHECK.

#### `jobs`

| Column | Значение |
|---|---|
| `id` | UUID text primary key |
| `payload` | CBOR полного `TransferJob` |
| `status` | CHECK enum, включая storage-only `interrupted` |
| `progress_cbor` | CBOR полного `TransferProgress` |
| `bytes_transferred`, `total_bytes` | Дублированные indexed/query-friendly counters |
| `files_completed`, `total_files` | Дублированные counters |
| `error` | Последняя fatal/retry message |
| `created_at`, `updated_at` | Unix milliseconds |

Индексы: `created_at DESC`, `status`. При decode проверяется, что дублированные counters совпадают с `progress_cbor`.

#### `job_files`

Composite primary key `(job_id, path)`, foreign key к `jobs` с cascade delete. Хранит `size`, status enum, `bytes_transferred`, optional error и `updated_at`. Index `(job_id, status)`.

#### `chunks`

Composite primary key `(job_id, path, chunk_index)`, foreign key к `job_files` с cascade delete. `source_hash` обязан иметь 32 bytes; это full-file source hash, а не chunk hash. Index `(job_id, path, source_hash)` ускоряет resume lookup.

#### `hash_cache`

Primary key `(path, size, mtime_ns, chunk_size)`. `path` имеет вид `path-blake3:<hex>` от native encoded path, поэтому raw path не сохраняется в этой таблице. `full_hash` — 32 bytes, `chunks_cbor` — descriptors, `updated_at` — Unix milliseconds. `put` сначала удаляет другие entries того же path hash: одновременно хранится только одна metadata/chunk-size версия пути.

### Retention

Автоматической очистки старых jobs/devices/hash cache нет. Завершённые chunk rows удаляются per file; строки jobs и job_files остаются. Удаление job через storage API каскадно удаляет files/chunks, но HTTP endpoint удаления job отсутствует.

## Trust sequence

### Identity creation

1. При отсутствии setting генерируется 32-byte Ed25519 signing key через OS RNG.
2. Public key получается из signing key.
3. BLAKE3(public key) обрезается до 16 bytes; UUID version bits ставятся в `8`, variant в RFC 9562. Это Device ID.
4. Независимо создаются self-signed certificate/private key для SAN `fastsync.local` и сохраняются как DER.
5. BLAKE3 certificate DER служит 32-byte channel binding/fingerprint.

Имя устройства можно менять без изменения Device ID; удаление signing key/DB меняет identity.

### Discovery и probe

1. Неподписанный announcement сообщает claimed Device ID и ports.
2. Receiver discovery использует source datagram IP, а не поле адреса.
3. Для discovered trust probe передаёт ожидаемый Device ID. Manual probe без предварительного ID принимает identity фактического endpoint.
4. TLS 1.3 server предъявляет self-signed certificate. Client не строит PKI chain, но TLS handshake всё равно доказывает possession certificate private key и шифрует канал.
5. Client подписывает Ed25519 transcript с protocol version, identity, nonce, features, public key и fingerprint увиденного TLS certificate.
6. Server проверяет, что binding равен fingerprint собственного certificate, Device ID выведен из key, а signature valid.
7. Server подписывает transcript, содержащий client transcript/signature, оба Device ID, keys/nonces, свою identity и тот же TLS binding.
8. Client проверяет binding против certificate текущей QUIC connection, expected Device ID при его наличии и server signature.
9. Оба агента записывают authenticated peer в `devices`; trust пока остаётся false.

### Explicit pin

`POST /api/devices/{id}/trust` снова делает probe, если key ещё не pinned, и записывает `(Device ID, exact Ed25519 public key)` в `trusted_peers`. При недоступном peer допускается pin только ранее authenticated key, уже сохранённого в `devices`; одного beacon без key недостаточно.

### Bilateral enforcement

- Outgoing engine после handshake требует, чтобы remote server key совпал с локальным pin.
- Incoming server перед каждым request, кроме `Ping`, перечитывает trust и требует pin client key.
- Поэтому A->B требует одновременно `A trusts B` и `B trusts A`.
- Untrust начинает действовать для следующих request streams даже в уже authenticated connection.
- Agent untrust также закрывает текущие inbound connections peer и отменяет active outgoing runners.
- Authenticated connection обновляет address trusted peer только при совпадающем pinned key.

Key rotation workflow отсутствует: нужно out-of-band проверить новую identity, удалить старый trust и выполнить pairing заново на обоих peers.

## Resume invariants

Resume не доверяет одной SQLite row. Для повторного использования должны одновременно выполняться следующие условия:

1. Sender повторяет job с тем же UUID. Новый `POST /api/jobs` создаёт новый UUID и не использует старые chunk rows.
2. Receiver заново получает destination root, mode и chunk size. В живом `IncomingJob` изменение этих параметров для одного `(peer, job)` отклоняется.
3. `HashedFile` заново проходит structural validation.
4. Staging path равен `destination_root/.fastsync-stage-<persistent UUID>/<wire-path>` и должен быть regular non-symlink exact-size file.
5. Completed row должна совпасть по job, relative path, chunk index и full source hash.
6. Index должен ссылаться на exact descriptor новой negotiation.
7. Receiver физически перечитывает region staging file и сравнивает chunk BLAKE3. Несовпавшая/устаревшая row удаляется.
8. Missing/resumed/reused sets должны быть disjoint, exhaustive и иметь точные byte totals; sender валидирует ответ.
9. Upload header должен в точности совпасть с negotiated index/offset/size/hash.
10. Stream обязан содержать ровно `size` bytes и EOF; chunk hash проверяется до записи completion row.
11. Для staging finalize требует completion row каждого descriptor и full-file hash всего файла; negotiated whole-file reuse, уже отмеченный `finalized`, является отдельным путём без chunk rows.
12. Только после `sync_all` и metadata application выполняется atomic replacement; затем chunk rows очищаются.

### Crash windows

Filesystem и SQLite не объединены одной транзакцией, поэтому correctness достигается повторной проверкой:

- crash после bytes, но до row: chunk будет передан заново;
- crash после `flush`/row, но до durable media: region BLAKE3 при resume обнаружит повреждение и удалит row;
- crash после полного staging file, но до finalize: все валидные regions будут resumed, затем full hash проверен;
- crash после atomic rename, но до очистки rows/status: negotiation хеширует existing destination, признаёт whole-file reuse, а finalize/complete сходятся к completed state;
- invalid staging type/size приводит к reinitialize и очистке rows для path.

Resume использует fixed offsets. Он не ищет совпавший контент в другом offset или другом файле.

## Failure model

| Failure | Поведение |
|---|---|
| Source root недоступен/не directory | Fatal job failure |
| Ошибка отдельного scan entry | Entry отсутствует в manifest, ошибка сохраняется; итог обычно `completed_with_errors` |
| Destination file/directory conflict | Path-level failure, остальные paths продолжаются |
| Destination lease/root overlap другого job | Retryable job-level conflict до filesystem mutation |
| Source metadata/hash изменился | File-level либо fatal error в зависимости от стадии; snapshot нет |
| Chunk hash/length mismatch | Non-retryable invalid data; staging file не финализируется |
| Authentication, pin или version mismatch | Non-retryable failure |
| Network/stream truncation | JobManager reconnect/retry до configured limit |
| Remote error | Retry только если remote выставил `retryable=true` |
| SQLite error | Обычно fatal для текущего request/job; best-effort logging не скрывает потерю persistence |
| Agent shutdown/crash | Active DB statuses становятся/будут `interrupted`; automatic resume нет |
| WebSocket lag | Сообщение `resync`; client перечитывает REST resources |
| Discovery receive I/O failure | Логируется и retry с 50 ms..1 s; send errors повторяются на следующем announce interval, manual connect остаётся |

Retry backoff job runner: 500 ms с doubling до 15 s. Retry counter не сбрасывается после частичного прогресса. Повторная попытка проходит scan/connect/compare/hash negotiation заново, но receiver reconciliation уменьшает network bytes.

File-level ошибки дедуплицируются по relative path внутри одной attempt. Fatal transfer wrapper записывает job `failed` и message. Incoming receiver хранит progress в основном по files и формирует aggregate при `CompleteJob`; live incoming progress не симметричен outgoing.

## Будущее разделение controller/agent

Текущий HTTP API нельзя просто открыть центральному controller: он не аутентифицирован, даёт операции trust/job и позволяет указать recipient path. Безопасное разделение должно сохранить следующие границы:

- controller управляет intent, расписанием и observability, но не получает identity private keys;
- агенты остаются владельцами trust pins, path allowlists, local credentials и filesystem policy;
- file data идёт agent-to-agent, а не через controller, если relay явно не требуется;
- remote control API получает mutual authentication, authorization, replay protection и audit;
- incoming job требует policy/approval на recipient, а не только trust sender key;
- API/protocol compatibility версионируются независимо;
- потеря controller не должна повреждать уже идущий data-plane transfer.

Никаких controller crates, auth API или remote orchestration в `0.1.0` нет.

## Milestones 0.1-0.4

Это roadmap-ориентир без сроков; только `0.1` подтверждён кодом.

| Milestone | Статус | Содержание |
|---|---|---|
| `0.1` | **Текущий, реализован** | One-way manual push, bilateral trust, discovery/manual probe, QUIC/TLS 1.3 protocol v2, fixed chunks/resume/reuse, Fast/Verified, SQLite, local UI/API, pause/cancel/retry, local benchmark |
| `0.2` | **План, кода нет** | Измеряемый network benchmark/reporting, hard resource caps, корректные per-stage metrics, small-file network batching и adaptive concurrency после отдельного protocol design |
| `0.3` | **План, кода нет** | Отделение authenticated controller от agents, стабильный control API, recipient path policy/approval, audit; data plane остаётся peer-to-peer |
| `0.4` | **План, кода нет** | Dry-run/plan и mirror semantics, watcher/incremental jobs, затем bidirectional sync только после явной conflict model |

Protocol v2 нельзя молча расширять несовместимым batching. Если `0.2+` потребует иной stream lifecycle, нужен новый protocol version или явно negotiated backward-compatible feature; текущая реализация принимает только exact v2.
