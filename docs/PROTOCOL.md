# FastSync wire protocol v2

Implementer-oriented описание соответствует типам `fastsync-protocol` и их использованию в `fastsync-transfer` версии `0.1.0`. Единственная поддерживаемая версия — `2`. Этот документ не добавляет предполагаемых сообщений или batching, которых нет в коде.

## Константы и термины

| Константа | Значение |
|---|---:|
| `PROTOCOL_VERSION` | `2` (`u16`) |
| CBOR length prefix | 4 bytes, unsigned big-endian |
| `MAX_FRAME_SIZE` | `16 * 1024 * 1024` bytes CBOR payload |
| Discovery maximum | 65 507 bytes IPv4 UDP payload |
| Handshake nonce | 32 bytes |
| Ed25519 public key | 32 bytes |
| Ed25519 signature | 64 bytes после parse/verification |
| TLS certificate binding | 32-byte BLAKE3 certificate DER |
| Content/chunk hash | 32-byte BLAKE3 |
| Sender compare batch | максимум 512 entries |
| Sender hash batch | максимум 64 files и frame limit |
| Chunks одного файла | максимум 131 072 |
| Transfer stream buffer | 256 KiB, implementation detail |

`client` — инициатор QUIC connection и sender job. `server` — принимающий QUIC endpoint и recipient. После handshake обе стороны криптографически аутентифицированы, но могут оставаться untrusted.

## Transport

- Transport: QUIC через Quinn.
- Cryptographic transport: rustls, только TLS 1.3.
- Server name при connect: `fastsync.local`.
- Server certificate: persistent self-signed certificate с SAN `fastsync.local`.
- TLS client certificate отсутствует; client identity доказывается application handshake.
- Клиентский certificate verifier не проверяет CA chain, hostname, validity time или PKI trust. Он принимает certificate для TLS, после чего application handshake подписывает binding к exact certificate.

Client и server задают QUIC idle timeout 10 минут, keepalive 10 секунд и максимум 512 concurrent bidirectional streams. Congestion control остаётся Quinn default. Реализация не задаёт ALPN как отдельный protocol discriminator.

## Discovery datagram

Discovery не использует CBOR framing. Это один UTF-8 JSON object в одном IPv4 UDP datagram:

```json
{
  "device_id": "11111111-2222-8333-8444-555555555555",
  "device_name": "workstation-a",
  "protocol_version": 2,
  "port": 39463,
  "http_port": 8765,
  "agent_version": "0.1.0",
  "features": [
    "blake3",
    "fixed-chunk-resume",
    "tls-certificate-binding",
    "http-api",
    "websocket-events"
  ]
}
```

Правила receiver:

1. Datagram больше 65 507 bytes отклоняется.
2. JSON должен deserialize в `DiscoveryAnnouncement`; неизвестные JSON fields Serde по умолчанию игнорирует.
3. Source должен быть IPv4.
4. `protocol_version` должен точно равняться `2`.
5. Собственный `device_id` игнорируется.
6. Transfer endpoint формируется как `(UDP source IP, announcement.port)`.
7. HTTP endpoint формируется как `(UDP source IP, announcement.http_port)`.

UDP source port не используется. Announcement не подписан и не является trust evidence. В нём нет IP field; даже если неизвестное поле `ip` будет добавлено, оно игнорируется.

## CBOR framing

Каждое framed value кодируется так:

```text
0                   1                   2                   3
0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
+---------------------------------------------------------------+
|                 CBOR payload length (u32 BE)                  |
+---------------------------------------------------------------+
|                     exactly N CBOR bytes ...                  |
+---------------------------------------------------------------+
```

Receiver проверяет declared length до allocation. Payload больше 16 MiB, обрезанный prefix/payload и trailing bytes в in-memory `decode_frame` отклоняются. CBOR decoder должен потребить ровно один value без trailing CBOR data.

Типы сериализуются `serde` + `ciborium`. Struct fields и internally/adjacently tagged enums используют имена, указанные ниже. Canonical CBOR, CDDL schema и независимые golden vectors в workspace пока не определены; для совместимости эталоном является crate `fastsync-protocol` и его tests.

## Version envelope

После handshake каждый logical request/response обёрнут в:

```text
Versioned<T> {
    protocol_version: u16,
    payload: T
}
```

Aliases:

```text
RequestFrame  = Versioned<Request>
ResponseFrame = Versioned<Response>
```

Handshake frames не используют `Versioned`, но сами содержат `protocol_version`. Discovery также содержит свою версию.

Получатель request с неизвестной version возвращает local-v2 `Response::Error` с code `unsupported_protocol`, если frame удалось deserialize. Получатель response с неизвестной version завершает RPC с `ProtocolVersionMismatch`. Downgrade или диапазона версий нет.

## Identity и Device ID

Ed25519 key pair создаётся один раз и сохраняется. Device ID вычисляется:

```text
digest = BLAKE3(ed25519_public_key)       // 32 bytes
uuid_bytes = digest[0..16]
uuid_bytes[6] = (uuid_bytes[6] & 0x0f) | 0x80   // UUID version 8
uuid_bytes[8] = (uuid_bytes[8] & 0x3f) | 0x80   // RFC variant
device_id = UUID(uuid_bytes)
```

Любой handshake, где заявленный ID не выводится из supplied public key, отклоняется.

TLS certificate binding:

```text
binding = BLAKE3(first_peer_certificate_der)
```

Certificate и Ed25519 key — разные persistent key pairs. Device ID зависит только от Ed25519 key.

## Handshake stream

Первый client-opened bidirectional stream каждой QUIC connection зарезервирован для handshake.

Lifecycle:

1. Client открывает bidirectional stream.
2. Client пишет один framed `ClientHandshake` и finish-ит send side.
3. Server читает и валидирует его.
4. Server пишет один framed `HandshakeResponse` и finish-ит send side.
5. При `Accepted` connection остаётся для RPC streams; при rejection handler завершается.

Handshake value не обёрнут в `Versioned`.

### `ClientHandshake`

| Поле | Тип | Проверка |
|---|---|---|
| `protocol_version` | `u16` | exactly `2` |
| `device_id` | UUID | derived from public key |
| `device_name` | string | nonempty, <= 1024 UTF-8 bytes |
| `agent_version` | string | <= 1024 bytes |
| `features` | array of strings | <= 64 values, каждое <= 1024 bytes |
| `nonce` | byte vector | exactly 32 bytes |
| `public_key` | byte vector | exactly 32 bytes |
| `tls_certificate_binding` | byte vector | exactly 32 bytes и равен server certificate fingerprint |
| `signature` | byte vector | valid strict Ed25519 over client transcript |

Handshake implementation объявляет три features: `blake3`, `fixed-chunk-resume`, `tls-certificate-binding`. HTTP/WS features присутствуют в discovery/API announcement, но не в transfer handshake.

### `HandshakeResponse`

Serde shape использует `status` и `data`:

```text
Accepted(ServerHandshake)   -> status = "accepted"
Rejected(HandshakeRejection)-> status = "rejected"
```

`ServerHandshake` содержит те же поля, что client type: version, Device ID/name/version/features, свой nonce/public key, server TLS certificate binding и signature.

`HandshakeRejection`:

```text
reason: RejectionReason
message: String
```

`RejectionReason` tagged как `code` + `details` и имеет variants:

- `protocol_version_mismatch { expected, received }`;
- `invalid_signature`;
- `invalid_public_key`;
- `tls_certificate_binding_mismatch`;
- `untrusted_device`;
- `malformed_handshake`;
- `other { code }`.

Текущий server принимает криптографически valid, но untrusted handshake, поэтому `untrusted_device` handshake rejection обычно не используется. Trust применяется к последующим non-Ping requests.

## Signed transcripts

Все integer lengths в transcript — `u64` big-endian. UUID добавляется как raw 16 bytes, protocol version как raw `u16` big-endian. Строки подписываются как UTF-8 bytes.

Helper:

```text
append_field(out, bytes):
    out += u64_be(bytes.length)
    out += bytes

append_features(out, features):
    out += u64_be(features.length)
    for feature in features:
        append_field(out, utf8(feature))
```

Client domain separator:

```text
"FastSync authenticated client handshake v2"
```

Client transcript:

```text
append_field(CLIENT_DOMAIN)
u16_be(client.protocol_version)
client.device_id[16]
append_field(utf8(client.device_name))
append_field(utf8(client.agent_version))
append_features(client.features)
append_field(client.nonce)
append_field(client.public_key)
append_field(client.tls_certificate_binding)
```

`client.signature = Ed25519.sign(client_transcript)`.

Server domain separator:

```text
"FastSync authenticated server handshake v2"
```

Server transcript:

```text
append_field(SERVER_DOMAIN)
append_field(client_transcript)
append_field(client.signature)
u16_be(server.protocol_version)
client.device_id[16]
server.device_id[16]
append_field(client.nonce)
append_field(server.nonce)
append_field(client.public_key)
append_field(server.public_key)
append_field(utf8(server.device_name))
append_field(utf8(server.agent_version))
append_features(server.features)
append_field(server.tls_certificate_binding)
```

`server.signature = Ed25519.sign(server_transcript)`.

Server transcript включает client signature и обе identity/nonces, поэтому ответ связан с конкретным client hello. Обе подписи связаны с exact server TLS certificate.

## Trust enforcement

Успешный handshake возвращает `PeerInfo.trusted`, вычисленный сравнением exact public key с локальной строкой `trusted_peers(device_id)`.

- Client transfer прекращается до manifest, если server не trusted локально.
- Server разрешает `Ping` authenticated untrusted peer.
- Перед каждым другим request stream server заново проверяет, что client public key pinned.
- Pin mismatch — authentication failure, а не автоматическое обновление.
- Локальная untrust operation закрывает активные inbound connections peer; outgoing JobManager отдельно отменяет его runners.

Protocol не содержит сообщения `Trust`. Trust — локальная HTTP/SQLite операция на каждом peer.

## RPC stream lifecycle

После handshake каждый request использует новый client-opened bidirectional stream:

```text
client                              server
  | -- RequestFrame ----------------> |
  | -- finish send side ------------> |
  |                                   | process
  | <--------------- ResponseFrame -- |
  | <------------- finish send side --|
```

Исключение — `UploadChunk`: raw bytes следуют до finish client send side.

Один stream содержит ровно один request и один response. Долгоживущего control stream, multiplexed series of messages внутри одного stream или server-initiated RPC в v2 нет. QUIC connection допускает несколько concurrent upload request streams.

Server ограничивает одновременно активные handlers 32 connections и 64 requests. Ожидание первого handshake stream и сам application handshake имеют timeout 15 секунд, framed request header — 30 секунд. Handshake/response frames имеют общий limit 16 MiB. Request limit выбирается по handshake-time trust snapshot: 16 MiB для trusted и 64 KiB для untrusted; live trust затем проверяется ещё раз после decode каждого non-Ping request.

## Core wire types

### `ManifestEntry`

```text
relative_path: String       // validated UTF-8 forward-slash path
size: u64                   // 0 для directory
mtime_ns: i64               // nanoseconds относительно Unix epoch
file_type: "file" | "directory"
read_only: bool
```

### `ChunkDescriptor`

```text
index: u64
offset: u64
size: u64
hash: [u8; 32]              // BLAKE3 exact chunk bytes
```

### `HashedFile`

```text
entry: ManifestEntry        // file only
hash: [u8; 32]              // BLAKE3 entire file
chunks: Vec<ChunkDescriptor>
```

### `VerificationMode`

Serde values: `fast`, `verified`.

## Request и Response enums

`Request` tagged полями `type` и `data`, variant names snake_case:

| Variant | Payload |
|---|---|
| `compare` | `CompareBatch` |
| `negotiate` | `HashedFileBatch` |
| `upload_chunk` | `ChunkHeader`, затем raw bytes |
| `finalize_file` | `FinalizeFileRequest` |
| `complete_job` | `CompleteJobRequest` |
| `ping` | `PingRequest` |

`Response` использует такую же `type`/`data` shape:

| Variant | Payload |
|---|---|
| `compare` | `CompareDecisionBatch` |
| `negotiate` | `FilePlanBatch` |
| `acknowledged` | `Acknowledgement` |
| `pong` | `PongResponse` |
| `error` | `RemoteError` |

## Phase 1: Compare

### Request

```text
CompareBatch {
    job_id: UUID,
    manifest_id: UUID,
    destination_root: String,
    verification_mode: VerificationMode,
    chunk_size: u64,
    sequence: u32,
    is_last: bool,
    entries: Vec<ManifestEntry>
}
```

`destination_root` — UTF-8 absolute native path, интерпретируемый receiver OS. Он не проходит relative wire-path validation. Receiver сначала проверяет overlap с active leases, затем создаёт/canonicalizes root и удерживает root lease до завершения attempt.

Sender implementation:

- сортированный manifest режется по 512 entries;
- `sequence` начинается с 0;
- batches отправляются последовательно;
- даже пустой manifest отправляет один empty batch с sequence 0 и `is_last=true`.

### Response

```text
CompareDecisionBatch {
    job_id: UUID,
    manifest_id: UUID,
    sequence: u32,
    is_last: bool,
    decisions: Vec<CompareDecision>
}

CompareDecision {
    path: String,
    action: CompareAction
}
```

Actions:

| Action | Семантика sender |
|---|---|
| `unchanged` | File считается завершённым/skipped, bytes идут в reused |
| `need_hash` | Source должен прислать `HashedFile` |
| `transfer` | Sender также хеширует; текущий receiver не выдаёт |
| `delete` | Sender трактует как file failure; delete protocol отсутствует |
| `conflict` | File failure, остальные paths продолжаются |

В `Fast` receiver возвращает `unchanged` regular file только при совпадении type, size, mtime_ns и read_only. В `Verified` regular files переходят к `need_hash`. Directories создаются сразу и отвечают `unchanged`, а metadata directory откладывается до complete.

Sender создаёт random `manifest_id` для каждой полной attempt и проверяет его в каждом response. Receiver считает ID одноразовым, sequence 0 начинает generation, а следующие batches обязаны иметь тот же ID и exact next sequence. Duplicate paths, file ancestors и case-only aliases отклоняются. На `is_last` receiver reconciles stale SQLite/runtime/staging/lease state предыдущей attempt, сохраняет exact committed entry map и только затем разрешает следующие phases. Final destination entries, отсутствующие в новом manifest, не удаляются.

До создания destination root/parents receiver резервирует root и manifest paths за `(peer_id, job_id)`. Равный, ancestor или descendant path другого job даёт retryable conflict. Empty manifest всё равно удерживает root lease до complete/disconnect.

## Phase 2: Hash negotiation

### Request

```text
HashedFileBatch {
    job_id: UUID,
    manifest_id: UUID,
    sequence: u32,
    is_last: bool,
    files: Vec<HashedFile>
}
```

Sender формирует максимум 64 files на batch и заранее проверяет encoded frame <= 16 MiB. Один `HashedFile`, который сам не помещается в frame или содержит больше 131 072 chunks, становится ошибкой этого файла и не отправляется. Descriptors одного файла на несколько protocol batches не делятся.

Если после compare нет files для hashing, negotiation requests отсутствуют.

### Response

```text
FilePlanBatch {
    job_id: UUID,
    manifest_id: UUID,
    sequence: u32,
    is_last: bool,
    plans: Vec<FilePlan>
}

FilePlan {
    path: String,
    missing_chunks: Vec<u64>,
    missing_bytes: u64,
    resumed_chunks: Vec<u64>,
    resumed_bytes: u64,
    reused_chunks: Vec<u64>,
    reused_bytes: u64
}
```

Категории:

- `missing`: bytes нужны по сети;
- `resumed`: bytes уже находятся в valid job-scoped staging file и подтверждены SQLite + повторным region hash;
- `reused`: bytes взяты из whole identical destination либо скопированы из matching same-offset chunk existing destination в staging file.

Plan обязан содержать каждый negotiated chunk ровно один раз. Byte totals должны быть суммой descriptor sizes. Sender проверяет path, indices, disjoint/exhaustive coverage и totals до upload.

Receiver требует active `manifest_id` и exact equality каждого `HashedFile.entry` с committed manifest; negotiation одного job выполняется эксклюзивно. Sender проверяет response job/manifest IDs, sequence, plan count, paths и duplicates. Текущий sender не сравнивает response `is_last`, но receiver его echo-ит; implementer не должен полагаться на отсутствие проверки.

Non-retryable remote error с path может исключить один файл из pending negotiation, после чего sender повторяет batch без него. Ошибка без узнаваемого path завершает job.

## Phase 3: Upload chunk

### Header

```text
ChunkHeader {
    job_id: UUID,
    manifest_id: UUID,
    path: String,
    index: u64,
    offset: u64,
    size: u64,
    hash: [u8; 32]
}
```

Stream bytes:

```text
[4-byte BE length][CBOR RequestFrame::UploadChunk(header)][exactly size raw bytes][FIN]
```

Нельзя добавлять второй header, padding или следующий chunk в тот же stream. Receiver после `size` bytes делает дополнительное чтение и требует EOF; trailing byte вызывает error.

Receiver checks:

1. `(peer_id, job_id)` имеет runtime incoming state и header `manifest_id` активен.
2. File path был negotiated в этой generation, а job всё ещё владеет destination lease.
3. Header index/offset/size/hash точно совпадает с descriptor.
4. Такой index не inflight.
5. Staging file существует, regular non-symlink и exact final size.
6. Получено ровно `size` bytes; отсутствие следующего body fragment или FIN более 60 секунд даёт network error.
7. BLAKE3 raw bytes равен header hash.
8. File output успешно flush-нут.
9. Completion row записана с full source hash negotiated file.

Success response:

```text
Response::Acknowledged(
    Acknowledgement::Chunk {
        job_id,
        manifest_id,
        path,
        index,
        size
    }
)
```

Sender также считает BLAKE3 во время чтения source. Даже если receiver подтвердил bytes, несовпадение с descriptor на sender приводит к source-changed failure.

Несколько chunk streams могут идти одновременно и писать разные offsets одного staging file. Два одновременных streams одного index запрещены.

## Phase 4: Finalize file

Request:

```text
FinalizeFileRequest {
    job_id: UUID,
    manifest_id: UUID,
    file: HashedFile
}
```

Полный descriptor повторяется, а не ссылается только на path/hash. Receiver требует active `manifest_id`, destination lease и exact equality с negotiated `HashedFile`.

До acknowledgement receiver:

1. сериализует finalize конкретного file mutex-ом;
2. требует отсутствие inflight chunks;
3. требует completion row каждого chunk, если file ещё не whole-file finalized/reused;
4. читает весь job-scoped staging file и проверяет full BLAKE3 и size;
5. вызывает file `sync_all`;
6. применяет mtime и read-only к staging file;
7. атомарно заменяет destination этим staging file;
8. удаляет completion rows;
9. обновляет receiver job_file.

Success:

```text
Acknowledgement::File { job_id, manifest_id, path }
```

Empty file содержит zero chunks, но всё равно проходит full empty-file hash/finalize. Старый destination не меняется при неуспешной полной проверке.

## Phase 5: Complete job

Request:

```text
CompleteJobRequest { job_id: UUID, manifest_id: UUID }
```

Receiver:

- требует active manifest ID и leases, затем применяет directory metadata от deepest к root-most manifest directory;
- помечает оставшиеся `pending/running` files как failed (`source did not finalize file`);
- агрегирует receiver progress из `job_files` и directory conflicts;
- сохраняет `completed` либо `completed_with_errors`;
- удаляет runtime `IncomingJob` и leases;
- удаляет job-scoped staging root best-effort только при clean completion.

Success:

```text
Acknowledgement::Job {
    job_id,
    manifest_id,
    completed_with_errors: bool,
    failed_files: u64
}
```

Если runtime state уже удалён, но SQLite job имеет terminal completed status и persisted manifest ID совпадает, повторный `CompleteJob` также получает acknowledgement. Это покрывает потерю последнего ответа, не позволяя старой attempt подтвердить более новую generation.

## Ping

```text
PingRequest { nonce: u64 }
PongResponse { nonce: u64 }
```

Nonce должен echo-иться точно. Probe генерирует random `u64`, проверяет ответ и закрывает connection. `Ping` — единственный request, доступный authenticated, но ещё untrusted peer.

## Acknowledgement representation

`Acknowledgement` само tagged как `type` + `data`:

- `chunk(ChunkAcknowledgement)`;
- `file(FileAcknowledgement)`;
- `job(JobAcknowledgement)`.

Sender проверяет job ID, manifest ID и остальные context fields, а не только variant.

## Remote errors

```text
RemoteError {
    code: RemoteErrorCode,
    message: String,
    retryable: bool,
    job_id: Option<UUID>,
    path: Option<String>
}
```

Codes:

```text
invalid_request
unsupported_protocol
unauthorized
job_not_found
path_not_found
invalid_chunk
hash_mismatch
conflict
storage
internal
```

Не все declared codes выделяются текущим error mapper: часть validation errors возвращается как `invalid_request`, а некоторые missing-state messages как `job_not_found`. Client должен в первую очередь соблюдать `retryable`, context и code, не разбирать human message, хотя текущий sender использует remote path для изоляции negotiation file failure.

Текущая retry policy считает retryable:

- Quinn/network errors;
- remote error только с `retryable=true`;
- protocol I/O, truncated prefix или truncated payload.

Version/auth/data/hash/storage errors обычно non-retryable. Protocol не задаёт retry count/backoff; agent default — 6 retries с exponential delay 0.5 s до cap 15 s.

## Resume protocol invariants

Resume — повторный полный lifecycle с тем же job ID, а не специальный request:

1. Sender создаёт новый одноразовый manifest ID и снова отправляет полный Compare manifest с тем же job ID.
2. Sender снова присылает full/chunk hashes.
3. Receiver сопоставляет persistent completion rows с full source hash.
4. Каждый candidate staging region физически хешируется.
5. FilePlan сообщает `resumed/reused/missing`.
6. Sender отправляет только `missing`.

Chunk completion key SQL не содержит chunk size или chunk hash отдельно. Безопасность обеспечивается сочетанием full source hash, index, заново validated descriptors и physical region BLAKE3. Изменение source full hash исключает старые rows.

Pause/cancel не являются wire messages. Pause останавливает локальное планирование sender; cancel закрывает connection и кооперативно прекращает hashing/chunk/finalize futures. Receiver сохраняет уже valid staging/chunks. Потеря последней connection, связанной с incoming job, переводит его в `interrupted`, освобождает leases и оставляет persistent resume state.

## Protocol compatibility

- Discovery exact-version mismatch отбрасывается без handshake.
- Handshake mismatch возвращает structured `protocol_version_mismatch { expected: 2, received }`.
- Каждый RequestFrame/ResponseFrame также требует exact v2.
- `agent_version` и `features` informational; feature negotiation и conditional behavior не реализованы.
- Нет downgrade, extension registry или tolerance к новому enum variant в старом Serde consumer.
- Любое несовместимое изменение enum/struct Serde shape, chunk stream lifecycle или path/hash semantics требует новой protocol version.
- Изменение только agent semver не делает peers несовместимыми, пока оба реально говорят exact v2 schema.

## Что именно не batch-ится

В v2 реализованы только два вида control-plane batching:

- до 512 `ManifestEntry` в `CompareBatch`;
- до 64 `HashedFile` в `HashedFileBatch`, с ограничением 16 MiB.

Не реализованы:

- объединение contents нескольких маленьких файлов;
- несколько chunks в одном upload stream;
- splitting descriptor одного очень chunked файла по нескольким negotiation frames;
- параллельная отправка compare/hash batches текущим sender;
- один finalize batch для нескольких files.

Даже файл меньше chunk size создаёт отдельный `UploadChunk` stream и отдельный `FinalizeFile` RPC. Нельзя оптимизировать это на одной стороне без изменения совместимого wire behavior.

## Security и filesystem authority

Wire path validation удерживает relative entries внутри переданного root лексически, receiver проверяет symlink components и не допускает пересекающиеся active destination leases. Однако `destination_root` целиком выбирает trusted sender и может указывать на любой absolute path, writable процессом receiver. Protocol v2 не содержит recipient approval, root capability token, allowlist или quota.

При реализации совместимого peer нельзя считать trust эквивалентом безопасной path policy. Проверки path не являются openat-style sandbox и предполагают, что локальный процесс не заменяет уже проверенные parents symlink/reparse points во время operation. FastSync также не нормализует Unicode NFC/NFD; normalization-insensitive filesystems с канонически эквивалентными именами не являются поддержанным alias-сценарием. До появления отдельной policy layer HTTP API следует держать на loopback, а FastSync запускать с минимально необходимыми filesystem permissions.
