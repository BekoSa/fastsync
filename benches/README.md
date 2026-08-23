# Benchmarks FastSync

Документ задаёт воспроизводимую процедуру, но не содержит заранее заявленных результатов. В текущем workspace нет Criterion benchmark и нет отдельной network benchmark subcommand. Реализованы:

- `generate-dataset` — детерминированная запись набора файлов;
- `benchmark` — **только локальное** сравнение filesystem copy strategies;
- обычный transfer job через UI/REST, который нужно использовать для двухмашинного network benchmark.

До прогона на описанном hardware нельзя заявлять saturation 1GbE, throughput или преимущество FastSync.

## Подготовка бинарника

Используйте release build одного и того же commit/source snapshot на обоих компьютерах:

```bash
cargo build --release -p fastsync
./target/release/fastsync --version
```

Windows PowerShell:

```powershell
cargo build --release -p fastsync
./target/release/fastsync.exe --version
```

Не включайте compile time или `cargo run` startup в timed interval. Запишите OS, kernel/build, Rust version, FastSync/commit, CPU, RAM, source/destination storage, filesystem, NIC/link speed, MTU, antivirus и power mode.

## Генератор dataset

Точный синтаксис:

```text
fastsync generate-dataset <OUTPUT> [OPTIONS]
fastsync dataset <OUTPUT> [OPTIONS]             # alias

--profile <small|medium|large|mixed>             default: mixed
--scale-percent <1..=1000>                       default: 100
--files <COUNT>                                  override total file count
--bytes-per-file <BYTES>                         conflicts with --bytes-per-file-mib
--bytes-per-file-mib <MIB>                       conflicts with --bytes-per-file
--max-total-bytes <BYTES>                        default: 4294967296 (4 GiB)
--seed <U64>                                     default: 77323794531073
--force                                          replace nonempty output directory
```

В исходном коде default seed записан как `0x4653594e4301`. Содержимое каждого файла детерминировано seed и global file index; это позволяет повторить логический dataset, но генератор не создаёт manifest/checksum-файл.

Команда отказывается работать, если environment variable `CI` существует, даже если она пустая. Existing nonempty output без `--force` отклоняется. `--force` рекурсивно удаляет весь указанный output, поэтому не направляйте его на каталог с нужными данными.

### Встроенные профили

Все размеры binary (`KiB = 1024`, `MiB = 1024^2`).

| Profile | Распределение | Files | Total bytes | Total size |
|---|---|---:|---:|---:|
| `small` | 1 024 x 4 KiB | 1 024 | 4 194 304 | 4 MiB |
| `medium` | 128 x 1 MiB | 128 | 134 217 728 | 128 MiB |
| `large` | 4 x 256 MiB | 4 | 1 073 741 824 | 1 GiB |
| `mixed` | 512 x 4 KiB, 64 x 1 MiB, 2 x 128 MiB | 578 | 337 641 472 | 322 MiB |

Files раскладываются как `<class>/set-NNNN/file-NNNNNNNN.bin`, по 128 files на `set-*`. `--scale-percent` масштабирует count каждого class через ceiling и минимум 1. При `--files` profile classes заменяются одним `custom` class; без size override representative size равен 4 KiB для `small`, 1 MiB для `medium/mixed`, 256 MiB для `large`.

### Примеры dataset

Default mixed:

```bash
./target/release/fastsync generate-dataset ./bench-data/mixed --profile mixed
```

Много маленьких файлов:

```bash
./target/release/fastsync generate-dataset ./bench-data/small-10k \
  --profile small \
  --files 10000 \
  --bytes-per-file 4096
```

Набор крупных файлов больше default safety limit:

```bash
./target/release/fastsync generate-dataset ./bench-data/large-8g \
  --profile large \
  --files 16 \
  --bytes-per-file-mib 512 \
  --max-total-bytes 8589934592
```

Уменьшенный smoke dataset:

```bash
./target/release/fastsync generate-dataset ./bench-data/smoke \
  --profile mixed \
  --scale-percent 10
```

Windows:

```powershell
./target/release/fastsync.exe generate-dataset C:\fastsync-bench\mixed --profile mixed
```

Генератор ограничивает count значением 1 000 000 и проверяет overflow/`max_total_bytes` до создания данных. Buffer записи — 1 MiB.

## Локальный benchmark

### Что он измеряет

`benchmark` сканирует source один раз, затем выполняет три стратегии в фиксированном порядке:

1. `sequential`, concurrency `1`: синхронный цикл `std::fs::copy`;
2. `scheduler-1`, concurrency `1`: `tokio::fs::copy` через `buffer_unordered(1)`;
3. `scheduler-configured`, concurrency `N`: `tokio::fs::copy` через `buffer_unordered(N)`.

Timer каждой стратегии включает создание destination directories и копирование regular files. Он не включает первоначальный source scan, подготовку benchmark root, BLAKE3, SQLite, QUIC, TLS, metadata finalize или сеть. После каждой стратегии copied byte count должен точно совпасть с manifest; scan с любой ошибкой отменяет benchmark.

Это microbenchmark scheduler/filesystem copy, а не benchmark FastSync transfer protocol.

### Точный CLI

```text
fastsync benchmark <SOURCE> <DESTINATION> [OPTIONS]
fastsync bench <SOURCE> <DESTINATION> [OPTIONS]   # alias

--concurrency <1..=1024>                         default: 8
--keep-output                                    сохранить все три copied trees
--force                                          заменить nonempty destination
--json                                           JSON report
```

Source и destination canonical paths не могут пересекаться ни в одном направлении. Destination должен быть empty/nonexistent; `--force` рекурсивно заменяет его. Без `--keep-output` каждая copied tree удаляется после измерения и пустой root также удаляется.

### Воспроизводимая процедура

1. Сгенерируйте dataset один раз вне timed run.
2. Поместите benchmark destination на тот storage, который хотите измерять. Если source и destination на одном устройстве, результат включает конкуренцию reads/writes этого устройства; на разных — нет.
3. Закройте indexing, backup и другие I/O jobs либо явно запишите, что они работали.
4. Выполните один unreported warm-up.
5. Выполните минимум 5 measured runs с одинаковым N.
6. Сохраните JSON каждого run и вычислите median; для tail latency используйте больше повторов, чем 5.

```bash
./target/release/fastsync benchmark \
  ./bench-data/mixed \
  ./bench-output \
  --concurrency 8 \
  --json
```

Через Cargo, если отдельный binary ещё не собран:

```bash
cargo run --release -p fastsync -- \
  benchmark ./bench-data/mixed ./bench-output --concurrency 8 --json
```

JSON shape:

```json
{
  "benchmark": "local_filesystem_copy",
  "source": "/absolute/source",
  "files": 578,
  "bytes": 337641472,
  "results": [
    {
      "strategy": "sequential",
      "concurrency": 1,
      "elapsed_seconds": 1.0,
      "mebibytes_per_second": 322.0,
      "files_per_second": 578.0
    },
    {
      "strategy": "scheduler-1",
      "concurrency": 1,
      "elapsed_seconds": 1.0,
      "mebibytes_per_second": 322.0,
      "files_per_second": 578.0
    },
    {
      "strategy": "scheduler-configured",
      "concurrency": 8,
      "elapsed_seconds": 1.0,
      "mebibytes_per_second": 322.0,
      "files_per_second": 578.0
    }
  ]
}
```

Числа в примере иллюстрируют schema, а не измеренный результат.

### Обязательное сравнение

Для каждого run сравните:

- `sequential` против `scheduler-1`: стоимость async scheduler при одинаковой logical concurrency;
- `scheduler-1` против `scheduler-configured(N)`: scaling только concurrency;
- `sequential` против `scheduler-configured(N)`: итоговый effect относительно простого baseline.

Формулы:

```text
speedup_vs_sequential = elapsed_sequential / elapsed_scheduler_N
speedup_vs_scheduler1 = elapsed_scheduler_1 / elapsed_scheduler_N
efficiency_N          = speedup_vs_scheduler1 / N
```

Всегда публикуйте absolute elapsed, MiB/s и files/s вместе со speedup. Fixed order `sequential -> scheduler-1 -> scheduler-N` может прогреть OS page cache в пользу поздних runs; встроенная команда не randomize-ит порядок. Поэтому этот microbenchmark сам по себе недостаточен для причинного performance claim.

## Двухмашинный network benchmark

### Топология

```text
Machine A / source NVMe / FastSync sender
              |
              | dedicated or quiet 1GbE LAN
              |
Machine B / destination NVMe / FastSync receiver
```

На обоих peers:

- используйте release binaries из одного source snapshot;
- разрешите UDP transfer port и завершите explicit trust на обеих сторонах;
- держите HTTP API на loopback;
- зафиксируйте link speed/duplex, MTU, адреса и route;
- синхронизируйте wall clocks для timestamp report, но elapsed измеряйте monotonic timer;
- используйте same-OS peers для baseline, чтобы не смешивать network result с различиями filesystem metadata/path semantics; sender при этом понимает Windows drive/UNC destination syntax независимо от своей ОС.

### Подготовка

На A сгенерируйте dataset. На B не создавайте внутри source overlap и убедитесь, что у FastSync есть права на benchmark parent. Для каждого measured run задавайте новый empty destination, например:

```text
/srv/fastsync-runs/mixed-c1-r01
/srv/fastsync-runs/mixed-c1-r02
/srv/fastsync-runs/mixed-c32-r01
```

FastSync не удаляет лишние файлы и не имеет HTTP delete-job/delete-tree endpoint. Cleanup выполняйте **после** остановки таймера и только для точно выделенного benchmark parent.

Получите peer ID на A:

```bash
curl --fail-with-body http://127.0.0.1:8765/api/devices
```

Убедитесь, что выбранный B имеет `trusted: true` и что B отдельно доверяет A.

### Создание timed job через REST

Для fresh-destination baseline рекомендуется `verified`, fixed chunk size и `retry_limit=0`: скрытый retry не исказит elapsed, а failed run будет явно отброшен/описан.

Concurrency 1:

```bash
curl --fail-with-body -X POST http://127.0.0.1:8765/api/jobs \
  -H 'Content-Type: application/json' \
  --data '{
    "peer_id": "PUT-RECIPIENT-UUID-HERE",
    "source_path": "/data/fastsync-bench/mixed",
    "destination_path": "/srv/fastsync-runs/mixed-c1-r01",
    "verification": "verified",
    "chunk_size_mib": 4,
    "concurrency": 1,
    "retry_limit": 0
  }'
```

Concurrency N меняет только поле и unique destination:

```json
{
  "peer_id": "PUT-RECIPIENT-UUID-HERE",
  "source_path": "/data/fastsync-bench/mixed",
  "destination_path": "/srv/fastsync-runs/mixed-c32-r01",
  "verification": "verified",
  "chunk_size_mib": 4,
  "concurrency": 32,
  "retry_limit": 0
}
```

Timer запускайте непосредственно перед POST и останавливайте при первом terminal status из `completed`, `completed_with_errors`, `failed`, `cancelled`. Poll:

```bash
curl --fail-with-body http://127.0.0.1:8765/api/jobs/JOB-UUID
```

Для Linux automation нужны как минимум `curl`, `jq` и внешний monotonic timer. Skeleton polling с интервалом 250 ms:

```bash
API=http://127.0.0.1:8765
JOB_ID=PUT-JOB-UUID-HERE

while true; do
  JOB=$(curl --fail --silent --show-error "$API/api/jobs/$JOB_ID") || exit 1
  STATUS=$(jq --raw-output '.status' <<<"$JOB")
  case "$STATUS" in
    completed|completed_with_errors|failed|cancelled) break ;;
  esac
  sleep 0.25
done

jq . <<<"$JOB"
```

Polling добавляет до 250 ms uncertainty к client-observed completion; уменьшайте interval одинаково для всех runs и записывайте его. Не используйте `created_at/updated_at` как единственный high-resolution timer: это Unix milliseconds, а `updated_at` отражает persistence events.

PowerShell позволяет обернуть POST и polling в monotonic `Stopwatch`:

```powershell
$api = "http://127.0.0.1:8765"
$body = @{
  peer_id = "PUT-RECIPIENT-UUID-HERE"
  source_path = "C:\fastsync-bench\mixed"
  destination_path = "D:\fastsync-runs\mixed-c1-r01"
  verification = "verified"
  chunk_size_mib = 4
  concurrency = 1
  retry_limit = 0
} | ConvertTo-Json

$timer = [System.Diagnostics.Stopwatch]::StartNew()
$job = Invoke-RestMethod -Method Post -Uri "$api/api/jobs" -ContentType "application/json" -Body $body
do {
  Start-Sleep -Milliseconds 250
  $job = Invoke-RestMethod -Uri "$api/api/jobs/$($job.id)"
} while ($job.status -notin @("completed", "completed_with_errors", "failed", "cancelled"))
$timer.Stop()

$job | ConvertTo-Json -Depth 8
$timer.Elapsed.TotalSeconds
```

### Матрица concurrency 1/N

Минимальная matrix для каждого dataset:

| Run group | Concurrency | Chunk | Mode | Destination |
|---|---:|---:|---|---|
| Baseline | `1` | `4 MiB` | `verified` | fresh |
| N1 | `8` | `4 MiB` | `verified` | fresh |
| N2 | `16` | `4 MiB` | `verified` | fresh |
| N3 | `32` | `4 MiB` | `verified` | fresh |

Выполните один warm-up, затем минимум 5 repeats каждой configuration. Чередуйте/randomize-ите порядок c1/c8/c16/c32 между repeats, чтобы thermal/cache/order effects не совпали с одним N.

Source hash cache сохраняется в FastSync SQLite. Первый job по source path может включать cold hashing, последующие используют cache при неизменной metadata. Публикуйте отдельно:

- **warm FastSync hash cache**: unreported warm-up перед matrix;
- **cold FastSync hash cache**: отдельный data directory/identity или уникальный source path для каждого run, с повторным pairing при новой identity;
- **OS page cache state**: warm/cold/unknown, без смешивания в одной series.

Не удаляйте production identity DB ради benchmark. Для isolated cold runs запускайте отдельный `--data-dir` и отдельные benchmark ports/identity.

### Fast и existing destination scenarios

Fresh destination не показывает главное различие `Fast`/`Verified`: оба режима хешируют и передают новые files. Дополнительные series:

| Scenario | Подготовка | Ожидание, которое нужно измерить, а не предполагать |
|---|---|---|
| Whole identical | Destination — копия source с сохранённой metadata | `Fast` может skip по metadata; `Verified` читает/hash-ит content |
| Same size, changed chunks | Измените deterministic regions destination | Receiver reuse совпавших fixed-offset chunks |
| Interrupted | Остановите sender после нескольких acknowledged chunks, затем Resume того же job | `resumed_bytes`, retransmitted bytes и recovery time |
| Many small files | `small`/custom 10k | Stream/finalize/filesystem overhead без network batching |

Для interrupted scenario не создавайте новый job: resume invariant требует тот же job UUID. Сохраняйте recipient `.fastsync-stage-<UUID>` tree и SQLite между остановкой и resume.

## Обязательные метрики

### Dataset и конфигурация

- profile/classes, seed, exact file count и logical bytes;
- source/destination filesystem и storage device;
- FastSync agent/protocol version и source snapshot identifier;
- mode, chunk bytes, concurrency, retry limit;
- fresh/identical/changed/interrupted destination;
- warm/cold FastSync hash cache и OS page cache;
- число repeats и порядок runs.

### Job result

Сохраните полный final JSON `GET /api/jobs/{id}`, включая:

- terminal `status`, job/error/file_errors;
- `total_files`, `completed_files`, `transferred_files`, `skipped_files`, `failed_files`;
- `total_bytes`, `transferred_bytes`, `reused_bytes`, `accounted_bytes`;
- client-observed monotonic elapsed;
- samples current throughput/queue/active streams во время run, если анализируется pipeline.

Job с `completed_with_errors`, retry или file error нельзя молча включать как успешный throughput sample. Его нужно исключить с причиной либо публиковать отдельной failure series.

Сохраните sender/receiver logs на время серии: успешный retry не представлен отдельным счётчиком в final job JSON и может быть виден только в логах/промежуточном `error` state.

### System metrics на обоих peers

- process CPU time/utilization и peak RSS;
- disk read/write bytes, throughput, latency/queue depth;
- NIC bytes/packets/errors/drops до и после run;
- link retransmission/loss indicators, если доступны для QUIC/network tooling;
- температура/throttling и antivirus activity при существенном влиянии.

FastSync API не экспортирует фактические QUIC/TLS bytes on wire, RTT, loss или Quinn congestion window. Для physical network throughput используйте NIC/OS counters. `transferred_bytes` — только acknowledged raw payload chunks.

## Расчёт результатов

Для fresh destination без ошибок/reuse:

```text
payload_MiB_per_s = transferred_bytes / 1048576 / elapsed_seconds
files_per_s       = completed_files / elapsed_seconds
speedup_N         = median_elapsed_concurrency_1 / median_elapsed_concurrency_N
efficiency_N      = speedup_N / N
wire_utilization  = NIC_payload_bits_per_second / measured_link_bits_per_second
```

Для resume/reuse публикуйте отдельно:

```text
network_fraction = transferred_bytes / total_bytes
reuse_fraction   = reused_bytes / total_bytes
```

Не используйте `accounted_bytes / elapsed` как network throughput: reused bytes не шли по сети. UI `throughput_mbps` — current decimal Mbps по delta acknowledged payload и становится 0 после 4 секунд без sample; это не average и не NIC rate.

Отчёт должен содержать median и разброс raw runs. P95 имеет смысл только при достаточном числе повторов. Не округляйте elapsed до целых секунд для small dataset.

## Сравнение local и network results

Local `sequential/scheduler-1/scheduler-N` отвечает на вопрос о базовом filesystem copy overhead конкретной машины. Two-machine `concurrency=1/N` включает scan, optional hashing/cache lookup, TLS/handshake, protocol, network, receiver I/O, full verification и finalize.

Эти числа нельзя напрямую вычитать как «network cost», потому что code paths различны:

- local benchmark не хеширует;
- local benchmark использует `fs::copy`, transfer — seek/read/write chunks и job-scoped staging;
- transfer делает SQLite writes и full staging-file verification;
- local benchmark выполняется на одном host, transfer — на двух.

Корректное сравнение показывает отдельные baselines и scaling trends, но не объявляет их эквивалентными workloads.

## Шаблон отчёта

```text
FastSync version / protocol:
Source snapshot:
Date:

Sender OS / CPU / RAM / disk / filesystem / NIC:
Receiver OS / CPU / RAM / disk / filesystem / NIC:
Network link / MTU / topology:
Antivirus / power policy:

Dataset profile / seed / files / bytes:
Destination scenario:
Mode / chunk / retry:
Cache state:
Repeats / order:

concurrency | elapsed raw | median | payload MiB/s | files/s | speedup vs c1
1           |             |        |               |         | 1.00
8           |             |        |               |         |
16          |             |        |               |         |
32          |             |        |               |         |

Transferred / reused bytes:
Errors/retries:
Peak CPU/RSS, disk and NIC observations:
Interpretation and uncontrolled factors:
```

## Проверки перед публикацией benchmark

Выполните текущий quality gate:

```bash
cargo fmt --all -- --check
cargo check --workspace --all-targets
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo test --workspace --all-features
cargo build --release -p fastsync
```

Минимальные focused tests:

```bash
cargo test -p fastsync-core
cargo test -p fastsync-protocol
cargo test -p fastsync-storage
cargo test -p fastsync-discovery
cargo test -p fastsync-transfer --test localhost_transfer
```

Если quality commands или loopback transfer не проходят на benchmark build, результаты помечаются недействительными до диагностики.

## Ограничения интерпретации

- Built-in local benchmark всегда запускает strategies в одном порядке.
- Network harness не встроен и зависит от polling/timer discipline пользователя.
- Нет small-file network batching и adaptive concurrency.
- Нет встроенных CPU/RSS/disk/NIC/QUIC loss exporters.
- Hash cache и OS page cache существенно меняют end-to-end elapsed.
- Incoming UI metrics неполны; authoritative job JSON для benchmark берётся на sender.
- Один удачный run не является performance claim; нужны raw repeats и описание среды.
