# enable-market

`enable-market` records primary-source market data into an immutable raw authority, derives deterministic Parquet datasets, loads those datasets idempotently into ClickHouse, and exposes bounded authenticated query and replay APIs. The reference production composition supports public live acquisition only for Binance Spot; this describes capability, not an observed deployment. The other venue packages and fixtures are verification surfaces, not live-composed production sources.

## Public-repository status

This public repository contains reusable code, synthetic evidence, a credential-free container definition, and the reference Compose topology in [`deploy/compose.yaml`](deploy/compose.yaml). It contains no deployment record, live endpoint binding, credential, bearer token, TLS key or certificate, operator path, schedule, raw venue payload, backup, or claim that a production deployment exists. Building an image, rendering the Compose file, and running synthetic verification do not prove live capture or production readiness.

A caller owns every environment binding, role-specific YAML file, secret file, persistent data root, listener, published port, retention decision, cadence, image digest, backup destination, TLS trust decision, and process-supervision decision. Missing values fail Compose interpolation or typed configuration validation before a runtime effect.

## Production slice

```text
Binance Spot public REST/WebSocket
        |
        v
collector -> bounded local segment spool -> immutable S3-compatible raw objects
    |                    |                              |
    |                    +-- crash recovery            +-- raw authority
    v                                                   |
PostgreSQL temporal catalog and publication ledger <---+
        |
        v
dataset builder -> deterministic committed Parquet -> idempotent ClickHouse loader
        |                                                    |
        +---------------- authenticated query/replay --------+
                                      |
                                      v
                         same-origin HTTPS dashboard
```

Raw native envelopes are authoritative. PostgreSQL records temporal catalog, publication, coverage, and dataset state; it is not a replacement raw-payload authority. Parquet and ClickHouse are reproducible derived projections. Replay keeps the committed native order and the query boundary retains its signed paging, bounded interval, response-size, TLS, and bearer-scope contracts.

## Typed production configuration

Each long-running or one-shot role receives its own caller-owned YAML file because `deployment.role` is singular and fail-closed.

| Role | Command | Required production section |
| --- | --- | --- |
| `migration-job` | `enable-market migrate` | `catalog` |
| `collector` | `enable-market collect` | `capture`, Binance Spot `sources`, `object_store`, `catalog`, writer lease identity |
| `catalog-sync` | `enable-market catalog sync` | Binance Spot `sources`, `object_store`, `catalog`, and the same canonical writer lease identity used by collection |
| `dataset-builder` | `enable-market export parquet` | Binance Spot `sources`, `dataset`, `object_store`, `catalog` |
| `warehouse-loader` | `enable-market load` | `dataset`, `object_store`, `catalog`, `warehouse` |
| `query-replay-server` | `enable-market serve` | `serve`, `object_store`, `catalog`, `warehouse`; serves the embedded dashboard at `/dashboard/` |

Production-only typed fields are:

- `capture.spool_root`, `capture.frame_bytes`, `capture.segment_max_bytes`, `capture.segment_max_age`, `capture.depth_snapshot_limit`, `capture.depth_snapshot_cadence`, `capture.reconnect_delay`, both decode/durable queue capacities, their high/low water marks, `capture.max_raw_message_bytes`, and `capture.pending_rest_capacity`. The collector requires exactly one Binance Spot source with explicit canonical-uppercase symbols. Its metadata request records an exact canonical `symbols` query and rejects a response that differs from that configured selection; it does not fetch the unbounded venue-wide `exchangeInfo` response. `runtime.spool_max_bytes` must cover `(1 + symbol count) * (2 * segment_max_bytes + 2 * frame_bytes)`: one WebSocket epoch plus one depth epoch for each symbol. The Compose mount target is `/var/lib/enable-market/spool`, so the collector YAML must declare that exact root. The collector also requires an explicit writer lease key and writer ID.
- `dataset.working_root` and `dataset.build_cadence`. Both the dataset-builder and warehouse-loader role files require them. The reference stack mounts separate caller-owned host roots at `/var/lib/enable-market/dataset` in each container; each role YAML must declare that exact in-container root. Production partitions are exact one-hour UTC windows, and row-group bytes must be 64, 256, or 1024 MiB with `zstd` compression. `dataset.derived_retention` remains an explicit caller policy value; the reference runtime never deletes committed immutable derived objects automatically.
- `serve.paging_key_ref`, `serve.principals[].id`, `serve.principals[].token_ref`, `serve.principals[].scopes`, `serve.max_query_interval`, `serve.page_token_ttl`, and `serve.read_header_timeout`, in addition to the TLS, listener, page-row, response-byte, and remaining HTTP timeout fields. Supported scopes are `catalog:read`, `coverage:read`, `query:read`, `replay:native`, `replay:normalized`, and `metrics:read`.

Route queue depth, concurrency, deadlines, replay byte limits, and buffers are not duplicated in YAML. The server's existing safety defaults remain the authority for those internal limits.

Configuration precedence is explicit flag override, then the corresponding `ENABLE_MARKET_...` environment key, then YAML, then a safety default. Dots become underscores: for example, `capture.segment_max_age` maps to `ENABLE_MARKET_CAPTURE_SEGMENT_MAX_AGE`. Collection-valued environment overrides use JSON; `ENABLE_MARKET_SERVE_PRINCIPALS` is a JSON array of objects with `id`, `token_ref`, and `scopes`. Only registered keys participate, and YAML decoding is strict.

### Secret-reference convention

A `*_ref` field names an environment variable; it never contains a path or secret. The root resolver interprets the referenced environment value in exactly two ways:

1. a value not beginning with `@` is the literal secret byte sequence;
2. `@/absolute/path` reads that one explicit absolute, bounded, regular, non-symlink file.

The Compose stack uses the second form, such as `MARKETDATA_TLS_CERT=@/run/secrets/tls_cert`. The YAML therefore contains `tls_cert_ref: MARKETDATA_TLS_CERT`, not a mounted path. References and resolved values are excluded from `PublicDigest`, error output, logs, and public evidence.

The five role YAML files in the reference stack must use these reference names where applicable:

- `MARKETDATA_OBJECT_CREDENTIALS`
- `MARKETDATA_CATALOG_DSN`
- `MARKETDATA_WAREHOUSE_DSN`
- `MARKETDATA_TLS_CERT`
- `MARKETDATA_TLS_KEY`
- `MARKETDATA_PAGING_KEY`
- `MARKETDATA_DASHBOARD_TOKEN`

Additional principals may use additional caller-declared environment reference names and Compose secret mounts; they must not reuse bearer material between principals.

Generate every bearer token and paging key independently from at least 32 bytes produced by a cryptographically secure pseudorandom number generator (CSPRNG), then encode those bytes losslessly for transport. Human-chosen strings and generators producing fewer than 32 random bytes do not meet this production secret requirement.

## Reference deployment

The Compose topology provides PostgreSQL, ClickHouse, MinIO-compatible object storage, a caller-directed bucket initialization job, migration, collector, dataset builder, catalog-aware loader, query server, and an explicit TLS probe. There is no standalone dashboard container: inert dashboard assets are compiled into the application image and the query server serves them at `/dashboard/` on the same authenticated API origin.

Only the caller-supplied `QUERY_PUBLISH_BINDING` is published. Database and object-store ports remain on the Compose network. The query role YAML must set `serve.listener` to a container-reachable address such as `0.0.0.0:8443`; `QUERY_INTERNAL_PORT` must be the same port, and `QUERY_PUBLISH_BINDING` must explicitly map the intended host address and port, for example `127.0.0.1:8443:8443`. These are examples, not defaults. The certificate must cover `QUERY_TLS_SERVER_NAME`.

The embedded dashboard:

- is an inert same-origin shell; it does not proxy, open another listener, inject a token, or contact storage directly;
- fetches every API and metrics response from the query server under the query server's TLS, bearer-scope, paging, interval, queue, deadline, and response-size controls;
- keeps bearer material only in browser memory or `sessionStorage`, never local storage, cookies, URLs, HTML, logs, or the image.

### Caller-owned inputs

Create a caller-owned POSIX environment file outside the repository containing every variable required by `deploy/compose.yaml`. Compose's `${NAME:?message}` expressions enumerate the complete set. The file must include:

- immutable digest references for the application, PostgreSQL, ClickHouse, MinIO, MinIO Client, and curl images;
- infrastructure credentials and the exact `OBJECT_BUCKET` declared by every role YAML;
- five role YAML paths and eight secret-file paths, including the query-server certificate chain, key, verification CA bundle, paging key, and dashboard principal token;
- PostgreSQL, ClickHouse, object-store, capture-spool, dataset-builder work, loader work, and backup roots;
- `QUERY_INTERNAL_PORT`, `QUERY_PUBLISH_BINDING`, and `QUERY_TLS_SERVER_NAME`.

Do not add this file or any referenced material to Git. `ENABLE_MARKET_IMAGE`, `POSTGRES_IMAGE`, `CLICKHOUSE_IMAGE`, `MINIO_IMAGE`, `MINIO_MC_IMAGE`, and `CURL_IMAGE` must use immutable `repository@sha256:<digest>` references; tags, including version tags, are mutable inputs.

The production warehouse selection is stricter than merely using a digest: `CLICKHOUSE_IMAGE` and every loader/query role's `warehouse.server_digest` must identify the embedded pinned selection `clickhouse/clickhouse-server@sha256:7c39abeb161d627fa3ca6a1e5f6241ecdc24501e8463486e61b80be3ab4471b0`, and `warehouse.batch_rows` must be `100000`. PostgreSQL's pinned major must be admitted by `catalog.server_majors`. The pinned MinIO Client and curl images must provide the `mc`, `curl`, and `/bin/sh` interfaces used by the one-shot jobs.

### Application image

[`Dockerfile`](Dockerfile) requires a caller-selected, digest-pinned Go 1.25.7 builder image through `GO_IMAGE`; it has no mutable default. The output is a non-root, static `scratch` image containing only the binary and the builder's CA bundle. Supply immutable build identity explicitly:

```sh
docker build \
  --build-arg "GO_IMAGE=$GO_IMAGE" \
  --build-arg "VERSION=$RELEASE_VERSION" \
  --build-arg "COMMIT=$RELEASE_COMMIT" \
  --build-arg "BUILD_DATE=$RELEASE_BUILD_DATE" \
  --tag "$APPLICATION_BUILD_TAG" .
```

`GO_IMAGE` must be an admitted `repository@sha256:<digest>` for Go 1.25.7 whose CA bundle is at `/etc/ssl/certs/ca-certificates.crt`. `RELEASE_COMMIT` names the exact source revision; `RELEASE_BUILD_DATE` is an explicit UTC build timestamp. Push the image through the caller's registry admission path, record the resulting repository digest, and use that immutable digest as `ENABLE_MARKET_IMAGE`. A local tag is not production evidence.

Set only the environment-file locator in the shell, then export its declared variables for the operator procedures below:

```sh
export MARKETDATA_ENV_FILE=/caller/owned/enable-market.env
set -a
. "$MARKETDATA_ENV_FILE"
set +a
```

The displayed path is a shell metavariable example, not a repository or deployment default. The caller must choose and protect the real path.

Before preflight, create every bind source and its parent outside the repository. Grant only the pinned container UID for that service the required access: PostgreSQL, ClickHouse, object-store, capture-spool, dataset-builder work, and loader work roots are writable; role YAML, DSN, credential, TLS, paging-key, and bearer files are read-only; the backup root is writable only during the declared backup/restore procedure. Compose must not be relied upon to create a root-owned directory with accidental permissions.

The object credentials referenced by the role YAML files must address the caller-declared MinIO identity and bucket. The `object-store-init` job uses the caller-supplied MinIO root identity only to create the exact `OBJECT_BUCKET` idempotently and confirm it exists. It does not create credentials, change anonymous access, enable versioning, configure retention, or apply object-lock policy. The caller must configure and verify those controls before admitting capture.

### Preflight and bring-up

Rendering validates required substitutions, YAML syntax, declared path values, and dependency shape without starting a service. It does not prove that a declared file exists or is readable; verify every caller-owned file separately before bring-up:

```sh
for path in \
  "$MIGRATION_CONFIG_FILE" "$COLLECTOR_CONFIG_FILE" "$DATASET_CONFIG_FILE" \
  "$LOADER_CONFIG_FILE" "$QUERY_CONFIG_FILE" "$OBJECT_CREDENTIALS_FILE" \
  "$CATALOG_DSN_FILE" "$WAREHOUSE_DSN_FILE" "$TLS_CERT_FILE" "$TLS_KEY_FILE" \
  "$TLS_CA_FILE" "$PAGING_KEY_FILE" "$DASHBOARD_TOKEN_FILE"; do
  test -f "$path" && test -r "$path" && test ! -L "$path"
done
```

```sh
docker compose --env-file "$MARKETDATA_ENV_FILE" -f deploy/compose.yaml config --quiet
images="$(docker compose --env-file "$MARKETDATA_ENV_FILE" -f deploy/compose.yaml config --images)"
printf '%s\n' "$images" |
  while IFS= read -r image; do
    case "$image" in
      *@sha256:*) digest="${image##*@sha256:}" ;;
      *) printf 'mutable image reference: %s\n' "$image" >&2; exit 1 ;;
    esac
    case "$digest" in
      *[!0-9a-fA-F]*|'') printf 'invalid image digest: %s\n' "$image" >&2; exit 1 ;;
    esac
    test "${#digest}" -eq 64 || {
      printf 'invalid image digest length: %s\n' "$image" >&2
      exit 1
    }
  done
```

The second command checks reference shape, not registry authenticity. The caller's image admission process must resolve and approve each digest for the intended platform.

Bring up stateful dependencies, explicitly run the caller-configured bucket initialization and migration jobs, then start the ordered application path:

```sh
docker compose --env-file "$MARKETDATA_ENV_FILE" -f deploy/compose.yaml up -d --wait postgres clickhouse object-store
docker compose --env-file "$MARKETDATA_ENV_FILE" -f deploy/compose.yaml run --rm object-store-init
docker compose --env-file "$MARKETDATA_ENV_FILE" -f deploy/compose.yaml run --rm migration
docker compose --env-file "$MARKETDATA_ENV_FILE" -f deploy/compose.yaml up -d collector dataset-builder loader query-server
docker compose --env-file "$MARKETDATA_ENV_FILE" -f deploy/compose.yaml --profile operations run --rm query-healthcheck
```

The final command makes two TLS-verified, hostname-verified requests over the Compose network: `/health/live` and `/health/ready`. It uses the caller's `TLS_CA_FILE`, requires the certificate identity in `QUERY_TLS_SERVER_NAME`, and fails on a non-2xx response. Do not replace it with `curl -k`.

The internal probe does not prove that the host publication, firewall, DNS, or upstream routing is reachable. From the intended client network, set `QUERY_PUBLIC_AUTHORITY` to the caller-published `host:port` and verify that boundary separately:

```sh
curl --fail --silent --show-error --cacert "$TLS_CA_FILE" \
  "https://$QUERY_PUBLIC_AUTHORITY/health/live"
curl --fail --silent --show-error --cacert "$TLS_CA_FILE" \
  "https://$QUERY_PUBLIC_AUTHORITY/health/ready"
```

`QUERY_PUBLIC_AUTHORITY` is a caller-owned operator input, not a Compose variable. It must name the certificate identity rather than an unrelated address.

Inspect bounded service state without exposing secrets:

```sh
docker compose --env-file "$MARKETDATA_ENV_FILE" -f deploy/compose.yaml ps
docker compose --env-file "$MARKETDATA_ENV_FILE" -f deploy/compose.yaml logs --no-log-prefix --since 10m collector dataset-builder loader query-server
```

The collector, dataset-builder, and loader container health checks execute only the image's synthetic role/authorization smoke contract. They do not test the running process's venue session, spool durability, object publication, catalog transaction, dataset commitment, or warehouse load. The query server deliberately has no synthetic Compose health check; use `query-healthcheck` for its real TLS listener.

`/health/live` proves that the serving process answered through TLS. `/health/ready` additionally proves that it is not draining and that its metadata read model reports ready. Neither endpoint proves Binance freshness, raw-object durability, catalog/object parity, committed dataset coverage, ClickHouse generation parity, replay completeness, backup recoverability, or a production soak. Before admitting the endpoint, an operator must inspect authenticated sources, coverage, datasets, metrics, and representative bounded query and native replay results.

### Dashboard and authentication

1. Open `https://<the caller-published authority>/dashboard/`; the trailing slash is intentional. Verify the expected certificate chain and DNS identity. There is no second dashboard listener or upstream proxy.
2. Enter the bearer token for a configured query-server principal. The shell retains it only in memory, or in `sessionStorage` when the operator explicitly selects that option.
3. Use **Sources** and **Coverage** to confirm Binance Spot catalog identity and time coverage; **Datasets** to confirm committed Parquet dataset IDs; **Query console** for bounded declarative pages; and **Pipeline telemetry** for capture, publication, loader, and serving pressure. Native and normalized replay are authenticated API streams, not browser views.
4. A `401` means the bearer token is absent, malformed, or unknown. A `403` means the authenticated principal lacks the route's required scope. The public dashboard and health assets do not bypass authentication on any data or metrics route.
5. Use **Clear token** before closing a shared browser. It clears in-memory and `sessionStorage` bearer state; closing the tab clears memory, while closing the browser session clears `sessionStorage`.

For non-browser verification, create a caller-owned mode-0600 curl config outside the repository with `cacert`, the published query URL, and an `Authorization: Bearer ...` header. Then run bounded checks without placing the token in the command line:

```sh
curl --fail --silent --show-error --config "$QUERY_CURL_CONFIG_FILE" \
  --url "https://$QUERY_PUBLIC_AUTHORITY/v1/catalog/sources"
curl --fail --silent --show-error --config "$QUERY_CURL_CONFIG_FILE" \
  --url "https://$QUERY_PUBLIC_AUTHORITY/v1/coverage"
```

`QUERY_CURL_CONFIG_FILE` and `QUERY_PUBLIC_AUTHORITY` are caller-owned operator inputs, not Compose variables. Scope principals narrowly. The dashboard operator normally needs `catalog:read`, `coverage:read`, `query:read`, `replay:native`, `replay:normalized`, and `metrics:read`; automation should receive only the scopes it uses.

To rotate a bearer token, write new material to the caller-owned secret file using the caller's secure atomic replacement procedure, recreate `query-server` so references are resolved again, rerun the TLS probe, verify authenticated reads with the new token, verify the old token receives `401`, and clear existing dashboard sessions:

```sh
docker compose --env-file "$MARKETDATA_ENV_FILE" -f deploy/compose.yaml up -d --no-deps --force-recreate query-server
docker compose --env-file "$MARKETDATA_ENV_FILE" -f deploy/compose.yaml --profile operations run --rm query-healthcheck
curl --fail --silent --show-error --config "$NEW_QUERY_CURL_CONFIG_FILE" \
  --url "https://$QUERY_PUBLIC_AUTHORITY/v1/catalog/sources"
old_status="$(curl --silent --output /dev/null --write-out '%{http_code}' \
  --config "$OLD_QUERY_CURL_CONFIG_FILE" \
  --url "https://$QUERY_PUBLIC_AUTHORITY/v1/catalog/sources")"
test "$old_status" = 401
```

`NEW_QUERY_CURL_CONFIG_FILE` and `OLD_QUERY_CURL_CONFIG_FILE` are temporary caller-owned mode-0600 configs with the respective bearer headers; securely remove the old one after the check. Certificate, key, paging-key, and DSN rotation likewise require query-server recreation and the relevant functional checks. Paging-key rotation intentionally invalidates outstanding page tokens. Never put resolved secret bytes in YAML or the environment file.

### Graceful shutdown

Stop writers and readers before infrastructure, then remove only containers and the Compose network. The bind-mounted caller data remains intact. Never add `--volumes` to this command unless destroying caller-owned data is the explicit goal.

```sh
docker compose --env-file "$MARKETDATA_ENV_FILE" -f deploy/compose.yaml stop query-server loader dataset-builder collector
docker compose --env-file "$MARKETDATA_ENV_FILE" -f deploy/compose.yaml down --remove-orphans
```

## Backup and restore

The reference procedure creates a stopped-writer backup of the three durable authorities/projections. It assumes the environment file has been sourced as shown above, the caller has verified free space, and `BACKUP_ROOT` is on storage independent of PostgreSQL, ClickHouse, and object-store roots. Capture spool and both dataset work roots are restart/recovery workspace, not substitutes for committed raw objects and catalog state.

### Backup

```sh
export BACKUP_ID="$(date -u +%Y%m%dT%H%M%SZ)"
mkdir -p "$BACKUP_ROOT/postgresql/$BACKUP_ID" "$BACKUP_ROOT/clickhouse" "$BACKUP_ROOT/object-store/$BACKUP_ID"
docker compose --env-file "$MARKETDATA_ENV_FILE" -f deploy/compose.yaml stop query-server loader dataset-builder collector
docker compose --env-file "$MARKETDATA_ENV_FILE" -f deploy/compose.yaml exec -T postgres sh -ec 'pg_dump --format=custom --no-owner --username "$POSTGRES_USER" --dbname "$POSTGRES_DB"' > "$BACKUP_ROOT/postgresql/$BACKUP_ID/catalog.dump"
docker compose --env-file "$MARKETDATA_ENV_FILE" -f deploy/compose.yaml exec -T -e BACKUP_ID="$BACKUP_ID" clickhouse sh -ec 'clickhouse-client --user "$CLICKHOUSE_USER" --password "$CLICKHOUSE_PASSWORD" --query "BACKUP DATABASE \`$CLICKHOUSE_DB\` TO Disk('\''backups'\'', '\''$BACKUP_ID.zip'\'')"'
docker compose --env-file "$MARKETDATA_ENV_FILE" -f deploy/compose.yaml stop object-store
tar -C "$OBJECT_DATA_ROOT" -cpf "$BACKUP_ROOT/object-store/$BACKUP_ID/data.tar" .
docker compose --env-file "$MARKETDATA_ENV_FILE" -f deploy/compose.yaml start object-store
shasum -a 256 "$BACKUP_ROOT/postgresql/$BACKUP_ID/catalog.dump" "$BACKUP_ROOT/clickhouse/$BACKUP_ID.zip" "$BACKUP_ROOT/object-store/$BACKUP_ID/data.tar" > "$BACKUP_ROOT/$BACKUP_ID.sha256"
docker compose --env-file "$MARKETDATA_ENV_FILE" -f deploy/compose.yaml run --rm object-store-init
docker compose --env-file "$MARKETDATA_ENV_FILE" -f deploy/compose.yaml up -d collector dataset-builder loader query-server
docker compose --env-file "$MARKETDATA_ENV_FILE" -f deploy/compose.yaml --profile operations run --rm query-healthcheck
```

Also save the five role YAML files, the six container image digests, application build identity, public configuration digests, bucket name, object-store versioning/retention policy, and the checksum file in the caller-owned backup record. Do not copy token, TLS private-key, DSN, or object credential bytes into this record; recover them from the caller's separate secret-management and rotation process. The raw object-store archive is authoritative; PostgreSQL and ClickHouse alone are not a complete backup.

The procedure does not itself prove the application flushed every accepted record before shutdown. Operators must observe the collector's graceful drain and reconcile the final catalog/object checkpoint before accepting the backup.

### Restore into isolated targets

Restore is destructive to the selected target databases. Verify the backup checksum, select an empty replacement object root and isolated PostgreSQL/ClickHouse targets, keep application services stopped, and use the compatible image digests and configuration generation recorded with the backup. Do not restore over a running production root.

```sh
shasum -a 256 -c "$BACKUP_ROOT/$BACKUP_ID.sha256"
docker compose --env-file "$MARKETDATA_ENV_FILE" -f deploy/compose.yaml stop query-server loader dataset-builder collector object-store
test -d "$OBJECT_DATA_ROOT" && test -z "$(find "$OBJECT_DATA_ROOT" -mindepth 1 -maxdepth 1 -print -quit)"
tar -C "$OBJECT_DATA_ROOT" -xpf "$BACKUP_ROOT/object-store/$BACKUP_ID/data.tar"
docker compose --env-file "$MARKETDATA_ENV_FILE" -f deploy/compose.yaml up -d --wait postgres clickhouse object-store
docker compose --env-file "$MARKETDATA_ENV_FILE" -f deploy/compose.yaml exec -T postgres sh -ec 'dropdb --if-exists --force --username "$POSTGRES_USER" "$POSTGRES_DB" && createdb --username "$POSTGRES_USER" "$POSTGRES_DB"'
docker compose --env-file "$MARKETDATA_ENV_FILE" -f deploy/compose.yaml exec -T postgres sh -ec 'pg_restore --exit-on-error --no-owner --username "$POSTGRES_USER" --dbname "$POSTGRES_DB"' < "$BACKUP_ROOT/postgresql/$BACKUP_ID/catalog.dump"
docker compose --env-file "$MARKETDATA_ENV_FILE" -f deploy/compose.yaml exec -T -e BACKUP_ID="$BACKUP_ID" clickhouse sh -ec 'clickhouse-client --user "$CLICKHOUSE_USER" --password "$CLICKHOUSE_PASSWORD" --multiquery --query "DROP DATABASE IF EXISTS \`$CLICKHOUSE_DB\` SYNC; RESTORE DATABASE \`$CLICKHOUSE_DB\` FROM Disk('\''backups'\'', '\''$BACKUP_ID.zip'\'')"'
docker compose --env-file "$MARKETDATA_ENV_FILE" -f deploy/compose.yaml run --rm object-store-init
docker compose --env-file "$MARKETDATA_ENV_FILE" -f deploy/compose.yaml run --rm migration
docker compose --env-file "$MARKETDATA_ENV_FILE" -f deploy/compose.yaml up -d --no-deps query-server
docker compose --env-file "$MARKETDATA_ENV_FILE" -f deploy/compose.yaml --profile operations run --rm query-healthcheck
```

With capture still stopped, compare the recorded and restored catalog snapshot IDs, object identities and versions, committed dataset manifests, ClickHouse load-generation receipts, coverage intervals, authenticated bounded queries, and an authenticated native replay. A checksum match proves backup bytes, not semantic parity. Only after the caller accepts that comparison should it start writers and derived workers:

```sh
docker compose --env-file "$MARKETDATA_ENV_FILE" -f deploy/compose.yaml up -d collector dataset-builder loader
```

## Explicit limitations

- Compose is a reference single-node topology, not high availability. It does not choose replication, failure domains, resource reservations, autoscaling, ingress, DNS, certificate issuance, firewall policy, retention, alert routing, or an unattended schedule.
- The stack creates only the exact caller-named MinIO bucket through the explicit initialization job. It does not create or rotate credentials, change bucket visibility, establish object lock/versioning/retention, issue TLS material, generate paging keys or bearer tokens, write role YAML, or create data directories.
- PostgreSQL, ClickHouse, and MinIO are private-network reference services here. The caller must decide whether its threat model also requires TLS and independently scoped service identities on those internal links.
- The object-store backup command is a cold physical archive and requires a filesystem/tooling combination that preserves the MinIO data root exactly. A managed S3 deployment needs its provider's version-aware backup and restore procedure instead.
- The embedded dashboard is not an authorization boundary. Every data and metrics request still crosses the query server's TLS, bearer, scope, paging, interval, queue, deadline, and response-size controls.
- No live deployment, production soak, backup restore drill, exchange-connectivity result, or private health state is recorded by this repository. Public synthetic fixtures, a rendered Compose model, passing probes, and release evidence cannot establish those claims; they require caller-owned live observation and durable evidence.
