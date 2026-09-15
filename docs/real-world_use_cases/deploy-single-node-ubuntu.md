# Deploying VarveDB on Ubuntu 24.04 — single node, `--mode all`

Runs everything — ingest, query, compact, process — in one `varvedb` process on
one box, as a systemd service, backed by local disk. No S3-compatible object
store, no second node.

## Prerequisites

Rust, `protoc`, and `python3-dev` to build; `python3-venv` at runtime if you
use the Processing Engine. See the build steps covered earlier — in short:

```sh
sudo apt update
sudo apt install -y build-essential pkg-config libssl-dev clang lld git curl \
  protobuf-compiler python3 python3-dev python3-pip python3-venv
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
source "$HOME/.cargo/env"
git clone https://github.com/varvesys/varvedb.git
cd varvedb
cargo build --profile release --bin varvedb
```

Use `--profile release` here (not the plain debug build used for local
testing) — this is a real, always-on service.

## 1. Create a system user and directories

```sh
sudo useradd --system --no-create-home --shell /usr/sbin/nologin varvedb
sudo mkdir -p /var/lib/varvedb/data /var/log/varvedb
sudo chown -R varvedb:varvedb /var/lib/varvedb /var/log/varvedb
```

## 2. Install the binary

```sh
sudo install -o root -g root -m 0755 target/release/varvedb /usr/local/bin/varvedb
varvedb --version
```

## 3. systemd unit

`/etc/systemd/system/varvedb.service`:

```ini
[Unit]
Description=VarveDB (single-node, --mode all)
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
Restart=on-failure
RestartSec=5
User=varvedb
Group=varvedb
UMask=0027
WorkingDirectory=/var/lib/varvedb
LimitNOFILE=65536
StandardOutput=journal
StandardError=journal

ExecStart=/usr/local/bin/varvedb serve \
  --node-id node-a \
  --cluster-id single-cluster \
  --mode all \
  --object-store file \
  --data-dir /var/lib/varvedb/data \
  --http-bind 0.0.0.0:8181 \
  --cluster-rpc-bind 0.0.0.0:8281 \
  --without-auth \
  --log-filter info

# Basic sandboxing — safe defaults for an unprivileged service user.
NoNewPrivileges=true
RestrictSUIDSGID=true
CapabilityBoundingSet=
AmbientCapabilities=
PrivateTmp=true
ProtectHome=true
ProtectSystem=strict
ReadWritePaths=/var/lib/varvedb /var/log/varvedb
RestrictNamespaces=true
# The embedded Processing Engine (part of --mode all) needs writable+executable
# mappings for its Python runtime; leave this false rather than the systemd
# default of true.
MemoryDenyWriteExecute=false

[Install]
WantedBy=multi-user.target
```

Notes on the flags:

- `--cluster-id single-cluster` is required for any `--mode` other than
  `core` — `all` needs it even on one node, since it's still a (one-node)
  cluster as far as the catalog is concerned. It must differ from `--node-id`.
- `--cluster-rpc-bind 0.0.0.0:8281` is published to the catalog as this node's
  peer address. It's unused today (no peers), harmless to leave bound, and is
  exactly what a future node B would need to be able to reach if you ever grow
  into the 2-node split instead of adding real shared storage.
- `--without-auth` — every CLI/API call is unauthenticated. Fine as long as
  this host isn't reachable from an untrusted network; put it behind a
  firewall (§4) and revisit before exposing it externally.

Reload and start:

```sh
sudo systemctl daemon-reload
sudo systemctl enable --now varvedb
sudo systemctl status varvedb
journalctl -u varvedb -f
```

## 4. Firewall (ufw)

```sh
sudo ufw allow 8181/tcp        # HTTP API — open to wherever your clients are
# 8281 (cluster-rpc-bind) has no peers yet; leave it closed until node B exists
sudo ufw enable
sudo ufw status
```

Scope `8181` to a specific subnet (`sudo ufw allow from 10.0.0.0/24 to any port 8181`)
rather than the world if this host has any public exposure at all — remember,
`--without-auth` means the port has no credential check of its own.

## 5. Verify

```sh
curl http://localhost:8181/health

varvedb create database sensors --host http://localhost:8181

curl -sS "http://localhost:8181/api/v3/write_lp?db=sensors&precision=second" \
  --data-binary "cpu,host=a usage=0.5 $(date +%s)"
# -> HTTP 204

curl -sS --get "http://localhost:8181/api/v3/query_sql" \
  --data-urlencode "db=sensors" --data-urlencode "q=SELECT * FROM cpu" \
  --data-urlencode "format=jsonl"
# {"host":"a","time":"...","usage":0.5}

curl -sS --get "http://localhost:8181/api/v3/query_sql" \
  --data-urlencode "db=_internal" \
  --data-urlencode "q=SELECT node_id, mode, state FROM system.nodes" \
  --data-urlencode "format=jsonl"
# {"node_id":"node-a","mode":["all"],"state":"running"}
```

Data lives under `/var/lib/varvedb/data/single-cluster/` (catalog) and
`/var/lib/varvedb/data/node-a/` (WAL, Parquet, snapshots — this node compacts
its own files in-process, so there's nothing else to run).

## Upgrading

```sh
sudo systemctl stop varvedb
sudo install -o root -g root -m 0755 target/release/varvedb /usr/local/bin/varvedb
sudo systemctl start varvedb
```

## Uninstalling

```sh
sudo systemctl disable --now varvedb
sudo rm /etc/systemd/system/varvedb.service
sudo systemctl daemon-reload
sudo rm /usr/local/bin/varvedb
# data is untouched — remove it explicitly if you actually want it gone:
sudo rm -rf /var/lib/varvedb /var/log/varvedb
sudo userdel varvedb
```

## Scaling out later

When you actually have a reason to split ingest/query from compaction across
two hosts, three things change from this doc:

1. **Stand up a real object store both hosts can reach** — a self-hosted
   S3-compatible server (Garage, MinIO, SeaweedFS, …) or cloud S3. This is the
   piece a single `--mode all` node doesn't need, and can't substitute for with
   local disk (see the top of this doc).
2. **Migrate the data.** `--data-dir` is just a root prefix — the paths
   underneath (`single-cluster/...` for the catalog, `node-a/...` for this
   node's data) are the same relative keys an S3 bucket would use. Copy the
   tree in with the same relative structure, e.g.:
   ```sh
   mc mirror /var/lib/varvedb/data/ myminio/varvedb/
   ```
3. **Change the `--mode` on node A and add node B**, both pointed at the new
   store:
   - node A: `--mode ingest,query` (drop `all`)
   - node B: `--mode compact` (a dedicated role — it cannot be combined with
     `query` or anything else; `resolve_modes` in
     `influxdb3_cluster/src/config.rs` rejects that combination outright)

   Both add `--object-store s3 --bucket varvedb --aws-endpoint http://<store-host>:<port> --aws-access-key-id ... --aws-secret-access-key ...`
   in place of `--object-store file --data-dir ...`, and node B needs its own
   `--cluster-rpc-bind` reachable from node A.

For what each role actually does day to day, see the
[Architecture section of the README](../../README.md#architecture) and the
worked, runnable examples in [cluster-playground.md](cluster-playground.md).
