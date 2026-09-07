//! Process-level end-to-end test for the cluster Processing Engine (roadmap "gap E",
//! the multi-process counterpart to the in-crate `pe_placement` unit suite).
//!
//! Stands up a real 3-node `feat/cluster-mode` cluster on localhost sharing one local
//! object store, seeds raw data split across two ingesters, and drives the official
//! InfluxData **downsampler** plugin plus a small `calendar_rollup` plugin to prove:
//!
//!   1. a trigger pinned to one node downsamples data written to *every* node
//!      (`influxdb3_local.query()` on a query-capable node reads cluster-wide);
//!   2. daily -> monthly -> yearly calendar rollups cascade with exact conservation;
//!   3. `TriggerPlacement` keeps the trigger (and its `request:` route) off the
//!      non-target nodes.
//!
//! Ignored by default: it spawns servers, needs the built binary and a Python >= 3.11.
//!
//! ```text
//! cargo build --bin influxdb3
//! cargo test -p influxdb3_cluster --test downsampler_e2e -- --ignored --nocapture
//! ```
//!
//! Binary lookup: `$INFLUXDB3_BIN`, else `<target>/{debug,release}/influxdb3`.

use std::net::TcpListener;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

use serde_json::{json, Value};

const FIXTURE: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/tests/downsampler_e2e");
/// sha256 of `tests/downsampler_e2e/vendor/downsampler.py` — see that file's `PROVENANCE`.
const DOWNSAMPLER_SHA256: &str =
    "801dda49f4035b0d691b7f1c6da03243de070ba97ef7c73a36f21bb47f803c0d";

// ---------------------------------------------------------------------------
// prerequisites
// ---------------------------------------------------------------------------

fn influxdb3_bin() -> PathBuf {
    if let Ok(p) = std::env::var("INFLUXDB3_BIN") {
        let p = PathBuf::from(p);
        assert!(p.is_file(), "INFLUXDB3_BIN={} is not a file", p.display());
        return p;
    }
    let target = std::env::var("CARGO_TARGET_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(|_| PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../target"));
    for profile in ["debug", "release"] {
        let cand = target.join(profile).join("influxdb3");
        if cand.is_file() {
            return cand;
        }
    }
    panic!(
        "influxdb3 binary not found under {} — run `cargo build --bin influxdb3` \
         (or set INFLUXDB3_BIN)",
        target.display()
    );
}

/// A `python3` on PATH whose stdlib has `tomllib` (>= 3.11, which the downsampler needs).
fn python3() -> String {
    for cand in ["python3", "python3.13", "python3.12", "python3.11"] {
        let ok = Command::new(cand)
            .args(["-c", "import tomllib, venv"])
            .status()
            .map(|s| s.success())
            .unwrap_or(false);
        if ok {
            return cand.to_string();
        }
    }
    panic!("no python3 >= 3.11 with `tomllib` on PATH — required by the downsampler plugin");
}

fn free_port() -> u16 {
    TcpListener::bind("127.0.0.1:0")
        .expect("bind ephemeral port")
        .local_addr()
        .unwrap()
        .port()
}

// ---------------------------------------------------------------------------
// cluster lifecycle
// ---------------------------------------------------------------------------

struct Cluster {
    children: Vec<(String, Child)>,
    http: [u16; 3], // node-a, node-b, node-c
    logs: PathBuf,
    _tmp: tempfile::TempDir,
}

impl Drop for Cluster {
    fn drop(&mut self) {
        for (_, c) in &mut self.children {
            let _ = c.kill();
            let _ = c.wait();
        }
    }
}

impl Cluster {
    fn url(&self, node: usize) -> String {
        format!("http://127.0.0.1:{}", self.http[node])
    }

    fn log_contains(&self, node: &str, needle: &str) -> bool {
        let path = self.logs.join(format!("{node}.log"));
        std::fs::read_to_string(path)
            .map(|s| s.contains(needle))
            .unwrap_or(false)
    }
}

#[allow(clippy::too_many_arguments)]
fn spawn(
    bin: &Path,
    tmp: &Path,
    logs: &Path,
    name: &str,
    http: u16,
    rpc: u16,
    mode: &str,
    plugin_dir: Option<&Path>,
) -> Child {
    let log = std::fs::File::create(logs.join(format!("{name}.log"))).unwrap();
    let mut cmd = Command::new(bin);
    cmd.args([
        "serve",
        "--node-id",
        name,
        "--http-bind",
        &format!("127.0.0.1:{http}"),
        "--cluster-rpc-bind",
        &format!("127.0.0.1:{rpc}"),
        "--mode",
        mode,
        "--object-store",
        "file",
        "--data-dir",
    ])
    .arg(tmp.join("data"))
    .args([
        "--cluster-id",
        "testcluster",
        "--without-auth",
        "--catalog-sync-interval",
        "1s",
        "--file-index-sync-interval",
        "1s",
        "--disable-package-management",
        "--log-filter",
        "info,influxdb3_processing_engine=debug",
    ]);
    if let Some(p) = plugin_dir {
        cmd.arg("--plugin-dir").arg(p);
        cmd.env("VIRTUAL_ENV", p.join(".venv"));
    }
    cmd.stdout(Stdio::from(log.try_clone().unwrap()))
        .stderr(Stdio::from(log));
    cmd.spawn().unwrap_or_else(|e| panic!("spawn {name}: {e}"))
}

fn poll<F: Fn() -> bool>(what: &str, timeout: Duration, f: F) {
    let deadline = Instant::now() + timeout;
    while Instant::now() < deadline {
        if f() {
            return;
        }
        std::thread::sleep(Duration::from_millis(500));
    }
    panic!("timed out waiting for: {what}");
}

// ---------------------------------------------------------------------------
// HTTP helpers
// ---------------------------------------------------------------------------

fn http() -> reqwest::blocking::Client {
    reqwest::blocking::Client::builder()
        .timeout(Duration::from_secs(120))
        .build()
        .unwrap()
}

fn healthy(client: &reqwest::blocking::Client, url: &str) -> bool {
    client
        .get(format!("{url}/health"))
        .send()
        .map(|r| r.status().is_success())
        .unwrap_or(false)
}

/// Run `SELECT ...` via `/api/v3/query_sql` (GET form), returning the parsed JSON rows.
fn query(client: &reqwest::blocking::Client, url: &str, db: &str, sql: &str) -> Vec<Value> {
    let resp = client
        .get(format!("{url}/api/v3/query_sql"))
        .query(&[("db", db), ("q", sql), ("format", "json")])
        .send()
        .unwrap_or_else(|e| panic!("query_sql send: {e}"));
    let status = resp.status();
    let text = resp.text().unwrap_or_default();
    assert!(status.is_success(), "query_sql {sql:?} -> {status}: {text}");
    serde_json::from_str::<Vec<Value>>(&text)
        .unwrap_or_else(|e| panic!("query_sql {sql:?} bad json ({e}): {text}"))
}

fn i64_of(rows: &[Value], col: &str) -> i64 {
    rows[0][col]
        .as_i64()
        .unwrap_or_else(|| panic!("column {col} not i64 in {:?}", rows.first()))
}

fn f64_of(rows: &[Value], col: &str) -> f64 {
    rows[0][col]
        .as_f64()
        .unwrap_or_else(|| panic!("column {col} not f64 in {:?}", rows.first()))
}

fn post_engine(
    client: &reqwest::blocking::Client,
    url: &str,
    path: &str,
    body: &Value,
) -> (u16, String) {
    let resp = client
        .post(format!("{url}/api/v3/engine/{path}"))
        .json(body)
        .send()
        .unwrap_or_else(|e| panic!("POST engine/{path}: {e}"));
    let code = resp.status().as_u16();
    (code, resp.text().unwrap_or_default())
}

fn run_bin(bin: &Path, args: &[&str]) {
    let out = Command::new(bin)
        .args(args)
        .output()
        .unwrap_or_else(|e| panic!("run {args:?}: {e}"));
    assert!(
        out.status.success(),
        "{args:?} failed:\nstdout: {}\nstderr: {}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr),
    );
}

// ---------------------------------------------------------------------------
// the test
// ---------------------------------------------------------------------------

#[test]
#[ignore = "process-level e2e; run with --ignored after `cargo build --bin influxdb3`"]
fn downsampler_cluster_wide_and_calendar_rollups() {
    let bin = influxdb3_bin();
    let py = python3();

    // vendored plugin integrity
    let vendored = PathBuf::from(FIXTURE).join("vendor/downsampler.py");
    let bytes = std::fs::read(&vendored).expect("read vendored downsampler.py");
    let got = sha256_hex(&bytes);
    assert_eq!(
        got, DOWNSAMPLER_SHA256,
        "vendored downsampler.py sha256 changed — update DOWNSAMPLER_SHA256 and PROVENANCE"
    );

    // temp layout
    let tmp = tempfile::TempDir::new().unwrap();
    let root = tmp.path();
    let plugins = root.join("plugins");
    let logs = root.join("logs");
    for d in [root.join("data"), plugins.clone(), logs.clone()] {
        std::fs::create_dir_all(d).unwrap();
    }
    std::fs::write(plugins.join("downsampler.py"), &bytes).unwrap();
    std::fs::copy(
        PathBuf::from(FIXTURE).join("calendar_rollup.py"),
        plugins.join("calendar_rollup.py"),
    )
    .unwrap();

    // offline venv (downsampler + calendar_rollup are stdlib-only, but --plugin-dir with
    // --disable-package-management still wants VIRTUAL_ENV to point at a real venv)
    let venv_ok = Command::new(&py)
        .args(["-m", "venv"])
        .arg(plugins.join(".venv"))
        .status()
        .map(|s| s.success())
        .unwrap_or(false);
    assert!(venv_ok, "python -m venv failed");

    // ports + spawn
    let http_ports = [free_port(), free_port(), free_port()];
    let rpc_ports = [free_port(), free_port(), free_port()];
    let names = ["node-a", "node-b", "node-c"];
    let modes = ["ingest", "ingest,query", "query"];
    let plugin_dirs = [None, Some(plugins.as_path()), None];

    let mut children = Vec::new();
    for (((name, mode), plugin_dir), (http_port, rpc_port)) in names
        .iter()
        .zip(modes)
        .zip(plugin_dirs)
        .zip(http_ports.iter().zip(rpc_ports.iter()))
    {
        children.push((
            name.to_string(),
            spawn(&bin, root, &logs, name, *http_port, *rpc_port, mode, plugin_dir),
        ));
        // node-a must be registered before later nodes pin a trigger to it / see it
        std::thread::sleep(Duration::from_millis(300));
    }
    let cluster = Cluster {
        children,
        http: http_ports,
        logs: logs.clone(),
        _tmp: tmp,
    };
    let c = http();

    for (i, name) in names.iter().enumerate() {
        poll(&format!("{name} /health"), Duration::from_secs(45), || {
            healthy(&c, &cluster.url(i))
        });
    }
    // all three nodes visible in the shared catalog (from node-b)
    poll("3 nodes registered", Duration::from_secs(45), || {
        let rows = query(&c, &cluster.url(1), "_internal", "SELECT node_id FROM system.nodes");
        rows.len() == 3
    });

    // database + seed
    run_bin(
        &bin,
        &["create", "database", "testdb", "--host", &cluster.url(1)],
    );
    let seed = Command::new(&py)
        .arg(PathBuf::from(FIXTURE).join("seed.py"))
        .arg(cluster.url(0))
        .arg(cluster.url(1))
        .output()
        .expect("run seed.py");
    assert!(
        seed.status.success(),
        "seed.py failed: {}",
        String::from_utf8_lossy(&seed.stderr)
    );

    // node-c (pure query node) already sees every raw point
    let raw = query(&c, &cluster.url(2), "testdb", "SELECT count(*) AS c FROM home");
    assert_eq!(i64_of(&raw, "c"), 8768, "node-c should see all raw points cluster-wide");

    // triggers, both pinned to node-b
    run_bin(
        &bin,
        &[
            "create", "trigger", "--host", &cluster.url(1), "--database", "testdb",
            "--path", "downsampler.py", "--trigger-spec", "request:downsample",
            "--trigger-arguments", "node_spec=nodes:node-b", "downsample_api",
        ],
    );
    run_bin(
        &bin,
        &[
            "create", "trigger", "--host", &cluster.url(1), "--database", "testdb",
            "--path", "calendar_rollup.py", "--trigger-spec", "request:rollup",
            "--trigger-arguments", "node_spec=nodes:node-b", "rollup_api",
        ],
    );

    // placement: node-a learns of the trigger and declines it (also assertion #9a)
    poll("node-a logs 'not placed' for downsample_api", Duration::from_secs(15), || {
        cluster.log_contains("node-a", "not placed on this node")
            && cluster.log_contains("node-a", "downsample_api")
    });
    poll("node-c logs 'not placed'", Duration::from_secs(15), || {
        cluster.log_contains("node-c", "not placed on this node")
    });

    // placement: the request route only exists on node-b
    let probe = json!({
        "source_measurement": "home", "target_measurement": "placement_probe",
        "interval": "1d", "calculations": [["temp", "sum"]],
        "backfill_start": "2025-12-30T00:00:00Z", "backfill_end": "2025-12-31T00:00:00Z"
    });
    assert_eq!(post_engine(&c, &cluster.url(0), "downsample", &probe).0, 404, "node-a route");
    assert_eq!(post_engine(&c, &cluster.url(2), "downsample", &probe).0, 404, "node-c route");

    // daily rollup — HTTP backfill over the full 3 years, on node-b
    let (st, body) = post_engine(
        &c,
        &cluster.url(1),
        "downsample",
        &json!({
            "source_measurement": "home", "target_measurement": "home_daily",
            "interval": "1d", "calculations": [["temp", "sum"]], "batch_size": "90d",
            "backfill_start": "2023-01-01T00:00:00Z", "backfill_end": "2026-01-01T00:00:00Z"
        }),
    );
    assert_eq!(st, 200, "backfill status; body: {body}");
    assert!(body.contains("completed"), "backfill body: {body}");

    // calendar cascade: daily -> monthly -> yearly
    for (src, tgt, bucket) in [
        ("home_daily", "home_monthly", "month"),
        ("home_monthly", "home_yearly", "year"),
    ] {
        let (st, body) = post_engine(
            &c,
            &cluster.url(1),
            "rollup",
            &json!({"source_measurement": src, "target_measurement": tgt, "bucket": bucket}),
        );
        assert_eq!(st, 200, "{bucket} rollup status; body: {body}");
        assert!(body.contains("\"written\""), "{bucket} rollup body: {body}");
    }

    // ---- assertions, all from node-c (independent query node) ----
    let n2 = &cluster.url(2);

    // (1) cluster-wide: every daily bucket saw all 4 raw points (2 from each ingester).
    //     record_count == 2 everywhere would mean node-b only read its own WAL.
    let d = query(
        &c,
        n2,
        "testdb",
        "SELECT count(*) AS rows, min(record_count) AS mn, max(record_count) AS mx, \
         sum(record_count) AS tot FROM home_daily",
    );
    assert_eq!(i64_of(&d, "rows"), 2192, "home_daily rows (1096 days * 2 rooms)");
    assert_eq!(i64_of(&d, "mn"), 4, "min per-bucket record_count (cluster-wide read)");
    assert_eq!(i64_of(&d, "mx"), 4, "max per-bucket record_count");
    assert_eq!(i64_of(&d, "tot"), 8768, "sum(record_count) == all raw points");

    // (2) exact aggregate values per room
    let k = query(
        &c, n2, "testdb",
        "SELECT min(temp_sum) AS mn, max(temp_sum) AS mx FROM home_daily WHERE room = 'Kitchen'",
    );
    assert_eq!(f64_of(&k, "mn"), 116.0);
    assert_eq!(f64_of(&k, "mx"), 116.0);

    // (3) calendar / leap-year aware monthly buckets: Feb record_count 112/116/112
    let feb = query(
        &c, n2, "testdb",
        "SELECT record_count AS rc FROM home_monthly \
         WHERE room = 'Kitchen' AND date_part('month', time) = 2 ORDER BY time",
    );
    let feb: Vec<i64> = feb.iter().map(|r| r["rc"].as_i64().unwrap()).collect();
    assert_eq!(feb, vec![112, 116, 112], "Feb 2023 / 2024(leap) / 2025");

    // (4) conservation through the cascade
    for lvl in ["home_monthly", "home_yearly"] {
        let s = query(&c, n2, "testdb", &format!("SELECT sum(record_count) AS t FROM {lvl}"));
        assert_eq!(i64_of(&s, "t"), 8768, "sum(record_count) conserved at {lvl}");
    }

    // (5) weighted mean preserved: temp_sum / record_count == raw AVG(temp)
    let wy = query(
        &c, n2, "testdb",
        "SELECT temp_sum / record_count AS w FROM home_yearly WHERE room = 'Kitchen' LIMIT 1",
    );
    assert_eq!(f64_of(&wy, "w"), 29.0);

    // (6) placement log lines (node-a checked in the poll above; assert node-c too)
    assert!(
        cluster.log_contains("node-c", "not placed on this node"),
        "node-c should have skipped the pinned trigger"
    );

    drop(cluster); // explicit: SIGKILL all three nodes
}

// ---------------------------------------------------------------------------
// tiny sha256 (avoid pulling a crate for one hash)
// ---------------------------------------------------------------------------

fn sha256_hex(data: &[u8]) -> String {
    // FIPS 180-4
    const K: [u32; 64] = [
        0x428a2f98, 0x71374491, 0xb5c0fbcf, 0xe9b5dba5, 0x3956c25b, 0x59f111f1, 0x923f82a4,
        0xab1c5ed5, 0xd807aa98, 0x12835b01, 0x243185be, 0x550c7dc3, 0x72be5d74, 0x80deb1fe,
        0x9bdc06a7, 0xc19bf174, 0xe49b69c1, 0xefbe4786, 0x0fc19dc6, 0x240ca1cc, 0x2de92c6f,
        0x4a7484aa, 0x5cb0a9dc, 0x76f988da, 0x983e5152, 0xa831c66d, 0xb00327c8, 0xbf597fc7,
        0xc6e00bf3, 0xd5a79147, 0x06ca6351, 0x14292967, 0x27b70a85, 0x2e1b2138, 0x4d2c6dfc,
        0x53380d13, 0x650a7354, 0x766a0abb, 0x81c2c92e, 0x92722c85, 0xa2bfe8a1, 0xa81a664b,
        0xc24b8b70, 0xc76c51a3, 0xd192e819, 0xd6990624, 0xf40e3585, 0x106aa070, 0x19a4c116,
        0x1e376c08, 0x2748774c, 0x34b0bcb5, 0x391c0cb3, 0x4ed8aa4a, 0x5b9cca4f, 0x682e6ff3,
        0x748f82ee, 0x78a5636f, 0x84c87814, 0x8cc70208, 0x90befffa, 0xa4506ceb, 0xbef9a3f7,
        0xc67178f2,
    ];
    let mut h: [u32; 8] = [
        0x6a09e667, 0xbb67ae85, 0x3c6ef372, 0xa54ff53a, 0x510e527f, 0x9b05688c, 0x1f83d9ab,
        0x5be0cd19,
    ];
    let mut msg = data.to_vec();
    let bitlen = (data.len() as u64) * 8;
    msg.push(0x80);
    while msg.len() % 64 != 56 {
        msg.push(0);
    }
    msg.extend_from_slice(&bitlen.to_be_bytes());

    for chunk in msg.chunks_exact(64) {
        let mut w = [0u32; 64];
        for i in 0..16 {
            w[i] = u32::from_be_bytes([
                chunk[i * 4],
                chunk[i * 4 + 1],
                chunk[i * 4 + 2],
                chunk[i * 4 + 3],
            ]);
        }
        for i in 16..64 {
            let s0 = w[i - 15].rotate_right(7) ^ w[i - 15].rotate_right(18) ^ (w[i - 15] >> 3);
            let s1 = w[i - 2].rotate_right(17) ^ w[i - 2].rotate_right(19) ^ (w[i - 2] >> 10);
            w[i] = w[i - 16]
                .wrapping_add(s0)
                .wrapping_add(w[i - 7])
                .wrapping_add(s1);
        }
        let (mut a, mut b, mut cc, mut d, mut e, mut f, mut g, mut hh) =
            (h[0], h[1], h[2], h[3], h[4], h[5], h[6], h[7]);
        for i in 0..64 {
            let s1 = e.rotate_right(6) ^ e.rotate_right(11) ^ e.rotate_right(25);
            let ch = (e & f) ^ ((!e) & g);
            let t1 = hh
                .wrapping_add(s1)
                .wrapping_add(ch)
                .wrapping_add(K[i])
                .wrapping_add(w[i]);
            let s0 = a.rotate_right(2) ^ a.rotate_right(13) ^ a.rotate_right(22);
            let maj = (a & b) ^ (a & cc) ^ (b & cc);
            let t2 = s0.wrapping_add(maj);
            hh = g;
            g = f;
            f = e;
            e = d.wrapping_add(t1);
            d = cc;
            cc = b;
            b = a;
            a = t1.wrapping_add(t2);
        }
        h[0] = h[0].wrapping_add(a);
        h[1] = h[1].wrapping_add(b);
        h[2] = h[2].wrapping_add(cc);
        h[3] = h[3].wrapping_add(d);
        h[4] = h[4].wrapping_add(e);
        h[5] = h[5].wrapping_add(f);
        h[6] = h[6].wrapping_add(g);
        h[7] = h[7].wrapping_add(hh);
    }
    h.iter().map(|x| format!("{x:08x}")).collect()
}

#[cfg(test)]
mod sha256_selfcheck {
    #[test]
    fn known_vectors() {
        assert_eq!(
            super::sha256_hex(b""),
            "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
        );
        assert_eq!(
            super::sha256_hex(b"abc"),
            "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"
        );
    }
}
