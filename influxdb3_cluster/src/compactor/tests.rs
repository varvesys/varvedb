use super::*;

/// Build a config whose knobs are easy to reason about in assertions.
fn config(
    max_input_size_bytes: u64,
    target_size_bytes: u64,
    max_inputs: usize,
) -> CompactionConfig {
    CompactionConfig {
        interval: humantime::Duration::from(Duration::from_secs(300)),
        min_age: humantime::Duration::from(Duration::from_secs(1200)),
        max_input_size_bytes,
        target_size_bytes,
        max_inputs,
        input_grace: humantime::Duration::from(Duration::from_secs(3600)),
    }
}

fn file(id: u64, chunk_time: i64, size_bytes: u64) -> ParquetFile {
    ParquetFile {
        id: ParquetFileId::from(id),
        path: format!("ingest-01/dbs/0/1/file-{id}.parquet").into(),
        size_bytes,
        row_count: 100,
        chunk_time,
        min_time: chunk_time,
        max_time: chunk_time + 1_000,
    }
}

const DB: DbId = DbId::new(0);
const TABLE: TableId = TableId::new(1);

#[test]
fn skips_files_that_are_not_yet_cold() {
    // Only files whose chunk_time is strictly older than the cutoff may be merged: a bucket that is
    // still receiving files would make the input set open, and a merge could race a new arrival.
    let files = vec![file(1, 100, 10), file(2, 200, 10), file(3, 300, 10)];
    let groups = select_merges(DB, TABLE, &files, 250, &config(1_000, 1_000, 100));

    assert_eq!(groups.len(), 1);
    let ids: Vec<u64> = groups[0].inputs.iter().map(|f| f.id.as_u64()).collect();
    assert_eq!(ids, vec![1, 2], "the file at chunk_time 300 is still warm");
}

#[test]
fn skips_files_that_are_already_large() {
    // A file at or above the input ceiling is already worth reading directly; rewriting it costs
    // more than it saves.
    let files = vec![file(1, 100, 10), file(2, 100, 5_000), file(3, 100, 10)];
    let groups = select_merges(DB, TABLE, &files, 1_000, &config(1_000, 100_000, 100));

    assert_eq!(groups.len(), 1);
    let ids: Vec<u64> = groups[0].inputs.iter().map(|f| f.id.as_u64()).collect();
    assert_eq!(ids, vec![1, 3]);
}

#[test]
fn never_emits_a_group_of_one() {
    // Rewriting a single file changes nothing but its name, and still costs a read, a write and a
    // snapshot.
    let files = vec![file(1, 100, 10), file(2, 100, 5_000)];
    let groups = select_merges(DB, TABLE, &files, 1_000, &config(1_000, 100_000, 100));
    assert!(groups.is_empty(), "got: {groups:?}");
}

#[test]
fn splits_on_target_size() {
    let files = vec![
        file(1, 100, 60),
        file(2, 200, 60),
        file(3, 300, 60),
        file(4, 400, 60),
    ];
    // `target_size_bytes` is a ceiling, not a goal: a group never exceeds it. At 130 a third
    // 60-byte file would overshoot, so each group closes at two.
    let groups = select_merges(DB, TABLE, &files, 1_000, &config(1_000, 130, 100));

    assert_eq!(groups.len(), 2);
    assert_eq!(groups[0].inputs.len(), 2);
    assert_eq!(groups[1].inputs.len(), 2);
}

#[test]
fn a_target_smaller_than_two_files_yields_nothing() {
    // Every group would be a singleton, and a singleton merge is pure cost. Worth pinning: a
    // misconfigured target should quietly do nothing rather than rewrite files one at a time.
    let files = vec![file(1, 100, 60), file(2, 200, 60), file(3, 300, 60)];
    let groups = select_merges(DB, TABLE, &files, 1_000, &config(1_000, 100, 100));
    assert!(groups.is_empty(), "got: {groups:?}");
}

#[test]
fn splits_on_max_inputs() {
    let files: Vec<ParquetFile> = (1..=5).map(|i| file(i, i as i64 * 100, 1)).collect();
    let groups = select_merges(DB, TABLE, &files, 1_000, &config(1_000, 100_000, 2));

    // 5 files at 2 per group leaves a trailing group of 1, which is dropped.
    assert_eq!(groups.len(), 2);
    assert!(groups.iter().all(|g| g.inputs.len() == 2));
}

#[test]
fn packs_in_chunk_time_order() {
    // Merging time-adjacent files keeps the output's min/max tight, so time-range filters can still
    // skip it. Packing in arrival order would smear one file across the whole retention window.
    let files = vec![file(1, 300, 10), file(2, 100, 10), file(3, 200, 10)];
    let groups = select_merges(DB, TABLE, &files, 1_000, &config(1_000, 25, 100));

    assert_eq!(
        groups.len(),
        1,
        "third file forms a group of one and is dropped"
    );
    let times: Vec<i64> = groups[0].inputs.iter().map(|f| f.chunk_time).collect();
    assert_eq!(times, vec![100, 200]);
}

#[test]
fn merge_group_spans_its_inputs() {
    let group = MergeGroup {
        db_id: DB,
        table_id: TABLE,
        inputs: vec![file(1, 300, 10), file(2, 100, 20)],
    };
    assert_eq!(group.chunk_time(), 100, "filed under its earliest input");
    assert_eq!(group.min_time(), 100);
    assert_eq!(group.max_time(), 1_300);
    assert_eq!(group.total_size(), 30);
    assert_eq!(group.total_rows(), 200);
}

#[test]
fn ingesting_peers_are_compactable_whether_or_not_they_serve_queries() {
    assert!(is_compactable(&[NodeMode::Ingest]));

    // Serving queries is no longer disqualifying. The inputs are protected by the commit order,
    // not by the target's role: the compactor only schedules deletion after the owner
    // acknowledges, and the owner only acknowledges after persisting the manifest. A notice that
    // never lands therefore never leads to a deletion.
    assert!(is_compactable(&[NodeMode::Ingest, NodeMode::Query]));

    // Nothing to compact.
    assert!(!is_compactable(&[NodeMode::Query]));

    // Two compactors targeting one prefix would race.
    assert!(!is_compactable(&[NodeMode::Ingest, NodeMode::Compact]));

    // `Core` and `All` fall out as not compactable, but for an unrelated reason: this predicate
    // matches `NodeMode::Ingest` specifically, and `Core` is its own catalog variant rather than a
    // union of Ingest and Query. A `--mode core` node does ingest, so on the reasoning above it
    // ought to qualify — `peer_buffer_chunks` works around the same quirk with an explicit `Core`
    // arm. Left as-is deliberately: widening it is a separate decision from removing the
    // query-serving exclusion.
    assert!(!is_compactable(&[NodeMode::Core]));
    assert!(!is_compactable(&[NodeMode::All]));
}

#[test]
fn select_targets_covers_peers_and_never_self_unless_asked() {
    let nodes: Vec<(Arc<str>, Vec<NodeMode>)> = vec![
        ("self01".into(), vec![NodeMode::All]),
        ("ing02".into(), vec![NodeMode::Ingest]),
        ("iq03".into(), vec![NodeMode::Ingest, NodeMode::Query]),
        ("qry04".into(), vec![NodeMode::Query]),
        ("cmp05".into(), vec![NodeMode::Compact]),
    ];
    let me: Arc<str> = "self01".into();

    // compact_self = false: peers only, self excluded even though another node lists it.
    let without_self = select_targets(&nodes, &me, false);
    assert_eq!(
        without_self.iter().map(|s| s.to_string()).collect::<Vec<_>>(),
        vec!["ing02", "iq03"],
        "only ingesting non-compactors, and never self"
    );

    // compact_self = true: same peers, then self appended last.
    let with_self = select_targets(&nodes, &me, true);
    assert_eq!(
        with_self.iter().map(|s| s.to_string()).collect::<Vec<_>>(),
        vec!["ing02", "iq03", "self01"],
    );
}

#[test]
fn select_targets_adds_self_even_before_its_registration_lands() {
    // A lone `--mode all` node whose own `register_node` has not yet been observed still compacts
    // its own prefix.
    let me: Arc<str> = "solo".into();
    assert_eq!(
        select_targets(&[], &me, true)
            .iter()
            .map(|s| s.to_string())
            .collect::<Vec<_>>(),
        vec!["solo"],
    );
    // ...and a compact-only node in the same situation gets nothing, which is what triggers the
    // "no compactable peers" warning.
    assert!(select_targets(&[], &me, false).is_empty());
}

#[test]
fn output_path_discriminator_is_out_of_gen1_reach() {
    // gen1 uses chunk_ordinal only as a count of Arrow-varchar splits, so it stays small. Setting
    // the high bit puts compaction output where a gen1 file cannot collide with it.
    let group = MergeGroup {
        db_id: DB,
        table_id: TABLE,
        inputs: vec![file(1, 100, 10), file(2, 200, 10)],
    };
    assert!(output_path_discriminator(&group) >= 0x8000_0000);
}

#[test]
fn output_path_discriminator_is_stable_across_input_order() {
    // The output path is a pure function of the inputs, so a retry after an unacknowledged merge
    // rewrites the same object instead of stranding a second orphan beside the first.
    let forward = MergeGroup {
        db_id: DB,
        table_id: TABLE,
        inputs: vec![file(1, 100, 10), file(2, 200, 10), file(3, 300, 10)],
    };
    let reversed = MergeGroup {
        db_id: DB,
        table_id: TABLE,
        inputs: vec![file(3, 300, 10), file(2, 200, 10), file(1, 100, 10)],
    };
    assert_eq!(
        output_path_discriminator(&forward),
        output_path_discriminator(&reversed)
    );
}

#[test]
fn output_path_discriminator_distinguishes_different_input_sets() {
    let a = MergeGroup {
        db_id: DB,
        table_id: TABLE,
        inputs: vec![file(1, 100, 10), file(2, 200, 10)],
    };
    let b = MergeGroup {
        db_id: DB,
        table_id: TABLE,
        inputs: vec![file(1, 100, 10), file(3, 300, 10)],
    };
    assert_ne!(
        output_path_discriminator(&a),
        output_path_discriminator(&b),
        "distinct merges must not share an output path"
    );
}

// ---- the pending-deletion queue survives a restart ----

mod pending_queue {
    use super::*;
    use influxdb3_write::persister::Persister;
    use iox_time::{MockProvider, Time};
    use object_store::ObjectStore;
    use object_store::memory::InMemory;
    use std::sync::Arc;
    use std::time::Duration;

    const CLUSTER: &str = "mycluster";

    fn deleter(store: Arc<dyn ObjectStore>, now_ns: i64, grace: Duration) -> InputDeleter {
        let time: Arc<dyn TimeProvider> =
            Arc::new(MockProvider::new(Time::from_timestamp_nanos(now_ns)));
        let persister = Arc::new(Persister::new(store, "cmp01", Arc::clone(&time), None));
        InputDeleter::new(persister, time, grace, CLUSTER.into())
    }

    fn inputs(paths: &[&str]) -> Vec<ParquetFile> {
        paths
            .iter()
            .enumerate()
            .map(|(i, p)| ParquetFile {
                path: (*p).into(),
                ..file(i as u64 + 1, 100, 10)
            })
            .collect()
    }

    #[tokio::test]
    async fn a_restarted_compactor_still_owes_what_it_scheduled() {
        // The leak this exists to close. The owner drops these files from its `PersistedFiles` the
        // moment it acknowledges the merge, so if the compactor forgets them nothing else tracks
        // them and they occupy storage permanently.
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let grace = Duration::from_secs(3600);

        let first = deleter(Arc::clone(&store), 0, grace);
        first
            .schedule(
                &Arc::from("ing01"),
                &inputs(&["ing01/a.parquet", "ing01/b.parquet"]),
            )
            .await;

        // A new process, same cluster prefix, well after the grace has elapsed.
        let restarted = deleter(Arc::clone(&store), 2 * 3600 * 1_000_000_000, grace);
        assert_eq!(
            restarted.pending.lock().len(),
            0,
            "starts empty before loading"
        );
        restarted.load().await;
        assert_eq!(
            restarted.pending.lock().len(),
            2,
            "must recover what the previous process owed"
        );
    }

    #[tokio::test]
    async fn reclaiming_shortens_the_saved_queue() {
        // Otherwise a restart would retry deletions that already happened — harmless against
        // object store, but it would keep the queue growing forever.
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let grace = Duration::from_secs(60);

        let d = deleter(Arc::clone(&store), 0, grace);
        d.schedule(&Arc::from("ing01"), &inputs(&["ing01/a.parquet"]))
            .await;

        // Past due.
        let later = deleter(Arc::clone(&store), 120 * 1_000_000_000, grace);
        later.load().await;
        assert_eq!(later.pending.lock().len(), 1);
        later.reclaim_due().await;
        assert_eq!(later.pending.lock().len(), 0);

        // A third process must see the emptied queue, not the original one.
        let third = deleter(Arc::clone(&store), 240 * 1_000_000_000, grace);
        third.load().await;
        assert_eq!(
            third.pending.lock().len(),
            0,
            "the save after reclaim must have landed"
        );
    }

    #[tokio::test]
    async fn an_unreadable_queue_starts_empty_rather_than_failing() {
        // The safety bias: leaking an object is recoverable, acting on a half-read list is not.
        // A corrupt queue must never panic or error a compactor into a crash loop.
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        store
            .put(
                &object_store::path::Path::from("mycluster/compactor/pending-deletions.json"),
                object_store::PutPayload::from_static(b"{ this is not json"),
            )
            .await
            .unwrap();

        let d = deleter(Arc::clone(&store), 0, Duration::from_secs(60));
        d.load().await;

        assert_eq!(d.pending.lock().len(), 0);
    }

    #[tokio::test]
    async fn no_queue_yet_is_not_an_error() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let d = deleter(store, 0, Duration::from_secs(60));
        d.load().await;
        assert_eq!(d.pending.lock().len(), 0);
    }
}
