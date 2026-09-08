use super::*;

#[test]
fn snapshot() {
    let mut tracker = SnapshotTracker::new(2, Gen1Duration::new_1m(), None);
    let p1 = WalPeriod::new(
        WalFileSequenceNumber::new(1),
        Timestamp::new(0),
        Timestamp::new(60_000000000),
    );
    let p2 = WalPeriod::new(
        WalFileSequenceNumber::new(2),
        Timestamp::new(800000001),
        Timestamp::new(119_900000000),
    );
    let p3 = WalPeriod::new(
        WalFileSequenceNumber::new(3),
        Timestamp::new(60_600000000),
        Timestamp::new(120_900000000),
    );
    let p4 = WalPeriod::new(
        WalFileSequenceNumber::new(4),
        Timestamp::new(120_800000001),
        Timestamp::new(240_000000000),
    );
    let p5 = WalPeriod::new(
        WalFileSequenceNumber::new(5),
        Timestamp::new(180_800000001),
        Timestamp::new(299_000000000),
    );
    let p6 = WalPeriod::new(
        WalFileSequenceNumber::new(6),
        Timestamp::new(240_800000001),
        Timestamp::new(360_100000000),
    );

    assert!(tracker.snapshot(false).is_none());
    tracker.add_wal_period(p1.clone());
    assert!(tracker.snapshot(false).is_none());
    tracker.add_wal_period(p2.clone());
    assert!(tracker.snapshot(false).is_none());
    tracker.add_wal_period(p3.clone());
    assert_eq!(
        tracker.snapshot(false),
        Some(SnapshotDetails {
            snapshot_sequence_number: SnapshotSequenceNumber::new(1),
            end_time_marker: 120_000000000,
            first_wal_sequence_number: WalFileSequenceNumber::new(1),
            last_wal_sequence_number: WalFileSequenceNumber::new(2),
            forced: false,
        })
    );
    tracker.add_wal_period(p4.clone());
    assert_eq!(tracker.snapshot(false), None);
    tracker.add_wal_period(p5.clone());
    assert_eq!(
        tracker.snapshot(false),
        Some(SnapshotDetails {
            snapshot_sequence_number: SnapshotSequenceNumber::new(2),
            end_time_marker: 240_000000000,
            first_wal_sequence_number: WalFileSequenceNumber::new(3),
            last_wal_sequence_number: WalFileSequenceNumber::new(3),
            forced: false,
        })
    );

    assert_eq!(tracker.wal_periods, vec![p4.clone(), p5.clone()]);

    tracker.add_wal_period(p6.clone());
    assert_eq!(
        tracker.snapshot(false),
        Some(SnapshotDetails {
            snapshot_sequence_number: SnapshotSequenceNumber::new(3),
            end_time_marker: 360_000000000,
            first_wal_sequence_number: WalFileSequenceNumber::new(4),
            last_wal_sequence_number: WalFileSequenceNumber::new(5),
            forced: false,
        })
    );

    assert!(tracker.snapshot(false).is_none());
}

#[test]
fn snapshot_future_data_forces_snapshot() {
    let mut tracker = SnapshotTracker::new(2, Gen1Duration::new_1m(), None);
    let p1 = WalPeriod::new(
        WalFileSequenceNumber::new(1),
        Timestamp::new(0),
        Timestamp::new(300_100000000),
    );
    let p2 = WalPeriod::new(
        WalFileSequenceNumber::new(2),
        Timestamp::new(30_000000000),
        Timestamp::new(59_900000000),
    );
    let p3 = WalPeriod::new(
        WalFileSequenceNumber::new(3),
        Timestamp::new(60_000000000),
        Timestamp::new(60_900000000),
    );
    let p4 = WalPeriod::new(
        WalFileSequenceNumber::new(4),
        Timestamp::new(90_000000000),
        Timestamp::new(120_000000000),
    );
    let p5 = WalPeriod::new(
        WalFileSequenceNumber::new(5),
        Timestamp::new(120_000000000),
        Timestamp::new(150_000000000),
    );
    let p6 = WalPeriod::new(
        WalFileSequenceNumber::new(6),
        Timestamp::new(150_000000000),
        Timestamp::new(180_100000000),
    );

    tracker.add_wal_period(p1.clone());
    tracker.add_wal_period(p2.clone());
    tracker.add_wal_period(p3.clone());
    assert!(tracker.snapshot(false).is_none());
    tracker.add_wal_period(p4.clone());
    assert!(tracker.snapshot(false).is_none());
    tracker.add_wal_period(p5.clone());
    assert!(tracker.snapshot(false).is_none());
    tracker.add_wal_period(p6.clone());

    assert_eq!(
        tracker.snapshot(false),
        Some(SnapshotDetails {
            snapshot_sequence_number: SnapshotSequenceNumber::new(1),
            end_time_marker: 360000000000,
            first_wal_sequence_number: WalFileSequenceNumber::new(1),
            last_wal_sequence_number: WalFileSequenceNumber::new(6),
            forced: true,
        })
    );
}

#[test]
fn reserve_snapshot_sequence_number_advances_monotonically() {
    let mut tracker = SnapshotTracker::new(2, Gen1Duration::new_1m(), None);
    assert_eq!(
        tracker.last_snapshot_sequence_number(),
        SnapshotSequenceNumber::new(0)
    );

    let first = tracker.reserve_snapshot_sequence_number();
    let second = tracker.reserve_snapshot_sequence_number();

    assert_eq!(first, SnapshotSequenceNumber::new(1));
    assert_eq!(second, SnapshotSequenceNumber::new(2));
    assert_eq!(tracker.last_snapshot_sequence_number(), second);
}

#[test]
fn a_reservation_is_never_reused_by_a_real_snapshot() {
    // This is the whole point of reserving rather than reading-and-adding-one. A caller that merely
    // read `last_snapshot_sequence_number` and added one would hand the same number to the flush
    // path, and `persist_snapshot` is an unconditional PUT — so whichever manifest was written first
    // would be silently destroyed.
    let mut tracker = SnapshotTracker::new(1, Gen1Duration::new_1m(), None);

    let reserved = tracker.reserve_snapshot_sequence_number();

    for i in 1..=3 {
        tracker.add_wal_period(WalPeriod::new(
            WalFileSequenceNumber::new(i),
            Timestamp::new((i as i64 - 1) * 60_000000000),
            Timestamp::new(i as i64 * 60_000000000),
        ));
    }

    let details = tracker
        .snapshot(false)
        .expect("enough wal periods to snapshot");

    assert_ne!(
        details.snapshot_sequence_number, reserved,
        "a real snapshot must not reuse a reserved number"
    );
    assert!(
        details.snapshot_sequence_number > reserved,
        "the tracker only moves forward: got {:?} after reserving {:?}",
        details.snapshot_sequence_number,
        reserved
    );
}

#[test]
fn an_unused_reservation_only_leaves_a_gap() {
    // Nothing downstream requires contiguity — replay compares a WAL file's recorded sequence
    // against the pre-replay high-water mark, and every other consumer orders by `>` alone.
    let mut tracker = SnapshotTracker::new(1, Gen1Duration::new_1m(), None);

    let _abandoned = tracker.reserve_snapshot_sequence_number();
    let next = tracker.reserve_snapshot_sequence_number();

    assert_eq!(next, SnapshotSequenceNumber::new(2));
    assert_eq!(tracker.last_snapshot_sequence_number(), next);
}
