//! Concurrent history checking for snapshot isolation.
//!
//! Worker threads run randomized transaction mixes against one database
//! while every attempt records its snapshot, reads, writes, and outcome.
//! A single-threaded checker then validates the whole history against the
//! promised semantics: snapshot-consistent reads, first-committer-wins
//! conflicts, unique commit timestamps, and a final state equal to the
//! committed writes folded in timestamp order, surviving a restart.
//!
//! Runs are seeded so a failure names the seed that produced it.

use quantadb_mvcc::{MvccDatabase, MvccOptions, Timestamp, TransactionError};
use std::collections::BTreeMap;
use std::sync::Arc;
use std::thread;

const WORKERS: usize = 8;
const TRANSACTIONS_PER_WORKER: usize = 40;
const KEY_SPACE: u64 = 16;

#[derive(Debug, Clone, PartialEq, Eq)]
enum Outcome {
    Committed(Timestamp),
    Conflicted,
    RolledBack,
    ReadOnly,
}

#[derive(Debug, Clone)]
struct Attempt {
    seed: u64,
    snapshot: Timestamp,
    reads: Vec<(Vec<u8>, Option<Vec<u8>>)>,
    writes: BTreeMap<Vec<u8>, Option<Vec<u8>>>,
    outcome: Outcome,
}

fn key(id: u64) -> Vec<u8> {
    format!("hist:{id:02}").into_bytes()
}

fn next_random(state: &mut u64) -> u64 {
    *state ^= *state << 13;
    *state ^= *state >> 7;
    *state ^= *state << 17;
    *state
}

fn worker_history(database: &MvccDatabase, seed: u64) -> Vec<Attempt> {
    let mut rng = seed;
    let mut attempts = Vec::with_capacity(TRANSACTIONS_PER_WORKER);

    for round in 0..TRANSACTIONS_PER_WORKER {
        let mut transaction = database.begin().expect("begin transaction");
        let mut attempt = Attempt {
            seed,
            snapshot: transaction.snapshot(),
            reads: Vec::new(),
            writes: BTreeMap::new(),
            outcome: Outcome::RolledBack,
        };

        let operations = 1 + next_random(&mut rng) % 4;
        for _ in 0..operations {
            let target = key(next_random(&mut rng) % KEY_SPACE);
            match next_random(&mut rng) % 100 {
                0..=49 => {
                    let observed = transaction.get(&target).expect("read");
                    if !attempt.writes.contains_key(&target) {
                        attempt.reads.push((target, observed));
                    }
                }
                50..=84 => {
                    let value = format!("s{seed}:r{round}:{}", next_random(&mut rng)).into_bytes();
                    transaction.put(target.clone(), value.clone()).expect("put");
                    attempt.writes.insert(target, Some(value));
                }
                _ => {
                    transaction.delete(target.clone()).expect("delete");
                    attempt.writes.insert(target, None);
                }
            }
        }

        if next_random(&mut rng) % 100 < 80 {
            match transaction.commit() {
                Ok(result) => {
                    attempt.outcome = match result.timestamp {
                        Some(timestamp) => Outcome::Committed(timestamp),
                        None => Outcome::ReadOnly,
                    };
                }
                Err(TransactionError::WriteConflict { .. }) => {
                    attempt.outcome = Outcome::Conflicted;
                }
                Err(error) => panic!("unexpected commit failure (seed {seed}): {error}"),
            }
        } else {
            transaction.rollback().expect("rollback");
            attempt.outcome = Outcome::RolledBack;
        }
        attempts.push(attempt);
    }
    attempts
}

fn committed_writes(history: &[Attempt]) -> Vec<(Timestamp, &Attempt)> {
    let mut committed = history
        .iter()
        .filter_map(|attempt| match attempt.outcome {
            Outcome::Committed(timestamp) => Some((timestamp, attempt)),
            _ => None,
        })
        .collect::<Vec<_>>();
    committed.sort_by_key(|(timestamp, _)| *timestamp);
    committed
}

fn expected_value_at(
    committed: &[(Timestamp, &Attempt)],
    key: &[u8],
    snapshot: Timestamp,
) -> Option<Vec<u8>> {
    committed
        .iter()
        .rev()
        .filter(|(timestamp, _)| *timestamp <= snapshot)
        .find_map(|(_, attempt)| attempt.writes.get(key))
        .and_then(Clone::clone)
}

fn check_history(history: &[Attempt]) -> BTreeMap<Vec<u8>, Vec<u8>> {
    let committed = committed_writes(history);

    for window in committed.windows(2) {
        assert_ne!(
            window[0].0, window[1].0,
            "two transactions share commit timestamp {:?}",
            window[0].0
        );
    }

    for (timestamp, attempt) in &committed {
        for key in attempt.writes.keys() {
            let intruder = committed.iter().find(|(other_timestamp, other_attempt)| {
                *other_timestamp > attempt.snapshot
                    && other_timestamp < timestamp
                    && other_attempt.writes.contains_key(key)
            });
            assert!(
                intruder.is_none(),
                "first-committer-wins violated (seed {}): the commit at {timestamp:?} from \
                 snapshot {:?} overlapped {:?} on key {:?}",
                attempt.seed,
                attempt.snapshot,
                intruder.map(|(other_timestamp, _)| other_timestamp),
                String::from_utf8_lossy(key),
            );
        }
    }

    for attempt in history {
        for (key, observed) in &attempt.reads {
            let expected = expected_value_at(&committed, key, attempt.snapshot);
            assert_eq!(
                observed,
                &expected,
                "read at snapshot {:?} (seed {}) observed the wrong value for {:?}",
                attempt.snapshot,
                attempt.seed,
                String::from_utf8_lossy(key),
            );
        }
    }

    let mut expected_state = BTreeMap::new();
    for (_, attempt) in &committed {
        for (key, value) in &attempt.writes {
            match value {
                Some(value) => {
                    expected_state.insert(key.clone(), value.clone());
                }
                None => {
                    expected_state.remove(key);
                }
            }
        }
    }
    expected_state
}

fn assert_database_matches(
    database: &MvccDatabase,
    expected_state: &BTreeMap<Vec<u8>, Vec<u8>>,
    context: &str,
) {
    for id in 0..KEY_SPACE {
        let target = key(id);
        assert_eq!(
            database.get(&target).expect("final read"),
            expected_state.get(&target).cloned(),
            "{context}: key {:?} diverged from the folded history",
            String::from_utf8_lossy(&target),
        );
    }
    let transaction = database.begin().expect("begin verification scan");
    let scanned = transaction
        .scan_prefix(b"hist:")
        .expect("verification scan")
        .into_iter()
        .collect::<BTreeMap<_, _>>();
    transaction.rollback().expect("rollback verification scan");
    assert_eq!(
        &scanned, expected_state,
        "{context}: prefix scan diverged from the folded history"
    );
}

fn run_seeded(seed: u64) {
    let directory = tempfile::tempdir().expect("tempdir");
    let database =
        Arc::new(MvccDatabase::open(directory.path(), MvccOptions::default()).expect("open"));

    let mut histories = Vec::new();
    thread::scope(|scope| {
        let mut handles = Vec::new();
        for worker in 0..WORKERS {
            let database = Arc::clone(&database);
            let worker_seed = seed
                .wrapping_mul(0x9e37_79b9_7f4a_7c15)
                .wrapping_add(worker as u64 + 1);
            handles.push(scope.spawn(move || worker_history(&database, worker_seed)));
        }
        for handle in handles {
            histories.extend(handle.join().expect("worker thread"));
        }
    });

    let expected_state = check_history(&histories);
    assert_database_matches(&database, &expected_state, "live database");

    let stats = database.stats().expect("stats");
    assert_eq!(stats.active_transactions, 0, "seed {seed}: {stats:?}");
    assert_eq!(stats.write_intents, 0, "seed {seed}: {stats:?}");

    Arc::try_unwrap(database)
        .map_err(|_| "database still shared")
        .expect("sole owner")
        .shutdown()
        .expect("shutdown");

    let reopened = MvccDatabase::open(directory.path(), MvccOptions::default()).expect("reopen");
    assert_database_matches(&reopened, &expected_state, "reopened database");
    reopened.shutdown().expect("shutdown reopened");
}

#[test]
fn concurrent_histories_satisfy_snapshot_isolation_seed_a() {
    run_seeded(0x5157_ab1e);
}

#[test]
fn concurrent_histories_satisfy_snapshot_isolation_seed_b() {
    run_seeded(0xdead_f00d);
}
