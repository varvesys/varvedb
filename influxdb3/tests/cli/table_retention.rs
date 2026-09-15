//! Mirrors `db_retention.rs` one-for-one for table-level retention
//! (`docs/design/table-level-retention.md`, all three phases: `system.tables` /
//! `show retention` read-back, `update table`, and `create table --retention-period`).
//!
//! `test_create_table_with_retention_period` covers the phase-3 flag directly;
//! the rest of the suite drives retention via `update table` (set, unset — raw
//! column empty, invalid input, update-to-a-new-value, clear — falls back to
//! the database's own retention, unlike the database suite which has no
//! further fallback — and update-after-delete), since that path already
//! exercises the same underlying catalog calls regardless of which command
//! reaches them.

use crate::server::{ConfigProvider, TestServer};
use serde_json::Value;
use test_helpers::assert_contains;

#[test_log::test(tokio::test)]
async fn test_create_table_with_retention_period() {
    let server = TestServer::configure().with_no_admin_token().spawn().await;
    let args = &["--tls-ca", "../testing-certs/rootCA.pem"];
    let db_name = "test_table_db_create";
    let table_name = "cpu";

    server
        .run(vec!["create", "database", db_name], args)
        .expect("create database should succeed");

    // Set the table's retention period at creation time (phase 3), in one command.
    let result = server
        .run(
            vec![
                "create",
                "table",
                "--database",
                db_name,
                table_name,
                "--tags",
                "room",
                "--fields",
                "temp:float64",
                "--retention-period",
                "30d",
            ],
            args,
        )
        .expect("create table with retention period should succeed");

    assert_contains!(
        &result,
        format!("Table \"{db_name}\".\"{table_name}\" created successfully")
    );

    let json_args = &[
        "--tls-ca",
        "../testing-certs/rootCA.pem",
        "--format",
        "json",
    ];

    let result = server
        .run(
            vec![
                "query",
                "-d",
                "_internal",
                &format!(
                    "SELECT retention_period_ns FROM system.tables WHERE table_name='{table_name}' AND database_name='{db_name}'"
                ),
            ],
            json_args,
        )
        .expect("query should succeed");

    assert_eq!(&result, "[{\"retention_period_ns\":2592000000000000}]");
}

#[test_log::test(tokio::test)]
async fn test_update_table_with_retention_period() {
    let server = TestServer::configure().with_no_admin_token().spawn().await;
    let args = &["--tls-ca", "../testing-certs/rootCA.pem"];
    let db_name = "test_table_db";
    let table_name = "cpu";

    server
        .run(vec!["create", "database", db_name], args)
        .expect("create database should succeed");
    server
        .run(
            vec![
                "create", "table", "--database", db_name, table_name, "--tags", "room",
                "--fields", "temp:float64",
            ],
            args,
        )
        .expect("create table should succeed");

    // Set the table's own retention period
    let result = server
        .run(
            vec![
                "update",
                "table",
                "--database",
                db_name,
                table_name,
                "--retention-period",
                "30d",
            ],
            args,
        )
        .expect("update table retention period should succeed");

    assert_contains!(
        &result,
        format!("Table \"{db_name}\".\"{table_name}\" updated successfully")
    );

    let args = &[
        "--tls-ca",
        "../testing-certs/rootCA.pem",
        "--format",
        "json",
    ];

    let result = server
        .run(
            vec![
                "query",
                "-d",
                "_internal",
                &format!(
                    "SELECT retention_period_ns FROM system.tables WHERE table_name='{table_name}' AND database_name='{db_name}'"
                ),
            ],
            args,
        )
        .expect("query should succeed");

    assert_eq!(&result, "[{\"retention_period_ns\":2592000000000000}]");
}

#[test_log::test(tokio::test)]
async fn test_table_without_retention_period() {
    let server = TestServer::configure().with_no_admin_token().spawn().await;
    let args = &["--tls-ca", "../testing-certs/rootCA.pem"];
    let db_name = "test_table_db2";
    let table_name = "cpu";

    server
        .run(vec!["create", "database", db_name], args)
        .expect("create database should succeed");
    server
        .run(
            vec![
                "create", "table", "--database", db_name, table_name, "--tags", "room",
                "--fields", "temp:float64",
            ],
            args,
        )
        .expect("create table should succeed");

    // No `update table` call — the table carries no retention override of its own.
    let args = &[
        "--tls-ca",
        "../testing-certs/rootCA.pem",
        "--format",
        "json",
    ];

    let result = server
        .run(
            vec![
                "query",
                "-d",
                "_internal",
                &format!(
                    "SELECT retention_period_ns FROM system.tables WHERE table_name='{table_name}' AND database_name='{db_name}'"
                ),
            ],
            args,
        )
        .expect("query should succeed");

    assert_eq!(&result, "[{}]");
}

#[test_log::test(tokio::test)]
async fn test_update_table_with_invalid_retention_period() {
    let server = TestServer::configure().with_no_admin_token().spawn().await;
    let args = &["--tls-ca", "../testing-certs/rootCA.pem"];
    let db_name = "test_table_db3";
    let table_name = "cpu";

    server
        .run(vec!["create", "database", db_name], args)
        .expect("create database should succeed");
    server
        .run(
            vec![
                "create", "table", "--database", db_name, table_name, "--tags", "room",
                "--fields", "temp:float64",
            ],
            args,
        )
        .expect("create table should succeed");

    let result = server.run(
        vec![
            "update",
            "table",
            "--database",
            db_name,
            table_name,
            "--retention-period",
            "invalid",
        ],
        args,
    );

    assert!(
        result.is_err(),
        "updating table with invalid retention period should fail"
    );
}

#[test_log::test(tokio::test)]
async fn test_update_table_retention_period() {
    let server = TestServer::configure().with_no_admin_token().spawn().await;
    let args = &["--tls-ca", "../testing-certs/rootCA.pem"];
    let db_name = "test_table_db_update";
    let table_name = "cpu";

    server
        .run(vec!["create", "database", db_name], args)
        .expect("create database should succeed");
    server
        .run(
            vec![
                "create", "table", "--database", db_name, table_name, "--tags", "room",
                "--fields", "temp:float64",
            ],
            args,
        )
        .expect("create table should succeed");

    server
        .run(
            vec![
                "update",
                "table",
                "--database",
                db_name,
                table_name,
                "--retention-period",
                "30d",
            ],
            args,
        )
        .expect("update table retention period should succeed");

    // Update it again to a different value
    let result = server
        .run(
            vec![
                "update",
                "table",
                "--database",
                db_name,
                table_name,
                "--retention-period",
                "60d",
            ],
            args,
        )
        .expect("update table retention period should succeed");

    assert_contains!(
        &result,
        format!("Table \"{db_name}\".\"{table_name}\" updated successfully")
    );

    let args = &[
        "--tls-ca",
        "../testing-certs/rootCA.pem",
        "--format",
        "json",
    ];

    let result = server
        .run(
            vec![
                "query",
                "-d",
                "_internal",
                &format!(
                    "SELECT retention_period_ns FROM system.tables WHERE table_name='{table_name}' AND database_name='{db_name}'"
                ),
            ],
            args,
        )
        .expect("query should succeed");

    assert_eq!(&result, "[{\"retention_period_ns\":5184000000000000}]"); // 60 days in nanoseconds
}

#[test_log::test(tokio::test)]
async fn test_clear_table_retention_period() {
    let server = TestServer::configure().with_no_admin_token().spawn().await;
    let args = &["--tls-ca", "../testing-certs/rootCA.pem"];
    // Give the database its own retention so clearing the table's override has
    // somewhere meaningful to fall back to — this is the one place table-level
    // retention has richer behaviour than the database suite it mirrors.
    let db_name = "test_table_db_clear";
    let table_name = "cpu";

    server
        .run(
            vec!["create", "database", db_name, "--retention-period", "30d"],
            args,
        )
        .expect("create database should succeed");
    server
        .run(
            vec![
                "create", "table", "--database", db_name, table_name, "--tags", "room",
                "--fields", "temp:float64",
            ],
            args,
        )
        .expect("create table should succeed");

    server
        .run(
            vec![
                "update",
                "table",
                "--database",
                db_name,
                table_name,
                "--retention-period",
                "7d",
            ],
            args,
        )
        .expect("update table retention period should succeed");

    // Clear the table's own retention period (set to none)
    let result = server
        .run(
            vec![
                "update",
                "table",
                "--database",
                db_name,
                table_name,
                "--retention-period",
                "none",
            ],
            args,
        )
        .expect("clear table retention period should succeed");

    assert_contains!(
        &result,
        format!("Table \"{db_name}\".\"{table_name}\" updated successfully")
    );

    let json_args = &[
        "--tls-ca",
        "../testing-certs/rootCA.pem",
        "--format",
        "json",
    ];

    // The table's own column is empty again (cleared, not just reset to a value)...
    let result = server
        .run(
            vec![
                "query",
                "-d",
                "_internal",
                &format!(
                    "SELECT retention_period_ns FROM system.tables WHERE table_name='{table_name}' AND database_name='{db_name}'"
                ),
            ],
            json_args,
        )
        .expect("query should succeed");
    assert_eq!(&result, "[{}]");

    // ...but the *effective* retention (what the retention handler actually
    // enforces) falls back to the database's 30d, not to "infinite".
    let result = server
        .run(
            vec!["show", "retention", "--database", db_name],
            json_args,
        )
        .expect("show retention should succeed");
    assert_contains!(&result, "\"retention_period\":\"30d\"");
}

#[test_log::test(tokio::test)]
async fn test_update_table_retention_after_delete() {
    // Updating the retention period of a soft-deleted table should fail, the
    // same way updating a soft-deleted database's retention does.

    let server = TestServer::configure().with_no_admin_token().spawn().await;
    let args = &["--tls-ca", "../testing-certs/rootCA.pem"];
    let db_name = "test_table_db_deleted";
    let table_name = "cpu";

    server
        .run(vec!["create", "database", db_name], args)
        .expect("create database should succeed");
    server
        .run(
            vec![
                "create", "table", "--database", db_name, table_name, "--tags", "room",
                "--fields", "temp:float64",
            ],
            args,
        )
        .expect("create table should succeed");

    // Delete the table
    let delete_output = server
        .delete_table(db_name, table_name)
        .run()
        .expect("delete table should succeed");
    assert_contains!(
        &delete_output,
        format!("Table \"{db_name}\".\"{table_name}\" deleted successfully")
    );

    // A soft delete renames the table in place, just like a soft-deleted
    // database — find that renamed name via system.tables before trying to
    // update it.
    let json_args = &[
        "--tls-ca",
        "../testing-certs/rootCA.pem",
        "--format",
        "json",
    ];
    let show_output = server
        .run(
            vec![
                "query",
                "-d",
                "_internal",
                &format!(
                    "SELECT table_name, deleted FROM system.tables WHERE database_name='{db_name}'"
                ),
            ],
            json_args,
        )
        .expect("query should succeed");
    let tables: Vec<Value> =
        serde_json::from_str(&show_output).expect("system.tables output should be valid json");
    let deleted_table_name = tables
        .into_iter()
        .find_map(|entry| {
            let deleted = entry.get("deleted").and_then(Value::as_bool).unwrap_or(false);
            let name = entry.get("table_name").and_then(Value::as_str);
            match (deleted, name) {
                (true, Some(name)) if name.starts_with(table_name) => Some(name.to_string()),
                _ => None,
            }
        })
        .expect("deleted table name should be visible via system.tables");

    let err = server
        .run(
            vec![
                "update",
                "table",
                "--database",
                db_name,
                &deleted_table_name,
                "--retention-period",
                "3h",
            ],
            args,
        )
        .expect_err("updating retention on a deleted table should fail");
    assert_contains!(
        &err.to_string(),
        "Update command failed: server responded with error [409 Conflict]: attempted to modify resource that was already deleted: "
    );
}
