mod common;

#[cfg(feature = "e2e-tests")]
use cassandra_protocol::frame::Version;
#[cfg(feature = "e2e-tests")]
use cdrs_tokio::query::PreparedQuery;
#[cfg(feature = "e2e-tests")]
use cdrs_tokio::query_values;
#[cfg(feature = "e2e-tests")]
use cdrs_tokio::types::prelude::Row;
#[cfg(feature = "e2e-tests")]
use cdrs_tokio::types::IntoRustByName;
#[cfg(feature = "e2e-tests")]
use common::*;

#[cfg(feature = "e2e-tests")]
async fn select_partition(session: &CurrentSession, select: &PreparedQuery) -> Vec<Row> {
    let mut rows = session
        .exec_with_values(select, query_values!(1))
        .await
        .expect("execute select")
        .response_body()
        .expect("body")
        .into_rows()
        .expect("rows");

    rows.sort_by_key(|row| {
        let ck: i32 = row.get_r_by_name("ck").expect("ck");
        ck
    });
    rows
}

#[cfg(feature = "e2e-tests")]
fn assert_altered_rows(rows: &[Row]) {
    assert_eq!(rows.len(), 2);
    assert!(rows.iter().all(|row| row.contains_column("new_col")));

    let first_value: String = rows[0].get_r_by_name("value").expect("value");
    let first_new_col: Option<i32> = rows[0].get_by_name("new_col").expect("new_col");
    assert_eq!(first_value, "a");
    assert_eq!(first_new_col, None);

    let second_value: String = rows[1].get_r_by_name("value").expect("value");
    let second_new_col: Option<i32> = rows[1].get_by_name("new_col").expect("new_col");
    assert_eq!(second_value, "b");
    assert_eq!(second_new_col, Some(42));
}

#[cfg(feature = "e2e-tests")]
async fn prepared_after_schema_change_test(session: CurrentSession, table: &str) {
    session
        .query(format!(
            "INSERT INTO {table} (pk, ck, value) VALUES (1, 1, 'a')"
        ))
        .await
        .expect("insert before alter");

    let select = session
        .prepare(format!("SELECT * FROM {table} WHERE pk = ?"))
        .await
        .expect("prepare");

    let rows = select_partition(&session, &select).await;
    assert_eq!(rows.len(), 1);
    assert!(!rows[0].contains_column("new_col"));
    let value: String = rows[0].get_r_by_name("value").expect("value");
    assert_eq!(value, "a");

    session
        .query(format!("ALTER TABLE {table} ADD new_col int"))
        .await
        .expect("alter table");
    session
        .query(format!(
            "INSERT INTO {table} (pk, ck, value, new_col) VALUES (1, 2, 'b', 42)"
        ))
        .await
        .expect("insert after alter");

    let rows = select_partition(&session, &select).await;
    assert_altered_rows(&rows);

    // the statement keeps working once the driver has caught up with the new result metadata
    let rows = select_partition(&session, &select).await;
    assert_altered_rows(&rows);
}

#[tokio::test]
#[cfg(feature = "e2e-tests")]
async fn prepared_after_schema_change_v4() {
    let session = setup_multiple(
        &[
            "DROP TABLE IF EXISTS cdrs_test.test_prepared_alter_v4",
            "CREATE TABLE cdrs_test.test_prepared_alter_v4 \
             (pk int, ck int, value text, PRIMARY KEY (pk, ck))",
        ],
        Version::V4,
    )
    .await
    .expect("setup");

    prepared_after_schema_change_test(session, "cdrs_test.test_prepared_alter_v4").await;
}

#[tokio::test]
#[cfg(feature = "e2e-tests")]
async fn prepared_after_schema_change_v5() {
    let session = setup_multiple(
        &[
            "DROP TABLE IF EXISTS cdrs_test.test_prepared_alter_v5",
            "CREATE TABLE cdrs_test.test_prepared_alter_v5 \
             (pk int, ck int, value text, PRIMARY KEY (pk, ck))",
        ],
        Version::V5,
    )
    .await
    .expect("setup");

    prepared_after_schema_change_test(session, "cdrs_test.test_prepared_alter_v5").await;
}
