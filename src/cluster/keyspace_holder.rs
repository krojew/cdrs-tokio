use tokio::sync::Mutex;

/// Cassandra tracks current keyspace per connection, so we need to remember it across all
/// connections in the pool.
#[derive(Default, Debug)]
pub struct KeyspaceHolder {
    current_keyspace: Mutex<Option<String>>
}

impl KeyspaceHolder {
    pub async fn current_keyspace(&self) -> Option<String> {
        self.current_keyspace.lock().await.clone()
    }

    pub async fn set_current_keyspace(&self, keyspace: &str) {
        *self.current_keyspace.lock().await = Some(keyspace.into());
    }
}
