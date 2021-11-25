use std::sync::Arc;

use arc_swap::ArcSwapOption;
use tokio::sync::watch::Sender;

/// Holds currently set global keyspace.
#[derive(Debug)]
pub struct KeyspaceHolder {
    current_keyspace: ArcSwapOption<String>,
    keyspace_sender: Sender<Option<String>>,
}

impl KeyspaceHolder {
    pub fn new(keyspace_sender: Sender<Option<String>>) -> Self {
        KeyspaceHolder {
            current_keyspace: Default::default(),
            keyspace_sender,
        }
    }

    #[inline]
    pub fn current_keyspace(&self) -> Option<Arc<String>> {
        self.current_keyspace.load().clone()
    }

    #[inline]
    pub fn update_current_keyspace(&self, keyspace: String) {
        self.update_current_keyspace_without_notification(keyspace.clone());
        let _ = self.keyspace_sender.send(Some(keyspace));
    }

    #[inline]
    pub fn update_current_keyspace_without_notification(&self, keyspace: String) {
        self.current_keyspace.store(Some(Arc::new(keyspace)));
    }
}
