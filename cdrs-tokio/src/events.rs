use std::iter::Iterator;
use std::sync::mpsc::{Receiver as StdReceiver, Sender as StdSender};
use tokio::sync::mpsc::Receiver;

use crate::error;
use crate::frame::events::{
    SchemaChange as FrameSchemaChange, ServerEvent as FrameServerEvent,
    SimpleServerEvent as FrameSimpleServerEvent,
};
use crate::frame::Frame;

/// Full Server Event which includes all details about occured change.
pub type ServerEvent = FrameServerEvent;

/// Simplified Server event. It should be used to represent an event
/// which consumer wants listen to.
pub type SimpleServerEvent = FrameSimpleServerEvent;

/// Reexport of `FrameSchemaChange`.
pub type SchemaChange = FrameSchemaChange;

/// Factory function which returns a `Listener` and related `EventStream.`
///
/// `Listener` provides only one function `start` to start listening. It
/// blocks a thread so should be moved into a separate one to no release
/// main thread.
///
/// `EventStream` is an iterator which returns new events once they come.
/// It is similar to `Receiver::iter`.
pub fn new_listener(tx: StdSender<ServerEvent>, rx: Receiver<Frame>) -> Listener {
    Listener { tx, rx }
}

/// `Listener` provides only one function `start` to start listening. It
/// blocks a thread so should be moved into a separate one to no release
/// main thread.

pub struct Listener {
    tx: StdSender<ServerEvent>,
    rx: Receiver<Frame>,
}

impl Listener {
    /// It starts a process of listening to new events.
    pub async fn start(mut self) -> error::Result<()> {
        loop {
            let event = self.rx.recv().await;
            match event {
                Some(event) => {
                    let event = event.body()?.into_server_event();

                    if let Some(event) = event {
                        if let Err(error) = self.tx.send(event.event) {
                            return Err(error::Error::General(error.to_string()));
                        }
                    }
                }
                None => break,
            }
        }

        Ok(())
    }
}

/// `EventStream` is an iterator which returns new events once they come.
/// It is similar to `Receiver::iter`.
pub struct EventStream {
    rx: StdReceiver<ServerEvent>,
}

impl EventStream {
    pub fn new(rx: StdReceiver<ServerEvent>) -> Self {
        EventStream { rx }
    }
}

impl Iterator for EventStream {
    type Item = ServerEvent;

    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        self.rx.recv().ok()
    }
}

impl From<EventStream> for EventStreamNonBlocking {
    #[inline]
    fn from(stream: EventStream) -> Self {
        Self { rx: stream.rx }
    }
}

/// `EventStreamNonBlocking` is an iterator which returns new events once they come.
/// It is similar to `Receiver::iter`. It's a non-blocking version of `EventStream`
#[derive(Debug)]
pub struct EventStreamNonBlocking {
    rx: StdReceiver<ServerEvent>,
}

impl Iterator for EventStreamNonBlocking {
    type Item = ServerEvent;

    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        self.rx.try_recv().ok()
    }
}
