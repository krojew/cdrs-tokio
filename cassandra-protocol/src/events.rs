use crate::frame::events::{
    SchemaChange as MessageSchemaChange, ServerEvent as MessageServerEvent,
    SimpleServerEvent as MessageSimpleServerEvent,
};

/// Full Server Event which includes all details about occurred change.
pub type ServerEvent = MessageServerEvent;

/// Simplified Server event. It should be used to represent an event
/// which consumer wants listen to.
pub type SimpleServerEvent = MessageSimpleServerEvent;

/// Reexport of `MessageSchemaChange`.
pub type SchemaChange = MessageSchemaChange;
