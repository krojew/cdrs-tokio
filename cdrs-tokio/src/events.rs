use crate::frame::events::{
    SchemaChange as FrameSchemaChange, ServerEvent as FrameServerEvent,
    SimpleServerEvent as FrameSimpleServerEvent,
};

/// Full Server Event which includes all details about occured change.
pub type ServerEvent = FrameServerEvent;

/// Simplified Server event. It should be used to represent an event
/// which consumer wants listen to.
pub type SimpleServerEvent = FrameSimpleServerEvent;

/// Reexport of `FrameSchemaChange`.
pub type SchemaChange = FrameSchemaChange;
