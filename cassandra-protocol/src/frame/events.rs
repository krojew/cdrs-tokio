use derive_more::Display;
use std::cmp::PartialEq;
use std::io::Cursor;

use crate::error;
use crate::frame::traits::FromCursor;
use crate::frame::Serialize;
use crate::types::{serialize_str, CInet, CIntShort, CString, CStringList};

// Event types
const TOPOLOGY_CHANGE: &str = "TOPOLOGY_CHANGE";
const STATUS_CHANGE: &str = "STATUS_CHANGE";
const SCHEMA_CHANGE: &str = "SCHEMA_CHANGE";

// Topology changes
const NEW_NODE: &str = "NEW_NODE";
const REMOVED_NODE: &str = "REMOVED_NODE";

// Status changes
const UP: &str = "UP";
const DOWN: &str = "DOWN";

// Schema changes
const CREATED: &str = "CREATED";
const UPDATED: &str = "UPDATED";
const DROPPED: &str = "DROPPED";

// Schema change targets
const KEYSPACE: &str = "KEYSPACE";
const TABLE: &str = "TABLE";
const TYPE: &str = "TYPE";
const FUNCTION: &str = "FUNCTION";
const AGGREGATE: &str = "AGGREGATE";

/// Simplified `ServerEvent` that does not contain details
/// about a concrete change. It may be useful for subscription
/// when you need only string representation of an event.
#[derive(Debug, PartialEq, Copy, Clone, Ord, PartialOrd, Eq, Hash)]
pub enum SimpleServerEvent {
    TopologyChange,
    StatusChange,
    SchemaChange,
}

impl SimpleServerEvent {
    pub fn as_string(&self) -> String {
        match *self {
            SimpleServerEvent::TopologyChange => String::from(TOPOLOGY_CHANGE),
            SimpleServerEvent::StatusChange => String::from(STATUS_CHANGE),
            SimpleServerEvent::SchemaChange => String::from(SCHEMA_CHANGE),
        }
    }
}

impl ToString for SimpleServerEvent {
    fn to_string(&self) -> String {
        self.as_string()
    }
}

impl From<ServerEvent> for SimpleServerEvent {
    fn from(event: ServerEvent) -> SimpleServerEvent {
        match event {
            ServerEvent::TopologyChange(_) => SimpleServerEvent::TopologyChange,
            ServerEvent::StatusChange(_) => SimpleServerEvent::StatusChange,
            ServerEvent::SchemaChange(_) => SimpleServerEvent::SchemaChange,
        }
    }
}

impl From<SimpleServerEvent> for String {
    fn from(event: SimpleServerEvent) -> Self {
        event.to_string()
    }
}

impl<'a> From<&'a ServerEvent> for SimpleServerEvent {
    fn from(event: &'a ServerEvent) -> SimpleServerEvent {
        match *event {
            ServerEvent::TopologyChange(_) => SimpleServerEvent::TopologyChange,
            ServerEvent::StatusChange(_) => SimpleServerEvent::StatusChange,
            ServerEvent::SchemaChange(_) => SimpleServerEvent::SchemaChange,
        }
    }
}

impl PartialEq<ServerEvent> for SimpleServerEvent {
    fn eq(&self, full_event: &ServerEvent) -> bool {
        self == &SimpleServerEvent::from(full_event)
    }
}

/// Full server event that contains all details about a concrete change.
#[derive(Debug, Clone, Ord, PartialOrd, Eq, PartialEq, Hash)]
pub enum ServerEvent {
    /// Events related to change in the cluster topology
    TopologyChange(TopologyChange),
    /// Events related to change of node status.
    StatusChange(StatusChange),
    /// Events related to schema change.
    SchemaChange(SchemaChange),
}

impl Serialize for ServerEvent {
    fn serialize(&self, cursor: &mut Cursor<&mut Vec<u8>>) {
        match &self {
            ServerEvent::TopologyChange(t) => {
                serialize_str(cursor, TOPOLOGY_CHANGE);
                t.serialize(cursor);
            }
            ServerEvent::StatusChange(s) => {
                serialize_str(cursor, STATUS_CHANGE);
                s.serialize(cursor);
            }
            ServerEvent::SchemaChange(s) => {
                serialize_str(cursor, SCHEMA_CHANGE);
                s.serialize(cursor);
            }
        }
    }
}

impl PartialEq<SimpleServerEvent> for ServerEvent {
    fn eq(&self, event: &SimpleServerEvent) -> bool {
        &SimpleServerEvent::from(self) == event
    }
}

impl FromCursor for ServerEvent {
    fn from_cursor(cursor: &mut Cursor<&[u8]>) -> error::Result<ServerEvent> {
        let event_type = CString::from_cursor(cursor)?;
        let event_type = event_type.as_str();
        match event_type {
            TOPOLOGY_CHANGE => Ok(ServerEvent::TopologyChange(TopologyChange::from_cursor(
                cursor,
            )?)),
            STATUS_CHANGE => Ok(ServerEvent::StatusChange(StatusChange::from_cursor(
                cursor,
            )?)),
            SCHEMA_CHANGE => Ok(ServerEvent::SchemaChange(SchemaChange::from_cursor(
                cursor,
            )?)),
            _ => Err(format!("Unexpected server event: {}", event_type).into()),
        }
    }
}

/// Events related to change in the cluster topology
#[derive(Debug, Copy, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct TopologyChange {
    pub change_type: TopologyChangeType,
    pub addr: CInet,
}

impl Serialize for TopologyChange {
    fn serialize(&self, cursor: &mut Cursor<&mut Vec<u8>>) {
        self.change_type.serialize(cursor);
        self.addr.serialize(cursor);
    }
}

impl FromCursor for TopologyChange {
    fn from_cursor(cursor: &mut Cursor<&[u8]>) -> error::Result<TopologyChange> {
        let change_type = TopologyChangeType::from_cursor(cursor)?;
        let addr = CInet::from_cursor(cursor)?;

        Ok(TopologyChange { change_type, addr })
    }
}

#[derive(Debug, Copy, Clone, PartialEq, Ord, PartialOrd, Eq, Hash, Display)]
pub enum TopologyChangeType {
    NewNode,
    RemovedNode,
}

impl Serialize for TopologyChangeType {
    fn serialize(&self, cursor: &mut Cursor<&mut Vec<u8>>) {
        match &self {
            TopologyChangeType::NewNode => serialize_str(cursor, NEW_NODE),
            TopologyChangeType::RemovedNode => serialize_str(cursor, REMOVED_NODE),
        }
    }
}

impl FromCursor for TopologyChangeType {
    fn from_cursor(cursor: &mut Cursor<&[u8]>) -> error::Result<TopologyChangeType> {
        CString::from_cursor(cursor).and_then(|tc| {
            let tc = tc.as_str();
            match tc {
                NEW_NODE => Ok(TopologyChangeType::NewNode),
                REMOVED_NODE => Ok(TopologyChangeType::RemovedNode),
                _ => Err(format!(
                    "Unexpected topology change type received from Cluster: {}",
                    tc
                )
                .into()),
            }
        })
    }
}

/// Events related to change of node status.
#[derive(Debug, Copy, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct StatusChange {
    pub change_type: StatusChangeType,
    pub addr: CInet,
}

impl Serialize for StatusChange {
    fn serialize(&self, cursor: &mut Cursor<&mut Vec<u8>>) {
        self.change_type.serialize(cursor);
        self.addr.serialize(cursor);
    }
}

impl FromCursor for StatusChange {
    fn from_cursor(cursor: &mut Cursor<&[u8]>) -> error::Result<StatusChange> {
        let change_type = StatusChangeType::from_cursor(cursor)?;
        let addr = CInet::from_cursor(cursor)?;

        Ok(StatusChange { change_type, addr })
    }
}

#[derive(Debug, Copy, Clone, PartialEq, Eq, Hash, PartialOrd, Ord, Display)]
pub enum StatusChangeType {
    Up,
    Down,
}

impl Serialize for StatusChangeType {
    fn serialize(&self, cursor: &mut Cursor<&mut Vec<u8>>) {
        match self {
            StatusChangeType::Up => serialize_str(cursor, UP),
            StatusChangeType::Down => serialize_str(cursor, DOWN),
        }
    }
}

impl FromCursor for StatusChangeType {
    fn from_cursor(cursor: &mut Cursor<&[u8]>) -> error::Result<StatusChangeType> {
        CString::from_cursor(cursor).and_then(|sct| {
            let sct = sct.as_str();
            match sct {
                UP => Ok(StatusChangeType::Up),
                DOWN => Ok(StatusChangeType::Down),
                _ => Err(format!("Unexpected status change type: {}", sct).into()),
            }
        })
    }
}

/// Events related to schema change.
#[derive(Debug, Clone, Ord, PartialOrd, Eq, PartialEq, Hash)]
pub struct SchemaChange {
    pub change_type: SchemaChangeType,
    pub target: SchemaChangeTarget,
    pub options: SchemaChangeOptions,
}

impl Serialize for SchemaChange {
    fn serialize(&self, cursor: &mut Cursor<&mut Vec<u8>>) {
        self.change_type.serialize(cursor);
        self.target.serialize(cursor);
        self.options.serialize(cursor);
    }
}

impl FromCursor for SchemaChange {
    fn from_cursor(cursor: &mut Cursor<&[u8]>) -> error::Result<SchemaChange> {
        let change_type = SchemaChangeType::from_cursor(cursor)?;
        let target = SchemaChangeTarget::from_cursor(cursor)?;
        let options = SchemaChangeOptions::from_cursor_and_target(cursor, &target)?;

        Ok(SchemaChange {
            change_type,
            target,
            options,
        })
    }
}

/// Represents type of changes.
#[derive(Debug, Copy, Clone, PartialEq, Eq, Hash, PartialOrd, Ord, Display)]
pub enum SchemaChangeType {
    Created,
    Updated,
    Dropped,
}

impl Serialize for SchemaChangeType {
    fn serialize(&self, cursor: &mut Cursor<&mut Vec<u8>>) {
        match self {
            SchemaChangeType::Created => serialize_str(cursor, CREATED),
            SchemaChangeType::Updated => serialize_str(cursor, UPDATED),
            SchemaChangeType::Dropped => serialize_str(cursor, DROPPED),
        }
    }
}

impl FromCursor for SchemaChangeType {
    fn from_cursor(cursor: &mut Cursor<&[u8]>) -> error::Result<SchemaChangeType> {
        CString::from_cursor(cursor).and_then(|ct| {
            let ct = ct.as_str();
            match ct {
                CREATED => Ok(SchemaChangeType::Created),
                UPDATED => Ok(SchemaChangeType::Updated),
                DROPPED => Ok(SchemaChangeType::Dropped),
                _ => Err(format!("Unexpected schema change type: {}", ct).into()),
            }
        })
    }
}

/// Refers to a target of changes were made.
#[derive(Debug, Copy, Clone, PartialEq, Eq, Hash, PartialOrd, Ord, Display)]
pub enum SchemaChangeTarget {
    Keyspace,
    Table,
    Type,
    Function,
    Aggregate,
}

impl Serialize for SchemaChangeTarget {
    fn serialize(&self, cursor: &mut Cursor<&mut Vec<u8>>) {
        match self {
            SchemaChangeTarget::Keyspace => serialize_str(cursor, KEYSPACE),
            SchemaChangeTarget::Table => serialize_str(cursor, TABLE),
            SchemaChangeTarget::Type => serialize_str(cursor, TYPE),
            SchemaChangeTarget::Function => serialize_str(cursor, FUNCTION),
            SchemaChangeTarget::Aggregate => serialize_str(cursor, AGGREGATE),
        }
    }
}

impl FromCursor for SchemaChangeTarget {
    fn from_cursor(cursor: &mut Cursor<&[u8]>) -> error::Result<SchemaChangeTarget> {
        CString::from_cursor(cursor).and_then(|t| {
            let t = t.as_str();
            match t {
                KEYSPACE => Ok(SchemaChangeTarget::Keyspace),
                TABLE => Ok(SchemaChangeTarget::Table),
                TYPE => Ok(SchemaChangeTarget::Type),
                FUNCTION => Ok(SchemaChangeTarget::Function),
                AGGREGATE => Ok(SchemaChangeTarget::Aggregate),
                _ => Err(format!("Unexpected schema change target: {}", t).into()),
            }
        })
    }
}

/// Information about changes made.
#[derive(Debug, Clone, Ord, PartialOrd, Eq, PartialEq, Hash)]
pub enum SchemaChangeOptions {
    /// Changes related to keyspaces. Contains keyspace name.
    Keyspace(String),
    /// Changes related to tables. Contains keyspace and table names.
    TableType(String, String),
    /// Changes related to functions and aggregations. Contains:
    /// * keyspace containing the user defined function/aggregate
    /// * the function/aggregate name
    /// * list of strings, one string for each argument type (as CQL type)
    FunctionAggregate(String, String, Vec<String>),
}

impl Serialize for SchemaChangeOptions {
    fn serialize(&self, cursor: &mut Cursor<&mut Vec<u8>>) {
        match self {
            SchemaChangeOptions::Keyspace(ks) => {
                serialize_str(cursor, ks);
            }
            SchemaChangeOptions::TableType(ks, t) => {
                serialize_str(cursor, ks);
                serialize_str(cursor, t);
            }
            SchemaChangeOptions::FunctionAggregate(ks, fa_name, list) => {
                serialize_str(cursor, ks);
                serialize_str(cursor, fa_name);

                let len = list.len() as CIntShort;
                len.serialize(cursor);
                list.iter().for_each(|x| serialize_str(cursor, x));
            }
        }
    }
}

impl SchemaChangeOptions {
    fn from_cursor_and_target(
        cursor: &mut Cursor<&[u8]>,
        target: &SchemaChangeTarget,
    ) -> error::Result<SchemaChangeOptions> {
        Ok(match *target {
            SchemaChangeTarget::Keyspace => SchemaChangeOptions::from_cursor_keyspace(cursor)?,
            SchemaChangeTarget::Table | SchemaChangeTarget::Type => {
                SchemaChangeOptions::from_cursor_table_type(cursor)?
            }
            SchemaChangeTarget::Function | SchemaChangeTarget::Aggregate => {
                SchemaChangeOptions::from_cursor_function_aggregate(cursor)?
            }
        })
    }

    fn from_cursor_keyspace(cursor: &mut Cursor<&[u8]>) -> error::Result<SchemaChangeOptions> {
        Ok(SchemaChangeOptions::Keyspace(
            CString::from_cursor(cursor)?.into_plain(),
        ))
    }

    fn from_cursor_table_type(cursor: &mut Cursor<&[u8]>) -> error::Result<SchemaChangeOptions> {
        let keyspace = CString::from_cursor(cursor)?.into_plain();
        let name = CString::from_cursor(cursor)?.into_plain();
        Ok(SchemaChangeOptions::TableType(keyspace, name))
    }

    fn from_cursor_function_aggregate(
        cursor: &mut Cursor<&[u8]>,
    ) -> error::Result<SchemaChangeOptions> {
        let keyspace = CString::from_cursor(cursor)?.into_plain();
        let name = CString::from_cursor(cursor)?.into_plain();
        let types = CStringList::from_cursor(cursor)?.into_plain();
        Ok(SchemaChangeOptions::FunctionAggregate(
            keyspace, name, types,
        ))
    }
}

#[cfg(test)]
fn test_encode_decode(bytes: &[u8], expected: ServerEvent) {
    let mut ks: Cursor<&[u8]> = Cursor::new(bytes);
    let event = ServerEvent::from_cursor(&mut ks).unwrap();
    assert_eq!(expected, event);

    let mut buffer = Vec::new();
    let mut cursor = Cursor::new(&mut buffer);
    expected.serialize(&mut cursor);
    assert_eq!(buffer, bytes);
}

#[cfg(test)]
mod simple_server_event_test {
    use super::*;

    #[test]
    fn as_string() {
        assert_eq!(
            SimpleServerEvent::TopologyChange.as_string(),
            "TOPOLOGY_CHANGE".to_string()
        );
        assert_eq!(
            SimpleServerEvent::StatusChange.as_string(),
            "STATUS_CHANGE".to_string()
        );
        assert_eq!(
            SimpleServerEvent::SchemaChange.as_string(),
            "SCHEMA_CHANGE".to_string()
        );
    }
}

#[cfg(test)]
mod topology_change_type_test {
    use super::*;
    use crate::frame::traits::FromCursor;
    use std::io::Cursor;

    #[test]
    fn from_cursor() {
        let a = &[0, 8, 78, 69, 87, 95, 78, 79, 68, 69];
        let mut new_node: Cursor<&[u8]> = Cursor::new(a);
        assert_eq!(
            TopologyChangeType::from_cursor(&mut new_node).unwrap(),
            TopologyChangeType::NewNode
        );

        let b = &[0, 12, 82, 69, 77, 79, 86, 69, 68, 95, 78, 79, 68, 69];
        let mut removed_node: Cursor<&[u8]> = Cursor::new(b);
        assert_eq!(
            TopologyChangeType::from_cursor(&mut removed_node).unwrap(),
            TopologyChangeType::RemovedNode
        );
    }

    #[test]
    fn serialize() {
        {
            let a = &[0, 8, 78, 69, 87, 95, 78, 79, 68, 69];
            let mut buffer = Vec::new();
            let mut cursor = Cursor::new(&mut buffer);
            let new_node = TopologyChangeType::NewNode;
            new_node.serialize(&mut cursor);
            assert_eq!(buffer, a);
        }

        {
            let b = &[0, 12, 82, 69, 77, 79, 86, 69, 68, 95, 78, 79, 68, 69];
            let mut buffer = Vec::new();
            let mut cursor = Cursor::new(&mut buffer);
            let removed_node = TopologyChangeType::RemovedNode;
            removed_node.serialize(&mut cursor);
            assert_eq!(buffer, b);
        }
    }

    #[test]
    #[should_panic]
    fn from_cursor_wrong() {
        let a = &[0, 1, 78];
        let mut wrong: Cursor<&[u8]> = Cursor::new(a);
        let _ = TopologyChangeType::from_cursor(&mut wrong).unwrap();
    }
}

#[cfg(test)]
mod status_change_type_test {
    use super::*;
    use crate::frame::traits::FromCursor;
    use std::io::Cursor;

    #[test]
    fn from_cursor() {
        let a = &[0, 2, 85, 80];
        let mut up: Cursor<&[u8]> = Cursor::new(a);
        assert_eq!(
            StatusChangeType::from_cursor(&mut up).unwrap(),
            StatusChangeType::Up
        );

        let b = &[0, 4, 68, 79, 87, 78];
        let mut down: Cursor<&[u8]> = Cursor::new(b);
        assert_eq!(
            StatusChangeType::from_cursor(&mut down).unwrap(),
            StatusChangeType::Down
        );
    }

    #[test]
    fn serialize() {
        {
            let a = &[0, 2, 85, 80];
            let mut buffer = Vec::new();
            let mut cursor = Cursor::new(&mut buffer);
            let up = StatusChangeType::Up;
            up.serialize(&mut cursor);
            assert_eq!(buffer, a);
        }

        {
            let b = &[0, 4, 68, 79, 87, 78];
            let mut buffer = Vec::new();
            let mut cursor = Cursor::new(&mut buffer);
            let down = StatusChangeType::Down;
            down.serialize(&mut cursor);
            assert_eq!(buffer, b);
        }
    }

    #[test]
    fn from_cursor_wrong() {
        let a = &[0, 1, 78];
        let mut wrong: Cursor<&[u8]> = Cursor::new(a);
        let err = StatusChangeType::from_cursor(&mut wrong)
            .unwrap_err()
            .to_string();

        assert_eq!("General error: Unexpected status change type: N", err);
    }
}

#[cfg(test)]
mod schema_change_type_test {
    use super::*;
    use crate::frame::traits::FromCursor;
    use std::io::Cursor;

    #[test]
    fn from_cursor() {
        let a = &[0, 7, 67, 82, 69, 65, 84, 69, 68];
        let mut created: Cursor<&[u8]> = Cursor::new(a);
        assert_eq!(
            SchemaChangeType::from_cursor(&mut created).unwrap(),
            SchemaChangeType::Created
        );

        let b = &[0, 7, 85, 80, 68, 65, 84, 69, 68];
        let mut updated: Cursor<&[u8]> = Cursor::new(b);
        assert_eq!(
            SchemaChangeType::from_cursor(&mut updated).unwrap(),
            SchemaChangeType::Updated
        );

        let c = &[0, 7, 68, 82, 79, 80, 80, 69, 68];
        let mut dropped: Cursor<&[u8]> = Cursor::new(c);
        assert_eq!(
            SchemaChangeType::from_cursor(&mut dropped).unwrap(),
            SchemaChangeType::Dropped
        );
    }

    #[test]
    fn serialize() {
        {
            let a = &[0, 7, 67, 82, 69, 65, 84, 69, 68];
            let mut buffer = Vec::new();
            let mut cursor = Cursor::new(&mut buffer);
            let created = SchemaChangeType::Created;
            created.serialize(&mut cursor);
            assert_eq!(buffer, a);
        }
        {
            let b = &[0, 7, 85, 80, 68, 65, 84, 69, 68];
            let mut buffer = Vec::new();
            let mut cursor = Cursor::new(&mut buffer);
            let updated = SchemaChangeType::Updated;
            updated.serialize(&mut cursor);
            assert_eq!(buffer, b);
        }

        {
            let c = &[0, 7, 68, 82, 79, 80, 80, 69, 68];
            let mut buffer = Vec::new();
            let mut cursor = Cursor::new(&mut buffer);
            let dropped = SchemaChangeType::Dropped;
            dropped.serialize(&mut cursor);
            assert_eq!(buffer, c);
        }
    }

    #[test]
    #[should_panic]
    fn from_cursor_wrong() {
        let a = &[0, 1, 78];
        let mut wrong: Cursor<&[u8]> = Cursor::new(a);
        let _ = SchemaChangeType::from_cursor(&mut wrong).unwrap();
    }
}

#[cfg(test)]
mod schema_change_target_test {
    use super::*;
    use crate::frame::traits::FromCursor;
    use std::io::Cursor;

    #[test]
    #[allow(clippy::many_single_char_names)]
    fn schema_change_target() {
        {
            let bytes = &[0, 8, 75, 69, 89, 83, 80, 65, 67, 69];
            let mut keyspace: Cursor<&[u8]> = Cursor::new(bytes);
            assert_eq!(
                SchemaChangeTarget::from_cursor(&mut keyspace).unwrap(),
                SchemaChangeTarget::Keyspace
            );
        }

        let b = &[0, 5, 84, 65, 66, 76, 69];
        let mut table: Cursor<&[u8]> = Cursor::new(b);
        assert_eq!(
            SchemaChangeTarget::from_cursor(&mut table).unwrap(),
            SchemaChangeTarget::Table
        );

        let c = &[0, 4, 84, 89, 80, 69];
        let mut _type: Cursor<&[u8]> = Cursor::new(c);
        assert_eq!(
            SchemaChangeTarget::from_cursor(&mut _type).unwrap(),
            SchemaChangeTarget::Type
        );

        let d = &[0, 8, 70, 85, 78, 67, 84, 73, 79, 78];
        let mut function: Cursor<&[u8]> = Cursor::new(d);
        assert_eq!(
            SchemaChangeTarget::from_cursor(&mut function).unwrap(),
            SchemaChangeTarget::Function
        );

        let e = &[0, 9, 65, 71, 71, 82, 69, 71, 65, 84, 69];
        let mut aggregate: Cursor<&[u8]> = Cursor::new(e);
        assert_eq!(
            SchemaChangeTarget::from_cursor(&mut aggregate).unwrap(),
            SchemaChangeTarget::Aggregate
        );
    }

    #[test]
    fn serialize() {
        {
            let a = &[0, 8, 75, 69, 89, 83, 80, 65, 67, 69];
            let mut buffer = Vec::new();
            let mut cursor = Cursor::new(&mut buffer);
            let keyspace = SchemaChangeTarget::Keyspace;
            keyspace.serialize(&mut cursor);
            assert_eq!(buffer, a);
        }

        {
            let b = &[0, 5, 84, 65, 66, 76, 69];
            let mut buffer = Vec::new();
            let mut cursor = Cursor::new(&mut buffer);
            let table = SchemaChangeTarget::Table;
            table.serialize(&mut cursor);
            assert_eq!(buffer, b);
        }

        {
            let c = &[0, 4, 84, 89, 80, 69];
            let mut buffer = Vec::new();
            let mut cursor = Cursor::new(&mut buffer);
            let target_type = SchemaChangeTarget::Type;
            target_type.serialize(&mut cursor);
            assert_eq!(buffer, c);
        }

        {
            let d = &[0, 8, 70, 85, 78, 67, 84, 73, 79, 78];
            let mut buffer = Vec::new();
            let mut cursor = Cursor::new(&mut buffer);
            let function = SchemaChangeTarget::Function;
            function.serialize(&mut cursor);
            assert_eq!(buffer, d);
        }

        {
            let e = &[0, 9, 65, 71, 71, 82, 69, 71, 65, 84, 69];
            let mut buffer = Vec::new();
            let mut cursor = Cursor::new(&mut buffer);
            let aggregate = SchemaChangeTarget::Aggregate;
            aggregate.serialize(&mut cursor);
            assert_eq!(buffer, e);
        }
    }

    #[test]
    #[should_panic]
    fn from_cursor_wrong() {
        let a = &[0, 1, 78];
        let mut wrong: Cursor<&[u8]> = Cursor::new(a);
        let _ = SchemaChangeTarget::from_cursor(&mut wrong).unwrap();
    }
}

#[cfg(test)]
mod server_event {
    use super::*;

    #[test]
    fn topology_change_new_node() {
        let bytes = &[
            // topology change
            0, 15, 84, 79, 80, 79, 76, 79, 71, 89, 95, 67, 72, 65, 78, 71, 69, // new node
            0, 8, 78, 69, 87, 95, 78, 79, 68, 69, //
            4, 127, 0, 0, 1, 0, 0, 0, 1, // 127.0.0.1:1
        ];

        let expected = ServerEvent::TopologyChange(TopologyChange {
            change_type: TopologyChangeType::NewNode,
            addr: CInet::new("127.0.0.1:1".parse().unwrap()),
        });

        test_encode_decode(bytes, expected);
    }

    #[test]
    fn topology_change_removed_node() {
        let bytes = &[
            // topology change
            0, 15, 84, 79, 80, 79, 76, 79, 71, 89, 95, 67, 72, 65, 78, 71, 69,
            // removed node
            0, 12, 82, 69, 77, 79, 86, 69, 68, 95, 78, 79, 68, 69, //
            4, 127, 0, 0, 1, 0, 0, 0, 1, // 127.0.0.1:1
        ];

        let expected = ServerEvent::TopologyChange(TopologyChange {
            change_type: TopologyChangeType::RemovedNode,
            addr: CInet::new("127.0.0.1:1".parse().unwrap()),
        });

        test_encode_decode(bytes, expected);
    }

    #[test]
    fn status_change_up() {
        let bytes = &[
            // status change
            0, 13, 83, 84, 65, 84, 85, 83, 95, 67, 72, 65, 78, 71, 69, // up
            0, 2, 85, 80, //
            4, 127, 0, 0, 1, 0, 0, 0, 1, // 127.0.0.1:1
        ];

        let expected = ServerEvent::StatusChange(StatusChange {
            change_type: StatusChangeType::Up,
            addr: CInet::new("127.0.0.1:1".parse().unwrap()),
        });

        test_encode_decode(bytes, expected);
    }

    #[test]
    fn status_change_down() {
        let bytes = &[
            // status change
            0, 13, 83, 84, 65, 84, 85, 83, 95, 67, 72, 65, 78, 71, 69, // down
            0, 4, 68, 79, 87, 78, //
            4, 127, 0, 0, 1, 0, 0, 0, 1, // 127.0.0.1:1
        ];

        let expected = ServerEvent::StatusChange(StatusChange {
            change_type: StatusChangeType::Down,
            addr: CInet::new("127.0.0.1:1".parse().unwrap()),
        });

        test_encode_decode(bytes, expected);
    }

    #[test]
    fn schema_change_created() {
        // keyspace
        {
            let bytes = &[
                // schema change
                0, 13, 83, 67, 72, 69, 77, 65, 95, 67, 72, 65, 78, 71, 69, // created
                0, 7, 67, 82, 69, 65, 84, 69, 68, // keyspace
                0, 8, 75, 69, 89, 83, 80, 65, 67, 69, // my_ks
                0, 5, 109, 121, 95, 107, 115,
            ];
            let expected = ServerEvent::SchemaChange(SchemaChange {
                change_type: SchemaChangeType::Created,
                target: SchemaChangeTarget::Keyspace,
                options: SchemaChangeOptions::Keyspace("my_ks".to_string()),
            });

            test_encode_decode(bytes, expected);
        }

        // table
        {
            let bytes = &[
                // schema change
                0, 13, 83, 67, 72, 69, 77, 65, 95, 67, 72, 65, 78, 71, 69, // created
                0, 7, 67, 82, 69, 65, 84, 69, 68, // table
                0, 5, 84, 65, 66, 76, 69, // my_ks
                0, 5, 109, 121, 95, 107, 115, // my_table
                0, 8, 109, 121, 95, 116, 97, 98, 108, 101,
            ];
            let expected = ServerEvent::SchemaChange(SchemaChange {
                change_type: SchemaChangeType::Created,
                target: SchemaChangeTarget::Table,
                options: SchemaChangeOptions::TableType(
                    "my_ks".to_string(),
                    "my_table".to_string(),
                ),
            });
            test_encode_decode(bytes, expected);
        }

        // type
        {
            let bytes = &[
                // schema change
                0, 13, 83, 67, 72, 69, 77, 65, 95, 67, 72, 65, 78, 71, 69, // created
                0, 7, 67, 82, 69, 65, 84, 69, 68, // type
                0, 4, 84, 89, 80, 69, // my_ks
                0, 5, 109, 121, 95, 107, 115, // my_table
                0, 8, 109, 121, 95, 116, 97, 98, 108, 101,
            ];
            let expected = ServerEvent::SchemaChange(SchemaChange {
                change_type: SchemaChangeType::Created,
                target: SchemaChangeTarget::Type,
                options: SchemaChangeOptions::TableType(
                    "my_ks".to_string(),
                    "my_table".to_string(),
                ),
            });
            test_encode_decode(bytes, expected);
        }

        {
            // function
            let bytes = &[
                // schema change
                0, 13, 83, 67, 72, 69, 77, 65, 95, 67, 72, 65, 78, 71, 69, // created
                0, 7, 67, 82, 69, 65, 84, 69, 68, // function
                0, 8, 70, 85, 78, 67, 84, 73, 79, 78, // my_ks
                0, 5, 109, 121, 95, 107, 115, // name
                0, 4, 110, 97, 109, 101, // empty list of parameters
                0, 0,
            ];
            let expected = ServerEvent::SchemaChange(SchemaChange {
                change_type: SchemaChangeType::Created,
                target: SchemaChangeTarget::Function,
                options: SchemaChangeOptions::FunctionAggregate(
                    "my_ks".to_string(),
                    "name".to_string(),
                    Vec::new(),
                ),
            });
            test_encode_decode(bytes, expected);
        }

        {
            // aggregate
            let bytes = &[
                // schema change
                0, 13, 83, 67, 72, 69, 77, 65, 95, 67, 72, 65, 78, 71, 69, // created
                0, 7, 67, 82, 69, 65, 84, 69, 68, // aggregate
                0, 9, 65, 71, 71, 82, 69, 71, 65, 84, 69, // my_ks
                0, 5, 109, 121, 95, 107, 115, // name
                0, 4, 110, 97, 109, 101, // empty list of parameters
                0, 0,
            ];
            let expected = ServerEvent::SchemaChange(SchemaChange {
                change_type: SchemaChangeType::Created,
                target: SchemaChangeTarget::Aggregate,
                options: SchemaChangeOptions::FunctionAggregate(
                    "my_ks".to_string(),
                    "name".to_string(),
                    Vec::new(),
                ),
            });
            test_encode_decode(bytes, expected);
        }
    }

    #[test]
    fn schema_change_updated() {
        // keyspace
        {
            let bytes = &[
                // schema change
                0, 13, 83, 67, 72, 69, 77, 65, 95, 67, 72, 65, 78, 71, 69, // updated
                0, 7, 85, 80, 68, 65, 84, 69, 68, // keyspace
                0, 8, 75, 69, 89, 83, 80, 65, 67, 69, // my_ks
                0, 5, 109, 121, 95, 107, 115,
            ];
            let expected = ServerEvent::SchemaChange(SchemaChange {
                change_type: SchemaChangeType::Updated,
                target: SchemaChangeTarget::Keyspace,
                options: SchemaChangeOptions::Keyspace("my_ks".to_string()),
            });
            test_encode_decode(bytes, expected);
        }

        // table
        {
            let bytes = &[
                // schema change
                0, 13, 83, 67, 72, 69, 77, 65, 95, 67, 72, 65, 78, 71, 69, // updated
                0, 7, 85, 80, 68, 65, 84, 69, 68, // table
                0, 5, 84, 65, 66, 76, 69, // my_ks
                0, 5, 109, 121, 95, 107, 115, // my_table
                0, 8, 109, 121, 95, 116, 97, 98, 108, 101,
            ];
            let expected = ServerEvent::SchemaChange(SchemaChange {
                change_type: SchemaChangeType::Updated,
                target: SchemaChangeTarget::Table,
                options: SchemaChangeOptions::TableType(
                    "my_ks".to_string(),
                    "my_table".to_string(),
                ),
            });
            test_encode_decode(bytes, expected);
        }

        // type
        {
            let bytes = &[
                // schema change
                0, 13, 83, 67, 72, 69, 77, 65, 95, 67, 72, 65, 78, 71, 69, // updated
                0, 7, 85, 80, 68, 65, 84, 69, 68, // type
                0, 4, 84, 89, 80, 69, // my_ks
                0, 5, 109, 121, 95, 107, 115, // my_table
                0, 8, 109, 121, 95, 116, 97, 98, 108, 101,
            ];
            let expected = ServerEvent::SchemaChange(SchemaChange {
                change_type: SchemaChangeType::Updated,
                target: SchemaChangeTarget::Type,
                options: SchemaChangeOptions::TableType(
                    "my_ks".to_string(),
                    "my_table".to_string(),
                ),
            });
            test_encode_decode(bytes, expected);
        }

        // function
        {
            let bytes = &[
                // schema change
                0, 13, 83, 67, 72, 69, 77, 65, 95, 67, 72, 65, 78, 71, 69, // updated
                0, 7, 85, 80, 68, 65, 84, 69, 68, // function
                0, 8, 70, 85, 78, 67, 84, 73, 79, 78, // my_ks
                0, 5, 109, 121, 95, 107, 115, // name
                0, 4, 110, 97, 109, 101, // empty list of parameters
                0, 0,
            ];
            let expected = ServerEvent::SchemaChange(SchemaChange {
                change_type: SchemaChangeType::Updated,
                target: SchemaChangeTarget::Function,
                options: SchemaChangeOptions::FunctionAggregate(
                    "my_ks".to_string(),
                    "name".to_string(),
                    Vec::new(),
                ),
            });
            test_encode_decode(bytes, expected);
        }

        // aggreate
        {
            let bytes = &[
                // schema change
                0, 13, 83, 67, 72, 69, 77, 65, 95, 67, 72, 65, 78, 71, 69, // updated
                0, 7, 85, 80, 68, 65, 84, 69, 68, // aggregate
                0, 9, 65, 71, 71, 82, 69, 71, 65, 84, 69, // my_ks
                0, 5, 109, 121, 95, 107, 115, // name
                0, 4, 110, 97, 109, 101, // empty list of parameters
                0, 0,
            ];
            let expected = ServerEvent::SchemaChange(SchemaChange {
                change_type: SchemaChangeType::Updated,
                target: SchemaChangeTarget::Aggregate,
                options: SchemaChangeOptions::FunctionAggregate(
                    "my_ks".to_string(),
                    "name".to_string(),
                    Vec::new(),
                ),
            });
            test_encode_decode(bytes, expected);
        }
    }

    #[test]
    fn schema_change_dropped() {
        // keyspace
        {
            let bytes = &[
                // schema change
                0, 13, 83, 67, 72, 69, 77, 65, 95, 67, 72, 65, 78, 71, 69, // dropped
                0, 7, 68, 82, 79, 80, 80, 69, 68, // keyspace
                0, 8, 75, 69, 89, 83, 80, 65, 67, 69, // my_ks
                0, 5, 109, 121, 95, 107, 115,
            ];
            let expected = ServerEvent::SchemaChange(SchemaChange {
                change_type: SchemaChangeType::Dropped,
                target: SchemaChangeTarget::Keyspace,
                options: SchemaChangeOptions::Keyspace("my_ks".to_string()),
            });
            test_encode_decode(bytes, expected);
        }

        // table
        {
            let bytes = &[
                // schema change
                0, 13, 83, 67, 72, 69, 77, 65, 95, 67, 72, 65, 78, 71, 69, // dropped
                0, 7, 68, 82, 79, 80, 80, 69, 68, // table
                0, 5, 84, 65, 66, 76, 69, // my_ks
                0, 5, 109, 121, 95, 107, 115, // my_table
                0, 8, 109, 121, 95, 116, 97, 98, 108, 101,
            ];
            let expected = ServerEvent::SchemaChange(SchemaChange {
                change_type: SchemaChangeType::Dropped,
                target: SchemaChangeTarget::Table,
                options: SchemaChangeOptions::TableType(
                    "my_ks".to_string(),
                    "my_table".to_string(),
                ),
            });
            test_encode_decode(bytes, expected);
        }

        // type
        {
            let bytes = &[
                // schema change
                0, 13, 83, 67, 72, 69, 77, 65, 95, 67, 72, 65, 78, 71, 69, // dropped
                0, 7, 68, 82, 79, 80, 80, 69, 68, // type
                0, 4, 84, 89, 80, 69, // my_ks
                0, 5, 109, 121, 95, 107, 115, // my_table
                0, 8, 109, 121, 95, 116, 97, 98, 108, 101,
            ];
            let expected = ServerEvent::SchemaChange(SchemaChange {
                change_type: SchemaChangeType::Dropped,
                target: SchemaChangeTarget::Type,
                options: SchemaChangeOptions::TableType(
                    "my_ks".to_string(),
                    "my_table".to_string(),
                ),
            });
            test_encode_decode(bytes, expected);
        }

        // function
        {
            let bytes = &[
                // schema change
                0, 13, 83, 67, 72, 69, 77, 65, 95, 67, 72, 65, 78, 71, 69, // dropped
                0, 7, 68, 82, 79, 80, 80, 69, 68, // function
                0, 8, 70, 85, 78, 67, 84, 73, 79, 78, // my_ks
                0, 5, 109, 121, 95, 107, 115, // name
                0, 4, 110, 97, 109, 101, // empty list of parameters
                0, 0,
            ];
            let expected = ServerEvent::SchemaChange(SchemaChange {
                change_type: SchemaChangeType::Dropped,
                target: SchemaChangeTarget::Function,
                options: SchemaChangeOptions::FunctionAggregate(
                    "my_ks".to_string(),
                    "name".to_string(),
                    Vec::new(),
                ),
            });
            test_encode_decode(bytes, expected);
        }

        // function
        {
            let bytes = &[
                // schema change
                0, 13, 83, 67, 72, 69, 77, 65, 95, 67, 72, 65, 78, 71, 69, // dropped
                0, 7, 68, 82, 79, 80, 80, 69, 68, // aggregate
                0, 9, 65, 71, 71, 82, 69, 71, 65, 84, 69, // my_ks
                0, 5, 109, 121, 95, 107, 115, // name
                0, 4, 110, 97, 109, 101, // empty list of parameters
                0, 0,
            ];
            let expected = ServerEvent::SchemaChange(SchemaChange {
                change_type: SchemaChangeType::Dropped,
                target: SchemaChangeTarget::Aggregate,
                options: SchemaChangeOptions::FunctionAggregate(
                    "my_ks".to_string(),
                    "name".to_string(),
                    Vec::new(),
                ),
            });
            test_encode_decode(bytes, expected);
        }
    }
}
