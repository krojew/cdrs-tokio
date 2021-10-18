use derive_more::Constructor;

/// A request executed by a `Session`.
#[derive(Constructor)]
pub struct Request<'a> {
    pub keyspace: Option<&'a str>,
}
