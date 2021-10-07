/// A request executed by a `Session`.
pub struct Request<'a> {
    pub keyspace: Option<&'a str>,
}

impl<'a> Request<'a> {
    pub fn new(keyspace: Option<&'a str>) -> Self {
        Request { keyspace }
    }
}
