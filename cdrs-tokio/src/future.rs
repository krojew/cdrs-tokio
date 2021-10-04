/// An owned dynamically typed [`Future`] for use in cases where you can't
/// statically type your result or need to add some indirection.
pub type BoxFuture<'a, T> = futures::future::BoxFuture<'a, T>;
