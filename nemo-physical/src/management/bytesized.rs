use bytesize::ByteSize;

/// Objects that are able calculate their current size in bytes
pub trait ByteSized {
    /// Return the number of bytes this object consumes
    fn size_bytes(&self) -> ByteSize;
}
