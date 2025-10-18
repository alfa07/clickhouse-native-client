pub mod numeric;
pub mod string;
pub mod nullable;
pub mod array;
pub mod tuple;
pub mod uuid;
pub mod ipv4;
pub mod ipv6;
pub mod decimal;
pub mod enum_column;
pub mod date;
pub mod nothing;
pub mod map;
pub mod lowcardinality;
pub mod geo;

// Re-export column types for easier access
pub use numeric::*;
pub use string::{ColumnString, ColumnFixedString};
pub use nullable::ColumnNullable;
pub use array::ColumnArray;
pub use tuple::ColumnTuple;
pub use uuid::{ColumnUuid, Uuid};
pub use ipv4::ColumnIpv4;
pub use ipv6::ColumnIpv6;
pub use decimal::ColumnDecimal;
pub use enum_column::{ColumnEnum8, ColumnEnum16};
pub use date::{ColumnDate, ColumnDate32, ColumnDateTime, ColumnDateTime64};
pub use nothing::ColumnNothing;
pub use map::ColumnMap;
pub use lowcardinality::ColumnLowCardinality;

use crate::types::Type;
use crate::{Error, Result};
use bytes::{Bytes, BytesMut};
use std::sync::Arc;

/// Reference to a column (using Arc for cheap cloning)
pub type ColumnRef = Arc<dyn Column>;

/// Base trait for all column types
/// Note: We use byte buffers instead of generic readers/writers to make the trait dyn-compatible
pub trait Column: Send + Sync {
    /// Get the type of this column
    fn column_type(&self) -> &Type;

    /// Get the number of rows in this column
    fn size(&self) -> usize;

    /// Clear all data from the column
    fn clear(&mut self);

    /// Reserve capacity for at least `new_cap` elements
    fn reserve(&mut self, new_cap: usize);

    /// Append another column's data to this column
    fn append_column(&mut self, other: ColumnRef) -> Result<()>;

    /// Load column data from byte buffer
    fn load_from_buffer(&mut self, buffer: &mut &[u8], rows: usize) -> Result<()>;

    /// Save column data to byte buffer
    fn save_to_buffer(&self, buffer: &mut BytesMut) -> Result<()>;

    /// Create an empty clone of this column (same type, no data)
    fn clone_empty(&self) -> ColumnRef;

    /// Create a slice of this column
    fn slice(&self, begin: usize, len: usize) -> Result<ColumnRef>;

    /// Downcast to a concrete column type
    fn as_any(&self) -> &dyn std::any::Any;

    /// Downcast to a mutable concrete column type
    fn as_any_mut(&mut self) -> &mut dyn std::any::Any;
}

/// Helper trait for column types that can be downcasted
pub trait ColumnTyped<T>: Column {
    /// Get value at index
    fn get(&self, index: usize) -> Option<T>;

    /// Append a value to the column
    fn append(&mut self, value: T);
}

/// Trait for columns that support iteration
pub trait ColumnIter<T> {
    type Iter<'a>: Iterator<Item = T>
    where
        Self: 'a;

    fn iter(&self) -> Self::Iter<'_>;
}

#[cfg(test)]
mod tests {
    use super::*;

    // Tests will be in individual column implementations
}
