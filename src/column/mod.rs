//! # Column Module
//!
//! This module provides implementations for all ClickHouse column types used
//! in the native TCP protocol.
//!
//! ## ClickHouse Documentation
//!
//! - [Data Types Overview](https://clickhouse.com/docs/en/sql-reference/data-types)
//! - [Nullable Type](https://clickhouse.com/docs/en/sql-reference/data-types/nullable)
//! - [Array Type](https://clickhouse.com/docs/en/sql-reference/data-types/array)
//! - [LowCardinality Type](https://clickhouse.com/docs/en/sql-reference/data-types/lowcardinality)
//! - [Tuple Type](https://clickhouse.com/docs/en/sql-reference/data-types/tuple)
//! - [Map Type](https://clickhouse.com/docs/en/sql-reference/data-types/map)
//!
//! ## Type Nesting Restrictions
//!
//! ClickHouse enforces strict rules about type nesting. The following
//! combinations are **NOT allowed**:
//!
//! | Invalid Nesting | Error | Workaround |
//! |----------------|-------|------------|
//! | `Nullable(Array(...))` | "Nested type Array(...) cannot be inside Nullable type" (Error 43) | Use `Array(Nullable(...))` |
//! | `Nullable(LowCardinality(...))` | "Nested type LowCardinality(...) cannot be inside Nullable type" | Use `LowCardinality(Nullable(...))` |
//! | `Nullable(Array(LowCardinality(...)))` | Same as above | Use `Array(LowCardinality(Nullable(...)))` or `Array(Nullable(LowCardinality(...)))` |
//!
//! **Correct Nesting Order:**
//! - ✅ `Array(Nullable(T))` - Array of nullable elements
//! - ✅ `Array(LowCardinality(T))` - Array of low-cardinality elements
//! - ✅ `Array(LowCardinality(Nullable(T)))` - Array of nullable
//!   low-cardinality elements
//! - ✅ `LowCardinality(Nullable(T))` - Low-cardinality column with nullable
//!   values
//!
//! **References:**
//! - [ClickHouse Issue #1062](https://github.com/ClickHouse/ClickHouse/issues/1062)
//!   - Arrays cannot be nullable
//! - [ClickHouse Issue #42456](https://github.com/ClickHouse/ClickHouse/issues/42456)
//!   - LowCardinality cannot be inside Nullable

/// Array column type (`Array(T)`).
pub mod array;
/// Column value extraction and insertion helpers.
pub mod column_value;
/// Date and DateTime column types.
pub mod date;
/// Decimal column types (`Decimal32`, `Decimal64`, `Decimal128`).
pub mod decimal;
/// Enum8 and Enum16 column types.
pub mod enum_column;
/// Geo type helpers (Point, Ring, Polygon, MultiPolygon).
pub mod geo;
/// IPv4 column type.
pub mod ipv4;
/// IPv6 column type.
pub mod ipv6;
/// LowCardinality column type (dictionary encoding).
pub mod lowcardinality;
/// Map column type (`Map(K, V)`).
pub mod map;
/// Nothing/Void column type.
pub mod nothing;
/// Nullable column type (`Nullable(T)`).
pub mod nullable;
/// Numeric column types (integers, floats, bool).
pub mod numeric;
/// String and FixedString column types.
pub mod string;
/// Tuple column type (`Tuple(T1, T2, ...)`).
pub mod tuple;
/// UUID column type.
pub mod uuid;

// Re-export column types for easier access
pub use array::{
    ColumnArray,
    ColumnArrayT,
};
pub use date::{
    ColumnDate,
    ColumnDate32,
    ColumnDateTime,
    ColumnDateTime64,
};
pub use decimal::ColumnDecimal;
pub use enum_column::{
    ColumnEnum16,
    ColumnEnum8,
};
pub use ipv4::ColumnIpv4;
pub use ipv6::ColumnIpv6;
pub use lowcardinality::ColumnLowCardinality;
pub use map::ColumnMap;
pub use nothing::ColumnNothing;
pub use nullable::ColumnNullable;
pub use numeric::*;
pub use string::{
    ColumnFixedString,
    ColumnString,
};
pub use tuple::ColumnTuple;
pub use uuid::{
    ColumnUuid,
    Uuid,
};

use crate::{
    types::Type,
    Result,
};
use bytes::BytesMut;
use std::sync::Arc;

/// Reference to a column (using Arc for cheap cloning)
pub type ColumnRef = Arc<dyn Column>;

/// Base trait for all column types
/// Note: We use byte buffers instead of generic readers/writers to make the
/// trait dyn-compatible
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

    /// Load column prefix from byte buffer (for types that need prefix data)
    /// Default implementation is a no-op. Override for types like
    /// LowCardinality. This matches C++ clickhouse-cpp's LoadPrefix pattern.
    fn load_prefix(
        &mut self,
        _buffer: &mut &[u8],
        _rows: usize,
    ) -> Result<()> {
        // Default: no prefix data to read
        Ok(())
    }

    /// Load column data from byte buffer
    fn load_from_buffer(
        &mut self,
        buffer: &mut &[u8],
        rows: usize,
    ) -> Result<()>;

    /// Save column prefix to byte buffer (for types that need prefix data)
    /// Default implementation is a no-op. Override for types like
    /// LowCardinality, Array with special nested types. This matches C++
    /// clickhouse-cpp's SavePrefix pattern.
    fn save_prefix(&self, _buffer: &mut BytesMut) -> Result<()> {
        // Default: no prefix data to write
        Ok(())
    }

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

/// Trait for columns that support iteration over their values.
pub trait ColumnIter<T> {
    /// The iterator type returned by [`iter`](Self::iter).
    type Iter<'a>: Iterator<Item = T>
    where
        Self: 'a;

    /// Returns an iterator over the column values.
    fn iter(&self) -> Self::Iter<'_>;
}

#[cfg(test)]
#[cfg_attr(coverage_nightly, coverage(off))]
mod tests {
    // Tests will be in individual column implementations
}
