# Typed Column Variants - Usage Examples

This document demonstrates the new typed variants of `ColumnMap` and `ColumnTuple`, which provide compile-time type safety and convenient construction.

## ColumnMapT<K, V>

### Overview

`ColumnMapT<K, V>` is a typed wrapper around `ColumnMap` that provides:
- Compile-time type safety for key and value column types
- Type inference from provided columns
- Typed access to keys and values

### Usage Examples

```rust
use clickhouse_client::column::{ColumnMapT, ColumnString, ColumnUInt32};
use std::sync::Arc;

// Create with type inference - no need to specify Map type!
let keys = Arc::new(ColumnString::new(Type::string()));
let values = Arc::new(ColumnUInt32::new());

// Type is automatically inferred as Map(String, UInt32)
let map = ColumnMapT::from_keys_values(keys, values);

// Access typed keys and values
let keys_ref: &ColumnString = map.keys().unwrap();
let values_ref: &ColumnUInt32 = map.values().unwrap();
```

### Converting from ColumnMap

```rust
// Convert an existing ColumnMap to typed variant
let map_type = Type::Map {
    key_type: Box::new(Type::Simple(TypeCode::String)),
    value_type: Box::new(Type::Simple(TypeCode::UInt32)),
};
let map = ColumnMap::new(map_type);

// Convert to typed map
let typed_map = ColumnMapT::<ColumnString, ColumnUInt32>::try_from_map(map)?;
```

## ColumnTupleT<T0, T1, ...>

### Overview

`ColumnTupleT` provides typed wrappers for tuples with compile-time type safety.
Supports tuples of size 1-12 (similar to Rust's standard library).

### Usage Examples

#### Two-Element Tuple

```rust
use clickhouse_client::column::{ColumnTupleT, ColumnUInt64, ColumnString};
use std::sync::Arc;

// Create with type inference
let col1 = Arc::new(ColumnUInt64::new());
let col2 = Arc::new(ColumnString::new(Type::string()));

// Type is automatically Tuple(UInt64, String)
let tuple = ColumnTupleT::from_columns((col1, col2));

// Access typed columns
let (c1, c2) = tuple.columns()?;
// c1 is &ColumnUInt64
// c2 is &ColumnString
```

#### Three-Element Tuple

```rust
use clickhouse_client::column::{ColumnTupleT, ColumnUInt64, ColumnString, ColumnUInt32};

let col1 = Arc::new(ColumnUInt64::new());
let col2 = Arc::new(ColumnString::new(Type::string()));
let col3 = Arc::new(ColumnUInt32::new());

// Tuple(UInt64, String, UInt32)
let tuple = ColumnTupleT::from_columns((col1, col2, col3));

let (c1, c2, c3) = tuple.columns()?;
```

### Converting from ColumnTuple

```rust
// Convert an existing ColumnTuple to typed variant
let types = vec![Type::uint64(), Type::string()];
let tuple_type = Type::tuple(types);

let col1 = Arc::new(ColumnUInt64::new()) as ColumnRef;
let col2 = Arc::new(ColumnString::new(Type::string())) as ColumnRef;

let tuple = ColumnTuple::new(tuple_type, vec![col1, col2]);

// Convert to typed tuple
let typed_tuple = ColumnTupleT::<ColumnUInt64, ColumnString>::try_from_tuple(tuple)?;
```

## Benefits

### Type Inference

The main benefit is that Rust's type inference can determine the complex ClickHouse types automatically:

```rust
// Before (untyped):
let map_type = Type::Map {
    key_type: Box::new(Type::Simple(TypeCode::String)),
    value_type: Box::new(Type::Simple(TypeCode::UInt32)),
};
let map = ColumnMap::new(map_type);

// After (typed):
let map = ColumnMapT::from_keys_values(
    Arc::new(ColumnString::new(Type::string())),
    Arc::new(ColumnUInt32::new())
);
// Type is inferred automatically!
```

### Compile-Time Safety

Type mismatches are caught at compile time rather than runtime:

```rust
// This won't compile - type mismatch!
let keys = Arc::new(ColumnString::new(Type::string()));
let values = Arc::new(ColumnUInt32::new());
let map = ColumnMapT::<ColumnUInt64, ColumnUInt32>::from_keys_values(keys, values);
//                     ^^^^^^^^^^^^^^^^
// Compiler error: expected ColumnUInt64, found ColumnString
```

## Implementation Details

### ColumnMapT<K, V>

- Internally wraps `ColumnMap` which stores data as `Array(Tuple(K, V))`
- Provides `keys()` and `values()` methods for typed access
- Implements the `Column` trait, so can be used anywhere a column is needed

### ColumnTupleT

- Uses a macro to generate implementations for tuples of size 1-12
- Each size has its own concrete type
- Pattern matches C++ clickhouse-cpp's `ColumnTupleT<...>` variadic template

## Comparison with C++ clickhouse-cpp

This implementation mirrors the C++ design:

**C++ (clickhouse-cpp):**
```cpp
auto keys = std::make_shared<ColumnString>();
auto values = std::make_shared<ColumnUInt32>();
auto map = std::make_shared<ColumnMapT<ColumnString, ColumnUInt32>>(keys, values);
```

**Rust (this implementation):**
```rust
let keys = Arc::new(ColumnString::new(Type::string()));
let values = Arc::new(ColumnUInt32::new());
let map = ColumnMapT::from_keys_values(keys, values);
```

Both provide type inference and compile-time safety!
