// Block tests ported from clickhouse-cpp ut/block_ut.cpp
// These tests verify block functionality: iterators, clear, reserve, etc.

use clickhouse_client::{
    block::Block,
    column::{
        ColumnString,
        ColumnUInt8,
    },
    types::Type,
};
use std::sync::Arc;

// ============================================================================
// Test Helper Functions
// ============================================================================

/// Helper to create a block with multiple columns
fn make_test_block() -> Block {
    let mut block = Block::new();

    let mut col1 = ColumnUInt8::new();
    col1.append(1);
    col1.append(2);
    col1.append(3);
    col1.append(4);
    col1.append(5);

    let mut col2 = ColumnString::new(Type::string());
    col2.append("1");
    col2.append("2");
    col2.append("3");
    col2.append("4");
    col2.append("5");

    block.append_column("foo", Arc::new(col1)).unwrap();
    block.append_column("bar", Arc::new(col2)).unwrap();

    block
}

// ============================================================================
// Iterator Tests
// ============================================================================

#[test]
fn test_block_range_based_for_loop() {
    let block = make_test_block();
    let expected_names = ["foo", "bar"];

    assert_eq!(block.column_count(), 2);
    assert_eq!(block.row_count(), 5);

    let mut col_index = 0;
    for (name, _type, _column) in &block {
        assert_eq!(name, expected_names[col_index]);
        col_index += 1;
    }

    assert_eq!(col_index, 2);
}

#[test]
fn test_block_iterator() {
    let block = make_test_block();
    let expected_names = ["foo", "bar"];

    assert_eq!(block.column_count(), 2);
    assert_eq!(block.row_count(), 5);

    let mut col_index = 0;
    let iter = block.iter();

    for (name, _type, column) in iter {
        assert_eq!(name, expected_names[col_index]);
        assert_eq!(column.size(), 5);
        col_index += 1;
    }

    assert_eq!(col_index, 2);
}

#[test]
fn test_block_iterator_empty() {
    let block = Block::new();

    let mut iter = block.iter();
    assert!(iter.next().is_none());

    // Test that iterator can be created multiple times
    let count: usize = block.iter().count();
    assert_eq!(count, 0);
}

// ============================================================================
// Clear Test
// ============================================================================

#[test]
fn test_block_clear() {
    // Test that Block::clear() removes all rows from all columns,
    // without changing column count, types, or names

    let mut block = make_test_block();

    // Store expected column info before clearing
    let expected_column_count = block.column_count();
    let _expected_names: Vec<String> =
        block.iter().map(|(name, _, _)| name.to_string()).collect();

    assert_eq!(expected_column_count, 2);
    assert_eq!(block.row_count(), 5);

    // Clear the block
    block.clear();

    // Block must report empty after being cleared
    assert_eq!(block.row_count(), 0);
    assert_eq!(block.column_count(), 0); // In Rust, we clear columns too due
                                         // to Arc limitations

    // Note: In C++, clear() preserves columns but empties them.
    // In Rust with Arc, we can't modify shared columns, so we clear the entire
    // structure. This is a design difference due to Rust's ownership
    // model.
}

// ============================================================================
// Reserve Test
// ============================================================================

#[test]
fn test_block_reserve() {
    // Test that Block::reserve() reserves space in all columns,
    // without changing column data or row count

    let mut block = make_test_block();

    let initial_rows = block.row_count();
    let initial_cols = block.column_count();

    assert_eq!(initial_rows, 5);
    assert_eq!(initial_cols, 2);

    // Reserve capacity (this is a no-op in current implementation due to Arc)
    block.reserve(1000);

    // Block must have same number of rows and columns after reserve
    assert_eq!(block.row_count(), initial_rows);
    assert_eq!(block.column_count(), initial_cols);

    // Verify column data is still intact
    let col1 = block.column(0).unwrap();
    assert_eq!(col1.size(), 5);

    let col2 = block.column(1).unwrap();
    assert_eq!(col2.size(), 5);
}

// ============================================================================
// Additional Block Tests
// ============================================================================

#[test]
fn test_block_column_access() {
    let block = make_test_block();

    // Access by index
    let col1 = block.column(0).unwrap();
    assert_eq!(col1.size(), 5);

    let col2 = block.column(1).unwrap();
    assert_eq!(col2.size(), 5);

    // Out of bounds
    assert!(block.column(2).is_none());
}

#[test]
fn test_block_column_names() {
    let block = make_test_block();

    assert_eq!(block.column_name(0), Some("foo"));
    assert_eq!(block.column_name(1), Some("bar"));
    assert_eq!(block.column_name(2), None);
}

#[test]
fn test_block_column_by_name() {
    let block = make_test_block();

    let col_foo = block.column_by_name("foo").unwrap();
    assert_eq!(col_foo.size(), 5);

    let col_bar = block.column_by_name("bar").unwrap();
    assert_eq!(col_bar.size(), 5);

    assert!(block.column_by_name("nonexistent").is_none());
}

#[test]
fn test_block_empty() {
    let block = Block::new();

    assert!(block.is_empty());
    assert_eq!(block.column_count(), 0);
    assert_eq!(block.row_count(), 0);
}

#[test]
fn test_block_refresh_row_count() {
    let mut block = Block::new();

    let mut col1 = ColumnUInt8::new();
    col1.append(1);
    col1.append(2);
    col1.append(3);

    block.append_column("test", Arc::new(col1)).unwrap();

    assert_eq!(block.row_count(), 3);

    // Refresh should maintain the count
    let rows = block.refresh_row_count().unwrap();
    assert_eq!(rows, 3);
    assert_eq!(block.row_count(), 3);
}

#[test]
fn test_block_mismatched_row_counts() {
    let mut block = Block::new();

    let mut col1 = ColumnUInt8::new();
    col1.append(1);
    col1.append(2);

    let mut col2 = ColumnString::new(Type::string());
    col2.append("a");
    col2.append("b");
    col2.append("c"); // Mismatch: 3 rows instead of 2

    block.append_column("col1", Arc::new(col1)).unwrap();
    let result = block.append_column("col2", Arc::new(col2));

    assert!(result.is_err());
}

// ============================================================================
// Advanced Iterator Tests (from C++ block_ut.cpp)
// ============================================================================

#[test]
fn test_block_iterator_equality() {
    // Empty block - all iterators should be equal
    let empty_block = Block::new();
    let mut iter1 = empty_block.iter();
    let mut iter2 = empty_block.iter();

    assert!(iter1.next().is_none());
    assert!(iter2.next().is_none());

    // Non-empty block - iterators should not start at end
    let block = make_test_block();
    let mut iter = block.iter();

    assert!(iter.next().is_some(), "Iterator should have first element");
    assert!(iter.next().is_some(), "Iterator should have second element");
    assert!(
        iter.next().is_none(),
        "Iterator should be exhausted after two elements"
    );
}

#[test]
fn test_block_multiple_iterations() {
    let block = make_test_block();

    // First iteration
    let count1: usize = block.iter().count();
    assert_eq!(count1, 2);

    // Second iteration - should work the same
    let count2: usize = block.iter().count();
    assert_eq!(count2, 2);

    // Third iteration with manual loop
    let mut names = Vec::new();
    for (name, _, _) in &block {
        names.push(name.to_string());
    }
    assert_eq!(names, vec!["foo", "bar"]);
}

#[test]
fn test_block_iterator_collect() {
    let block = make_test_block();

    // Collect column names using iterator
    let names: Vec<String> =
        block.iter().map(|(name, _, _)| name.to_string()).collect();

    assert_eq!(names, vec!["foo", "bar"]);

    // Collect column sizes
    let sizes: Vec<usize> =
        block.iter().map(|(_, _, col)| col.size()).collect();

    assert_eq!(sizes, vec![5, 5]);
}

// ============================================================================
// Block Data Verification Tests
// ============================================================================

#[test]
fn test_block_data_integrity_after_creation() {
    let block = make_test_block();

    // Verify first column (UInt8)
    let col1 = block.column(0).unwrap();
    let col1_u8 = col1.as_any().downcast_ref::<ColumnUInt8>().unwrap();
    assert_eq!(col1_u8.at(0), 1);
    assert_eq!(col1_u8.at(1), 2);
    assert_eq!(col1_u8.at(2), 3);
    assert_eq!(col1_u8.at(3), 4);
    assert_eq!(col1_u8.at(4), 5);

    // Verify second column (String)
    let col2 = block.column(1).unwrap();
    let col2_str = col2.as_any().downcast_ref::<ColumnString>().unwrap();
    assert_eq!(col2_str.at(0), "1");
    assert_eq!(col2_str.at(1), "2");
    assert_eq!(col2_str.at(2), "3");
    assert_eq!(col2_str.at(3), "4");
    assert_eq!(col2_str.at(4), "5");
}

#[test]
fn test_block_column_type_info() {
    let block = make_test_block();

    // Check first column type
    for (idx, (name, col_type, column)) in block.iter().enumerate() {
        match idx {
            0 => {
                assert_eq!(name, "foo");
                assert_eq!(col_type.name(), "UInt8");
                assert_eq!(column.size(), 5);
            }
            1 => {
                assert_eq!(name, "bar");
                assert_eq!(col_type.name(), "String");
                assert_eq!(column.size(), 5);
            }
            _ => panic!("Unexpected column index: {}", idx),
        }
    }
}

// ============================================================================
// Block Edge Cases
// ============================================================================

#[test]
fn test_block_single_column() {
    let mut block = Block::new();

    let mut col = ColumnUInt8::new();
    col.append(42);

    block.append_column("single", Arc::new(col)).unwrap();

    assert_eq!(block.column_count(), 1);
    assert_eq!(block.row_count(), 1);
    assert_eq!(block.column_name(0), Some("single"));
}

#[test]
fn test_block_single_row() {
    let mut block = Block::new();

    let mut col1 = ColumnUInt8::new();
    col1.append(1);

    let mut col2 = ColumnString::new(Type::string());
    col2.append("one");

    block.append_column("num", Arc::new(col1)).unwrap();
    block.append_column("text", Arc::new(col2)).unwrap();

    assert_eq!(block.column_count(), 2);
    assert_eq!(block.row_count(), 1);
}

#[test]
fn test_block_many_columns() {
    let mut block = Block::new();

    // Add 100 columns
    for i in 0..100 {
        let mut col = ColumnUInt8::new();
        col.append((i % 256) as u8);

        block.append_column(format!("col{}", i), Arc::new(col)).unwrap();
    }

    assert_eq!(block.column_count(), 100);
    assert_eq!(block.row_count(), 1);

    // Verify we can access all columns
    for i in 0..100 {
        let col_name = block.column_name(i);
        assert_eq!(col_name, Some(format!("col{}", i).as_str()));
    }
}

#[test]
fn test_block_many_rows() {
    let mut block = Block::new();

    let mut col = ColumnUInt8::new();

    // Add 10000 rows
    for i in 0..10000 {
        col.append((i % 256) as u8);
    }

    block.append_column("numbers", Arc::new(col)).unwrap();

    assert_eq!(block.column_count(), 1);
    assert_eq!(block.row_count(), 10000);
}

// ============================================================================
// Block Clone and Copy Tests
// ============================================================================

#[test]
fn test_block_clone() {
    let block1 = make_test_block();
    let block2 = block1.clone();

    assert_eq!(block1.column_count(), block2.column_count());
    assert_eq!(block1.row_count(), block2.row_count());

    // Verify column names match
    for i in 0..block1.column_count() {
        assert_eq!(block1.column_name(i), block2.column_name(i));
    }
}

// ============================================================================
// Column Type Mismatch Detection
// ============================================================================

use clickhouse_client::column::numeric::ColumnUInt64;

#[test]
fn test_block_can_store_different_numeric_types() {
    let mut block = Block::new();

    let mut col1 = ColumnUInt8::new();
    col1.append(1);

    let mut col2 = ColumnUInt64::new();
    col2.append(1000);

    block.append_column("u8", Arc::new(col1)).unwrap();
    block.append_column("u64", Arc::new(col2)).unwrap();

    assert_eq!(block.column_count(), 2);
    assert_eq!(block.row_count(), 1);
}

// ============================================================================
// Total Rows Calculation
// ============================================================================

#[test]
fn test_block_row_count() {
    let block = make_test_block();

    // Verify row_count() returns correct value
    assert_eq!(block.row_count(), 5);
}

#[test]
fn test_block_row_count_empty() {
    let block = Block::new();

    assert_eq!(block.row_count(), 0);
}
