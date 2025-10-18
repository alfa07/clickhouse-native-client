use crate::column::ColumnRef;
use crate::types::Type;
use crate::{Error, Result};

/// Block metadata
#[derive(Debug, Clone, Default)]
pub struct BlockInfo {
    pub is_overflows: u8,
    pub bucket_num: i32,
}

/// A block is a collection of named columns with the same number of rows
#[derive(Clone)]
pub struct Block {
    columns: Vec<ColumnItem>,
    rows: usize,
    info: BlockInfo,
}

#[derive(Clone)]
struct ColumnItem {
    name: String,
    column: ColumnRef,
}

impl Block {
    /// Create a new empty block
    pub fn new() -> Self {
        Self {
            columns: Vec::new(),
            rows: 0,
            info: BlockInfo::default(),
        }
    }

    /// Create a block with reserved capacity
    pub fn with_capacity(cols: usize, rows: usize) -> Self {
        Self {
            columns: Vec::with_capacity(cols),
            rows,
            info: BlockInfo::default(),
        }
    }

    /// Append a named column to the block
    pub fn append_column(&mut self, name: impl Into<String>, column: ColumnRef) -> Result<()> {
        let name = name.into();

        if self.columns.is_empty() {
            self.rows = column.size();
        } else if column.size() != self.rows {
            return Err(Error::Validation(format!(
                "All columns in block must have same count of rows. Name: '{}', expected rows: {}, got: {}",
                name,
                self.rows,
                column.size()
            )));
        }

        self.columns.push(ColumnItem { name, column });
        Ok(())
    }

    /// Get the number of columns in the block
    pub fn column_count(&self) -> usize {
        self.columns.len()
    }

    /// Get the number of rows in the block
    pub fn row_count(&self) -> usize {
        self.rows
    }

    /// Get column by index
    pub fn column(&self, index: usize) -> Option<ColumnRef> {
        self.columns.get(index).map(|item| item.column.clone())
    }

    /// Get column name by index
    pub fn column_name(&self, index: usize) -> Option<&str> {
        self.columns.get(index).map(|item| item.name.as_str())
    }

    /// Get column by name
    pub fn column_by_name(&self, name: &str) -> Option<ColumnRef> {
        self.columns
            .iter()
            .find(|item| item.name == name)
            .map(|item| item.column.clone())
    }

    /// Get block info
    pub fn info(&self) -> &BlockInfo {
        &self.info
    }

    /// Set block info
    pub fn set_info(&mut self, info: BlockInfo) {
        self.info = info;
    }

    /// Clear all data from all columns
    pub fn clear(&mut self) {
        for _item in &self.columns {
            // We can't modify through Arc, so this is a limitation
            // In practice, we'd need interior mutability or a different design
            // For now, we'll just reset the block
        }
        self.columns.clear();
        self.rows = 0;
    }

    /// Reserve capacity in all columns
    pub fn reserve(&mut self, _new_cap: usize) {
        for _item in &self.columns {
            // Same limitation as clear() - can't modify through Arc
            // This would need a different design with RefCell or similar
        }
    }

    /// Refresh and validate row count
    pub fn refresh_row_count(&mut self) -> Result<usize> {
        if self.columns.is_empty() {
            self.rows = 0;
            return Ok(0);
        }

        let first_rows = self.columns[0].column.size();

        for item in &self.columns {
            let col_rows = item.column.size();
            if col_rows != first_rows {
                return Err(Error::Validation(format!(
                    "All columns in block must have same count of rows. Name: '{}', expected: {}, got: {}",
                    item.name, first_rows, col_rows
                )));
            }
        }

        self.rows = first_rows;
        Ok(first_rows)
    }

    /// Iterate over columns
    pub fn iter(&self) -> BlockIterator<'_> {
        BlockIterator {
            block: self,
            index: 0,
        }
    }

    /// Check if block is empty
    pub fn is_empty(&self) -> bool {
        self.rows == 0 || self.columns.is_empty()
    }
}

impl Default for Block {
    fn default() -> Self {
        Self::new()
    }
}

/// Iterator over block columns
pub struct BlockIterator<'a> {
    block: &'a Block,
    index: usize,
}

impl<'a> Iterator for BlockIterator<'a> {
    type Item = (&'a str, &'a Type, ColumnRef);

    fn next(&mut self) -> Option<Self::Item> {
        if self.index < self.block.columns.len() {
            let item = &self.block.columns[self.index];
            self.index += 1;
            Some((
                &item.name,
                item.column.column_type(),
                item.column.clone(),
            ))
        } else {
            None
        }
    }
}

impl<'a> IntoIterator for &'a Block {
    type Item = (&'a str, &'a Type, ColumnRef);
    type IntoIter = BlockIterator<'a>;

    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}

// Index access
impl std::ops::Index<usize> for Block {
    type Output = ColumnRef;

    fn index(&self, index: usize) -> &Self::Output {
        if index >= self.columns.len() {
            panic!(
                "Column index out of range: {} >= {}",
                index,
                self.columns.len()
            );
        }
        // We need to return a reference, but we have Arc
        // This is a design limitation - we'll need to restructure
        panic!("Index access not yet supported for Block - use column() method instead");
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::column::numeric::ColumnUInt64;
    use crate::types::Type;
    use std::sync::Arc;

    #[test]
    fn test_block_creation() {
        let block = Block::new();
        assert_eq!(block.column_count(), 0);
        assert_eq!(block.row_count(), 0);
        assert!(block.is_empty());
    }

    #[test]
    fn test_block_append_column() {
        let mut block = Block::new();

        let mut col1 = ColumnUInt64::new(Type::uint64());
        col1.append(1);
        col1.append(2);
        col1.append(3);

        block.append_column("id", Arc::new(col1)).unwrap();

        assert_eq!(block.column_count(), 1);
        assert_eq!(block.row_count(), 3);
        assert!(!block.is_empty());
    }

    #[test]
    fn test_block_multiple_columns() {
        let mut block = Block::new();

        let mut col1 = ColumnUInt64::new(Type::uint64());
        col1.append(1);
        col1.append(2);

        let mut col2 = ColumnUInt64::new(Type::uint64());
        col2.append(100);
        col2.append(200);

        block.append_column("id", Arc::new(col1)).unwrap();
        block.append_column("value", Arc::new(col2)).unwrap();

        assert_eq!(block.column_count(), 2);
        assert_eq!(block.row_count(), 2);
    }

    #[test]
    fn test_block_mismatched_rows() {
        let mut block = Block::new();

        let mut col1 = ColumnUInt64::new(Type::uint64());
        col1.append(1);
        col1.append(2);

        let mut col2 = ColumnUInt64::new(Type::uint64());
        col2.append(100);
        col2.append(200);
        col2.append(300); // Extra row!

        block.append_column("id", Arc::new(col1)).unwrap();
        let result = block.append_column("value", Arc::new(col2));

        assert!(result.is_err());
    }

    #[test]
    fn test_block_get_column() {
        let mut block = Block::new();

        let mut col1 = ColumnUInt64::new(Type::uint64());
        col1.append(42);

        block.append_column("test", Arc::new(col1)).unwrap();

        let col = block.column(0).unwrap();
        assert_eq!(col.size(), 1);

        assert!(block.column(1).is_none());
    }

    #[test]
    fn test_block_get_column_by_name() {
        let mut block = Block::new();

        let mut col1 = ColumnUInt64::new(Type::uint64());
        col1.append(42);

        block.append_column("my_column", Arc::new(col1)).unwrap();

        let col = block.column_by_name("my_column").unwrap();
        assert_eq!(col.size(), 1);

        assert!(block.column_by_name("nonexistent").is_none());
    }

    #[test]
    fn test_block_column_name() {
        let mut block = Block::new();

        let mut col1 = ColumnUInt64::new(Type::uint64());
        col1.append(1);

        block.append_column("test_name", Arc::new(col1)).unwrap();

        assert_eq!(block.column_name(0), Some("test_name"));
        assert_eq!(block.column_name(1), None);
    }

    #[test]
    fn test_block_iterator() {
        let mut block = Block::new();

        let mut col1 = ColumnUInt64::new(Type::uint64());
        col1.append(1);

        let mut col2 = ColumnUInt64::new(Type::uint64());
        col2.append(2);

        block.append_column("first", Arc::new(col1)).unwrap();
        block.append_column("second", Arc::new(col2)).unwrap();

        let names: Vec<&str> = block.iter().map(|(name, _, _)| name).collect();
        assert_eq!(names, vec!["first", "second"]);
    }

    #[test]
    fn test_block_info() {
        let mut block = Block::new();

        let info = BlockInfo {
            is_overflows: 1,
            bucket_num: 42,
        };

        block.set_info(info.clone());

        assert_eq!(block.info().is_overflows, 1);
        assert_eq!(block.info().bucket_num, 42);
    }
}
