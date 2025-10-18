use crate::block::{Block, BlockInfo};
use crate::column::ColumnRef;
use crate::compression::{compress, decompress};
use crate::connection::Connection;
use crate::protocol::CompressionMethod;
use crate::types::Type;
use crate::{Error, Result};
use bytes::{Buf, BufMut, BytesMut};
use std::sync::Arc;

/// Minimum revision constants
const DBMS_MIN_REVISION_WITH_TEMPORARY_TABLES: u64 = 50264;
const DBMS_MIN_REVISION_WITH_BLOCK_INFO: u64 = 51903;
const DBMS_MIN_REVISION_WITH_CUSTOM_SERIALIZATION: u64 = 54454;

/// Create a column instance for the given type
/// This is used internally by column types like Array and Nullable
pub fn create_column(type_: &Type) -> Result<ColumnRef> {
    use crate::column::array::ColumnArray;
    use crate::column::date::{ColumnDate, ColumnDate32, ColumnDateTime, ColumnDateTime64};
    use crate::column::decimal::ColumnDecimal;
    use crate::column::enum_column::{ColumnEnum8, ColumnEnum16};
    use crate::column::ipv4::ColumnIpv4;
    use crate::column::ipv6::ColumnIpv6;
    use crate::column::lowcardinality::ColumnLowCardinality;
    use crate::column::map::ColumnMap;
    use crate::column::nothing::ColumnNothing;
    use crate::column::nullable::ColumnNullable;
    use crate::column::numeric::*;
    use crate::column::string::{ColumnFixedString, ColumnString};
    use crate::column::uuid::ColumnUuid;

    match type_ {
        Type::Simple(code) => {
            use crate::types::TypeCode;
            match code {
                TypeCode::UInt8 => Ok(Arc::new(ColumnUInt8::new(type_.clone()))),
                TypeCode::UInt16 => Ok(Arc::new(ColumnUInt16::new(type_.clone()))),
                TypeCode::UInt32 => Ok(Arc::new(ColumnUInt32::new(type_.clone()))),
                TypeCode::UInt64 => Ok(Arc::new(ColumnUInt64::new(type_.clone()))),
                TypeCode::Int8 => Ok(Arc::new(ColumnInt8::new(type_.clone()))),
                TypeCode::Int16 => Ok(Arc::new(ColumnInt16::new(type_.clone()))),
                TypeCode::Int32 => Ok(Arc::new(ColumnInt32::new(type_.clone()))),
                TypeCode::Int64 => Ok(Arc::new(ColumnInt64::new(type_.clone()))),
                TypeCode::Float32 => Ok(Arc::new(ColumnFloat32::new(type_.clone()))),
                TypeCode::Float64 => Ok(Arc::new(ColumnFloat64::new(type_.clone()))),
                TypeCode::String => Ok(Arc::new(ColumnString::new(type_.clone()))),
                TypeCode::Date => Ok(Arc::new(ColumnDate::new(type_.clone()))),
                TypeCode::Date32 => Ok(Arc::new(ColumnDate32::new(type_.clone()))),
                TypeCode::UUID => Ok(Arc::new(ColumnUuid::new(type_.clone()))),
                TypeCode::IPv4 => Ok(Arc::new(ColumnIpv4::new(type_.clone()))),
                TypeCode::IPv6 => Ok(Arc::new(ColumnIpv6::new(type_.clone()))),
                TypeCode::Void => Ok(Arc::new(ColumnNothing::new(type_.clone()))),
                _ => Err(Error::Protocol(format!("Unsupported type: {}", type_.name()))),
            }
        }
        Type::FixedString { .. } => Ok(Arc::new(ColumnFixedString::new(type_.clone()))),
        Type::DateTime { .. } => {
            // Use specialized ColumnDateTime with timezone support
            Ok(Arc::new(ColumnDateTime::new(type_.clone())))
        }
        Type::DateTime64 { .. } => {
            // Use specialized ColumnDateTime64 with precision and timezone
            Ok(Arc::new(ColumnDateTime64::new(type_.clone())))
        }
        Type::Enum8 { .. } => {
            // Use specialized ColumnEnum8 with name-value mapping
            Ok(Arc::new(ColumnEnum8::new(type_.clone())))
        }
        Type::Enum16 { .. } => {
            // Use specialized ColumnEnum16 with name-value mapping
            Ok(Arc::new(ColumnEnum16::new(type_.clone())))
        }
        Type::Decimal { .. } => {
            // Use specialized ColumnDecimal with precision and scale
            Ok(Arc::new(ColumnDecimal::new(type_.clone())))
        }
        Type::Nullable { .. } => {
            Ok(Arc::new(ColumnNullable::new(type_.clone())))
        }
        Type::Array { .. } => {
            Ok(Arc::new(ColumnArray::new(type_.clone())))
        }
        Type::Map { .. } => {
            Ok(Arc::new(ColumnMap::new(type_.clone())))
        }
        Type::LowCardinality { .. } => {
            Ok(Arc::new(ColumnLowCardinality::new(type_.clone())))
        }
        Type::Tuple { item_types } => {
            // Create empty columns for each tuple element
            let mut columns = Vec::new();
            for item_type in item_types {
                columns.push(create_column(item_type)?);
            }
            Ok(Arc::new(crate::column::ColumnTuple::new(
                type_.clone(),
                columns,
            )))
        }
    }
}

/// Reader for blocks from network
pub struct BlockReader {
    server_revision: u64,
    compression: Option<CompressionMethod>,
}

impl BlockReader {
    /// Create a new block reader
    pub fn new(server_revision: u64) -> Self {
        Self {
            server_revision,
            compression: None,
        }
    }

    /// Enable compression
    pub fn with_compression(mut self, method: CompressionMethod) -> Self {
        self.compression = Some(method);
        self
    }

    /// Read a block from the connection
    /// Note: Caller is responsible for skipping temp table name if needed (matches C++ ReadBlock)
    pub async fn read_block(&self, conn: &mut Connection) -> Result<Block> {
        // Read the block data
        let block_data = if let Some(_compression_method) = self.compression {
            // Read compressed data: checksum (16) + header (9) + compressed data (N)
            // First read checksum
            let checksum = conn.read_bytes(16).await?;

            // Read header to determine compressed size
            let method = conn.read_u8().await?;
            let compressed_size = conn.read_u32().await? as usize;
            let uncompressed_size = conn.read_u32().await?;

            // Read the remaining compressed data
            let compressed_data_len = compressed_size.saturating_sub(9);
            let compressed_data = conn.read_bytes(compressed_data_len).await?;

            // Build the full compressed block for decompression
            let mut full_block = BytesMut::with_capacity(16 + 9 + compressed_data_len);
            full_block.extend_from_slice(&checksum);
            full_block.put_u8(method);
            full_block.put_u32_le(compressed_size as u32);
            full_block.put_u32_le(uncompressed_size);
            full_block.extend_from_slice(&compressed_data);

            // Decompress
            decompress(&full_block)?
        } else {
            // Read uncompressed - we'll read into a buffer as we parse
            // For now, create an empty buffer and read fields directly
            BytesMut::new().into()
        };

        // Parse block from buffer (or read directly if uncompressed)
        if self.compression.is_some() {
            self.parse_block_from_buffer(&mut &block_data[..])
        } else {
            self.read_block_direct(conn).await
        }
    }

    /// Read block directly from connection (uncompressed)
    async fn read_block_direct(&self, conn: &mut Connection) -> Result<Block> {
        let mut block = Block::new();

        // Read block info if supported
        if self.server_revision >= DBMS_MIN_REVISION_WITH_BLOCK_INFO {
            let info = self.read_block_info(conn).await?;
            block.set_info(info);
        }

        // Read column count and row count
        let num_columns = conn.read_varint().await? as usize;
        let num_rows = conn.read_varint().await? as usize;

        // Read each column
        for _ in 0..num_columns {
            let name = conn.read_string().await?;
            let type_name = conn.read_string().await?;

            // Check for custom serialization
            if self.server_revision >= DBMS_MIN_REVISION_WITH_CUSTOM_SERIALIZATION {
                let custom_len = conn.read_u8().await?;
                if custom_len > 0 {
                    return Err(Error::Protocol(
                        "Custom serialization not supported".to_string(),
                    ));
                }
            }

            // Parse the type
            let column_type = Type::parse(&type_name)?;

            // Create column and load data
            let column = self.create_column_by_type(&column_type)?;

            if num_rows > 0 {
                // Read column data directly from async stream
                // For uncompressed blocks, we can read data type by type
                self.load_column_data_async(conn, &column_type, num_rows).await?;
            }

            block.append_column(name, column)?;
        }

        Ok(block)
    }

    /// Load column data from async connection (for uncompressed blocks)
    fn load_column_data_async<'a>(
        &'a self,
        conn: &'a mut Connection,
        type_: &'a Type,
        num_rows: usize,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<()>> + 'a>> {
        Box::pin(async move {
            self.load_column_data_impl(conn, type_, num_rows).await
        })
    }

    /// Implementation of load_column_data_async
    async fn load_column_data_impl(&self, conn: &mut Connection, type_: &Type, num_rows: usize) -> Result<()> {
        use crate::types::TypeCode;

        match type_ {
            Type::Simple(code) => {
                match code {
                    // Fixed-size numeric types - read all bytes at once
                    TypeCode::UInt8 | TypeCode::Int8 => {
                        let _ = conn.read_bytes(num_rows * 1).await?;
                    }
                    TypeCode::UInt16 | TypeCode::Int16 | TypeCode::Date => {
                        let _ = conn.read_bytes(num_rows * 2).await?;
                    }
                    TypeCode::UInt32 | TypeCode::Int32 | TypeCode::Float32 | TypeCode::Date32 => {
                        let _ = conn.read_bytes(num_rows * 4).await?;
                    }
                    TypeCode::UInt64 | TypeCode::Int64 | TypeCode::Float64 => {
                        let _ = conn.read_bytes(num_rows * 8).await?;
                    }
                    // String - variable length, read each string
                    TypeCode::String => {
                        for _ in 0..num_rows {
                            let len = conn.read_varint().await? as usize;
                            let _ = conn.read_bytes(len).await?;
                        }
                    }
                    // Void/Nothing - skip bytes (1 byte per row)
                    TypeCode::Void => {
                        let _ = conn.read_bytes(num_rows).await?;
                    }
                    // UUID, IPv4, IPv6
                    TypeCode::UUID => {
                        let _ = conn.read_bytes(num_rows * 16).await?;
                    }
                    TypeCode::IPv4 => {
                        let _ = conn.read_bytes(num_rows * 4).await?;
                    }
                    TypeCode::IPv6 => {
                        let _ = conn.read_bytes(num_rows * 16).await?;
                    }
                    _ => {
                        return Err(Error::Protocol(format!(
                            "Uncompressed reading not implemented for type: {}",
                            type_.name()
                        )));
                    }
                }
            }
            Type::FixedString { size } => {
                // FixedString - read fixed bytes per row
                let _ = conn.read_bytes(num_rows * size).await?;
            }
            Type::DateTime { .. } => {
                // DateTime is stored as UInt32 (4 bytes)
                let _ = conn.read_bytes(num_rows * 4).await?;
            }
            Type::DateTime64 { .. } => {
                // DateTime64 is stored as Int64 (8 bytes)
                let _ = conn.read_bytes(num_rows * 8).await?;
            }
            Type::Enum8 { .. } => {
                // Enum8 is stored as Int8 (1 byte)
                let _ = conn.read_bytes(num_rows * 1).await?;
            }
            Type::Enum16 { .. } => {
                // Enum16 is stored as Int16 (2 bytes)
                let _ = conn.read_bytes(num_rows * 2).await?;
            }
            Type::Nullable { nested_type } => {
                // Read null mask first (one byte per row)
                let _ = conn.read_bytes(num_rows).await?;
                // Then read nested data (recursive call via boxed wrapper)
                self.load_column_data_async(conn, nested_type, num_rows).await?;
            }
            Type::Array { item_type: _ } => {
                // Read offsets array (one UInt64 per row)
                let _ = conn.read_bytes(num_rows * 8).await?;
                // Read total count of items from last offset
                // For simplicity, just try to read the nested column
                // This is approximate - we'd need to parse offsets properly
                // For now, return an error since arrays in uncompressed blocks are complex
                return Err(Error::Protocol(
                    "Uncompressed reading not fully implemented for Array types".to_string(),
                ));
            }
            _ => {
                return Err(Error::Protocol(format!(
                    "Uncompressed reading not implemented for complex type: {}",
                    type_.name()
                )));
            }
        }

        Ok(())
    }

    /// Read block info
    async fn read_block_info(&self, conn: &mut Connection) -> Result<BlockInfo> {
        let _num1 = conn.read_varint().await?;
        let is_overflows = conn.read_u8().await?;
        let _num2 = conn.read_varint().await?;
        let bucket_num = conn.read_i32().await?;
        let _num3 = conn.read_varint().await?;

        Ok(BlockInfo {
            is_overflows,
            bucket_num,
        })
    }

    /// Parse block from buffer (compressed data)
    fn parse_block_from_buffer(&self, buffer: &mut &[u8]) -> Result<Block> {
        let mut block = Block::new();

        // Read block info if supported
        if self.server_revision >= DBMS_MIN_REVISION_WITH_BLOCK_INFO {
            let info = self.read_block_info_from_buffer(buffer)?;
            block.set_info(info);
        }

        // Read column count and row count
        let num_columns = read_varint(buffer)? as usize;
        let num_rows = read_varint(buffer)? as usize;

        // Read each column
        for _ in 0..num_columns {
            let name = read_string(buffer)?;
            let type_name = read_string(buffer)?;

            // Check for custom serialization
            if self.server_revision >= DBMS_MIN_REVISION_WITH_CUSTOM_SERIALIZATION {
                if buffer.is_empty() {
                    return Err(Error::Protocol("Unexpected end of block data".to_string()));
                }
                let custom_len = buffer[0];
                buffer.advance(1);

                if custom_len > 0 {
                    return Err(Error::Protocol(
                        "Custom serialization not supported".to_string(),
                    ));
                }
            }

            // Parse the type
            let column_type = Type::parse(&type_name)?;

            // Create column and load data
            let mut column = self.create_column_by_type(&column_type)?;

            if num_rows > 0 {
                // Load column data from buffer
                Arc::get_mut(&mut column)
                    .ok_or_else(|| Error::Protocol("Column not mutable".to_string()))?
                    .load_from_buffer(buffer, num_rows)?;
            }

            block.append_column(name, column)?;
        }

        Ok(block)
    }

    /// Read block info from buffer
    fn read_block_info_from_buffer(&self, buffer: &mut &[u8]) -> Result<BlockInfo> {
        let _num1 = read_varint(buffer)?;

        if buffer.is_empty() {
            return Err(Error::Protocol("Unexpected end reading block info".to_string()));
        }
        let is_overflows = buffer[0];
        buffer.advance(1);

        let _num2 = read_varint(buffer)?;

        if buffer.len() < 4 {
            return Err(Error::Protocol("Unexpected end reading bucket_num".to_string()));
        }
        let bucket_num = i32::from_le_bytes([buffer[0], buffer[1], buffer[2], buffer[3]]);
        buffer.advance(4);

        let _num3 = read_varint(buffer)?;

        Ok(BlockInfo {
            is_overflows,
            bucket_num,
        })
    }

    /// Create a column by type
    fn create_column_by_type(&self, type_: &Type) -> Result<ColumnRef> {
        use crate::column::array::ColumnArray;
        use crate::column::nullable::ColumnNullable;
        use crate::column::numeric::*;
        use crate::column::string::{ColumnFixedString, ColumnString};

        match type_ {
            Type::Simple(code) => {
                use crate::types::TypeCode;
                match code {
                    TypeCode::UInt8 => Ok(Arc::new(ColumnUInt8::new(type_.clone()))),
                    TypeCode::UInt16 => Ok(Arc::new(ColumnUInt16::new(type_.clone()))),
                    TypeCode::UInt32 => Ok(Arc::new(ColumnUInt32::new(type_.clone()))),
                    TypeCode::UInt64 => Ok(Arc::new(ColumnUInt64::new(type_.clone()))),
                    TypeCode::Int8 => Ok(Arc::new(ColumnInt8::new(type_.clone()))),
                    TypeCode::Int16 => Ok(Arc::new(ColumnInt16::new(type_.clone()))),
                    TypeCode::Int32 => Ok(Arc::new(ColumnInt32::new(type_.clone()))),
                    TypeCode::Int64 => Ok(Arc::new(ColumnInt64::new(type_.clone()))),
                    TypeCode::Float32 => Ok(Arc::new(ColumnFloat32::new(type_.clone()))),
                    TypeCode::Float64 => Ok(Arc::new(ColumnFloat64::new(type_.clone()))),
                    TypeCode::String => Ok(Arc::new(ColumnString::new(type_.clone()))),
                    _ => Err(Error::Protocol(format!("Unsupported type: {}", type_.name()))),
                }
            }
            Type::FixedString { .. } => Ok(Arc::new(ColumnFixedString::new(type_.clone()))),
            Type::DateTime { .. } => {
                // DateTime is stored as UInt32 (Unix timestamp)
                Ok(Arc::new(ColumnUInt32::new(type_.clone())))
            }
            Type::DateTime64 { .. } => {
                // DateTime64 is stored as Int64 (Unix timestamp with precision)
                Ok(Arc::new(ColumnInt64::new(type_.clone())))
            }
            Type::Enum8 { .. } => {
                // Enum8 is stored as Int8
                Ok(Arc::new(ColumnInt8::new(type_.clone())))
            }
            Type::Enum16 { .. } => {
                // Enum16 is stored as Int16
                Ok(Arc::new(ColumnInt16::new(type_.clone())))
            }
            Type::Nullable { .. } => {
                Ok(Arc::new(ColumnNullable::new(type_.clone())))
            }
            Type::Array { .. } => {
                Ok(Arc::new(ColumnArray::new(type_.clone())))
            }
            _ => Err(Error::Protocol(format!(
                "Unsupported column type: {}",
                type_.name()
            ))),
        }
    }
}

/// Writer for blocks to network
pub struct BlockWriter {
    server_revision: u64,
    compression: Option<CompressionMethod>,
}

impl BlockWriter {
    /// Create a new block writer
    pub fn new(server_revision: u64) -> Self {
        Self {
            server_revision,
            compression: None,
        }
    }

    /// Enable compression
    pub fn with_compression(mut self, method: CompressionMethod) -> Self {
        self.compression = Some(method);
        self
    }

    /// Write a block to the connection
    pub async fn write_block(&self, conn: &mut Connection, block: &Block) -> Result<()> {
        eprintln!("[DEBUG] Writing block: {} columns, {} rows", block.column_count(), block.row_count());

        // Skip temporary table name if protocol supports it
        if self.server_revision >= DBMS_MIN_REVISION_WITH_TEMPORARY_TABLES {
            eprintln!("[DEBUG] Writing empty temp table name");
            conn.write_string("").await?;
        }

        // Serialize block to buffer
        let mut buffer = BytesMut::new();
        self.write_block_to_buffer(&mut buffer, block)?;
        eprintln!("[DEBUG] Block serialized to {} bytes", buffer.len());

        // Compress if needed
        if let Some(compression_method) = self.compression {
            let compressed = compress(compression_method, &buffer)?;
            eprintln!("[DEBUG] Compressed to {} bytes (includes 16-byte checksum + 9-byte header)", compressed.len());
            // Compressed data already includes checksum + header, write it directly
            conn.write_bytes(&compressed).await?;
        } else {
            // Write uncompressed
            eprintln!("[DEBUG] Writing uncompressed block");
            conn.write_bytes(&buffer).await?;
        }

        conn.flush().await?;
        eprintln!("[DEBUG] Block write complete");
        Ok(())
    }

    /// Write block to buffer
    fn write_block_to_buffer(&self, buffer: &mut BytesMut, block: &Block) -> Result<()> {
        // Write block info if supported
        if self.server_revision >= DBMS_MIN_REVISION_WITH_BLOCK_INFO {
            write_varint(buffer, 1);
            buffer.put_u8(block.info().is_overflows);
            write_varint(buffer, 2);
            buffer.put_i32_le(block.info().bucket_num);
            write_varint(buffer, 0);
        }

        // Write column count and row count
        write_varint(buffer, block.column_count() as u64);
        write_varint(buffer, block.row_count() as u64);

        // Write each column
        for (name, type_, column) in block.iter() {
            write_string(buffer, name);
            write_string(buffer, &type_.name());

            // Custom serialization flag
            if self.server_revision >= DBMS_MIN_REVISION_WITH_CUSTOM_SERIALIZATION {
                buffer.put_u8(0); // No custom serialization
            }

            // Write column data (only if rows > 0)
            if block.row_count() > 0 {
                column.save_to_buffer(buffer)?;
            }
        }

        Ok(())
    }
}

// Helper functions
fn read_varint(buffer: &mut &[u8]) -> Result<u64> {
    let mut result: u64 = 0;
    let mut shift = 0;

    loop {
        if buffer.is_empty() {
            return Err(Error::Protocol(
                "Unexpected end of buffer reading varint".to_string(),
            ));
        }

        let byte = buffer[0];
        buffer.advance(1);

        result |= ((byte & 0x7F) as u64) << shift;

        if byte & 0x80 == 0 {
            break;
        }

        shift += 7;
        if shift >= 64 {
            return Err(Error::Protocol("Varint overflow".to_string()));
        }
    }

    Ok(result)
}

fn write_varint(buffer: &mut BytesMut, mut value: u64) {
    loop {
        let mut byte = (value & 0x7F) as u8;
        value >>= 7;

        if value != 0 {
            byte |= 0x80;
        }

        buffer.put_u8(byte);

        if value == 0 {
            break;
        }
    }
}

fn read_string(buffer: &mut &[u8]) -> Result<String> {
    let len = read_varint(buffer)? as usize;

    if buffer.len() < len {
        return Err(Error::Protocol(format!(
            "Not enough data for string: need {}, have {}",
            len,
            buffer.len()
        )));
    }

    let string_data = &buffer[..len];
    let s = String::from_utf8(string_data.to_vec())
        .map_err(|e| Error::Protocol(format!("Invalid UTF-8 in string: {}", e)))?;

    buffer.advance(len);
    Ok(s)
}

fn write_string(buffer: &mut BytesMut, s: &str) {
    write_varint(buffer, s.len() as u64);
    buffer.put_slice(s.as_bytes());
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::column::numeric::ColumnUInt64;
    use crate::types::Type;

    #[test]
    fn test_block_writer_serialization() {
        let mut block = Block::new();

        let mut col = ColumnUInt64::new(Type::uint64());
        col.append(1);
        col.append(2);
        col.append(3);

        block.append_column("id", Arc::new(col)).unwrap();

        let writer = BlockWriter::new(54449);
        let mut buffer = BytesMut::new();

        writer.write_block_to_buffer(&mut buffer, &block).unwrap();

        // Verify buffer is not empty
        assert!(!buffer.is_empty());
    }

    #[test]
    fn test_block_reader_parser() {
        // Create a block
        let mut block = Block::new();

        let mut col = ColumnUInt64::new(Type::uint64());
        col.append(42);
        col.append(100);

        block.append_column("test_col", Arc::new(col)).unwrap();

        // Serialize it
        let writer = BlockWriter::new(54449);
        let mut buffer = BytesMut::new();
        writer.write_block_to_buffer(&mut buffer, &block).unwrap();

        // Deserialize it
        let reader = BlockReader::new(54449);
        let mut read_buffer = &buffer[..];
        let decoded_block = reader.parse_block_from_buffer(&mut read_buffer).unwrap();

        assert_eq!(decoded_block.column_count(), 1);
        assert_eq!(decoded_block.row_count(), 2);
        assert_eq!(decoded_block.column_name(0), Some("test_col"));
    }

    #[test]
    fn test_block_roundtrip_multiple_columns() {
        let mut block = Block::new();

        let mut col1 = ColumnUInt64::new(Type::uint64());
        col1.append(1);
        col1.append(2);

        let mut col2 = ColumnUInt64::new(Type::uint64());
        col2.append(100);
        col2.append(200);

        block.append_column("id", Arc::new(col1)).unwrap();
        block.append_column("value", Arc::new(col2)).unwrap();

        // Serialize
        let writer = BlockWriter::new(54449);
        let mut buffer = BytesMut::new();
        writer.write_block_to_buffer(&mut buffer, &block).unwrap();

        // Deserialize
        let reader = BlockReader::new(54449);
        let mut read_buffer = &buffer[..];
        let decoded = reader.parse_block_from_buffer(&mut read_buffer).unwrap();

        assert_eq!(decoded.column_count(), 2);
        assert_eq!(decoded.row_count(), 2);
    }
}
