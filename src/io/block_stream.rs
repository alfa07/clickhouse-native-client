use crate::{
    block::{
        Block,
        BlockInfo,
    },
    column::ColumnRef,
    compression::{
        compress,
        decompress,
    },
    connection::Connection,
    io::buffer_utils,
    protocol::CompressionMethod,
    types::Type,
    Error,
    Result,
};
use bytes::{
    Buf,
    BufMut,
    BytesMut,
};
use std::sync::Arc;
use tracing::debug;

/// Minimum revision constants
const DBMS_MIN_REVISION_WITH_TEMPORARY_TABLES: u64 = 50264;
const DBMS_MIN_REVISION_WITH_BLOCK_INFO: u64 = 51903;
const DBMS_MIN_REVISION_WITH_CUSTOM_SERIALIZATION: u64 = 54454;

/// Create a column instance for the given type
/// This is used internally by column types like Array and Nullable
pub fn create_column(type_: &Type) -> Result<ColumnRef> {
    use crate::column::{
        array::ColumnArray,
        date::{
            ColumnDate,
            ColumnDate32,
            ColumnDateTime,
            ColumnDateTime64,
        },
        decimal::ColumnDecimal,
        enum_column::{
            ColumnEnum16,
            ColumnEnum8,
        },
        ipv4::ColumnIpv4,
        ipv6::ColumnIpv6,
        lowcardinality::ColumnLowCardinality,
        map::ColumnMap,
        nothing::ColumnNothing,
        nullable::ColumnNullable,
        numeric::*,
        string::{
            ColumnFixedString,
            ColumnString,
        },
        uuid::ColumnUuid,
    };

    match type_ {
        Type::Simple(code) => {
            use crate::types::TypeCode;
            match code {
                TypeCode::UInt8 => Ok(Arc::new(ColumnUInt8::new())),
                TypeCode::UInt16 => Ok(Arc::new(ColumnUInt16::new())),
                TypeCode::UInt32 => Ok(Arc::new(ColumnUInt32::new())),
                TypeCode::UInt64 => Ok(Arc::new(ColumnUInt64::new())),
                TypeCode::UInt128 => Ok(Arc::new(ColumnUInt128::new())),
                TypeCode::Int8 => Ok(Arc::new(ColumnInt8::new())),
                TypeCode::Int16 => Ok(Arc::new(ColumnInt16::new())),
                TypeCode::Int32 => Ok(Arc::new(ColumnInt32::new())),
                TypeCode::Int64 => Ok(Arc::new(ColumnInt64::new())),
                TypeCode::Int128 => Ok(Arc::new(ColumnInt128::new())),
                TypeCode::Float32 => Ok(Arc::new(ColumnFloat32::new())),
                TypeCode::Float64 => Ok(Arc::new(ColumnFloat64::new())),
                TypeCode::String => {
                    Ok(Arc::new(ColumnString::new(type_.clone())))
                }
                TypeCode::Date => Ok(Arc::new(ColumnDate::new(type_.clone()))),
                TypeCode::Date32 => {
                    Ok(Arc::new(ColumnDate32::new(type_.clone())))
                }
                TypeCode::UUID => Ok(Arc::new(ColumnUuid::new(type_.clone()))),
                TypeCode::IPv4 => Ok(Arc::new(ColumnIpv4::new(type_.clone()))),
                TypeCode::IPv6 => Ok(Arc::new(ColumnIpv6::new(type_.clone()))),
                TypeCode::Void => {
                    Ok(Arc::new(ColumnNothing::new(type_.clone())))
                }
                // Geo types are compound types built from Tuple and Array
                // They use the same column implementation but preserve the geo
                // type name
                TypeCode::Point => {
                    // Point is Tuple(Float64, Float64)
                    let columns: Vec<ColumnRef> = vec![
                        Arc::new(ColumnFloat64::new()) as ColumnRef,
                        Arc::new(ColumnFloat64::new()) as ColumnRef,
                    ];
                    Ok(Arc::new(crate::column::ColumnTuple::new(
                        type_.clone(),
                        columns,
                    )))
                }
                TypeCode::Ring => {
                    // Ring is Array(Point) - manually create with Point nested
                    // type
                    let point_type = Type::Simple(TypeCode::Point);
                    let nested = create_column(&point_type)?;
                    Ok(Arc::new(ColumnArray::from_parts(
                        type_.clone(),
                        nested,
                    )))
                }
                TypeCode::Polygon => {
                    // Polygon is Array(Ring) - manually create with Ring
                    // nested type
                    let ring_type = Type::Simple(TypeCode::Ring);
                    let nested = create_column(&ring_type)?;
                    Ok(Arc::new(ColumnArray::from_parts(
                        type_.clone(),
                        nested,
                    )))
                }
                TypeCode::MultiPolygon => {
                    // MultiPolygon is Array(Polygon) - manually create with
                    // Polygon nested type
                    let polygon_type = Type::Simple(TypeCode::Polygon);
                    let nested = create_column(&polygon_type)?;
                    Ok(Arc::new(ColumnArray::from_parts(
                        type_.clone(),
                        nested,
                    )))
                }
                _ => Err(Error::Protocol(format!(
                    "Unsupported type: {}",
                    type_.name()
                ))),
            }
        }
        Type::FixedString { .. } => {
            Ok(Arc::new(ColumnFixedString::new(type_.clone())))
        }
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
        Type::Array { .. } => Ok(Arc::new(ColumnArray::new(type_.clone()))),
        Type::Map { .. } => Ok(Arc::new(ColumnMap::new(type_.clone()))),
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
        Self { server_revision, compression: None }
    }

    /// Enable compression
    pub fn with_compression(mut self, method: CompressionMethod) -> Self {
        self.compression = Some(method);
        self
    }

    /// Read and decompress a single compressed frame from the connection.
    async fn read_compressed_frame(
        &self,
        conn: &mut Connection,
    ) -> Result<bytes::Bytes> {
        let checksum = conn.read_bytes(16).await?;
        let method = conn.read_u8().await?;
        let compressed_size = conn.read_u32().await? as usize;
        let uncompressed_size = conn.read_u32().await?;

        let compressed_data_len = compressed_size.saturating_sub(9);
        let compressed_data = conn.read_bytes(compressed_data_len).await?;

        let mut full_block =
            BytesMut::with_capacity(16 + 9 + compressed_data_len);
        full_block.extend_from_slice(&checksum);
        full_block.put_u8(method);
        full_block.put_u32_le(compressed_size as u32);
        full_block.put_u32_le(uncompressed_size);
        full_block.extend_from_slice(&compressed_data);

        decompress(&full_block)
    }

    /// Read a block from the connection.
    ///
    /// For compressed connections, ClickHouse may split a single logical
    /// block across multiple compressed frames (each frame ≤
    /// max_compress_block_size, typically 1 MB). This method reads frames
    /// until the accumulated decompressed data forms a complete block.
    ///
    /// Note: Caller is responsible for skipping temp table name if needed
    /// (matches C++ ReadBlock / CompressedInput).
    pub async fn read_block(&self, conn: &mut Connection) -> Result<Block> {
        if self.compression.is_none() {
            return self.read_block_direct(conn).await;
        }

        let mut accumulated: Vec<u8> = Vec::new();
        const MAX_FRAMES: usize = 4096;

        for _ in 0..MAX_FRAMES {
            let frame = self.read_compressed_frame(conn).await?;
            accumulated.extend_from_slice(&frame);

            let mut slice: &[u8] = &accumulated;
            match self.parse_block_from_buffer(&mut slice) {
                Ok(block) => return Ok(block),
                Err(e) => {
                    let msg = e.to_string();
                    let is_underflow = msg.contains("Not enough data")
                        || msg.contains("Buffer underflow")
                        || msg.contains("Unexpected end");
                    if !is_underflow {
                        return Err(e);
                    }
                }
            }
        }

        Err(Error::Protocol(
            "Compressed block exceeded maximum frame count".to_string(),
        ))
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
            if self.server_revision
                >= DBMS_MIN_REVISION_WITH_CUSTOM_SERIALIZATION
            {
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
                self.load_column_data_async(conn, &column_type, num_rows)
                    .await?;
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
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<()>> + 'a>>
    {
        Box::pin(async move {
            self.load_column_data_impl(conn, type_, num_rows).await
        })
    }

    /// Implementation of load_column_data_async
    async fn load_column_data_impl(
        &self,
        conn: &mut Connection,
        type_: &Type,
        num_rows: usize,
    ) -> Result<()> {
        use crate::types::TypeCode;

        // Try to use the storage_size_bytes helper for fixed-size types
        if let Some(size_per_row) = type_.storage_size_bytes() {
            // Fixed-size type - read all rows at once
            let _ = conn.read_bytes(num_rows * size_per_row).await?;
            return Ok(());
        }

        // Handle variable-length and complex types
        match type_ {
            Type::Simple(TypeCode::String) => {
                // String - variable length, read each string
                for _ in 0..num_rows {
                    let len = conn.read_varint().await? as usize;
                    let _ = conn.read_bytes(len).await?;
                }
            }
            Type::Nullable { nested_type } => {
                // Read null mask first (one byte per row)
                let _ = conn.read_bytes(num_rows).await?;
                // Then read nested data (recursive call via boxed wrapper)
                self.load_column_data_async(conn, nested_type, num_rows)
                    .await?;
            }
            Type::Array { item_type } => {
                // Array wire format:
                // 1. Offsets array (UInt64 per row, cumulative counts)
                // 2. Nested data (item_type × total_items)

                if num_rows == 0 {
                    return Ok(());
                }

                // Read offsets array (UInt64 per row)
                let offsets_data = conn.read_bytes(num_rows * 8).await?;

                // Parse the last offset to get total item count
                // Offsets are cumulative, so last offset = total items
                let last_offset_bytes =
                    &offsets_data[offsets_data.len() - 8..];
                let total_items = u64::from_le_bytes([
                    last_offset_bytes[0],
                    last_offset_bytes[1],
                    last_offset_bytes[2],
                    last_offset_bytes[3],
                    last_offset_bytes[4],
                    last_offset_bytes[5],
                    last_offset_bytes[6],
                    last_offset_bytes[7],
                ]) as usize;

                // Recursively read nested column data
                if total_items > 0 {
                    self.load_column_data_async(conn, item_type, total_items)
                        .await?;
                }
            }
            Type::Tuple { item_types } => {
                // Tuple wire format: each element serialized sequentially
                // Read each tuple element's column data
                for item_type in item_types {
                    self.load_column_data_async(conn, item_type, num_rows)
                        .await?;
                }
            }
            Type::Map { key_type, value_type } => {
                // Map wire format is Array(Tuple(K, V))
                // We read it as: offsets array + tuple data

                if num_rows == 0 {
                    return Ok(());
                }

                // Read offsets array (UInt64 per row)
                let offsets_data = conn.read_bytes(num_rows * 8).await?;

                // Parse the last offset to get total number of map entries
                let last_offset_bytes =
                    &offsets_data[offsets_data.len() - 8..];
                let total_entries = u64::from_le_bytes([
                    last_offset_bytes[0],
                    last_offset_bytes[1],
                    last_offset_bytes[2],
                    last_offset_bytes[3],
                    last_offset_bytes[4],
                    last_offset_bytes[5],
                    last_offset_bytes[6],
                    last_offset_bytes[7],
                ]) as usize;

                // Read tuple data: key column + value column
                if total_entries > 0 {
                    // Read key column
                    self.load_column_data_async(conn, key_type, total_entries)
                        .await?;
                    // Read value column
                    self.load_column_data_async(
                        conn,
                        value_type,
                        total_entries,
                    )
                    .await?;
                }
            }
            Type::FixedString { size } => {
                // FixedString - fixed size per row
                let _ = conn.read_bytes(num_rows * size).await?;
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
    async fn read_block_info(
        &self,
        conn: &mut Connection,
    ) -> Result<BlockInfo> {
        let _num1 = conn.read_varint().await?;
        let is_overflows = conn.read_u8().await?;
        let _num2 = conn.read_varint().await?;
        let bucket_num = conn.read_i32().await?;
        let _num3 = conn.read_varint().await?;

        Ok(BlockInfo { is_overflows, bucket_num })
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
        let num_columns = buffer_utils::read_varint(buffer)? as usize;
        let num_rows = buffer_utils::read_varint(buffer)? as usize;

        // Read each column
        for _ in 0..num_columns {
            let name = buffer_utils::read_string(buffer)?;
            let type_name = buffer_utils::read_string(buffer)?;

            // Check for custom serialization
            if self.server_revision
                >= DBMS_MIN_REVISION_WITH_CUSTOM_SERIALIZATION
            {
                if buffer.is_empty() {
                    return Err(Error::Protocol(
                        "Unexpected end of block data".to_string(),
                    ));
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
                let column_mut =
                    Arc::get_mut(&mut column).ok_or_else(|| {
                        Error::Protocol("Column not mutable".to_string())
                    })?;

                // Load prefix data first (for LowCardinality, etc.)
                column_mut.load_prefix(buffer, num_rows)?;

                // Load column body data
                column_mut.load_from_buffer(buffer, num_rows)?;
            }

            block.append_column(name, column)?;
        }

        Ok(block)
    }

    /// Read block info from buffer
    fn read_block_info_from_buffer(
        &self,
        buffer: &mut &[u8],
    ) -> Result<BlockInfo> {
        let _num1 = buffer_utils::read_varint(buffer)?;

        if buffer.is_empty() {
            return Err(Error::Protocol(
                "Unexpected end reading block info".to_string(),
            ));
        }
        let is_overflows = buffer[0];
        buffer.advance(1);

        let _num2 = buffer_utils::read_varint(buffer)?;

        if buffer.len() < 4 {
            return Err(Error::Protocol(
                "Unexpected end reading bucket_num".to_string(),
            ));
        }
        let bucket_num =
            i32::from_le_bytes([buffer[0], buffer[1], buffer[2], buffer[3]]);
        buffer.advance(4);

        let _num3 = buffer_utils::read_varint(buffer)?;

        Ok(BlockInfo { is_overflows, bucket_num })
    }

    /// Create a column by type
    fn create_column_by_type(&self, type_: &Type) -> Result<ColumnRef> {
        use crate::column::{
            array::ColumnArray,
            date::{
                ColumnDate,
                ColumnDate32,
                ColumnDateTime,
                ColumnDateTime64,
            },
            decimal::ColumnDecimal,
            enum_column::{
                ColumnEnum16,
                ColumnEnum8,
            },
            ipv4::ColumnIpv4,
            ipv6::ColumnIpv6,
            lowcardinality::ColumnLowCardinality,
            map::ColumnMap,
            nothing::ColumnNothing,
            nullable::ColumnNullable,
            numeric::*,
            string::{
                ColumnFixedString,
                ColumnString,
            },
            uuid::ColumnUuid,
        };

        match type_ {
            Type::Simple(code) => {
                use crate::types::TypeCode;
                match code {
                    TypeCode::UInt8 => Ok(Arc::new(ColumnUInt8::new())),
                    TypeCode::UInt16 => Ok(Arc::new(ColumnUInt16::new())),
                    TypeCode::UInt32 => Ok(Arc::new(ColumnUInt32::new())),
                    TypeCode::UInt64 => Ok(Arc::new(ColumnUInt64::new())),
                    TypeCode::UInt128 => Ok(Arc::new(ColumnUInt128::new())),
                    TypeCode::Int8 => Ok(Arc::new(ColumnInt8::new())),
                    TypeCode::Int16 => Ok(Arc::new(ColumnInt16::new())),
                    TypeCode::Int32 => Ok(Arc::new(ColumnInt32::new())),
                    TypeCode::Int64 => Ok(Arc::new(ColumnInt64::new())),
                    TypeCode::Int128 => Ok(Arc::new(ColumnInt128::new())),
                    TypeCode::Float32 => Ok(Arc::new(ColumnFloat32::new())),
                    TypeCode::Float64 => Ok(Arc::new(ColumnFloat64::new())),
                    TypeCode::String => {
                        Ok(Arc::new(ColumnString::new(type_.clone())))
                    }
                    TypeCode::Date => {
                        Ok(Arc::new(ColumnDate::new(type_.clone())))
                    }
                    TypeCode::Date32 => {
                        Ok(Arc::new(ColumnDate32::new(type_.clone())))
                    }
                    TypeCode::UUID => {
                        Ok(Arc::new(ColumnUuid::new(type_.clone())))
                    }
                    TypeCode::IPv4 => {
                        Ok(Arc::new(ColumnIpv4::new(type_.clone())))
                    }
                    TypeCode::IPv6 => {
                        Ok(Arc::new(ColumnIpv6::new(type_.clone())))
                    }
                    TypeCode::Void => {
                        Ok(Arc::new(ColumnNothing::new(type_.clone())))
                    }
                    _ => Err(Error::Protocol(format!(
                        "Unsupported type: {}",
                        type_.name()
                    ))),
                }
            }
            Type::FixedString { .. } => {
                Ok(Arc::new(ColumnFixedString::new(type_.clone())))
            }
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
            Type::Map { .. } => Ok(Arc::new(ColumnMap::new(type_.clone()))),
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
}

/// Writer for blocks to network
pub struct BlockWriter {
    server_revision: u64,
    compression: Option<CompressionMethod>,
}

impl BlockWriter {
    /// Create a new block writer
    pub fn new(server_revision: u64) -> Self {
        Self { server_revision, compression: None }
    }

    /// Enable compression
    pub fn with_compression(mut self, method: CompressionMethod) -> Self {
        self.compression = Some(method);
        self
    }

    /// Write a block to the connection
    pub async fn write_block(
        &self,
        conn: &mut Connection,
        block: &Block,
    ) -> Result<()> {
        self.write_block_with_temp_table(conn, block, true).await
    }

    /// Write a block to the connection (with optional temp table name)
    ///
    /// If `write_temp_table_name` is true, writes an empty temp table name
    /// before the block. For external tables, set to false since the table
    /// name was already written.
    pub async fn write_block_with_temp_table(
        &self,
        conn: &mut Connection,
        block: &Block,
        write_temp_table_name: bool,
    ) -> Result<()> {
        debug!(
            "Writing block: {} columns, {} rows",
            block.column_count(),
            block.row_count()
        );

        // Optionally write temporary table name if protocol supports it
        if write_temp_table_name
            && self.server_revision >= DBMS_MIN_REVISION_WITH_TEMPORARY_TABLES
        {
            debug!("Writing empty temp table name");
            conn.write_string("").await?;
        }

        // Serialize block to buffer
        let mut buffer = BytesMut::new();
        self.write_block_to_buffer(&mut buffer, block)?;
        debug!("Block serialized to {} bytes", buffer.len());

        // Compress if needed
        if let Some(compression_method) = self.compression {
            let compressed = compress(compression_method, &buffer)?;
            debug!("Compressed to {} bytes (includes 16-byte checksum + 9-byte header)", compressed.len());
            // Compressed data already includes checksum + header, write it
            // directly
            conn.write_bytes(&compressed).await?;
        } else {
            // Write uncompressed
            debug!("Writing uncompressed block");
            conn.write_bytes(&buffer).await?;
        }

        conn.flush().await?;
        debug!("Block write complete");
        Ok(())
    }

    /// Write block to buffer
    fn write_block_to_buffer(
        &self,
        buffer: &mut BytesMut,
        block: &Block,
    ) -> Result<()> {
        // Write block info if supported
        if self.server_revision >= DBMS_MIN_REVISION_WITH_BLOCK_INFO {
            buffer_utils::write_varint(buffer, 1);
            buffer.put_u8(block.info().is_overflows);
            buffer_utils::write_varint(buffer, 2);
            buffer.put_i32_le(block.info().bucket_num);
            buffer_utils::write_varint(buffer, 0);
        }

        // Write column count and row count
        buffer_utils::write_varint(buffer, block.column_count() as u64);
        buffer_utils::write_varint(buffer, block.row_count() as u64);

        // Write each column
        for (name, type_, column) in block.iter() {
            buffer_utils::write_string(buffer, name);
            buffer_utils::write_string(buffer, &type_.name());

            // Custom serialization flag
            if self.server_revision
                >= DBMS_MIN_REVISION_WITH_CUSTOM_SERIALIZATION
            {
                buffer.put_u8(0); // No custom serialization
            }

            // Write column data (only if rows > 0)
            if block.row_count() > 0 {
                column.save_prefix(buffer)?; // Phase 1: Write prefix data (for LowCardinality, etc.)
                column.save_to_buffer(buffer)?; // Phase 2: Write body data
            }
        }

        Ok(())
    }
}

// Helper functions - now using centralized buffer_utils
// (Functions removed - using buffer_utils::{read_varint, write_varint,
// read_string, write_string})

#[cfg(test)]
#[cfg_attr(coverage_nightly, coverage(off))]
mod tests {
    use super::*;
    use crate::column::numeric::ColumnUInt64;

    #[test]
    fn test_block_writer_serialization() {
        let mut block = Block::new();

        let mut col = ColumnUInt64::new();
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

        let mut col = ColumnUInt64::new();
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
        let decoded_block =
            reader.parse_block_from_buffer(&mut read_buffer).unwrap();

        assert_eq!(decoded_block.column_count(), 1);
        assert_eq!(decoded_block.row_count(), 2);
        assert_eq!(decoded_block.column_name(0), Some("test_col"));
    }

    #[test]
    fn test_block_roundtrip_multiple_columns() {
        let mut block = Block::new();

        let mut col1 = ColumnUInt64::new();
        col1.append(1);
        col1.append(2);

        let mut col2 = ColumnUInt64::new();
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
        let decoded =
            reader.parse_block_from_buffer(&mut read_buffer).unwrap();

        assert_eq!(decoded.column_count(), 2);
        assert_eq!(decoded.row_count(), 2);
    }
}
