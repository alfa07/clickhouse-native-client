use crate::{
    protocol::CompressionMethod,
    Error,
    Result,
};
use bytes::{
    Buf,
    BufMut,
    Bytes,
    BytesMut,
};
use cityhash_rs::cityhash_102_128;

/// Compression header size (9 bytes: 1 byte method + 4 bytes compressed + 4
/// bytes uncompressed)
const HEADER_SIZE: usize = 9;

/// Checksum size (16 bytes for CityHash128)
const CHECKSUM_SIZE: usize = 16;

/// Compression method byte values (from ClickHouse CompressionMethodByte)
#[repr(u8)]
enum CompressionMethodByte {
    None = 0x02,
    LZ4 = 0x82,
    ZSTD = 0x90,
}

/// Maximum compressed block size (1GB)
const MAX_COMPRESSED_SIZE: usize = 0x40000000;

/// Compress data using the specified method
pub fn compress(method: CompressionMethod, data: &[u8]) -> Result<Bytes> {
    match method {
        CompressionMethod::None => {
            // No compression, but still add header
            compress_none(data)
        }
        CompressionMethod::LZ4 => compress_lz4(data),
        CompressionMethod::ZSTD => compress_zstd(data),
    }
}

/// Decompress data (auto-detects compression method from header)
pub fn decompress(data: &[u8]) -> Result<Bytes> {
    if data.len() < CHECKSUM_SIZE + HEADER_SIZE {
        return Err(Error::Compression(
            "Data too small for checksum and compression header".to_string(),
        ));
    }

    // Skip checksum (first 16 bytes) - we could verify it but for now we trust
    // the TCP layer
    let data_without_checksum = &data[CHECKSUM_SIZE..];

    let method = data_without_checksum[0];
    let mut reader = &data_without_checksum[1..];

    // Read compressed size (4 bytes) and uncompressed size (4 bytes)
    let compressed_size = reader.get_u32_le() as usize;
    let uncompressed_size = reader.get_u32_le() as usize;

    // Validate sizes
    if compressed_size > MAX_COMPRESSED_SIZE {
        return Err(Error::Compression(format!(
            "Compressed size too large: {}",
            compressed_size
        )));
    }

    if uncompressed_size > MAX_COMPRESSED_SIZE {
        return Err(Error::Compression(format!(
            "Uncompressed size too large: {}",
            uncompressed_size
        )));
    }

    // The remaining data after header
    let compressed_data = &data_without_checksum[HEADER_SIZE..];

    match method {
        0x02 => {
            // No compression
            if compressed_data.len() != uncompressed_size {
                return Err(Error::Compression(format!(
                    "Uncompressed data size mismatch: expected {}, got {}",
                    uncompressed_size,
                    compressed_data.len()
                )));
            }
            Ok(Bytes::copy_from_slice(compressed_data))
        }
        0x82 => {
            // LZ4
            decompress_lz4(compressed_data, uncompressed_size)
        }
        0x90 => {
            // ZSTD
            decompress_zstd(compressed_data, uncompressed_size)
        }
        _ => Err(Error::Compression(format!(
            "Unknown compression method: 0x{:02x}",
            method
        ))),
    }
}

/// Compress using LZ4
fn compress_lz4(data: &[u8]) -> Result<Bytes> {
    let max_compressed_size = lz4::block::compress_bound(data.len())?;
    let mut compressed = vec![0u8; max_compressed_size];

    let compressed_size =
        lz4::block::compress_to_buffer(data, None, false, &mut compressed)?;

    compressed.truncate(compressed_size);

    // Build header + compressed data
    let mut header_and_data =
        BytesMut::with_capacity(HEADER_SIZE + compressed_size);

    // Write header
    header_and_data.put_u8(CompressionMethodByte::LZ4 as u8);
    header_and_data.put_u32_le((HEADER_SIZE + compressed_size) as u32); // Total size including header
    header_and_data.put_u32_le(data.len() as u32); // Uncompressed size

    // Write compressed data
    header_and_data.put_slice(&compressed);

    // Compute CityHash128 checksum of header + compressed data
    let checksum = cityhash_102_128(&header_and_data);

    // Build final output with checksum
    // CityHash128 returns u128, write as (high64, low64) - reverse of typical
    // order
    let mut output =
        BytesMut::with_capacity(CHECKSUM_SIZE + header_and_data.len());
    output.put_u64_le((checksum >> 64) as u64); // High 64 bits first
    output.put_u64_le(checksum as u64); // Low 64 bits second
    output.put_slice(&header_and_data);

    Ok(output.freeze())
}

/// Decompress LZ4 data
fn decompress_lz4(data: &[u8], uncompressed_size: usize) -> Result<Bytes> {
    let decompressed =
        lz4::block::decompress(data, Some(uncompressed_size as i32))?;

    if decompressed.len() != uncompressed_size {
        return Err(Error::Compression(format!(
            "LZ4 decompression size mismatch: expected {}, got {}",
            uncompressed_size,
            decompressed.len()
        )));
    }

    Ok(Bytes::from(decompressed))
}

/// Compress using ZSTD
fn compress_zstd(data: &[u8]) -> Result<Bytes> {
    let compressed = zstd::bulk::compress(data, 3) // Compression level 3
        .map_err(|e| {
            Error::Compression(format!("ZSTD compression failed: {}", e))
        })?;

    // Build header + compressed data
    let mut header_and_data =
        BytesMut::with_capacity(HEADER_SIZE + compressed.len());

    // Write header
    header_and_data.put_u8(CompressionMethodByte::ZSTD as u8);
    header_and_data.put_u32_le((HEADER_SIZE + compressed.len()) as u32); // Total size including header
    header_and_data.put_u32_le(data.len() as u32); // Uncompressed size

    // Write compressed data
    header_and_data.put_slice(&compressed);

    // Compute CityHash128 checksum of header + compressed data
    let checksum = cityhash_102_128(&header_and_data);

    // Build final output with checksum
    // CityHash128 returns u128, write as (high64, low64) - reverse of typical
    // order
    let mut output =
        BytesMut::with_capacity(CHECKSUM_SIZE + header_and_data.len());
    output.put_u64_le((checksum >> 64) as u64); // High 64 bits first
    output.put_u64_le(checksum as u64); // Low 64 bits second
    output.put_slice(&header_and_data);

    Ok(output.freeze())
}

/// Decompress ZSTD data
fn decompress_zstd(data: &[u8], uncompressed_size: usize) -> Result<Bytes> {
    let decompressed = zstd::bulk::decompress(data, uncompressed_size)
        .map_err(|e| {
            Error::Compression(format!("ZSTD decompression failed: {}", e))
        })?;

    if decompressed.len() != uncompressed_size {
        return Err(Error::Compression(format!(
            "ZSTD decompression size mismatch: expected {}, got {}",
            uncompressed_size,
            decompressed.len()
        )));
    }

    Ok(Bytes::from(decompressed))
}

/// No compression (just adds header)
fn compress_none(data: &[u8]) -> Result<Bytes> {
    // Build header + data
    let mut header_and_data =
        BytesMut::with_capacity(HEADER_SIZE + data.len());

    // Write header
    header_and_data.put_u8(CompressionMethodByte::None as u8);
    header_and_data.put_u32_le((HEADER_SIZE + data.len()) as u32); // Total size
    header_and_data.put_u32_le(data.len() as u32); // Uncompressed size (same as total)

    // Write uncompressed data
    header_and_data.put_slice(data);

    // Compute CityHash128 checksum of header + data
    let checksum = cityhash_102_128(&header_and_data);

    // Build final output with checksum
    let mut output =
        BytesMut::with_capacity(CHECKSUM_SIZE + header_and_data.len());
    output.put_u128_le(checksum); // CityHash128 as little-endian u128
    output.put_slice(&header_and_data);

    Ok(output.freeze())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_compress_decompress_none() {
        let original = b"Hello, ClickHouse!";

        let compressed = compress(CompressionMethod::None, original).unwrap();
        let decompressed = decompress(&compressed).unwrap();

        assert_eq!(&decompressed[..], original);
    }

    #[test]
    fn test_compress_decompress_lz4() {
        let original = b"Hello, ClickHouse! ".repeat(100);

        let compressed = compress(CompressionMethod::LZ4, &original).unwrap();
        let decompressed = decompress(&compressed).unwrap();

        assert_eq!(&decompressed[..], &original[..]);

        // Should achieve some compression
        assert!(compressed.len() < original.len());
    }

    #[test]
    fn test_compress_decompress_zstd() {
        let original =
            b"ClickHouse is a fast open-source column-oriented database"
                .repeat(50);

        let compressed = compress(CompressionMethod::ZSTD, &original).unwrap();
        let decompressed = decompress(&compressed).unwrap();

        assert_eq!(&decompressed[..], &original[..]);

        // Should achieve good compression
        assert!(compressed.len() < original.len());
    }

    #[test]
    fn test_empty_data() {
        let original = b"";

        // Should work with empty data
        let compressed = compress(CompressionMethod::LZ4, original).unwrap();
        let decompressed = decompress(&compressed).unwrap();

        assert_eq!(&decompressed[..], original);
    }

    #[test]
    fn test_large_data_lz4() {
        // Test with larger data
        let original = vec![42u8; 100_000];

        let compressed = compress(CompressionMethod::LZ4, &original).unwrap();
        let decompressed = decompress(&compressed).unwrap();

        assert_eq!(&decompressed[..], &original[..]);

        // Should compress very well (all same byte)
        assert!(compressed.len() < original.len() / 10);
    }

    #[test]
    fn test_invalid_compression_method() {
        let mut bad_data = vec![0xFFu8; 20]; // Invalid method byte
        bad_data[1..5].copy_from_slice(&20u32.to_le_bytes()); // compressed size
        bad_data[5..9].copy_from_slice(&10u32.to_le_bytes()); // uncompressed size

        let result = decompress(&bad_data);
        assert!(result.is_err());
    }

    #[test]
    fn test_header_too_small() {
        let bad_data = vec![0x82, 1, 2, 3]; // Only 4 bytes, need 9

        let result = decompress(&bad_data);
        assert!(result.is_err());
    }
}
