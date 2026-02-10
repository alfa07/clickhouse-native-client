use clickhouse_native_client::{
    column::{
        geo::*,
        *,
    },
    types::{
        Type,
        TypeCode,
    },
};

// ============================================================================
// Map Column Tests
// ============================================================================

#[test]
fn test_map_creation() {
    let map_type = Type::Map {
        key_type: Box::new(Type::Simple(TypeCode::String)),
        value_type: Box::new(Type::Simple(TypeCode::UInt32)),
    };

    let col = ColumnMap::new(map_type);
    assert_eq!(col.len(), 0);
    assert!(col.is_empty());
}

#[test]
fn test_map_type_check() {
    let map_type = Type::Map {
        key_type: Box::new(Type::Simple(TypeCode::String)),
        value_type: Box::new(Type::Simple(TypeCode::UInt32)),
    };

    let col = ColumnMap::new(map_type.clone());
    assert_eq!(col.column_type().name(), map_type.name());
}

#[test]
fn test_map_underlying_data() {
    let map_type = Type::Map {
        key_type: Box::new(Type::Simple(TypeCode::String)),
        value_type: Box::new(Type::Simple(TypeCode::UInt64)),
    };

    let col = ColumnMap::new(map_type);
    let data: &ColumnArray = col.data();
    assert_eq!(data.size(), 0);
}

#[test]
fn test_map_clone() {
    let map_type = Type::Map {
        key_type: Box::new(Type::Simple(TypeCode::Int32)),
        value_type: Box::new(Type::Simple(TypeCode::String)),
    };

    let col1 = ColumnMap::new(map_type);
    let col2 = col1.clone();

    assert_eq!(col1.len(), col2.len());
    assert_eq!(col1.column_type().name(), col2.column_type().name());
}

#[test]
fn test_map_clear() {
    let map_type = Type::Map {
        key_type: Box::new(Type::Simple(TypeCode::String)),
        value_type: Box::new(Type::Simple(TypeCode::UInt32)),
    };

    let mut col = ColumnMap::new(map_type);
    col.clear();

    assert_eq!(col.len(), 0);
    assert!(col.is_empty());
}

// ============================================================================
// LowCardinality Column Tests
// ============================================================================

#[test]
fn test_lowcardinality_creation() {
    let lc_type = Type::LowCardinality {
        nested_type: Box::new(Type::Simple(TypeCode::String)),
    };

    let col = ColumnLowCardinality::new(lc_type);
    assert_eq!(col.len(), 0);
    assert!(col.is_empty());
    assert_eq!(col.dictionary_size(), 0);
}

#[test]
fn test_lowcardinality_uint32() {
    let lc_type = Type::LowCardinality {
        nested_type: Box::new(Type::Simple(TypeCode::UInt32)),
    };

    let col = ColumnLowCardinality::new(lc_type);
    assert_eq!(col.dictionary_size(), 0);
    assert_eq!(col.size(), 0);
}

#[test]
fn test_lowcardinality_string() {
    let lc_type = Type::LowCardinality {
        nested_type: Box::new(Type::Simple(TypeCode::String)),
    };

    let col = ColumnLowCardinality::new(lc_type);
    assert!(col.is_empty());
}

#[test]
fn test_lowcardinality_clear() {
    let lc_type = Type::LowCardinality {
        nested_type: Box::new(Type::Simple(TypeCode::String)),
    };

    let mut col = ColumnLowCardinality::new(lc_type);
    col.clear();

    assert_eq!(col.len(), 0);
    assert!(col.is_empty());
}

#[test]
fn test_lowcardinality_dictionary() {
    let lc_type = Type::LowCardinality {
        nested_type: Box::new(Type::Simple(TypeCode::UInt64)),
    };

    let col = ColumnLowCardinality::new(lc_type);
    let dict = col.dictionary_ref();

    // Dictionary should exist but be empty
    assert_eq!(dict.size(), 0);
}

// ============================================================================
// Geo Type Tests
// ============================================================================

#[test]
fn test_point_type_structure() {
    let pt = point_type();
    match pt {
        Type::Tuple { item_types } => {
            assert_eq!(item_types.len(), 2);
            assert!(matches!(item_types[0], Type::Simple(TypeCode::Float64)));
            assert!(matches!(item_types[1], Type::Simple(TypeCode::Float64)));
        }
        _ => panic!("Expected Tuple type"),
    }
}

#[test]
fn test_ring_type_structure() {
    let ring = ring_type();
    match ring {
        Type::Array { item_type } => {
            match *item_type {
                Type::Tuple { item_types } => {
                    // Ring is Array(Tuple(Float64, Float64))
                    assert_eq!(item_types.len(), 2);
                }
                _ => panic!("Expected Tuple item type"),
            }
        }
        _ => panic!("Expected Array type"),
    }
}

#[test]
fn test_polygon_type_structure() {
    let polygon = polygon_type();
    match polygon {
        Type::Array { item_type } => {
            match *item_type {
                Type::Array { .. } => {
                    // Polygon is Array(Array(Point))
                }
                _ => panic!("Expected Array item type"),
            }
        }
        _ => panic!("Expected Array type"),
    }
}

#[test]
fn test_multi_polygon_type_structure() {
    let mp = multi_polygon_type();
    match mp {
        Type::Array { item_type } => {
            match *item_type {
                Type::Array { .. } => {
                    // MultiPolygon is Array(Array(Array(Point)))
                }
                _ => panic!("Expected Array item type"),
            }
        }
        _ => panic!("Expected Array type"),
    }
}

#[test]
fn test_point_type_name() {
    let pt = point_type();
    assert_eq!(pt.name(), "Tuple(Float64, Float64)");
}

#[test]
fn test_ring_type_name() {
    let ring = ring_type();
    assert_eq!(ring.name(), "Array(Tuple(Float64, Float64))");
}

#[test]
fn test_polygon_type_name() {
    let polygon = polygon_type();
    assert_eq!(polygon.name(), "Array(Array(Tuple(Float64, Float64)))");
}

#[test]
fn test_multi_polygon_type_name() {
    let mp = multi_polygon_type();
    assert_eq!(mp.name(), "Array(Array(Array(Tuple(Float64, Float64))))");
}

#[test]
fn test_geo_types_use_existing_columns() {
    // Geo types don't need special column implementations
    // They use existing ColumnArray and ColumnTuple

    // This test just verifies the types can be created
    let _point = point_type();
    let _ring = ring_type();
    let _polygon = polygon_type();
    let _multi_polygon = multi_polygon_type();
}

// ============================================================================
// Integration Tests
// ============================================================================

#[test]
fn test_map_with_different_key_types() {
    // String key
    let map1 = Type::Map {
        key_type: Box::new(Type::Simple(TypeCode::String)),
        value_type: Box::new(Type::Simple(TypeCode::UInt32)),
    };
    let col1 = ColumnMap::new(map1);
    assert!(col1.is_empty());

    // Int32 key
    let map2 = Type::Map {
        key_type: Box::new(Type::Simple(TypeCode::Int32)),
        value_type: Box::new(Type::Simple(TypeCode::String)),
    };
    let col2 = ColumnMap::new(map2);
    assert!(col2.is_empty());

    // UUID key
    let map3 = Type::Map {
        key_type: Box::new(Type::Simple(TypeCode::UUID)),
        value_type: Box::new(Type::Simple(TypeCode::Float64)),
    };
    let col3 = ColumnMap::new(map3);
    assert!(col3.is_empty());
}

#[test]
fn test_lowcardinality_with_different_nested_types() {
    // String nested type
    let lc1 = Type::LowCardinality {
        nested_type: Box::new(Type::Simple(TypeCode::String)),
    };
    let col1 = ColumnLowCardinality::new(lc1);
    assert_eq!(col1.dictionary_size(), 0);

    // UInt32 nested type
    let lc2 = Type::LowCardinality {
        nested_type: Box::new(Type::Simple(TypeCode::UInt32)),
    };
    let col2 = ColumnLowCardinality::new(lc2);
    assert_eq!(col2.dictionary_size(), 0);

    // IPv4 nested type
    let lc3 = Type::LowCardinality {
        nested_type: Box::new(Type::Simple(TypeCode::IPv4)),
    };
    let col3 = ColumnLowCardinality::new(lc3);
    assert_eq!(col3.dictionary_size(), 0);
}

#[test]
fn test_nested_map_in_lowcardinality() {
    // LowCardinality(Map(String, UInt32))
    let map_type = Type::Map {
        key_type: Box::new(Type::Simple(TypeCode::String)),
        value_type: Box::new(Type::Simple(TypeCode::UInt32)),
    };

    let lc_map = Type::LowCardinality { nested_type: Box::new(map_type) };

    let col = ColumnLowCardinality::new(lc_map);
    assert!(col.is_empty());
}
