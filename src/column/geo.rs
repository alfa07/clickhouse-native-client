/// Geo column types are wrappers around existing column types
/// - Point: Tuple(Float64, Float64)
/// - Ring: Array(Point)
/// - Polygon: Array(Ring)
/// - MultiPolygon: Array(Polygon)
///
/// These types use the existing ColumnTuple and ColumnArray
/// implementations with specific type constraints. No separate column
/// implementation is needed.
use crate::types::{
    Type,
    TypeCode,
};

/// Helper to create a Point type (Tuple(Float64, Float64))
pub fn point_type() -> Type {
    Type::Tuple {
        item_types: vec![
            Type::Simple(TypeCode::Float64),
            Type::Simple(TypeCode::Float64),
        ],
    }
}

/// Helper to create a Ring type (Array(Point))
pub fn ring_type() -> Type {
    Type::Array { item_type: Box::new(point_type()) }
}

/// Helper to create a Polygon type (Array(Ring))
pub fn polygon_type() -> Type {
    Type::Array { item_type: Box::new(ring_type()) }
}

/// Helper to create a MultiPolygon type (Array(Polygon))
pub fn multi_polygon_type() -> Type {
    Type::Array { item_type: Box::new(polygon_type()) }
}

#[cfg(test)]
#[cfg_attr(coverage_nightly, coverage(off))]
mod tests {
    use super::*;

    #[test]
    fn test_point_type() {
        let pt = point_type();
        match pt {
            Type::Tuple { item_types } => {
                assert_eq!(item_types.len(), 2);
                assert!(matches!(
                    item_types[0],
                    Type::Simple(TypeCode::Float64)
                ));
                assert!(matches!(
                    item_types[1],
                    Type::Simple(TypeCode::Float64)
                ));
            }
            _ => panic!("Expected Tuple type"),
        }
    }

    #[test]
    fn test_ring_type() {
        let ring = ring_type();
        match ring {
            Type::Array { item_type } => {
                match *item_type {
                    Type::Tuple { .. } => {
                        // Ring is Array(Tuple(Float64, Float64))
                    }
                    _ => panic!("Expected Tuple item type"),
                }
            }
            _ => panic!("Expected Array type"),
        }
    }

    #[test]
    fn test_polygon_type() {
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
    fn test_multi_polygon_type() {
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
    fn test_type_names() {
        assert_eq!(point_type().name(), "Tuple(Float64, Float64)");
        assert_eq!(ring_type().name(), "Array(Tuple(Float64, Float64))");
        // Polygon and MultiPolygon names will be more complex nested arrays
    }
}
