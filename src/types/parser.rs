//! Type parser - 1:1 port of clickhouse-cpp type_parser.cpp
//!
//! This module implements token-based type parsing with AST caching,
//! mirroring the C++ implementation exactly.
//!
//! **Reference:** `cpp/clickhouse-cpp/clickhouse/types/type_parser.{h,cpp}`

use super::TypeCode;
use crate::{
    Error,
    Result,
};
use std::{
    cell::RefCell,
    collections::HashMap,
};

/// Token types used during parsing
/// Mirrors C++ `TypeParser::Token::Type`
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum TokenType {
    Invalid = 0,
    Assign,
    Name,
    Number,
    #[allow(dead_code)]
    String,
    LPar, // Left parenthesis (
    RPar, // Right parenthesis )
    Comma,
    QuotedString, // String with quotation marks included
    Eos,          // End of string
}

/// Token with type and value
/// Mirrors C++ `TypeParser::Token`
#[derive(Debug, Clone)]
struct Token<'a> {
    token_type: TokenType,
    value: &'a str,
}

/// TypeAst meta-type classification
/// Mirrors C++ `TypeAst::Meta`
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TypeMeta {
    Array,
    Assign,
    Null,
    Nullable,
    Number,
    String,
    Terminal,
    Tuple,
    Enum,
    LowCardinality,
    SimpleAggregateFunction,
    Map,
}

/// Abstract Syntax Tree for a type definition
/// Mirrors C++ `struct TypeAst`
#[derive(Debug, Clone, PartialEq)]
pub struct TypeAst {
    /// Type's category
    pub meta: TypeMeta,
    /// Type code
    pub code: TypeCode,
    /// Type's name
    pub name: String,
    /// Value associated with the node (for fixed-width types and enum values)
    pub value: i64,
    /// String value (for timezone, enum names, etc.)
    pub value_string: String,
    /// Sub-elements of the type (for composite types, enum items)
    pub elements: Vec<TypeAst>,
}

impl Default for TypeAst {
    fn default() -> Self {
        Self {
            meta: TypeMeta::Terminal,
            code: TypeCode::Void,
            name: String::new(),
            value: 0,
            value_string: String::new(),
            elements: Vec::new(),
        }
    }
}

/// Type parser - mirrors C++ `class TypeParser`
pub struct TypeParser<'a> {
    /// Current position in input string
    cur: usize,
    /// Input string bytes
    input: &'a str,
    /// Stack of open elements during parsing
    open_elements: Vec<*mut TypeAst>,
    /// Current AST node being built
    current_type: Option<*mut TypeAst>,
}

impl<'a> TypeParser<'a> {
    /// Create a new parser for the given type name
    /// Mirrors C++ `TypeParser::TypeParser(const StringView& name)`
    pub fn new(name: &'a str) -> Self {
        Self {
            cur: 0,
            input: name,
            open_elements: Vec::new(),
            current_type: None,
        }
    }

    /// Parse the type string into a TypeAst
    /// Mirrors C++ `bool TypeParser::Parse(TypeAst* type)`
    pub fn parse(&mut self, type_ast: &mut TypeAst) -> bool {
        // Safety: We use raw pointers to match C++ semantics, but we ensure:
        // 1. Pointers are only used during parsing (within this function)
        // 2. No pointers escape this function
        // 3. TypeAst outlives all pointer operations

        let type_ptr: *mut TypeAst = type_ast as *mut TypeAst;
        self.current_type = Some(type_ptr);
        self.open_elements.push(type_ptr);

        let mut processed_tokens = 0;

        loop {
            let token = self.next_token();

            match token.token_type {
                TokenType::QuotedString => {
                    unsafe {
                        let current = self.current_type.unwrap();
                        (*current).meta = TypeMeta::String; // Use String meta for quoted strings
                                                            // Remove quotes from value
                        if token.value.len() >= 2 {
                            (*current).value_string = token.value
                                [1..token.value.len() - 1]
                                .to_string();
                        } else {
                            (*current).value_string = String::new();
                        }
                        (*current).code = TypeCode::String;
                    }
                }

                TokenType::Name => unsafe {
                    let current = self.current_type.unwrap();
                    (*current).meta = get_type_meta(token.value);
                    (*current).name = token.value.to_string();
                    (*current).code = get_type_code(token.value);
                },

                TokenType::Number => unsafe {
                    let current = self.current_type.unwrap();
                    (*current).meta = TypeMeta::Number;
                    (*current).value = token.value.parse::<i64>().unwrap_or(0);
                },

                TokenType::String => unsafe {
                    let current = self.current_type.unwrap();
                    (*current).meta = TypeMeta::String;
                    (*current).value_string = token.value.to_string();
                },

                TokenType::LPar => {
                    unsafe {
                        let current = self.current_type.unwrap();
                        (*current).elements.push(TypeAst::default());
                        self.open_elements.push(current);
                        // Get pointer to last element
                        let last_idx = (*current).elements.len() - 1;
                        let elements_ptr = (*current).elements.as_mut_ptr();
                        let new_current = elements_ptr.add(last_idx);
                        self.current_type = Some(new_current);
                    }
                }

                TokenType::RPar => {
                    self.open_elements.pop();
                    if let Some(&parent) = self.open_elements.last() {
                        self.current_type = Some(parent);
                    }
                }

                TokenType::Assign | TokenType::Comma => {
                    self.open_elements.pop();
                    if let Some(&parent) = self.open_elements.last() {
                        unsafe {
                            (*parent).elements.push(TypeAst::default());
                            self.open_elements.push(parent);
                            let last_idx = (*parent).elements.len() - 1;
                            let elements_ptr = (*parent).elements.as_mut_ptr();
                            let new_current = elements_ptr.add(last_idx);
                            self.current_type = Some(new_current);
                        }
                    }
                }

                TokenType::Eos => {
                    // Unbalanced braces/brackets is an error
                    if self.open_elements.len() != 1 {
                        return false;
                    }

                    // Empty input string
                    if processed_tokens == 0 {
                        return false;
                    }

                    return validate_ast(type_ast);
                }

                TokenType::Invalid => {
                    return false;
                }
            }

            processed_tokens += 1;
        }
    }

    /// Get next token from input
    /// Mirrors C++ `TypeParser::Token TypeParser::NextToken()`
    fn next_token(&mut self) -> Token<'a> {
        let bytes = self.input.as_bytes();

        // Skip whitespace
        while self.cur < bytes.len() {
            match bytes[self.cur] as char {
                ' ' | '\n' | '\t' | '\0' => {
                    self.cur += 1;
                    continue;
                }
                '=' => {
                    let start = self.cur;
                    self.cur += 1;
                    return Token {
                        token_type: TokenType::Assign,
                        value: &self.input[start..self.cur],
                    };
                }
                '(' => {
                    let start = self.cur;
                    self.cur += 1;
                    return Token {
                        token_type: TokenType::LPar,
                        value: &self.input[start..self.cur],
                    };
                }
                ')' => {
                    let start = self.cur;
                    self.cur += 1;
                    return Token {
                        token_type: TokenType::RPar,
                        value: &self.input[start..self.cur],
                    };
                }
                ',' => {
                    let start = self.cur;
                    self.cur += 1;
                    return Token {
                        token_type: TokenType::Comma,
                        value: &self.input[start..self.cur],
                    };
                }
                '\'' => {
                    // Quoted string
                    let start = self.cur;
                    self.cur += 1;

                    // Fast forward to closing quote
                    while self.cur < bytes.len() {
                        if bytes[self.cur] as char == '\'' {
                            self.cur += 1;
                            return Token {
                                token_type: TokenType::QuotedString,
                                value: &self.input[start..self.cur],
                            };
                        }
                        self.cur += 1;
                    }

                    return Token {
                        token_type: TokenType::QuotedString,
                        value: &self.input[start..self.cur],
                    };
                }
                _ => {
                    let start = self.cur;
                    let ch = bytes[self.cur] as char;

                    // Identifier (name)
                    if ch.is_alphabetic() || ch == '_' {
                        while self.cur < bytes.len() {
                            let c = bytes[self.cur] as char;
                            if !c.is_alphanumeric() && c != '_' {
                                break;
                            }
                            self.cur += 1;
                        }
                        return Token {
                            token_type: TokenType::Name,
                            value: &self.input[start..self.cur],
                        };
                    }

                    // Number
                    if ch.is_numeric() || ch == '-' {
                        self.cur += 1;
                        while self.cur < bytes.len() {
                            if !(bytes[self.cur] as char).is_numeric() {
                                break;
                            }
                            self.cur += 1;
                        }
                        return Token {
                            token_type: TokenType::Number,
                            value: &self.input[start..self.cur],
                        };
                    }

                    return Token {
                        token_type: TokenType::Invalid,
                        value: "",
                    };
                }
            }
        }

        Token { token_type: TokenType::Eos, value: "" }
    }
}

/// Get TypeMeta from type name
/// Mirrors C++ `GetTypeMeta(const StringView& name)`
fn get_type_meta(name: &str) -> TypeMeta {
    match name {
        "Array" => TypeMeta::Array,
        "Null" => TypeMeta::Null,
        "Nullable" => TypeMeta::Nullable,
        "Tuple" => TypeMeta::Tuple,
        "Enum8" | "Enum16" => TypeMeta::Enum,
        "LowCardinality" => TypeMeta::LowCardinality,
        "SimpleAggregateFunction" => TypeMeta::SimpleAggregateFunction,
        "Map" => TypeMeta::Map,
        _ => TypeMeta::Terminal,
    }
}

/// Get TypeCode from type name
/// Mirrors C++ `GetTypeCode(const std::string& name)`
fn get_type_code(name: &str) -> TypeCode {
    match name {
        "Void" => TypeCode::Void,
        "Int8" => TypeCode::Int8,
        "Int16" => TypeCode::Int16,
        "Int32" => TypeCode::Int32,
        "Int64" => TypeCode::Int64,
        "Bool" | "UInt8" => TypeCode::UInt8,
        "UInt16" => TypeCode::UInt16,
        "UInt32" => TypeCode::UInt32,
        "UInt64" => TypeCode::UInt64,
        "Float32" => TypeCode::Float32,
        "Float64" => TypeCode::Float64,
        "String" => TypeCode::String,
        "FixedString" => TypeCode::FixedString,
        "DateTime" => TypeCode::DateTime,
        "DateTime64" => TypeCode::DateTime64,
        "Date" => TypeCode::Date,
        "Date32" => TypeCode::Date32,
        "Array" => TypeCode::Array,
        "Nullable" => TypeCode::Nullable,
        "Tuple" => TypeCode::Tuple,
        "Enum8" => TypeCode::Enum8,
        "Enum16" => TypeCode::Enum16,
        "UUID" => TypeCode::UUID,
        "IPv4" => TypeCode::IPv4,
        "IPv6" => TypeCode::IPv6,
        "Int128" => TypeCode::Int128,
        "UInt128" => TypeCode::UInt128,
        "Decimal" => TypeCode::Decimal,
        "Decimal32" => TypeCode::Decimal32,
        "Decimal64" => TypeCode::Decimal64,
        "Decimal128" => TypeCode::Decimal128,
        "LowCardinality" => TypeCode::LowCardinality,
        "Map" => TypeCode::Map,
        "Point" => TypeCode::Point,
        "Ring" => TypeCode::Ring,
        "Polygon" => TypeCode::Polygon,
        "MultiPolygon" => TypeCode::MultiPolygon,
        _ => TypeCode::Void,
    }
}

/// Validate the parsed AST
/// Mirrors C++ `bool ValidateAST(const TypeAst& ast)`
fn validate_ast(ast: &TypeAst) -> bool {
    // Void terminal that is not actually "void" is an unknown type
    if ast.meta == TypeMeta::Terminal
        && ast.code == TypeCode::Void
        && !ast.name.eq_ignore_ascii_case("void")
        && !ast.name.is_empty()
    {
        return false;
    }

    true
}

// Thread-local cache for parsed type names
// Each thread maintains its own cache for zero-overhead lookups.
// Optimized for Rust: uses thread_local instead of global mutex (unlike C++
// implementation).
thread_local! {
    static TYPE_CACHE: RefCell<HashMap<String, TypeAst>> =
        RefCell::new(HashMap::new());
}

/// Parse a type name and return cached AST
/// Mirrors C++ `const TypeAst* ParseTypeName(const std::string& type_name)`
pub fn parse_type_name(type_name: &str) -> Result<TypeAst> {
    TYPE_CACHE.with(|cache| {
        // Try to get from thread-local cache
        if let Some(ast) = cache.borrow().get(type_name) {
            return Ok(ast.clone());
        }

        // Parse new AST
        let mut ast = TypeAst::default();
        let mut parser = TypeParser::new(type_name);

        if !parser.parse(&mut ast) {
            return Err(Error::Protocol(format!(
                "Failed to parse type: {}",
                type_name
            )));
        }

        // Cache the result in thread-local storage
        cache.borrow_mut().insert(type_name.to_string(), ast.clone());
        Ok(ast)
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_simple_types() {
        let ast = parse_type_name("Int32").unwrap();
        assert_eq!(ast.meta, TypeMeta::Terminal);
        assert_eq!(ast.code, TypeCode::Int32);
        assert_eq!(ast.name, "Int32");
    }

    #[test]
    fn test_array_type() {
        let ast = parse_type_name("Array(String)").unwrap();
        assert_eq!(ast.meta, TypeMeta::Array);
        assert_eq!(ast.code, TypeCode::Array);
        assert_eq!(ast.elements.len(), 1);
        assert_eq!(ast.elements[0].code, TypeCode::String);
    }

    #[test]
    fn test_nullable_type() {
        let ast = parse_type_name("Nullable(UInt64)").unwrap();
        assert_eq!(ast.meta, TypeMeta::Nullable);
        assert_eq!(ast.elements.len(), 1);
        assert_eq!(ast.elements[0].code, TypeCode::UInt64);
    }

    #[test]
    fn test_nested_types() {
        let ast = parse_type_name("Array(Nullable(String))").unwrap();
        assert_eq!(ast.meta, TypeMeta::Array);
        assert_eq!(ast.elements[0].meta, TypeMeta::Nullable);
        assert_eq!(ast.elements[0].elements[0].code, TypeCode::String);
    }

    #[test]
    fn test_fixed_string() {
        let ast = parse_type_name("FixedString(10)").unwrap();
        assert_eq!(ast.meta, TypeMeta::Terminal);
        assert_eq!(ast.code, TypeCode::FixedString);
        assert_eq!(ast.elements.len(), 1);
        assert_eq!(ast.elements[0].meta, TypeMeta::Number);
        assert_eq!(ast.elements[0].value, 10);
    }

    #[test]
    fn test_enum8() {
        let ast = parse_type_name("Enum8('red' = 1, 'green' = 2)").unwrap();
        assert_eq!(ast.meta, TypeMeta::Enum);
        assert_eq!(ast.code, TypeCode::Enum8);
        assert_eq!(ast.elements.len(), 4); // 'red', 1, 'green', 2
    }

    #[test]
    fn test_caching() {
        let ast1 = parse_type_name("String").unwrap();
        let ast2 = parse_type_name("String").unwrap();
        assert_eq!(ast1, ast2);

        // Verify it's actually cached by checking the thread-local cache
        TYPE_CACHE.with(|cache| {
            assert!(cache.borrow().contains_key("String"));
        });
    }
}
