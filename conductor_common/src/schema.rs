use serde::{Deserialize, Serialize};
use std::collections::HashMap;
/// Data types supported by conductor
#[derive(Debug, Clone, Copy, Deserialize, Serialize, PartialEq)]
pub enum DataTypes {
    Int,
    Float,
    Time,
    String,
    Binary,
    Bool,
    Double,
}

impl DataTypes {
    /// Converts the enum to a string representation which matches quest db data types.
    #[must_use]
    pub const fn to_quest_type_str(&self) -> &str {
        match self {
            DataTypes::Int => "long",
            DataTypes::Float => "float",
            DataTypes::Time => "timestamp",
            DataTypes::Binary => "binary",
            DataTypes::String => "string",
            DataTypes::Bool => "boolean",
            DataTypes::Double => "double",
        }
    }
}

pub type Schema = HashMap<String, DataTypes>;