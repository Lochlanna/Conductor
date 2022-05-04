use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use duplicate::duplicate;
use chrono::{DateTime, Utc, NaiveDate, NaiveDateTime};

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

/// Provides a function to retrieve conductor data types
pub trait ToConductorDataType {
    /// returns the Conductor data type for the implimenting type.
    ///
    /// # Example
    ///
    /// ```
    /// use conductor_common::producer::ToProducerData;
    /// use conductor_common::schema;
    /// struct CustomInt{}
    /// impl ToProducerData for CustomInt {
    ///     fn conductor_data_type() -> schema::DataTypes {
    ///         schema::DataTypes::Int
    ///     }
    /// }
    /// assert_eq!(CustomInt::conductor_data_type(), schema::DataTypes::Int);
    /// ```
    fn conductor_data_type() -> DataTypes;
}

#[duplicate(
int_type;
[ u8 ]; [ u16 ]; [ u32 ];
[ i8 ]; [ i16 ]; [ i32 ]; [ i64 ];
)]
impl ToConductorDataType for int_type {
    fn conductor_data_type() -> DataTypes {
        DataTypes::Int
    }
}

#[duplicate(
string_type;
[ String ]; [ str ];
)]
impl ToConductorDataType for string_type {
    fn conductor_data_type() -> DataTypes {
        DataTypes::String
    }
}

#[duplicate(
float_type;
[ f32 ]; [ f64 ];
)]
impl ToConductorDataType for float_type {
    fn conductor_data_type() -> DataTypes {
        DataTypes::Double
    }
}

impl ToConductorDataType for [u8] {
    fn conductor_data_type() -> DataTypes {
        DataTypes::Binary
    }
}

impl ToConductorDataType for bool {
    fn conductor_data_type() -> DataTypes {
        DataTypes::Bool
    }
}

#[duplicate(
time_type;
[ NaiveDate ]; [ NaiveDateTime ];
[ DateTime < Utc > ];
)]
impl ToConductorDataType for time_type {
    fn conductor_data_type() -> DataTypes {
        DataTypes::Time
    }
}

pub trait ConductorSchema {
    fn generate_schema() -> HashMap<String, DataTypes>;
}

pub trait SchemaHelpers {
    fn contains_column(&self, column_name: &str) -> bool;
}

pub type Schema = HashMap<String, DataTypes>;

impl SchemaHelpers for Schema {
    fn contains_column(&self, column_name: &str) -> bool {
        self.contains_key(column_name)
    }
}

/// A struct which assists in building a schema.
/// Most of the time this won't be necessary as the producer derive macro does this for you.
pub struct Builder {
    schema: Schema,
}

impl Default for Builder {
    fn default() -> Self {
        Self::new()
    }
}

impl Builder {
    #[must_use]
    pub fn new() -> Self {
        Self {
            schema: std::collections::HashMap::default()
        }
    }

    #[must_use]
    pub fn with_capacity(n: usize) -> Self {
        Self {
            schema: HashMap::with_capacity(n)
        }
    }

    #[must_use]
    pub fn add_column(mut self, name: String, col_type: DataTypes) -> Self {
        self.schema.insert(name, col_type);
        self
    }

    #[must_use]
    pub fn add_int(mut self, name: String) -> Self {
        self.schema.insert(name, DataTypes::Int);
        self
    }
    #[must_use]
    pub fn add_float(mut self, name: String) -> Self {
        self.schema.insert(name, DataTypes::Float);
        self
    }
    #[must_use]
    pub fn add_time(mut self, name: String) -> Self {
        self.schema.insert(name, DataTypes::Time);
        self
    }
    #[must_use]
    pub fn add_binary(mut self, name: String) -> Self {
        self.schema.insert(name, DataTypes::Binary);
        self
    }
    #[must_use]
    pub fn add_string(mut self, name: String) -> Self {
        self.schema.insert(name, DataTypes::String);
        self
    }
    #[must_use]
    pub fn add_bool(mut self, name: String) -> Self {
        self.schema.insert(name, DataTypes::Bool);
        self
    }
    #[must_use]
    pub fn add_double(mut self, name: String) -> Self {
        self.schema.insert(name, DataTypes::Double);
        self
    }

    #[allow(clippy::missing_const_for_fn)]
    #[must_use]
    pub fn build(self) -> Schema {
        self.schema
    }
}