use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use url::Url;

#[derive(Debug, Clone, Copy, Deserialize, Serialize)]
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
    pub fn to_quest_type_str(&self) -> &str {
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



#[derive(Debug, Clone, Copy, Deserialize, Serialize, PartialEq)]
pub enum ProducerErrorCode {
    NoError = 0,
    TimestampDefined = 1,
    NoMembers = 2,
    InvalidColumnNames = 3,
    TooManyColumns = 4, // who is doing this???
    InternalError = 5,
    InvalidUuid = 6,
    NameInvalid = 7,
    Unregistered = 8,
    InvalidData = 9,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct RegistrationResult {
    pub error: u8,
    pub uuid: Option<String>,
}

pub type Schema = HashMap<String, DataTypes>;

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct Registration {
    name: String,
    schema: Schema,
    use_custom_id: Option<String>, // this is to support devices without persistent storage such as an arduino. They can have a custom id
}

impl Registration {
    pub fn new(name: String, schema: Schema, custom_id: Option<String>) -> Registration {
        Registration {
            name,
            schema,
            use_custom_id: custom_id
        }
    }

    pub fn new_empty(name: String) -> Registration {
        Registration {
            name,
            schema: Default::default(),
            use_custom_id: None
        }
    }

    pub fn get_name(&self) -> &str {
        &self.name
    }

    pub fn set_name(&mut self, name: &String) {
        self.name.clone_from(name);
    }

    pub fn set_custom_id(&mut self, id: String) {
        self.use_custom_id = Some(id);
    }

    pub fn has_custom_id(&self) -> bool {
        self.use_custom_id.is_some()
    }
    pub fn get_custom_id(&self) -> Option<&str> {
        if let Some(c_id) = &self.use_custom_id {
            return Some(c_id.as_str());
        }
        None
    }

    pub fn add_column(&mut self, column_name: String, data_type: DataTypes) -> bool {
        self.schema.insert(column_name, data_type).is_some()
    }

    pub fn remove_column(&mut self, column_name: &String) -> bool {
        self.schema.remove(column_name).is_some()
    }

    pub fn contains_column(&self, column_name: &str) -> bool {
        self.schema.contains_key(column_name)
    }

    pub fn schema_len(&self) -> usize {
        self.schema.len()
    }

    pub fn get_schema(&self) -> &Schema {
        &self.schema
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct Emit {
    uuid: String,
    timestamp: Option<u64>,
    data: HashMap<String, serde_json::Value>,
}

impl Emit {
    pub fn new(uuid:String, timestamp: Option<u64>, data: HashMap<String, serde_json::Value>) -> Emit {
        Emit {
            uuid,
            timestamp,
            data
        }
    }
    pub fn new_empty(uuid:String, timestamp: Option<u64>) -> Emit {
        Emit {
            uuid,
            timestamp,
            data: Default::default()
        }
    }

    pub fn get_uuid(&self) -> &String {
        &self.uuid
    }

    pub fn get_timestamp(&self) -> Option<u64> {
        self.timestamp
    }

    pub fn get_data(&self) -> &HashMap<String, serde_json::Value> {
        &self.data
    }

    pub fn column_in_data(&self, column_name: &String) -> bool {
        self.data.contains_key(column_name)
    }

    pub fn get_value_for_column(&self, column_name: &String) -> Option<&serde_json::Value> {
        self.data.get(column_name)
    }

    pub fn insert_or_overwrite_column(&mut self, column_name: String, value: serde_json::Value) -> bool {
        match self.data.insert(column_name, value) {
            None => false,
            Some(_) => true
        }
    }

    pub fn remove_column(&mut self, column_name: &String) -> Option<serde_json::Value> {
        self.data.remove(column_name)
    }

    pub fn get_column_list(&self) -> Vec<&String> {
        self.data.keys().collect()
    }
}

#[derive(Debug, Clone, Copy, Deserialize, Serialize)]
pub struct EmitResult {
    pub error: u8,
}


pub struct SchemaBuilder {
    schema: Schema
}

impl SchemaBuilder {
    pub fn new() -> SchemaBuilder {
        SchemaBuilder {
            schema: Default::default()
        }
    }

    //noinspection RsSelfConvention
    pub fn with_capacity(n:usize) -> SchemaBuilder {
        SchemaBuilder {
            schema: HashMap::with_capacity(n)
        }
    }

    pub fn add_column(mut self, name: String, col_type: DataTypes) -> Self {
        self.schema.insert(name, col_type);
        self
    }

    pub fn add_int(mut self, name: String) -> Self {
        self.schema.insert(name,DataTypes::Int);
        self
    }
    pub fn add_float(mut self, name: String) -> Self {
        self.schema.insert(name,DataTypes::Float);
        self
    }
    pub fn add_time(mut self, name: String) -> Self {
        self.schema.insert(name,DataTypes::Time);
        self
    }
    pub fn add_binary(mut self, name: String) -> Self {
        self.schema.insert(name,DataTypes::Binary);
        self
    }
    pub fn add_string(mut self, name: String) -> Self {
        self.schema.insert(name,DataTypes::String);
        self
    }
    pub fn add_bool(mut self, name: String) -> Self {
        self.schema.insert(name,DataTypes::Bool);
        self
    }
    pub fn add_double(mut self, name: String) -> Self {
        self.schema.insert(name,DataTypes::Double);
        self
    }

    pub fn build(self) -> Schema {
        self.schema
    }
}


struct ConductorConfig {
    url: Url
}

trait ProducerVariables {
    fn set_uuid(&mut self, uuid: String);
    fn get_uuid(&self) -> Result<&str, &'static str>;
    fn get_conductor_config(&self) -> &ConductorConfig;
}

pub trait Producer {
    fn emit(&self)-> Result<(), &'static str>
    {
        Err("")
    }
    //Generate the schema for this struct and register it with conductor
    fn register(&mut self)-> Result<String, &'static str>
    {
        Err("")
    }
    fn is_registered(&self) -> Result<bool, &'static str>
    {
        Err("")
    }
    fn get_schema(&self) -> HashMap<String,DataTypes>;
}

pub trait ToProducerData {
    fn to_producer_data(&self) -> DataTypes;
}

impl ToProducerData for usize {
    fn to_producer_data(&self) -> DataTypes {
        DataTypes::Int
    }
}

impl ToProducerData for String {
    fn to_producer_data(&self) -> DataTypes {
        DataTypes::String
    }
}

impl ToProducerData for str {
    fn to_producer_data(&self) -> DataTypes {
        DataTypes::String
    }
}

impl ToProducerData for f64 {
    fn to_producer_data(&self) -> DataTypes {
        DataTypes::Double
    }
}

impl ToProducerData for f32 {
    fn to_producer_data(&self) -> DataTypes {
        DataTypes::Float
    }
}

#[cfg(test)]
mod tests {
    use super::SchemaBuilder;
    use crate::producer;
    #[test]
    fn it_works() {
        let schema = SchemaBuilder::new().add_binary(String::from("hello")).add_bool(String::from("hello world")).build();
        let value = schema.get("hello").expect("expected value wasn't in the schema");
        assert!(matches!(value, producer::DataTypes::Binary));
        assert_eq!(schema.contains_key("hello"), true);
    }
}