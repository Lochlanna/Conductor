use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use serde_json::Value;

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

pub type Schema = HashMap<&'_ str, DataTypes>;

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct Registration {
    name: &'_ str,
    schema: Schema,
    use_custom_id: Option<&'_ str>, // this is to support devices without persistent storage such as an arduino. They can have a custom id
}

impl Registration {
    pub fn new(name: &str, schema: Schema, custom_id: Option<&str>) -> Registration {
        Registration {
            name,
            schema,
            use_custom_id: custom_id
        }
    }

    pub fn new_empty(name: &str) -> Registration {
        Registration {
            name,
            schema: Default::default(),
            use_custom_id: None
        }
    }

    pub fn get_name(&self) -> &str {
        self.name
    }

    pub fn set_name(&mut self, name: &str) {
        self.name.clone_from(&name);
    }

    pub fn set_custom_id(&mut self, id: &str) {
        self.use_custom_id = Some(id);
    }

    pub fn has_custom_id(&self) -> bool {
        self.use_custom_id.is_some()
    }
    pub fn get_custom_id(&self) -> &Option<&str> {
        &self.use_custom_id
    }

    pub fn add_column(&mut self, column_name: &str, data_type: DataTypes) -> bool {
        self.schema.insert(column_name, data_type).is_some()
    }

    pub fn remove_column(&mut self, column_name: &str) -> bool {
        self.schema.remove(column_name).is_some()
    }

    pub fn contains_column(&self, column_name: &str) -> bool {
        self.schema.contains_key(column_name)
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