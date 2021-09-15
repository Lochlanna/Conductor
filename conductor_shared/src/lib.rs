pub mod producer {
    use serde::{Deserialize, Serialize};
    use std::collections::HashMap;
    #[derive(Debug, Clone, Deserialize, Serialize)]
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



    #[derive(Debug, Clone, Deserialize, Serialize, PartialEq)]
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
        pub name: String,
        pub schema: Schema,
        pub use_custom_id: Option<String>, // this is to support devices without persistant storage such as an arduino. They can have a custom id
    }

    #[derive(Debug, Clone, Deserialize, Serialize)]
    pub struct Emit {
        pub uuid: String,
        pub timestamp: Option<u64>,
        pub data: HashMap<String, serde_json::Value>,
    }

    #[derive(Debug, Clone, Deserialize, Serialize)]
    pub struct EmitResult {
        pub error: u8,
    }
}