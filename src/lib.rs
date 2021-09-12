pub mod producer_structs {
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

    pub fn to_solid_type_from_json(
        val: &serde_json::Value,
        data_type: &DataTypes,
    ) -> Result<Box<dyn postgres::types::ToSql + Sync + Send>, String> {
        match data_type {
            DataTypes::Int => match val.as_i64() {
                Some(v) => Ok(Box::new(v)),
                None => Err(format!(
                    "Not possible to convert json value to i64. Value: {:?}",
                    val
                )),
            },
            DataTypes::Float => {
                match val.as_f64() {
                    Some(v) => {
                        /*check that this will actually fit within an f32 bounds so the cast should? be safe.
                        use epsilon to make extra sure that this is an okay thing to do. 
                        There could be a time when a valid f32 value is rejected due to the epsilon difference but if your data
                        is that close use a double type...*/
                        if v > (f32::MAX as f64) - (f32::EPSILON as f64) || v < (f32::MIN as f64) + (f32::EPSILON as f64) {
                            return Err(format!("Not possible to convert json value to f32 (too big to fit). Value: {:?}", val));
                        }
                        // It should be safe to cast this to an f32. It fits
                        Ok(Box::new(v as f32))
                    },
                    None => Err(format!("Not possible to convert json value to f32 (Couldn't get f64 first). Value: {:?}", val)),
                }
            }
            DataTypes::Time => match serde_json::from_value::<chrono::NaiveDateTime>(val.clone()) {
                Ok(v) => Ok(Box::new(v)),
                Err(_) => Err(format!(
                    "Not possible to convert json value to naive date time. Value: {:?}",
                    val
                )),
            },
            DataTypes::String => match val.as_str() {
                Some(v) => Ok(Box::new(v.to_string())),
                None => Err(format!(
                    "Not possible to convert json value to string. Value: {:?}",
                    val
                )),
            },
            DataTypes::Bool => match val.as_bool() {
                Some(v) => Ok(Box::new(v)),
                None => Err(format!(
                    "Not possible to convert json value to bool. Value: {:?}",
                    val
                )),
            },
            DataTypes::Double => match val.as_f64() {
                Some(v) => Ok(Box::new(v)),
                None => Err(format!(
                    "Not possible to convert json value to double. Value: {:?}",
                    val
                )),
            },
            DataTypes::Binary => match serde_json::from_value::<Vec<u8>>(val.clone()) {
                Ok(v) => Ok(Box::new(v)),
                Err(_) => Err(format!(
                    "Not possible to convert json value to binary. Value: {:?}",
                    val
                )),
            },
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

    pub fn get_schema_as_json_str(schema: &Schema) -> String {
        match serde_json::to_string(schema) {
            Ok(v) => v,
            Err(err) => {
                log::error!("Couldn't serialize schema into json: {}", err);
                String::new()
            }
        }
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
