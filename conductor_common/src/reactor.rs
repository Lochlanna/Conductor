use serde::{Deserialize, Serialize};
use serde::de::DeserializeOwned;
use crate::schema;

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct ActionRegistration {
    name: String,
    input_schema: schema::Schema,
    output_schema: Option<schema::Schema>,
    use_custom_id: Option<String>
}

impl ActionRegistration {
    pub fn new(name: String, input_schema: schema::Schema, output_schema: Option<schema::Schema>, custom_id: Option<String>) -> Self {
        Self {
            name,
            input_schema,
            output_schema,
            use_custom_id: custom_id
        }
    }

    pub fn new_empty(name: String, custom_id: Option<String>) -> Self {
        Self {
            name,
            input_schema: std::collections::HashMap::default(),
            output_schema: None,
            use_custom_id: custom_id
        }
    }


    pub fn get_name(&self) -> &str {
        &self.name
    }
    pub fn get_custom_id(&self) -> Option<&str> {
        if let Some(c_id) = &self.use_custom_id {
            return Some(c_id.as_str());
        }
        None
    }
    pub fn has_custom_id(&self) -> bool {
        self.use_custom_id.is_some()
    }

    pub fn input_contains_column(&self, column_name: &str) -> bool {
        self.input_schema.contains_key(column_name)
    }

    pub fn output_contains_column(&self, column_name: &str) -> bool {
        if let Some(output_schema) = &self.output_schema {
            return output_schema.contains_key(column_name);
        }
        false
    }

    pub fn input_schema_len(&self) -> usize {
        self.input_schema.len()
    }
    pub fn output_schema_len(&self) -> usize {
        if let Some(output_schema) = &self.output_schema {
            return output_schema.len();
        }
        0
    }

    pub fn get_input_schema(&self) -> &schema::Schema {
        &self.input_schema
    }
    pub fn get_output_schema(&self) -> &Option<schema::Schema> {
        &self.output_schema
    }
}

pub struct Action<I: schema::ConductorSchema + Serialize + DeserializeOwned, O: schema::ConductorSchema + Serialize + DeserializeOwned> {
    input_data: I,
    output_data: O,
    custom_id: Option<String>,
    name: String
}
impl<I: schema::ConductorSchema + Serialize + DeserializeOwned,O: schema::ConductorSchema + Serialize + DeserializeOwned> Action<I, O> {
    pub fn new(input_data: I, output_data: O, custom_id: Option<String>, name: String) -> Self {
        Self { input_data, output_data, custom_id, name }
    }

    pub fn input_data(&self) -> &I {
        &self.input_data
    }
    pub fn input_schema() -> schema::Schema {
        I::generate_schema()
    }
    pub fn output_data(&self) -> &O {
        &self.output_data
    }
    pub fn output_schema() -> schema::Schema {
        O::generate_schema()
    }
    pub fn custom_id(&self) -> Option<&str> {
        match &self.custom_id {
            None => None,
            Some(id) => Some(id)
        }
    }
    pub fn name(&self) -> &str {
        &self.name
    }
}