use serde::{Deserialize, Serialize};
use crate::schema;

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct Registration {
    name: String,
    input_schema: schema::Schema,
    output_schema: Option<schema::Schema>,
    use_custom_id: Option<String>
}

impl Registration {
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