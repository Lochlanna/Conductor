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

pub trait Action<I, O> {
    fn input_data(&self) -> &I;
    fn input_schema() -> schema::Schema;
    fn output_data(&self) -> &O;
    fn output_schema() -> schema::Schema;
    fn custom_id(&self) -> Option<&str>;
    fn name(&self) -> &str;
}

pub struct BasicAction<I, O> {
    input_data: I,
    output_data: O,
    custom_id: Option<String>,
    name: String
}
impl<I,O> BasicAction<I, O> {
    pub fn new(input_data: I, output_data: O, custom_id: Option<String>, name: String) -> Self {
        Self { input_data, output_data, custom_id, name }
    }
}

impl<I, O> Action<I, O> for BasicAction<I, O>
    where I: schema::ConductorSchema + Serialize + DeserializeOwned, O: schema::ConductorSchema + Serialize + DeserializeOwned
 {

    fn input_data(&self) -> &I{
        &self.input_data
    }
    fn input_schema() -> schema::Schema {
        I::generate_schema()
    }
    fn output_data(&self) -> &O {
        &self.output_data
    }
    fn output_schema() -> schema::Schema {
        O::generate_schema()
    }
    fn custom_id(&self) -> Option<&str> {
        match &self.custom_id {
            None => None,
            Some(id) => Some(id)
        }
    }
    fn name(&self) -> &str {
        &self.name
    }
}


#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use crate::reactor::BasicAction;
    use crate::reactor::Action;
    use crate::schema::{ConductorSchema, DataTypes};
    use serde::{Deserialize, Serialize};

    #[derive(Debug, Clone, Deserialize, Serialize)]
    struct empty{
        hello: u8
    }
    impl ConductorSchema for empty {
        fn generate_schema() -> HashMap<String, DataTypes> {
            let mut x = HashMap::new();
            x.insert(String::from("hello"), DataTypes::Double);
            x
        }
    }

    #[test]
    fn test_new_action() {
        let a = BasicAction {
            input_data: empty{hello:8},
            output_data: empty{hello:8},
            custom_id: None,
            name: "the name".to_string()
        };
        a.input_data();
        let x = BasicAction::<empty, empty>::input_schema();
        assert!(x.contains_key("hello"));
        assert!(x.get("hello").is_some() && *x.get("hello").unwrap() == DataTypes::Double);
    }
}
