
mod blocking {
    use std::collections::HashMap;
    use conductor_shared::producer;



}

#[cfg(test)]
mod tests {
    use crate::blocking::SchemaBuilder;
    use conductor_shared::producer;
    use super::blocking;
    #[test]
    fn it_works() {
        let schema = SchemaBuilder::new().add_binary(String::from("hello")).add_bool(String::from("hello world")).build();
        let value = schema.get("hello").expect("expected value wasn't in the schema");
        assert!(matches!(value, producer::DataTypes::Binary));
        assert_eq!(schema.contains_key("hello"), true);
    }
}
