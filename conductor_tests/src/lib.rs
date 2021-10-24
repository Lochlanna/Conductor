#![allow(dead_code)]
mod tests {
    #[allow(unused_imports)]
    use conductor::producer::{ToProducerData, Base, SchemaBuilder};
    use conductor::schema::DataTypes;
    use serde::Serialize;

    #[derive(Clone, Debug, Serialize, conductor::derive::Producer)]
    struct TestDerive {
        id: u32,
        name: String,
        #[producer_skip_field]
        uuid: String
    }
    #[test]
    fn producer_derive() {
        let schema = TestDerive::generate_schema();
        assert!(schema.contains_key("id"));
        assert_eq!(schema["id"], DataTypes::Int);
        assert!(schema.contains_key("name"));
        assert_eq!(schema["name"], DataTypes::String);

        //ignore skipped fields
        assert_eq!(schema.contains_key("_uuid"), false);
    }

    #[test]
    fn schema_builder_basic() {
        let schema = SchemaBuilder::new().add_binary(String::from("hello")).add_bool(String::from("hello world")).build();
        let mut value = schema.get("hello").expect("expected value wasn't in the schema");
        assert!(matches!(value, DataTypes::Binary));
        value = schema.get("hello world").expect("expected value wasn't in the schema");
        assert!(matches!(value, DataTypes::Bool));
    }
}
