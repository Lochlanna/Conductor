#![allow(dead_code)]
mod tests {
    #[allow(unused_imports)]
    use conductor::producer::{DataTypes, ToProducerData};
    use serde::Serialize;
    #[derive(Clone, Debug, Serialize, conductor::Producer)]
    struct TestDerive {
        id: u32,
        name: String,
        #[producer_skip_field]
        uuid: String
    }
    #[test]
    fn producer_derive() {
        let test = TestDerive {
            id: 0,
            name: "".to_string(),
            uuid: "".to_string()
        };
        let schema = test.generate_schema();
        assert!(schema.contains_key("id"));
        assert_eq!(schema["id"], DataTypes::Int);
        assert!(schema.contains_key("name"));
        assert_eq!(schema["name"], DataTypes::String);

        //ignore skipped fields
        assert_eq!(schema.contains_key("_uuid"), false);
    }
}
