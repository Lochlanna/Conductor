
mod blocking {
    use conductor_derive::Producer;
    use std::collections::HashMap;
    use conductor_shared::producer::ToProducerData;

    #[derive(Debug, Producer)]
    pub struct MyCoolDataStructure {
        id: usize,
        wowow: f32,
        coolstring: String,
        cooldoubel: f64
    }

    impl MyCoolDataStructure {
        pub fn new() -> MyCoolDataStructure {
            MyCoolDataStructure {
                id: 0,
                wowow:32.0,
                coolstring: "".to_string(),
                cooldoubel: 0.0
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::blocking;
    use conductor_shared::producer::Producer;
    #[test]
    fn basic_producer_test() {
        let x = blocking::MyCoolDataStructure::new();
        println!("the schema is {:?}", x.get_schema())
    }
}
