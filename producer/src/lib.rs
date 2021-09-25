
mod blocking {
    use conductor_derive::Producer;
    #[derive(Debug, Producer)]
    pub struct MyCoolDataStructure {
        name: String,
        id: usize
    }

    impl MyCoolDataStructure {
        pub fn new() -> MyCoolDataStructure {
            MyCoolDataStructure {
                name: "hello".to_string(),
                id: 0
            }
        }
    }

}

#[cfg(test)]
mod tests {
    use super::blocking;
    #[test]
    fn basic_producer_test() {
        let x = blocking::MyCoolDataStructure::new();
    }
}
