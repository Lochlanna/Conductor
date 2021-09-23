
mod blocking {
    // use proc_macro::TokenStream;
    // use quote::quote;
    // use syn;

    // fn impl_hello_macro(ast: &syn::DeriveInput) -> TokenStream {
    //     let name = &ast.ident;
    //     let gen = quote! {
    //         impl HelloMacro for #name {
    //             fn hello_macro() {
    //                 println!("Hello, Macro! My name is {}!", stringify!(#name));
    //             }
    //         }
    //     };
    //     gen.into()
    // }
    //
    // #[proc_macro_derive(Producer)]
    // pub fn derive_producer(_item: TokenStream) -> TokenStream {
    //     // Construct a representation of Rust code as a syntax tree
    //     // that we can manipulate
    //     let ast = syn::parse(input).unwrap();
    //     println!("The inside of the derived producer is {:?}", ast);
    //     impl_hello_macro(&ast)
    // }
    //
    //
    // #[derive(Debug, Producer)]
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
