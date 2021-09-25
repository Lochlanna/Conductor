#![feature(proc_macro_diagnostic)]

use proc_macro::TokenStream;
use quote::quote;
use syn;
use syn::{DeriveInput, Item, Fields, Type, Data, DataStruct, FieldsNamed};
use url;
use url::Url;
use conductor_shared;
use syn::spanned::Spanned;



struct ConductorConfig {
    url: Url
}

trait ProducerVariables {
    fn set_uuid(&mut self, uuid: String);
    fn get_uuid(&self) -> Result<&str, &'static str>;
    fn get_conductor_config(&self) -> &ConductorConfig;
}

trait Producer<T: ProducerVariables = Self> {
    fn emit()-> Result<(), &'static str>;
    //Generate the schema for this struct and register it with conductor
    fn register()-> Result<String, &'static str>;//TODO make this return a real error
    fn is_registered() -> Result<bool, &'static str>;
}

trait

#[proc_macro_derive(Producer)]
pub fn derive_producer(input: TokenStream) -> TokenStream {
    // Construct a representation of Rust code as a syntax tree
    // that we can manipulate
    let item:DeriveInput = syn::parse(input).unwrap();
    match item.data {
        Data::Struct(struct_body) => {
            match struct_body.fields {
                Fields::Named(named_field) => {
                    for field in named_field.named {
                        match field.ty {
                            Type::Array(_) => {}
                            Type::BareFn(_) => {}
                            Type::Group(_) => {}
                            Type::ImplTrait(_) => {}
                            Type::Infer(_) => {}
                            Type::Macro(_) => {}
                            Type::Never(_) => {}
                            Type::Paren(_) => {}
                            Type::Path(_) => {}
                            Type::Ptr(_) => {}
                            Type::Reference(_) => {}
                            Type::Slice(_) => {}
                            Type::TraitObject(_) => {}
                            Type::Tuple(_) => {}
                            Type::Verbatim(_) => {}
                            Type::__TestExhaustive(_) => {}
                        }
                    }
                }
                Fields::Unnamed(_) => {}
                Fields::Unit => {}
            }
        }
        _ => {}
    }
    TokenStream::new()
}


#[cfg(test)]
mod tests {
    #[test]
    fn it_works() {
        let result = 2 + 2;
        assert_eq!(result, 4);
    }
}
