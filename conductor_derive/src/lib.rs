extern crate proc_macro;
use proc_macro::{TokenStream, Ident};
use quote::quote;
use syn;
use syn::{parse_macro_input, DeriveInput, Item, Fields, Type, Data, DataStruct, FieldsNamed, Field};

#[proc_macro_derive(Producer)]
pub fn derive_producer(input: TokenStream) -> TokenStream {
    // Construct a representation of Rust code as a syntax tree
    // that we can manipulate

    let item:DeriveInput = syn::parse(input).unwrap();

    let struct_name = item.ident;

    let struct_data = if let Data::Struct(struct_body) = item.data {
        struct_body
    } else {
        //TODO error
        return TokenStream::new();
    };

    let fields = if let Fields::Named(named_fields) = struct_data.fields {
        named_fields
    } else {
        //TODO error
        return TokenStream::new();
    };
    let mut fields_vec = Vec::new();
    for field in fields.named {
        fields_vec.push(field.ident.unwrap());
    }
    let tokens = quote! {
        impl conductor_shared::producer::Producer for #struct_name {
            fn get_schema(&self) ->  std::collections::HashMap<std::string::String,conductor_shared::producer::DataTypes> {
                let mut schema = std::collections::HashMap::new();
                #(
                    schema.insert(std::string::String::from(stringify!(#fields_vec)), self.#fields_vec.to_producer_data());
                )*
                schema
            }
        }
    };
    println!("Tokens {}", tokens);
    tokens.into()
}