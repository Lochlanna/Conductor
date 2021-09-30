extern crate proc_macro;
use proc_macro::{TokenStream};
use quote::quote;
use syn;
use syn::{DeriveInput, Fields, Data, Attribute};
use syn::spanned::Spanned;

#[proc_macro_derive(Producer, attributes(producer_skip_field))]
pub fn derive_producer(input: TokenStream) -> TokenStream {
    // Construct a representation of Rust code as a syntax tree
    // that we can manipulate

    let item:DeriveInput = syn::parse(input).unwrap();

    let struct_name = &item.ident;

    let struct_data = if let Data::Struct(struct_body) = &item.data {
        struct_body
    } else {
        return syn::Error::new(item.span() ,"Producer derive macro only works on structs").to_compile_error().into();
    };

    let fields = if let Fields::Named(named_fields) = &struct_data.fields {
        named_fields
    } else {
        return syn::Error::new(item.span(), "Named fields are missing").to_compile_error().into();
    };
    let mut fields_vec = Vec::new();
    let mut fields_type_vec = Vec::new();
    for field in &fields.named {
        let mut skip = false;
        for attr in &field.attrs {
            if attr.path.is_ident("producer_skip_field") {
                skip = true;
                break;
            }
        }
        if skip {
            continue;
        }

        fields_type_vec.push(&field.ty);
        fields_vec.push(field.ident.as_ref().unwrap());
    }
    let tokens = quote! {
        impl conductor_shared::producer::Producer for #struct_name {
            fn get_schema() ->  std::collections::HashMap<std::string::String,conductor_shared::producer::DataTypes> {
                let mut schema = std::collections::HashMap::new();
                #(
                    schema.insert(std::string::String::from(stringify!(#fields_vec)), #fields_type_vec::to_producer_data());
                )*
                schema
            }
        }
    };
    // println!("Tokens {}", tokens);
    tokens.into()
}