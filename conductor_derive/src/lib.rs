//! Contains procedural macros which can be used to rapidly and ergonomically build producers and reactors.
//! This crate is not intended to be used on it's own but makes up part of the Conductor stack.

extern crate proc_macro;
use proc_macro::{TokenStream};
use quote::quote;

use syn::{DeriveInput, Fields, Data};
use syn::spanned::Spanned;
use quote::TokenStreamExt;

///
/// Generates a list of tuples which contain the name, type and any annotations on each named
/// field on a struct.
///
/// # Errors
/// * If the given input is not a struct then an error is generated.
/// * If the given input doesn't have named fields then an error is generated.
///
/// # Arguments
///
/// * `item`: The input tokens to be processed.
///

fn get_fields_types(item:&DeriveInput) -> Result<(Vec<&syn::Ident>, Vec<&syn::Type> , &syn::Ident), TokenStream> {
    let struct_name = &item.ident;

    let struct_data = if let Data::Struct(struct_body) = &item.data {
        struct_body
    } else {
        return Err(syn::Error::new(item.span() ,"Producer derive macro only works on structs").to_compile_error().into());
    };

    let fields = if let Fields::Named(named_fields) = &struct_data.fields {
        named_fields
    } else {
        return Err(syn::Error::new(item.span(), "Named fields are missing").to_compile_error().into());
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
    Ok((fields_vec, fields_type_vec, struct_name))
}

///
/// This macro implements at least `conductor::producer::base` as well as the default implementation
/// of the blocking version of the producer trait. If Async is enabled the async version is also
/// implemented. This macro will only work on a struct with named fields.
///
/// Specifically this macro implements the generate_schema function which returns the conductor
/// schema for the struct. It's a static function and can therefore be defined at compile time.
/// It uses the named members of the struct as long as they have not been annotated with the
///  `#[producer_skip_field]` annotation. Members with this annotation will be skipped in the schema.
/// This is useful for storing data such as the conductor UUID in the struct.
///
/// # Panics
/// It will panic if the token stream provided is not able to be passed.
///
/// # Errors
/// Errors will be produced if the input is not a struct or if it has not got named fields.
///
/// # Examples
/// ```
/// # use conductor::producer::{DataTypes, ToProducerData};
/// #[derive(Clone, Debug, Serialize, conductor::derive::Producer)]
/// struct TestDerive {
///     id: u32,
///     name: String,
///     #[producer_skip_field]
///     uuid: String
///  }
///  let schema = TestDerive::generate_schema();
///  assert_eq!(schema["id"], DataTypes::Int);
///  assert_eq!(schema["name"], DataTypes::String);
///
///  //ignore skipped fields
///  assert_eq!(schema.contains_key("uuid"), false);
/// ```
#[proc_macro_derive(Producer, attributes(producer_skip_field))]
pub fn derive_producer(input: TokenStream) -> TokenStream {
    // Construct a representation of Rust code as a syntax tree
    // that we can manipulate

    let item:DeriveInput = syn::parse(input).expect("Couldn't pass input tokens");

    let (fields_vec, fields_type_vec, struct_name)  = match get_fields_types(&item) {
        Ok(sd) => sd,
        Err(err) => return err
    };

    let body_tokens = quote! {
        impl conductor::producer::Base for #struct_name {
            fn generate_schema() ->  std::collections::HashMap<std::string::String,conductor::producer::DataTypes> {
                let mut schema = std::collections::HashMap::new();
                #(
                    schema.insert(std::string::String::from(stringify!(#fields_vec)), #fields_type_vec::conductor_data_type());
                )*
                schema
            }
        }
    };
    let mut tokens = quote! {
        impl conductor::producer::Producer for #struct_name {}
    };
    tokens.append_all(body_tokens.clone());
    #[cfg(feature = "async")]
    {
        tokens.append_all(quote! {
            impl conductor::producer::AsyncProducer for #struct_name {}
        });
        tokens.append_all(body_tokens);
    }
    tokens.into()
}