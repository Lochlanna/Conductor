extern crate proc_macro;
use proc_macro::{TokenStream};
use quote::quote;

use syn::{DeriveInput, Fields, Data};
use syn::spanned::Spanned;
use quote::TokenStreamExt;

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
///
/// # Arguments
///TODO
/// * `input`:
///
/// returns: `TokenStream`
///
/// # Panics
/// TODO
/// # Examples
///TODO
/// ```
///
/// ```
#[proc_macro_derive(Producer, attributes(producer_skip_field))]
pub fn derive_producer(input: TokenStream) -> TokenStream {
    // Construct a representation of Rust code as a syntax tree
    // that we can manipulate

    let item:DeriveInput = syn::parse(input).unwrap();

    let (fields_vec, fields_type_vec, struct_name)  = match get_fields_types(&item) {
        Ok(sd) => sd,
        Err(err) => return err
    };

    let body_tokens = quote! {
        {
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
        impl conductor::producer::Producer for #struct_name
    };
    tokens.append_all(body_tokens.clone());
    #[cfg(feature = "async")]
    {
        tokens.append_all(quote! {
            impl conductor::producer::AsyncProducer for #struct_name
        });
        tokens.append_all(body_tokens);
    }
    println!("Tokens {}", tokens);
    tokens.into()
}