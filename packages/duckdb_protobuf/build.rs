use proc_macro2::TokenStream;
use prost_reflect::prost::encoding::WireType;
use prost_reflect::{Cardinality, DescriptorPool, Kind, MessageDescriptor};
use quote::quote;
use std::env;
use std::fs::File;
use std::io::{Read, Write};
use std::path::Path;
use std::str::FromStr;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("cargo:rerun-if-changed=./descriptor.pb");

    let buffer = {
        let mut file = File::open("./descriptor.pb")?;
        let mut buffer = Vec::new();
        file.read_to_end(&mut buffer)?;

        buffer
    };

    let message_descriptors = DescriptorPool::decode(buffer.as_slice())?;
    let message_impl = message_descriptors
        .all_messages()
        .map(|it| generate_code(&it));

    let generated_code = quote! {
        #(#message_impl)*
    };

    {
        let out_dir = env::var_os("OUT_DIR").unwrap();
        let dest_path = Path::new(&out_dir).join("generated.rs");
        let mut file = File::create(dest_path)?;

        let syntax_tree: syn::File = syn::parse2(generated_code)?;
        let code = prettyplease::unparse(&syntax_tree);
        file.write_all(code.as_bytes())?;
    }

    Ok(())
}

fn generate_code(message: &MessageDescriptor) -> TokenStream {
    let message_ident = TokenStream::from_str(message.name()).unwrap();

    let statements = message
        .fields()
        .into_iter()
        .enumerate()
        .filter_map(|(field_idx, field)| {
            let kind = field.kind();
            let wire_type = kind.wire_type();
            let tag = make_tag(field.number(), wire_type);

            let handle_kind = generate_impl_for_kind(kind, field_idx)?;
            let inner = match field.cardinality() {
                Cardinality::Repeated => {
                    quote! {
                        ctx.handle_repeated_field(
                            output_vector,
                            row_idx,
                            |ctx, output_vector, row_idx| {
                                #handle_kind

                                Ok(())
                            }
                        )?;
                    }
                }
                Cardinality::Optional | Cardinality::Required => handle_kind,
            };

            Some(quote! {
                #tag => {
                    let output_vector = target.get_vector(#field_idx);

                    #inner
                }
            })
        });

    quote! {
        pub struct #message_ident;

        impl crate::gen::ParseIntoDuckDB for #message_ident {
            fn parse(
                ctx: &mut ParseContext,
                row_idx: usize,
                target: &impl crate::read::VectorAccessor,
            ) -> anyhow::Result<()> {
                while let Some(tag) = ctx.read_varint::<u32>()? {
                    match tag {
                        #(#statements)*
                        tag => {
                            ctx.skip_tag(tag)?;
                        }
                    };
                };

                Ok(())
            }
        }
    }
}

fn generate_impl_for_kind(kind: Kind, field_idx: usize) -> Option<TokenStream> {
    let result = match kind {
        Kind::Message(message) => {
            let message_ident = TokenStream::from_str(message.name()).unwrap();

            quote! {
                let target = unsafe { crate::read::StructVector::new(output_vector) };
                let len = ctx.must_read_varint::<u64>()?;

                <#message_ident as crate::gen::ParseIntoDuckDB>::parse(
                    &mut ctx.next(len as _, #field_idx),
                    row_idx,
                    &target,
                )?;

                ctx.consume(len as _);
            }
        }
        Kind::String => {
            quote! {
                ctx.read_string(output_vector, row_idx)?;
            }
        }
        Kind::Double => {
            quote! {
                ctx.read_fixed_bytes::<8>(output_vector, row_idx)?;
            }
        }
        Kind::Float => {
            quote! {
                ctx.read_fixed_bytes::<4>(output_vector, row_idx)?;
            }
        }
        Kind::Int64 | Kind::Uint64 => {
            quote! {
                ctx.read_varint_value::<u64>(output_vector, row_idx)?;
            }
        }
        Kind::Int32 | Kind::Uint32 => {
            quote! {
                ctx.read_varint_value::<u32>(output_vector, row_idx)?;
            }
        }
        Kind::Bool => {
            quote! {
                ctx.read_bool_value(output_vector, row_idx)?;
            }
        }
        _ => return None,
    };

    Some(result)
}

pub fn make_tag(field_number: u32, wire_type: WireType) -> u32 {
    (field_number << 3) | (wire_type as u32)
}
