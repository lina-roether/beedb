use proc_macro2::{Ident, TokenStream};
use proc_macro_error::{abort, proc_macro_error};
use quote::quote;
use syn::{Data, DeriveInput, Type};

#[proc_macro_derive(ByteView, attributes(dynamically_sized))]
#[proc_macro_error]
pub fn derive_byte_view(item: proc_macro::TokenStream) -> proc_macro::TokenStream {
	let input: DeriveInput = syn::parse(item).unwrap();

	let Data::Struct(struct_data) = input.data else {
		abort!(input, "Only structs are supported for this derive macro");
	};

	let types: Vec<Type> = struct_data
		.fields
		.iter()
		.map(|field| field.ty.clone())
		.collect();

	let is_dyn_sized = input
		.attrs
		.iter()
		.any(|attr| attr.path().is_ident("dynamically_sized"));

	let assert = assert_byte_view(&types);

	let impl_block = if is_dyn_sized {
		implement_byte_view_for_unsized_type(input.ident, &types)
	} else {
		implement_byte_view_for_sized_type(input.ident)
	};

	quote! {
		#assert
		#impl_block
	}
	.into()
}

fn assert_byte_view(types: &[Type]) -> TokenStream {
	quote! {
		#(byte_view::assert_byte_view!(#types);)*
	}
}

fn implement_byte_view_for_sized_type(name: Ident) -> TokenStream {
	quote! {
		byte_view::unsafe_impl_byte_view_sized!(#name);
	}
}

fn implement_byte_view_for_unsized_type(name: Ident, types: &[Type]) -> TokenStream {
	if types.is_empty() {
		panic!("Need at least one type");
	}

	let aligments: Vec<TokenStream> = types
		.iter()
		.map(|ty| {
			quote! {
				<#ty as ByteView>::ALIGN
			}
		})
		.collect();
	let total_alignment = const_max(&aligments);

	let sized_types = &types[0..types.len() - 1];
	let slice_type = types.last().unwrap();
	let Type::Slice(slice) = slice_type else {
		abort!(
			slice_type,
			"For dynamically sized structs, only slice types are supported in this position"
		);
	};

	let item_type = slice.elem.as_ref().clone();

	quote! {
		unsafe impl ByteView for #name {
			const ALIGN: usize = #total_alignment;
			const MIN_SIZE: usize = #(std::mem::size_of::<#sized_types>())+*;

			unsafe fn from_bytes_unchecked(bytes: &[u8]) -> &Self {
				byte_view::transmute_unsized(bytes, (bytes.len() - Self::MIN_SIZE) / std::mem::size_of::<#item_type>())
			}

			unsafe fn from_bytes_mut_unchecked(bytes: &mut [u8]) -> &mut Self {
				byte_view::transmute_unsized_mut(bytes, (bytes.len() - Self::MIN_SIZE) / std::mem::size_of::<#item_type>())
			}
		}
	}
}

fn const_max(values: &[TokenStream]) -> TokenStream {
	if values.is_empty() {
		panic!("Need at least one item");
	}
	if values.len() == 1 {
		let mut ts = TokenStream::new();
		ts.extend(values[0].clone());
		return ts;
	}
	let first = values[0].clone();
	let max_of_rest = const_max(&values[1..]);
	quote! {
		{
			let rest = #max_of_rest;
			if #first > rest { #first } else { rest }
		}
	}
}
