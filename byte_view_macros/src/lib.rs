use proc_macro2::{Ident, Span, TokenStream};
use proc_macro_error::{abort, proc_macro_error};
use quote::{quote, ToTokens};
use syn::{Data, DeriveInput, GenericParam, Generics, Type};

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

	let impl_block = if is_dyn_sized {
		implement_byte_view_for_unsized_type(input.ident, input.generics, &types)
	} else {
		implement_byte_view_for_sized_type(input.ident, input.generics, &types)
	};

	quote! {
		#impl_block
	}
	.into()
}

fn generics_definition(generics: &Generics) -> TokenStream {
	if generics.params.is_empty() {
		return TokenStream::new();
	}

	let mut ts = TokenStream::new();
	generics.lt_token.unwrap().to_tokens(&mut ts);
	for param in &generics.params {
		param.to_tokens(&mut ts);
	}
	generics.gt_token.unwrap().to_tokens(&mut ts);

	ts
}

fn generics_application(generics: &Generics) -> TokenStream {
	if generics.params.is_empty() {
		return TokenStream::new();
	}

	let mut ts = TokenStream::new();
	generics.lt_token.unwrap().to_tokens(&mut ts);
	for param in &generics.params {
		match param {
			GenericParam::Const(p) => p.ident.to_tokens(&mut ts),
			GenericParam::Type(p) => p.ident.to_tokens(&mut ts),
			GenericParam::Lifetime(p) => p.to_tokens(&mut ts),
		}
	}
	generics.gt_token.unwrap().to_tokens(&mut ts);

	ts
}

fn implement_byte_view_for_sized_type(
	name: Ident,
	generics: Generics,
	types: &[Type],
) -> TokenStream {
	let gen_def = generics_definition(&generics);
	let gen_app = generics_application(&generics);
	let gen_where = generics.where_clause;

	let assert_mod_name = Ident::new(&format!("__asertions_{}", name), Span::call_site());

	quote! {
		mod #assert_mod_name {
			use super::*;

			impl #gen_def #name #gen_app #gen_where {
				const fn __assertions() {
					const fn assert_byte_view<T: ?Sized + ByteView>() {}
					#(assert_byte_view::<#types>();)*
				}
			}
		}

		unsafe impl #gen_def ByteView for #name #gen_app #gen_where {
			const ALIGN: usize = std::mem::align_of::<Self>();
			const MIN_SIZE: usize = std::mem::size_of::<Self>();

			unsafe fn from_bytes_unchecked(bytes: &[u8]) -> &Self {
				byte_view::transmute(bytes)
			}

			unsafe fn from_bytes_mut_unchecked(bytes: &mut [u8]) -> &mut Self {
				byte_view::transmute_mut(bytes)
			}
		}
	}
}

fn implement_byte_view_for_unsized_type(
	name: Ident,
	generics: Generics,
	types: &[Type],
) -> TokenStream {
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

	let gen_def = generics_definition(&generics);
	let gen_app = generics_application(&generics);
	let gen_where = generics.where_clause;

	let assert_mod_name = Ident::new(&format!("__asertions_{}", name), Span::call_site());

	let min_size: TokenStream = if sized_types.is_empty() {
		"0".parse().unwrap()
	} else {
		quote! { #(std::mem::size_of::<#sized_types>())+*}
	};

	quote! {
		mod #assert_mod_name {
			use super::*;

			impl #gen_def #name #gen_app #gen_where {
				const fn __assertions() {
					const fn assert_byte_view<T: ?Sized + ByteView>() {}
					#(assert_byte_view::<#types>();)*
				}
			}
		}

		unsafe impl #gen_def ByteView for #name #gen_app #gen_where {
			const ALIGN: usize = #total_alignment;
			const MIN_SIZE: usize = #min_size;

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
