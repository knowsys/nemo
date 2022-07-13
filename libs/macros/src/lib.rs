use proc_macro::TokenStream;
use proc_macro2::Literal;
use quote::{quote, ToTokens};
use syn::{parse, FnArg::Typed, ItemFn, Pat::Ident};

#[proc_macro_attribute]
pub fn traced(attrs: TokenStream, item: TokenStream) -> TokenStream {
    let target: Literal = match parse(attrs) {
        Ok(lit) => lit,
        _ => panic!("#[traced] must be applied with one string argument specifying the log target"),
    };

    let mut fun: ItemFn = match parse(item) {
        Ok(fun) => fun,
        Err(..) => panic!("#[traced] must be applied on functions"),
    };

    let args = fun
        .sig
        .inputs
        .iter()
        .filter_map(|input| match input {
            Typed(pat) => match &*pat.pat {
                Ident(ident) => Some(ident),
                _ => None,
            },
            _ => None,
        })
        .collect::<Vec<_>>();

    let name = &fun.sig.ident;
    let fmt_before = format!("{}{{:?}}", name);
    let fmt_after = format!("{}{{:?}} -> {{:?}}", name);
    let stmts = &fun.block.stmts;
    let block = quote! {{
        log::trace!(target: #target, #fmt_before, (#(#args)*,));
        let result = { #(#stmts)* };
        log::trace!(target: #target, #fmt_after, (#(#args)*,), result);
        result
    }
    };

    fun.block = parse(block.into()).expect("should parse");
    fun.to_token_stream().into()
}
