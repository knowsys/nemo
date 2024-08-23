/// A Parser for RFC 3987 IRIs
use nom::{
    branch::alt,
    character::complete::{digit0, one_of, satisfy},
    combinator::{opt, recognize},
    multi::{count, many0, many1, many_m_n},
    sequence::{delimited, pair, tuple},
};

use super::{
    old::token,
    rfc5234::{alpha, digit, hexdig},
    types::{IntermediateResult, Span},
};

use macros::traced;

#[traced("parser::iri")]
pub fn iri(input: Span) -> IntermediateResult<Span> {
    recognize(tuple((
        scheme,
        token(":"),
        ihier_part,
        opt(pair(token("?"), iquery)),
        opt(pair(token("#"), ifragment)),
    )))(input)
}

#[traced("parser::iri")]
fn ihier_part(input: Span) -> IntermediateResult<Span> {
    alt((
        recognize(tuple((token("//"), iauthority, ipath_abempty))),
        ipath_absolute,
        ipath_rootless,
        ipath_empty,
    ))(input)
}

#[traced("parser::iri")]
pub fn iri_reference(input: Span) -> IntermediateResult<Span> {
    alt((iri, irelative_ref))(input)
}

#[allow(dead_code)]
#[traced("parser::iri")]
pub fn absolute_iri(input: Span) -> IntermediateResult<Span> {
    recognize(tuple((
        scheme,
        token(":"),
        ihier_part,
        opt(pair(token("?"), iquery)),
    )))(input)
}

#[traced("parser::iri")]
pub fn irelative_ref(input: Span) -> IntermediateResult<Span> {
    recognize(tuple((
        irelative_part,
        opt(pair(token("?"), iquery)),
        opt(pair(token("#"), ifragment)),
    )))(input)
}

#[traced("parser::iri")]
fn irelative_part(input: Span) -> IntermediateResult<Span> {
    recognize(alt((
        recognize(tuple((token("//"), iauthority, ipath_abempty))),
        ipath_absolute,
        ipath_noscheme,
        ipath_empty,
    )))(input)
}

#[traced("parser::iri")]
fn iauthority(input: Span) -> IntermediateResult<Span> {
    recognize(tuple((
        opt(pair(iuserinfo, token("@"))),
        ihost,
        opt(pair(token(":"), port)),
    )))(input)
}

#[traced("parser::iri")]
fn iuserinfo(input: Span) -> IntermediateResult<Span> {
    recognize(many0(alt((
        iunreserved,
        pct_encoded,
        sub_delims,
        token(":"),
    ))))(input)
}

#[traced("parser::iri")]
fn ihost(input: Span) -> IntermediateResult<Span> {
    alt((ip_literal, ipv4_address, ireg_name))(input)
}

#[traced("parser::iri")]
fn ireg_name(input: Span) -> IntermediateResult<Span> {
    recognize(many0(alt((iunreserved, pct_encoded, sub_delims))))(input)
}

#[allow(dead_code)]
#[traced("parser::iri")]
pub fn ipath(input: Span) -> IntermediateResult<Span> {
    alt((
        ipath_abempty,
        ipath_absolute,
        ipath_noscheme,
        ipath_rootless,
        ipath_empty,
    ))(input)
}

#[traced("parser::iri")]
fn ipath_abempty(input: Span) -> IntermediateResult<Span> {
    recognize(many0(pair(token("/"), isegment)))(input)
}

#[traced("parser::iri")]
fn ipath_absolute(input: Span) -> IntermediateResult<Span> {
    recognize(pair(
        token("/"),
        opt(pair(isegment_nz, many0(pair(token("/"), isegment)))),
    ))(input)
}

#[traced("parser::iri")]
fn ipath_noscheme(input: Span) -> IntermediateResult<Span> {
    recognize(pair(isegment_nz_nc, many0(pair(token("/"), isegment))))(input)
}

#[traced("parser::iri")]
fn ipath_rootless(input: Span) -> IntermediateResult<Span> {
    recognize(pair(isegment_nz, many0(pair(token("/"), isegment))))(input)
}

#[traced("parser::iri")]
fn ipath_empty(input: Span) -> IntermediateResult<Span> {
    token("")(input)
}

#[traced("parser::iri")]
fn isegment(input: Span) -> IntermediateResult<Span> {
    recognize(many0(ipchar))(input)
}

#[traced("parser::iri")]
fn isegment_nz(input: Span) -> IntermediateResult<Span> {
    recognize(many1(ipchar))(input)
}

#[traced("parser::iri")]
fn isegment_nz_nc(input: Span) -> IntermediateResult<Span> {
    recognize(many1(alt((
        iunreserved,
        pct_encoded,
        sub_delims,
        token("@"),
    ))))(input)
}

#[traced("parser::iri")]
fn ipchar(input: Span) -> IntermediateResult<Span> {
    alt((iunreserved, pct_encoded, sub_delims, token(":"), token("@")))(input)
}

#[traced("parser::iri")]
fn iquery(input: Span) -> IntermediateResult<Span> {
    recognize(many0(alt((ipchar, iprivate, token("/"), token("?")))))(input)
}

#[traced("parser::iri")]
fn ifragment(input: Span) -> IntermediateResult<Span> {
    recognize(many0(alt((ipchar, token("/"), token("?")))))(input)
}

#[traced("parser::iri")]
fn iunreserved(input: Span) -> IntermediateResult<Span> {
    alt((
        alpha,
        digit,
        token("-"),
        token("."),
        token("_"),
        token("~"),
        ucschar,
    ))(input)
}

#[traced("parser::iri")]
fn ucschar(input: Span) -> IntermediateResult<Span> {
    recognize(satisfy(|c| {
        [
            0xa0u32..=0xd7ff,
            0xf900..=0xfdcf,
            0xfdf0..=0xffef,
            0x10000..=0x1fffd,
            0x20000..=0x2fffd,
            0x30000..=0x3fffd,
            0x40000..=0x4fffd,
            0x50000..=0x5fffd,
            0x60000..=0x6fffd,
            0x70000..=0x7fffd,
            0x80000..=0x8fffd,
            0x90000..=0x9fffd,
            0xa0000..=0xafffd,
            0xb0000..=0xbfffd,
            0xc0000..=0xcfffd,
            0xd0000..=0xdfffd,
            0xe0000..=0xefffd,
        ]
        .iter()
        .any(|range| range.contains(&c.into()))
    }))(input)
}

#[traced("parser::iri")]
fn iprivate(input: Span) -> IntermediateResult<Span> {
    recognize(satisfy(|c| {
        [0xe000u32..=0xf8ff, 0xf000..=0xffffd, 0x100000..=0x10fffd]
            .iter()
            .any(|range| range.contains(&c.into()))
    }))(input)
}

#[traced("parser::iri")]
fn scheme(input: Span) -> IntermediateResult<Span> {
    recognize(tuple((
        alpha,
        many0(alt((alpha, digit, token("+"), token("-"), token(".")))),
    )))(input)
}

#[traced("parser::iri")]
fn port(input: Span) -> IntermediateResult<Span> {
    digit0(input)
}

#[traced("parser::iri")]
fn ip_literal(input: Span) -> IntermediateResult<Span> {
    delimited(token("["), alt((ipv6_address, ipv_future)), token("]"))(input)
}

#[traced("parser::iri")]
fn ipv_future(input: Span) -> IntermediateResult<Span> {
    recognize(tuple((
        token("v"),
        hexdig,
        token("."),
        many1(alt((unreserved, sub_delims, token(":")))),
    )))(input)
}

#[traced("parser::iri")]
fn ipv6_address(input: Span) -> IntermediateResult<Span> {
    let h16_colon = || pair(h16, token(":"));
    alt((
        recognize(tuple((count(h16_colon(), 6), ls32))),
        recognize(tuple((token("::"), count(h16_colon(), 5), ls32))),
        recognize(tuple((h16, token("::"), count(h16_colon(), 4), ls32))),
        recognize(tuple((
            h16_colon(),
            h16,
            token("::"),
            count(h16_colon(), 3),
            ls32,
        ))),
        recognize(tuple((
            count(h16_colon(), 2),
            h16,
            token("::"),
            count(h16_colon(), 2),
            ls32,
        ))),
        recognize(tuple((
            count(h16_colon(), 3),
            h16,
            token("::"),
            h16_colon(),
            ls32,
        ))),
        recognize(tuple((count(h16_colon(), 4), h16, token("::"), ls32))),
        recognize(tuple((count(h16_colon(), 5), h16, token("::"), h16))),
        recognize(tuple((count(h16_colon(), 6), h16, token("::")))),
    ))(input)
}

#[traced("parser::iri")]
fn h16(input: Span) -> IntermediateResult<Span> {
    recognize(many_m_n(1, 4, hexdig))(input)
}

#[traced("parser::iri")]
fn ls32(input: Span) -> IntermediateResult<Span> {
    alt((recognize(tuple((h16, token(":"), h16))), ipv4_address))(input)
}

#[traced("parser::iri")]
fn ipv4_address(input: Span) -> IntermediateResult<Span> {
    recognize(tuple((
        dec_octet,
        token("."),
        dec_octet,
        token("."),
        dec_octet,
        token("."),
        dec_octet,
    )))(input)
}

#[traced("parser::iri")]
fn dec_octet(input: Span) -> IntermediateResult<Span> {
    alt((
        digit, // 0-9
        recognize(tuple((
            // 10-99
            satisfy(|c| ('1'..='9').contains(&c)),
            digit,
        ))),
        recognize(tuple((token("1"), digit, digit))), // 100-199
        recognize(tuple((
            // 200-249
            token("2"),
            satisfy(|c| ('0'..='4').contains(&c)),
            digit,
        ))),
        recognize(tuple((token("25"), satisfy(|c| ('0'..='5').contains(&c))))), // 250-255
    ))(input)
}

#[traced("parser::iri")]
fn pct_encoded(input: Span) -> IntermediateResult<Span> {
    recognize(tuple((token("%"), hexdig, hexdig)))(input)
}

#[traced("parser::iri")]
fn unreserved(input: Span) -> IntermediateResult<Span> {
    alt((alpha, digit, recognize(one_of(r#"-._~"#))))(input)
}

#[allow(dead_code)]
#[traced("parser::iri")]
pub fn reserved(input: Span) -> IntermediateResult<Span> {
    alt((gen_delims, sub_delims))(input)
}

#[traced("parser::iri")]
fn gen_delims(input: Span) -> IntermediateResult<Span> {
    recognize(one_of(r#":/?#[]@"#))(input)
}

#[traced("parser::iri")]
fn sub_delims(input: Span) -> IntermediateResult<Span> {
    recognize(one_of(r#"!$&'()*+,;="#))(input)
}
