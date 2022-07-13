/// A Parser for RFC 3987 IRIs
use nom::{
    branch::alt,
    bytes::complete::tag,
    character::complete::{digit0, one_of, satisfy},
    combinator::{opt, recognize},
    multi::{count, many0, many1, many_m_n},
    sequence::{delimited, pair, tuple},
};

use super::{
    rfc5234::{alpha, digit, hexdig},
    types::IntermediateResult,
};

use macros::traced;

#[traced("parser::iri")]
pub fn iri(input: &str) -> IntermediateResult<&str> {
    recognize(tuple((
        scheme,
        tag(":"),
        ihier_part,
        opt(pair(tag("?"), iquery)),
        opt(pair(tag("#"), ifragment)),
    )))(input)
}

#[traced("parser::iri")]
fn ihier_part(input: &str) -> IntermediateResult<&str> {
    alt((
        recognize(tuple((tag("//"), iauthority, ipath_abempty))),
        ipath_absolute,
        ipath_rootless,
        ipath_empty,
    ))(input)
}

#[traced("parser::iri")]
pub fn iri_reference(input: &str) -> IntermediateResult<&str> {
    alt((iri, irelative_ref))(input)
}

#[allow(dead_code)]
#[traced("parser::iri")]
pub fn absolute_iri(input: &str) -> IntermediateResult<&str> {
    recognize(tuple((
        scheme,
        tag(":"),
        ihier_part,
        opt(pair(tag("?"), iquery)),
    )))(input)
}

#[traced("parser::iri")]
pub fn irelative_ref(input: &str) -> IntermediateResult<&str> {
    recognize(tuple((
        irelative_part,
        opt(pair(tag("?"), iquery)),
        opt(pair(tag("#"), ifragment)),
    )))(input)
}

#[traced("parser::iri")]
fn irelative_part(input: &str) -> IntermediateResult<&str> {
    recognize(tuple((
        tag("//"),
        iauthority,
        ipath_abempty,
        ipath_absolute,
        ipath_noscheme,
        ipath_empty,
    )))(input)
}

#[traced("parser::iri")]
fn iauthority(input: &str) -> IntermediateResult<&str> {
    recognize(tuple((
        opt(pair(iuserinfo, tag("@"))),
        ihost,
        opt(pair(tag(":"), port)),
    )))(input)
}

#[traced("parser::iri")]
fn iuserinfo(input: &str) -> IntermediateResult<&str> {
    recognize(many0(alt((iunreserved, pct_encoded, sub_delims, tag(":")))))(input)
}

#[traced("parser::iri")]
fn ihost(input: &str) -> IntermediateResult<&str> {
    alt((ip_literal, ipv4_address, ireg_name))(input)
}

#[traced("parser::iri")]
fn ireg_name(input: &str) -> IntermediateResult<&str> {
    recognize(many0(alt((iunreserved, pct_encoded, sub_delims))))(input)
}

#[allow(dead_code)]
#[traced("parser::iri")]
pub fn ipath(input: &str) -> IntermediateResult<&str> {
    alt((
        ipath_abempty,
        ipath_absolute,
        ipath_noscheme,
        ipath_rootless,
        ipath_empty,
    ))(input)
}

#[traced("parser::iri")]
fn ipath_abempty(input: &str) -> IntermediateResult<&str> {
    recognize(many0(pair(tag("/"), isegment)))(input)
}

#[traced("parser::iri")]
fn ipath_absolute(input: &str) -> IntermediateResult<&str> {
    recognize(pair(
        tag("/"),
        opt(pair(isegment_nz, many0(pair(tag("/"), isegment)))),
    ))(input)
}

#[traced("parser::iri")]
fn ipath_noscheme(input: &str) -> IntermediateResult<&str> {
    recognize(pair(isegment_nz_nc, many0(pair(tag("/"), isegment))))(input)
}

#[traced("parser::iri")]
fn ipath_rootless(input: &str) -> IntermediateResult<&str> {
    recognize(pair(isegment_nz, many0(pair(tag("/"), isegment))))(input)
}

#[traced("parser::iri")]
fn ipath_empty(input: &str) -> IntermediateResult<&str> {
    tag("")(input)
}

#[traced("parser::iri")]
fn isegment(input: &str) -> IntermediateResult<&str> {
    recognize(many0(ipchar))(input)
}

#[traced("parser::iri")]
fn isegment_nz(input: &str) -> IntermediateResult<&str> {
    recognize(many1(ipchar))(input)
}

#[traced("parser::iri")]
fn isegment_nz_nc(input: &str) -> IntermediateResult<&str> {
    recognize(many1(alt((iunreserved, pct_encoded, sub_delims, tag("@")))))(input)
}

#[traced("parser::iri")]
fn ipchar(input: &str) -> IntermediateResult<&str> {
    alt((iunreserved, pct_encoded, sub_delims, tag(":"), tag("@")))(input)
}

#[traced("parser::iri")]
fn iquery(input: &str) -> IntermediateResult<&str> {
    recognize(many0(alt((ipchar, iprivate, tag("/"), tag("?")))))(input)
}

#[traced("parser::iri")]
fn ifragment(input: &str) -> IntermediateResult<&str> {
    recognize(many0(alt((ipchar, tag("/"), tag("?")))))(input)
}

#[traced("parser::iri")]
fn iunreserved(input: &str) -> IntermediateResult<&str> {
    alt((
        alpha,
        digit,
        tag("-"),
        tag("."),
        tag("_"),
        tag("~"),
        ucschar,
    ))(input)
}

#[traced("parser::iri")]
fn ucschar(input: &str) -> IntermediateResult<&str> {
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
fn iprivate(input: &str) -> IntermediateResult<&str> {
    recognize(satisfy(|c| {
        [0xe000u32..=0xf8ff, 0xf000..=0xffffd, 0x100000..=0x10fffd]
            .iter()
            .any(|range| range.contains(&c.into()))
    }))(input)
}

#[traced("parser::iri")]
fn scheme(input: &str) -> IntermediateResult<&str> {
    recognize(tuple((
        alpha,
        many0(alt((alpha, digit, tag("+"), tag("-"), tag(".")))),
    )))(input)
}

#[traced("parser::iri")]
fn port(input: &str) -> IntermediateResult<&str> {
    digit0(input)
}

#[traced("parser::iri")]
fn ip_literal(input: &str) -> IntermediateResult<&str> {
    delimited(tag("["), alt((ipv6_address, ipv_future)), tag("]"))(input)
}

#[traced("parser::iri")]
fn ipv_future(input: &str) -> IntermediateResult<&str> {
    recognize(tuple((
        tag("v"),
        hexdig,
        tag("."),
        many1(alt((unreserved, sub_delims, tag(":")))),
    )))(input)
}

#[traced("parser::iri")]
fn ipv6_address(input: &str) -> IntermediateResult<&str> {
    let h16_colon = || pair(h16, tag(":"));
    alt((
        recognize(tuple((count(h16_colon(), 6), ls32))),
        recognize(tuple((tag("::"), count(h16_colon(), 5), ls32))),
        recognize(tuple((h16, tag("::"), count(h16_colon(), 4), ls32))),
        recognize(tuple((
            h16_colon(),
            h16,
            tag("::"),
            count(h16_colon(), 3),
            ls32,
        ))),
        recognize(tuple((
            count(h16_colon(), 2),
            h16,
            tag("::"),
            count(h16_colon(), 2),
            ls32,
        ))),
        recognize(tuple((
            count(h16_colon(), 3),
            h16,
            tag("::"),
            h16_colon(),
            ls32,
        ))),
        recognize(tuple((count(h16_colon(), 4), h16, tag("::"), ls32))),
        recognize(tuple((count(h16_colon(), 5), h16, tag("::"), h16))),
        recognize(tuple((count(h16_colon(), 6), h16, tag("::")))),
    ))(input)
}

#[traced("parser::iri")]
fn h16(input: &str) -> IntermediateResult<&str> {
    recognize(many_m_n(1, 4, hexdig))(input)
}

#[traced("parser::iri")]
fn ls32(input: &str) -> IntermediateResult<&str> {
    alt((recognize(tuple((h16, tag(":"), h16))), ipv4_address))(input)
}

#[traced("parser::iri")]
fn ipv4_address(input: &str) -> IntermediateResult<&str> {
    recognize(tuple((
        dec_octet,
        tag("."),
        dec_octet,
        tag("."),
        dec_octet,
        tag("."),
        dec_octet,
    )))(input)
}

#[traced("parser::iri")]
fn dec_octet(input: &str) -> IntermediateResult<&str> {
    alt((
        digit, // 0-9
        recognize(tuple((
            // 10-99
            satisfy(|c| ('1'..='9').contains(&c)),
            digit,
        ))),
        recognize(tuple((tag("1"), digit, digit))), // 100-199
        recognize(tuple((
            // 200-249
            tag("2"),
            satisfy(|c| ('0'..='4').contains(&c)),
            digit,
        ))),
        recognize(tuple((tag("25"), satisfy(|c| ('0'..='5').contains(&c))))), // 250-255
    ))(input)
}

#[traced("parser::iri")]
fn pct_encoded(input: &str) -> IntermediateResult<&str> {
    recognize(tuple((tag("%"), hexdig, hexdig)))(input)
}

#[traced("parser::iri")]
fn unreserved(input: &str) -> IntermediateResult<&str> {
    alt((alpha, digit, recognize(one_of(r#"-._~"#))))(input)
}

#[allow(dead_code)]
#[traced("parser::iri")]
pub fn reserved(input: &str) -> IntermediateResult<&str> {
    alt((gen_delims, sub_delims))(input)
}

#[traced("parser::iri")]
fn gen_delims(input: &str) -> IntermediateResult<&str> {
    recognize(one_of(r#":/?#[]@"#))(input)
}

#[traced("parser::iri")]
fn sub_delims(input: &str) -> IntermediateResult<&str> {
    recognize(one_of(r#"!$&'()*+,;="#))(input)
}
