//! Benchmarks for the parser and the AST-to-rule-model translation.

use criterion::{Criterion, Throughput, criterion_group, criterion_main};
use nemo::{parser::Parser, rule_file::RuleFile, rule_model::programs::handle::ProgramHandle};
use std::hint::black_box;

/// Program consisting of many rules, each joining many atoms.
const RULES: &str = include_str!("../../resources/benchmarks/variable-order-stress.rls");
/// Program consisting of many ground facts.
const FACTS: &str = include_str!("../../resources/benchmarks/fact-stress.rls");

/// Parse the given program, discarding the result.
fn parse(input: &str) {
    let parser = Parser::initialize(input);
    let _ = black_box(parser.parse());
}

/// Parse the given program and translate it into the rule model.
fn parse_and_translate(input: &str) {
    let file = RuleFile::new(input.to_owned(), String::from("bench"));
    let _ = black_box(ProgramHandle::from_file(&file));
}

/// Number of samples per benchmark.
const SAMPLE_SIZE: usize = 10;

fn benchmark_parse(criterion: &mut Criterion) {
    let mut group = criterion.benchmark_group("parse");
    group.sample_size(SAMPLE_SIZE);

    group.throughput(Throughput::Bytes(RULES.len() as u64));
    group.bench_function("rules", |bencher| bencher.iter(|| parse(black_box(RULES))));

    group.throughput(Throughput::Bytes(FACTS.len() as u64));
    group.bench_function("facts", |bencher| bencher.iter(|| parse(black_box(FACTS))));

    group.finish();
}

fn benchmark_translate(criterion: &mut Criterion) {
    let mut group = criterion.benchmark_group("parse+translate");
    group.sample_size(SAMPLE_SIZE);

    group.throughput(Throughput::Bytes(RULES.len() as u64));
    group.bench_function("rules", |bencher| {
        bencher.iter(|| parse_and_translate(black_box(RULES)))
    });

    group.finish();
}

criterion_group!(benches, benchmark_parse, benchmark_translate);
criterion_main!(benches);
