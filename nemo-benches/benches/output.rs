use criterion::{criterion_group, criterion_main, BatchSize, Criterion};
use nemo::model::PrimitiveType;
use nemo_physical::datatypes::data_value::{DataValueIteratorT, PhysicalString};
use rand::{distributions::Alphanumeric, prelude::*};
use rand_pcg::Pcg64;

pub fn benchmark_input(c: &mut Criterion) {
    let mut rng = Pcg64::seed_from_u64(21564);

    let strings = (0..100000)
        .map(|_| {
            (&mut rng)
                .sample_iter(Alphanumeric)
                .take(20)
                .map(char::from)
                .collect::<String>()
        })
        .collect::<Vec<_>>();

    let physical_strings = strings
        .iter()
        .map(|s| PhysicalString::from(format!("STRING:{s}")))
        .collect::<Vec<_>>();
    let physical_constants = strings
        .iter()
        .map(|s| PhysicalString::from(format!("CONSTANT:{s}")))
        .collect::<Vec<_>>();

    let mut physical_output = c.benchmark_group("output_physical_values");

    physical_output.bench_function("physical_strings", |b| {
        b.iter_batched(
            || physical_strings.clone(),
            |output| {
                for _ in output.into_iter() {}
            },
            BatchSize::SmallInput,
        )
    });

    physical_output.bench_function("physical_constants", |b| {
        b.iter_batched(
            || physical_constants.clone(),
            |output| {
                for _ in output.into_iter() {}
            },
            BatchSize::SmallInput,
        )
    });

    physical_output.finish();

    let mut logical_output = c.benchmark_group("output_logical_values");

    logical_output.bench_function("logical_strings", |b| {
        b.iter_batched(
            || physical_strings.clone(),
            |output| {
                for _ in PrimitiveType::String.primitive_logical_value_iterator(
                    DataValueIteratorT::String(Box::new(output.into_iter())),
                ) {}
            },
            BatchSize::SmallInput,
        )
    });

    logical_output.bench_function("strings_in_logical_any", |b| {
        b.iter_batched(
            || physical_strings.clone(),
            |output| {
                for _ in PrimitiveType::Any.primitive_logical_value_iterator(
                    DataValueIteratorT::String(Box::new(output.into_iter())),
                ) {}
            },
            BatchSize::SmallInput,
        )
    });

    logical_output.bench_function("constants_in_logical_any", |b| {
        b.iter_batched(
            || physical_constants.clone(),
            |output| {
                for _ in PrimitiveType::Any.primitive_logical_value_iterator(
                    DataValueIteratorT::String(Box::new(output.into_iter())),
                ) {}
            },
            BatchSize::SmallInput,
        )
    });

    logical_output.finish();

    let mut serialized_output = c.benchmark_group("output_serialized_values");

    serialized_output.bench_function("logical_strings", |b| {
        b.iter_batched(
            || physical_strings.clone(),
            |output| {
                for _ in PrimitiveType::String
                    .serialize_output(DataValueIteratorT::String(Box::new(output.into_iter())))
                {
                }
            },
            BatchSize::SmallInput,
        )
    });

    serialized_output.bench_function("strings_in_logical_any", |b| {
        b.iter_batched(
            || physical_strings.clone(),
            |output| {
                for _ in PrimitiveType::Any
                    .serialize_output(DataValueIteratorT::String(Box::new(output.into_iter())))
                {
                }
            },
            BatchSize::SmallInput,
        )
    });

    serialized_output.bench_function("constants_in_logical_any", |b| {
        b.iter_batched(
            || physical_constants.clone(),
            |output| {
                for _ in PrimitiveType::Any
                    .serialize_output(DataValueIteratorT::String(Box::new(output.into_iter())))
                {
                }
            },
            BatchSize::SmallInput,
        )
    });

    serialized_output.finish();
}

criterion_group!(benches, benchmark_input);
criterion_main!(benches);
