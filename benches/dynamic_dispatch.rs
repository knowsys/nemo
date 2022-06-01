use criterion::{criterion_group, criterion_main, Criterion};
use rand::prelude::*;
use rand_pcg::Pcg64;

trait MyTrait {
    fn a(&self) -> usize;

    fn b(&self) -> usize;
}

struct FirstImpl {
    x: usize,
    y: usize,
}

impl FirstImpl {
    fn new(rng: &mut Pcg64) -> Self {
        Self {
            x: rng.gen::<usize>(),
            y: rng.gen::<usize>(),
        }
    }
}

impl MyTrait for FirstImpl {
    fn a(&self) -> usize {
        self.x + self.y
    }

    fn b(&self) -> usize {
        self.y - self.x
    }
}

struct SecondImpl {
    x: usize,
    y: usize,
}

impl SecondImpl {
    fn new(rng: &mut Pcg64) -> Self {
        Self {
            x: rng.gen::<usize>(),
            y: rng.gen::<usize>(),
        }
    }
}

impl MyTrait for SecondImpl {
    fn a(&self) -> usize {
        self.x - self.y
    }

    fn b(&self) -> usize {
        self.y + self.x
    }
}

enum TraitEnum {
    First(FirstImpl),
    Second(SecondImpl),
}

impl MyTrait for TraitEnum {
    fn a(&self) -> usize {
        match self {
            TraitEnum::First(i) => i.a(),
            TraitEnum::Second(i) => i.a(),
        }
    }

    fn b(&self) -> usize {
        match self {
            TraitEnum::First(i) => i.b(),
            TraitEnum::Second(i) => i.b(),
        }
    }
}

fn call_a_dyn(vector: &Vec<Box<dyn MyTrait>>, target: &mut Vec<usize>) {
    target.clear();
    for elem in vector {
        target.push(elem.a());
    }
}

fn call_a_enum(vector: &Vec<TraitEnum>, target: &mut Vec<usize>) {
    target.clear();

    for elem in vector {
        target.push(elem.a());
    }
}

pub fn benchmark_dyn(c: &mut Criterion) {
    const NUM_ELEMENTS: usize = 10000000;

    let mut rng = Pcg64::seed_from_u64(21564);
    let mut dyn_vector = Vec::<Box<dyn MyTrait>>::with_capacity(NUM_ELEMENTS);
    let mut enum_vector = Vec::<TraitEnum>::with_capacity(NUM_ELEMENTS);
    let mut target_dyn = Vec::<usize>::with_capacity(NUM_ELEMENTS);
    let mut target_enum = Vec::<usize>::with_capacity(NUM_ELEMENTS);

    for _i in 0..NUM_ELEMENTS {
        if rng.gen::<usize>() % 2 == 0 {
            dyn_vector.push(Box::new(FirstImpl::new(&mut rng)));
            enum_vector.push(TraitEnum::First(FirstImpl::new(&mut rng)));
        } else {
            dyn_vector.push(Box::new(SecondImpl::new(&mut rng)));
            enum_vector.push(TraitEnum::Second(SecondImpl::new(&mut rng)));
        }
    }

    let mut group_enum = c.benchmark_group("dynamic dispatch [enum]");
    group_enum.sample_size(100);

    group_enum.bench_function("call_a_enum", |b| {
        b.iter(|| call_a_enum(&enum_vector, &mut target_enum))
    });
    group_enum.finish();

    let mut group_dyn = c.benchmark_group("dynamic dispatch [dyn]");
    group_dyn.sample_size(100);

    group_dyn.bench_function("call_a_dyn", |b| {
        b.iter(|| call_a_dyn(&dyn_vector, &mut target_dyn))
    });
    group_dyn.finish();
}

criterion_group!(benches, benchmark_dyn);
criterion_main!(benches);
