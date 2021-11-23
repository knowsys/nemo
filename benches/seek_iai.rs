use std::ops::Deref;

use iai::main;
use rand::prelude::*;
use rand_pcg::Pcg64;
use stage2::physical::columns::{ColumnScan, GenericColumnScan, VectorColumn};

pub fn bench() {
    let mut rng = Pcg64::seed_from_u64(21564);
    let mut data: Vec<usize> = Vec::new();
    let seek: Vec<usize>;
    for i in 0..10000001 {
        data.push(rng.gen::<usize>());
    }
    let values = (
        data[rng.gen_range(0..10000000)],
        data[rng.gen_range(0..10000000)],
    );
    let (randa, randb) = if values.0 < values.1 {
        values
    } else {
        (values.1, values.0)
    };
    let test_column = VectorColumn::new(data);
    let mut gc = GenericColumnScan::new(&test_column);
    gc.seek(randa);
    gc.seek(randb);
}
main!(bench);
