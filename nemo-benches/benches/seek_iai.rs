use nemo_physical::columnar::{
    column_types::vector::ColumnVector,
    traits::{column::Column, columnscan::ColumnScan},
};
use rand::prelude::*;
use rand_pcg::Pcg64;

pub fn bench() {
    let mut rng = Pcg64::seed_from_u64(21564);
    let mut data: Vec<usize> = Vec::new();
    for _i in 0..10000001 {
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
    let test_column = ColumnVector::new(data);
    let mut gc = test_column.iter();
    gc.seek(randa);
    gc.seek(randb);
}
#[cfg(unix)]
iai::main!(bench);
#[cfg(not(unix))]
pub fn main() {
    println!("Plattform does not support iai/valgrind");
}
