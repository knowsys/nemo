#![feature(macro_metavar_expr)]

use operators::time_addition;

use crate::{
    gen_trie::{time_load_new_trie, time_load_old_trie},
    navigation::{
        time_navigation_simple, time_navigation_simple_dfs, time_navigation_simple_dfs_all,
        time_navigation_simple_old,
    },
};

mod bitvector;
mod gen_trie;
mod navigation;
mod old;
mod operators;

// fn main() {
//     println!("1_000_000");
//     time_bitvec(1_000_000, 4);
//     time_bitvec(1_000_000, 8);
//     time_bitvec(1_000_000, 16);
//     println!("10_000_000");
//     time_bitvec(10_000_000, 4);
//     time_bitvec(10_000_000, 8);
//     time_bitvec(10_000_000, 16);
//     println!("100_000_000");
//     time_bitvec(100_000_000, 4);
//     time_bitvec(100_000_000, 8);
//     time_bitvec(100_000_000, 16);
// }

// fn main() {
//     println!("1_000_000");
//     time_addition(1_000_000);
//     println!("10_000_000");
//     time_addition(10_000_000);
//     println!("100_000_000");
//     time_addition(100_000_000);
// }

// fn main() {
//     let sizes = vec![1_000_000, 10_000_000];
//     let arities = vec![2, 3, 4];

//     for size in sizes {
//         for &arity in &arities {
//             println!("{size}, {arity}");
//             time_navigation_simple(size, arity);
//             time_navigation_simple_old(size, arity);
//             time_navigation_simple_dfs(size, arity);
//             time_navigation_simple_dfs_all(size, arity);
//         }
//     }
// }

fn main() {
    let sizes = vec![1_000_000, 10_000_000];
    let arities = vec![2, 3, 4];

    for size in sizes {
        for &arity in &arities {
            println!("{size}, {arity}");
            time_load_old_trie(size, arity);
            time_load_new_trie(size, arity);
        }
    }
}
