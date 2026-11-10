//! Stopping a run from another thread, and continuing it.
//!
//! Usage: `cargo run -p nemo --example cancel`

use std::time::Duration;

use nemo::{codes, engine::Engine, values::DataValue};

/// The number of edges of the chain, which is also the number of iterations that a run takes.
const LENGTH: i64 = 1000;

/// Returns the number of facts of `reach` that the engine holds.
fn reached(engine: &Engine) -> usize {
    engine.count("reach").expect("the program derives `reach`")
}

fn main() {
    // The edges form one chain, so reasoning derives one fact per iteration.
    let mut engine = Engine::new();
    engine
        .import_rows(
            "edge",
            (0..LENGTH).map(|node| vec![DataValue::integer(node), DataValue::integer(node + 1)]),
        )
        .expect("the rows are kept for the load");
    engine
        .load_source(
            "chain.rls",
            "reach(0) . reach(?y) :- reach(?x), edge(?x, ?y) .",
        )
        .expect("the program loads");

    // A cancel token can be cloned and used from any thread.
    // This thread cancels the run after a deadline.
    let token = engine.cancel_token();
    let watchdog = std::thread::spawn(move || {
        std::thread::sleep(Duration::from_millis(10));
        token.cancel();
    });

    // `reason` checks the token between rule applications.
    // A cancelled run returns an error, with the tables consistent and its progress kept.
    match engine.reason() {
        Ok(()) => println!("the run reached the fixpoint before the deadline"),
        Err(stopped) => {
            assert_eq!(stopped.code(), codes::CANCELLED, "{stopped}");
            println!(
                "cancelled after {} rule applications, with {} facts of `reach`",
                engine.applications(),
                reached(&engine)
            );
            println!("at the fixpoint: {}", engine.is_at_fixpoint());
        }
    }
    watchdog.join().expect("the watchdog ends");

    // A method that checks the token clears it when it starts,
    // so the next run continues from the progress of the cancelled one.
    engine.reason().expect("reasoning reaches the fixpoint");
    println!(
        "at the fixpoint after {} rule applications, with {} facts of `reach`",
        engine.applications(),
        reached(&engine)
    );
}
