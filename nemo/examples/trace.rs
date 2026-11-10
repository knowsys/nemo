//! Asking why a fact holds: the derivation of a fact, as a tree and as data.
//!
//! Usage: `cargo run -p nemo --example trace`

use nemo::{engine::Engine, fact, trace::Proof};

/// A recursive rule, and a rule with a negated atom.
const PROGRAM: &str = "
    parent(alice, bob) . parent(bob, carol) . parent(carol, dave) .
    ancestor(?x, ?y) :- parent(?x, ?y) .
    ancestor(?x, ?z) :- parent(?x, ?y), ancestor(?y, ?z) .
    founder(?x) :- parent(?x, ?y), ~parent(?z, ?x) .
";

/// Returns the depth of a derivation:
/// the number of rule applications on the longest path from the fact to the loaded facts.
fn depth(proof: Proof<'_>) -> usize {
    match proof.derivation() {
        None => 0,
        Some(_) => 1 + proof.premises().map(depth).max().unwrap_or(0),
    }
}

fn main() {
    let mut engine = Engine::new();
    engine
        .load_source("family.rls", PROGRAM)
        .expect("the program loads");
    // Traces describe the facts at the fixpoint, so the engine reasons first.
    engine.reason().expect("reasoning reaches the fixpoint");

    let response = engine
        .trace(&[
            fact!(ancestor(alice, dave)),
            fact!(founder(alice)),
            fact!(ancestor(dave, alice)),
        ])
        .expect("the facts are over the program's predicates");

    // One tree per requested fact, for a terminal.
    print!("{}", response.ascii());

    // The same response is one graph of facts, which a `Proof` walks like a tree.
    // A derived fact names its rule, the values of the rule's variables,
    // and the negated atoms that hold because nothing matches them.
    for (index, traced) in response.facts.iter().enumerate() {
        let Some(proof) = response.proof(index) else {
            println!("\n{} does not hold", traced.fact);
            continue;
        };
        let rule = proof.rule().expect("a rule derives the fact");
        let line = rule.source.as_ref().map_or(0, |source| source.line);
        println!(
            "\n{} holds by the rule on line {line}, at depth {}",
            traced.fact,
            depth(proof)
        );
        let derivation = proof.derivation().expect("a rule derives the fact");
        for binding in &derivation.assignment {
            println!("  {} = {}", binding.variable, binding.value);
        }
        for absent in &derivation.absences {
            println!("  nothing matches {absent}");
        }
    }
}
