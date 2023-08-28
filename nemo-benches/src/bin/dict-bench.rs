use std::env;
use std::fs::File;
use std::io::BufReader;
use std::io::prelude::*;
use std::io::stdin;
use flate2::read::MultiGzDecoder;

// use text_io::read;

use nemo::meta::{timing::TimedDisplay, TimedCode};
use nemo_physical::dictionary::{Dictionary,string_dictionary::StringDictionary,prefixed_string_dictionary::PrefixedStringDictionary};

fn create_dictionary(dict_type: &str) -> Box<dyn Dictionary> {
    match dict_type {
        "hash" => {
            println!("Using StringDictionary.");
            Box::new(StringDictionary::new())
        },
        "prefix" => {
            println!("Using PrefixedStringDictionary.");
            Box::new(PrefixedStringDictionary::new())
        },
        _ => panic!("Unexpected dictionary type '{}'.", dict_type),
    }
}


fn main() {
    TimedCode::instance().start();

    let args: Vec<_> = env::args().collect();
    if args.len() <3 {
        println!("Usage: dict-bench <filename> <dicttype>");
        println!("  <filename> File with dictionary entries, one per line, possibly with duplicates.");
        println!("  <dicttype> Identifier for the dictionary to test, e.g., \"hash\" or \"prefix\".");
    }

    let filename = &args[1];
    let dicttype = &args[2];

    let reader = BufReader::new(MultiGzDecoder::new(File::open(filename).expect("Cannot open file.")));

    let mut dict = create_dictionary(dicttype);
    let mut count = 0;

    TimedCode::instance().sub("Dictionary loading").start();

    println!("Starting experiments ...");

    for l in reader.lines()  {
        dict.add(l.unwrap());
        count = count + 1;
    }

    TimedCode::instance().sub("Dictionary loading").stop();

    println!(
        "Processed {} strings (dictionary containts {} unique strings).",
        count,
        dict.len()
    );

    TimedCode::instance().stop();

    println!(
        "\n{}",
        TimedCode::instance().create_tree_string(
            "dict-bench",
            &[
                TimedDisplay::default(),
                TimedDisplay::default(),
                TimedDisplay::new(nemo::meta::timing::TimedSorting::LongestThreadTime, 0)
            ]
        )
    );

    println!("All done. Press return to end benchmark (and free all memory).");

    let mut s=String::new();
    stdin().read_line(&mut s).expect("No string entered?");
    
    if dict.len() == 123456789 { // FWIW, prevent dict from going out of scope before really finishing
        println!("Today is your lucky day.");
    }

}