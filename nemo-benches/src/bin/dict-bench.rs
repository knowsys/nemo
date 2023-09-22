use flate2::read::MultiGzDecoder;
use std::env;
use std::fs::File;
use std::io::prelude::*;
use std::io::stdin;
use std::io::BufReader;

use nemo::meta::{timing::TimedDisplay, TimedCode};
use nemo_physical::dictionary::{
    hash_map_dictionary::HashMapDictionary, meta_dictionary::MetaDictionary,
    prefixed_string_dictionary::PrefixedStringDictionary, string_dictionary::StringDictionary,
    AddResult, Dictionary,
};

fn create_dictionary(dict_type: &str) -> Box<dyn Dictionary> {
    match dict_type {
        "hash" => {
            println!("Using StringDictionary.");
            Box::new(StringDictionary::new())
        }
        "hashmap" => {
            println!("Using HashMapDictionary.");
            Box::new(HashMapDictionary::new())
        }
        "prefix" => {
            println!("Using PrefixedStringDictionary.");
            Box::new(PrefixedStringDictionary::new())
        }
        "meta" => {
            println!("Using MetaDictionary.");
            Box::new(MetaDictionary::new())
        }
        _ => panic!("Unexpected dictionary type '{}'.", dict_type),
    }
}

fn main() {
    env_logger::init();
    TimedCode::instance().start();

    let args: Vec<_> = env::args().collect();
    if args.len() < 3 {
        println!("Usage: dict-bench <filename> <dicttype>");
        println!(
            "  <filename> File with dictionary entries, one per line, possibly with duplicates."
        );
        println!(
            "  <dicttype> Identifier for the dictionary to test, e.g., \"hash\" or \"prefix\"."
        );
    }

    let filename = &args[1];
    let dicttype = &args[2];

    let reader = BufReader::new(MultiGzDecoder::new(
        File::open(filename).expect("Cannot open file."),
    ));

    let mut dict = create_dictionary(dicttype);
    let mut count_lines = 0;
    let mut count_unique = 0;
    let mut bytes = 0;

    TimedCode::instance().sub("Dictionary filling").start();

    println!("Starting to fill dictionary ...");

    for l in reader.lines() {
        let s = l.unwrap();
        let b = s.len();

        let entry_status = dict.add_string(s);
        match entry_status {
            AddResult::Fresh(_value) => {
                bytes = bytes + b;
                count_unique += 1;
            }
            AddResult::Known(_value) => {}
            AddResult::Rejected => {}
        }

        count_lines += 1;
    }

    TimedCode::instance().sub("Dictionary filling").stop();

    println!(
        "Processed {} strings (dictionary contains {} unique strings with {} bytes overall).",
        count_lines, count_unique, bytes
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
    let mut s = String::new();
    stdin().read_line(&mut s).expect("No string entered?");

    if dict.len() == 123456789 {
        // FWIW, prevent dict from going out of scope before really finishing
        println!("Today is your lucky day.");
    }
}
