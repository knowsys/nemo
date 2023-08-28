use std::env;
use std::fs::File;
use std::io::BufReader;
use std::io::prelude::*;
use flate2::read::MultiGzDecoder;

use nemo::meta::TimedCode;
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

    let overall_time = TimedCode::instance().total_system_time().as_millis();
    println!("Starting experiments ...");

    for l in reader.lines()  {
        dict.add(l.unwrap());
        count = count + 1;
    }

    println!(
        "Finished in {}ms. Processed {} strings (dictionary containts {} unique strings).",
        overall_time.to_string(),
        count,
        dict.len()
    );
}