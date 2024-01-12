use flate2::read::MultiGzDecoder;
use nemo_physical::datavalues::AnyDataValue;
use nemo_physical::dictionary::meta_dv_dict::MetaDvDictionary;
use nemo_physical::dictionary::string_dictionary::BenchmarkStringDictionary;
use nemo_physical::dictionary::DvDict;
use std::env;
use std::fs::File;
use std::io::prelude::*;
use std::io::stdin;
use std::io::BufReader;

use nemo::meta::{timing::TimedDisplay, TimedCode};
use nemo_physical::dictionary::{
    hash_map_dictionary::HashMapDictionary, meta_dictionary::MetaDictionary, AddResult, Dictionary,
};

enum DictEnum {
    StringHash(HashMapDictionary),
    StringMeta(MetaDictionary),
    StringBuffer(BenchmarkStringDictionary),
    DvMeta(MetaDvDictionary),
}
impl DictEnum {
    fn from_dict_type(dict_type: &str) -> Self {
        match dict_type {
            "oldhash" => {
                println!("Using string-based HashMapDictionary.");
                DictEnum::StringHash(HashMapDictionary::new())
            }
            "oldmeta" => {
                println!("Using string-based MetaDictionary.");
                DictEnum::StringMeta(MetaDictionary::new())
            }
            "string" => {
                println!("Using StringDictionary (plain strings, not DataValues).");
                DictEnum::StringBuffer(Default::default())
            }
            "meta" => {
                println!("Using MetaDvDictionary.");
                DictEnum::DvMeta(MetaDvDictionary::new())
            }
            _ => panic!("Unexpected dictionary type '{}'.", dict_type),
        }
    }

    fn add(&mut self, string: String, dv: AnyDataValue) -> AddResult {
        match self {
            DictEnum::StringHash(dict) => dict.add_string(string),
            DictEnum::StringMeta(dict) => dict.add_string(string),
            DictEnum::StringBuffer(dict) => dict.add_str(string.as_str()),
            DictEnum::DvMeta(dict) => dict.add_datavalue(dv),
        }
    }

    fn len(&mut self) -> usize {
        match &self {
            DictEnum::StringHash(dict) => dict.len(),
            DictEnum::StringMeta(dict) => dict.len(),
            DictEnum::StringBuffer(dict) => dict.len(),
            DictEnum::DvMeta(dict) => dict.len(),
        }
    }
}

fn main() {
    env_logger::init();
    TimedCode::instance().start();

    let args: Vec<_> = env::args().collect();
    if args.len() < 3 {
        println!("Usage: dict-bench <filename> <dicttype> <nonstop>");
        println!(
            "  <filename> File with dictionary entries, one per line, possibly with duplicates."
        );
        println!(
            "  <dicttype> Identifier for the dictionary to test, e.g., \"meta\" or \"string\"."
        );
        println!(
            "  <nonstop> If anything is given here, the program will terminate without asking for a prompt."
        );
    }

    let filename = &args[1];
    let dicttype = &args[2];

    let reader = BufReader::new(MultiGzDecoder::new(
        File::open(filename).expect("Cannot open file."),
    ));

    let mut dict = DictEnum::from_dict_type(dicttype);
    let mut count_lines = 0;
    let mut count_unique = 0;
    let mut bytes = 0;

    TimedCode::instance().sub("Dictionary filling").start();

    println!("Starting to fill dictionary ...");

    for l in reader.lines() {
        let s = l.unwrap();
        let b = s.len();

        let dv: AnyDataValue;
        // Simple ad hoc parsing to get realisitc data; some errors would be ok
        match s.chars().nth(0) {
            Some('"') => {
                if let Some(type_pos) = s.find("\"^^<") {
                    // println!("Lit {} | {}", &s[1..type_pos], &s[type_pos+4..s.len()-1]);
                    if let Ok(val) = AnyDataValue::new_from_typed_literal(
                        s[1..s.len() - 1].to_string(),
                        s[type_pos + 4..s.len() - 1].to_string(),
                    ) {
                        dv = val;
                    } else {
                        dv = AnyDataValue::new_string(s.clone());
                    }
                } else if let Some(at_pos) = s.rfind("\"@") {
                    if at_pos > 0 {
                        // println!("Lang {} | {}", &s[1..at_pos], &s[at_pos+2..s.len()]);
                        dv = AnyDataValue::new_language_tagged_string(
                            s[1..at_pos].to_string(),
                            s[at_pos + 2..s.len()].to_string(),
                        );
                    } else {
                        // avoid panics when for strings like "@sometag"
                        dv = AnyDataValue::new_string(s[1..s.len() - 1].to_string());
                    }
                } else {
                    //println!("String {}", &s[1..s.len()-1]);
                    dv = AnyDataValue::new_string(s[1..s.len() - 1].to_string());
                }
            }
            Some('<') => {
                dv = AnyDataValue::new_iri(s[1..s.len() - 1].to_string());
            }
            _ => {
                dv = AnyDataValue::new_string(s.clone());
            }
        }

        let add_result = dict.add(s, dv);

        match add_result {
            AddResult::Fresh(_value) => {
                bytes += b;
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

    if args.len() < 4 {
        println!("All done. Press return to end benchmark (and free all memory).");
        let mut s = String::new();
        stdin().read_line(&mut s).expect("No string entered?");
    }

    if dict.len() == 123456789 {
        // FWIW, prevent dict from going out of scope before really finishing
        println!("Today is your lucky day.");
    }
}
