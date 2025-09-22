use flate2::read::MultiGzDecoder;
use nemo_physical::datavalues::AnyDataValue;
use nemo_physical::datavalues::DataValue;
use nemo_physical::datavalues::ValueDomain;
use nemo_physical::dictionary::DvDict;
use nemo_physical::dictionary::meta_dv_dict::MetaDvDictionary;
use nemo_physical::dictionary::old_dictionaries::dictionary::Dictionary;
use nemo_physical::dictionary::old_dictionaries::hash_map_dictionary::HashMapDictionary;
use nemo_physical::dictionary::old_dictionaries::meta_dictionary::MetaDictionary;
use nemo_physical::dictionary::string_dictionary::BenchmarkStringDictionary;
use nemo_physical::management::bytesized::ByteSized;
use std::env;
use std::fs::File;
use std::io::BufReader;
use std::io::prelude::*;
use std::io::stdin;

use core::cmp;

use nemo_physical::dictionary::AddResult;
use nemo_physical::meta::timing::{TimedCode, TimedDisplay, TimedSorting};

/// If true, additional statistics about the length of some entry values
/// will be gathered. This needs extra memory and prints longer reports.
/// Only for internal testing.
const GATHER_STATISTICS: bool = false;

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
            _ => panic!("Unexpected dictionary type '{dict_type}'."),
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

    fn size_bytes(&mut self) -> u64 {
        match &self {
            DictEnum::StringHash(_) => 0,
            DictEnum::StringMeta(_) => 0,
            DictEnum::StringBuffer(dict) => dict.size_bytes(),
            DictEnum::DvMeta(dict) => dict.size_bytes(),
        }
    }
}

fn main() {
    //env_logger::init();
    TimedCode::instance().start();

    let args: Vec<_> = env::args().collect();
    if args.len() < 3 {
        println!("Usage: dict-bench <filename> <dicttype> <nonstop>");
        println!("  <filename> File with dictionary entries, one per line,");
        println!("             possibly with duplicates.");

        println!("  <dicttype> Identifier for the dictionary to test.");
        println!("             One of \"oldhash\", \"oldmeta\", \"string\", or \"meta\".");
        println!("  <nonstop>  If anything is given here, the program will terminate");
        println!("             without asking for a prompt.");
        std::process::exit(0);
    }

    let filename = &args[1];
    let dicttype = &args[2];

    let reader = BufReader::new(MultiGzDecoder::new(
        File::open(filename).expect("Cannot open file."),
    ));

    let mut dict = DictEnum::from_dict_type(dicttype);
    let mut count_lines = 0;
    let mut count_added = 0;
    let mut count_rejected = 0;
    let mut bytes_added = 0;
    let mut bytes_rejected = 0;

    // Used only for additional statistics:
    let mut iri_lengths: Vec<u64>;
    let mut string_lengths: Vec<u64>;
    let max_length = 513; // maximal data length we still care about for the histograms
    if GATHER_STATISTICS {
        iri_lengths = vec![0; max_length + 1]; // histogram of IRI lengths
        string_lengths = vec![0; max_length + 1]; // histogram of string literal value lengths
    } else {
        iri_lengths = vec![];
        string_lengths = vec![];
    }

    TimedCode::instance().sub("Dictionary filling").start();

    println!("Starting to fill dictionary ...");

    for l in reader.lines() {
        let s = l.unwrap();
        let b = s.len();

        let dv: AnyDataValue;
        // Simple ad hoc parsing to get realisitc data; some errors would be ok
        match s.chars().next() {
            Some('"') => {
                if let Some(type_pos) = s.find("\"^^<") {
                    // println!("Lit {} | {}", &s[1..type_pos], &s[type_pos+4..s.len()-1]);
                    match AnyDataValue::new_from_typed_literal(
                        s[1..type_pos].to_string(),
                        s[type_pos + 4..s.len() - 1].to_string(),
                    ) {
                        Ok(val) => dv = val,
                        Err(err) => {
                            println!("Error parsing value `{s}`: {err:?}");
                            dv = AnyDataValue::new_plain_string(s.clone());
                        }
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
                        dv = AnyDataValue::new_plain_string(s[1..s.len() - 1].to_string());
                    }
                } else {
                    // println!("String {}", &s[1..s.len()-1]);
                    dv = AnyDataValue::new_plain_string(s[1..s.len() - 1].to_string());
                }
            }
            Some('<') => {
                dv = AnyDataValue::new_iri(s[1..s.len() - 1].to_string());
            }
            _ => {
                dv = AnyDataValue::new_plain_string(s.clone());
            }
        }

        let value_domain = dv.value_domain();
        let add_result = dict.add(s, dv);

        match add_result {
            AddResult::Fresh(_value) => {
                bytes_added += b;
                count_added += 1;

                if GATHER_STATISTICS {
                    match value_domain {
                        ValueDomain::Iri => {
                            iri_lengths[cmp::min(b, max_length)] += 1;
                        }
                        ValueDomain::LanguageTaggedString => {
                            string_lengths[cmp::min(b, max_length)] += 1;
                        }
                        _ => {}
                    }
                }
            }
            AddResult::Known(_value) => {}
            AddResult::Rejected => {
                bytes_rejected += b;
                count_rejected += 1;
            }
        }

        count_lines += 1;
    }

    TimedCode::instance().sub("Dictionary filling").stop();

    println!("Processed {count_lines} strings.");
    println!(
        "  Dictionary accepted data for {count_added} unique strings with {bytes_added} bytes overall."
    );
    println!(
        "  Dictionary rejected {count_rejected} (non-unique) strings with {bytes_rejected} bytes overall."
    );
    println!("  Dictionary reports own size as {}.", dict.size_bytes());

    TimedCode::instance().stop();

    println!(
        "\n{}",
        TimedCode::instance().create_tree_string(
            "dict-bench",
            &[
                TimedDisplay::default(),
                TimedDisplay::default(),
                TimedDisplay::new(TimedSorting::LongestThreadTime, 0)
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

    if GATHER_STATISTICS {
        let mut count_less: u64 = 0;
        let mut count_more: u64 = 0;
        println!("Lengths of unique IRIs:");
        for (pos, count) in iri_lengths.iter().enumerate() {
            println!("  {pos:>5}: {count:>20}");
            if pos < 256 {
                count_less += count;
            } else {
                count_more += count;
            }
        }
        println!("The last line summarises all longer cases.");
        println!("{count_less} IRIs up to length <256, {count_more} IRIs beyond that length.");

        count_less = 0;
        count_more = 0;
        println!("Lengths of unique strings:");
        for (pos, count) in string_lengths.iter().enumerate() {
            println!("  {pos:>5}: {count:>20}");
            if pos < 256 {
                count_less += count;
            } else {
                count_more += count;
            }
        }
        println!("The last line summarises all longer cases.");
        println!(
            "{count_less} strings up to length <256, {count_more} strings beyond that length."
        );
    }
}
